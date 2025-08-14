# scripts/build_index_from_api_transcripts.py
# Aggregates ALL sources per video:
#   - Local captions: .srt/.sbv  (src="srt")
#   - Public timedtext captions: VTT (src="transcript")
#   - Chapters from description (src="chapters")
#   - Title + description fallback (src="title"/"description")
#
# Segments are deduplicated per video. Higher-quality sources win on duplicate text/timestamp.

import os, json, time, re, glob, random
import requests, srt
from concurrent.futures import ThreadPoolExecutor, as_completed

API_KEY      = os.environ["YOUTUBE_API_KEY"]
HANDLE       = os.getenv("YOUTUBE_HANDLE", "@blackoutapp")
SRT_DIR      = os.getenv("SRT_DIR", "srt")
PREF_LANGS   = ["en", "en-US", "en-GB", "en-CA", "en-AU"]
ALLOW_AUTO   = True

WORKERS      = int(os.getenv("WORKERS", "8"))
PRINT_EVERY  = int(os.getenv("PRINT_EVERY", "10"))
LIMIT        = int(os.getenv("LIMIT", "0"))
MAX_DESC_CHARS = 5000

TIMESTAMP_RE  = re.compile(r'\b(?:(\d{1,2}):)?(\d{1,2}):(\d{2})\b')
SBV_TIMECODE  = re.compile(r'^\s*(\d{1,2}:)?\d{1,2}:\d{2}(?:\.\d+)?\s*,\s*(\d{1,2}:)?\d{1,2}:\d{2}(?:\.\d+)?\s*$')

# Used for dedupe preference (higher is better)
SOURCE_RANK = {"srt": 3, "transcript": 2, "chapters": 1, "title": 0.2, "description": 0.1}

def uploads_playlist_id():
    r = requests.get("https://www.googleapis.com/youtube/v3/channels",
        params={"part":"contentDetails","forHandle":HANDLE,"key":API_KEY}, timeout=30)
    r.raise_for_status()
    it = r.json().get("items", [])
    if not it: raise SystemExit(f"No channel for {HANDLE}")
    return it[0]["contentDetails"]["relatedPlaylists"]["uploads"]

def list_uploads(pid):
    out, page = [], None
    while True:
        r = requests.get("https://www.googleapis.com/youtube/v3/playlistItems",
            params={"part":"contentDetails,snippet","playlistId":pid,"maxResults":50,
                    "pageToken":page,"key":API_KEY}, timeout=30)
        r.raise_for_status()
        j = r.json()
        for it in j.get("items", []):
            vid = it["contentDetails"]["videoId"]
            out.append({
                "id": vid,
                "title": it["snippet"]["title"],
                "published": it["contentDetails"].get("videoPublishedAt"),
                "url": f"https://youtu.be/{vid}",
            })
        page = j.get("nextPageToken")
        if not page: break
    return out

# ---------- helpers ----------
def normalize_text(s):
    if not s: return ""
    s = re.sub(r"\s+", " ", s.replace("\u00A0", " ")).strip()
    return s

def vtt_to_seconds(ts):
    ts = ts.replace(",", ".")
    parts = ts.split(":")
    if len(parts) == 3:
        h, m, s = parts
    else:
        h, m, s = "0", parts[0], parts[1]
    return int(float(h)*3600 + float(m)*60 + float(s))

def _sbv_time_to_seconds(tc: str) -> int:
    parts = tc.strip().split(':')
    if len(parts) == 3:
        h, m, s = int(parts[0]), int(parts[1]), float(parts[2])
    else:
        h, m, s = 0, int(parts[0]), float(parts[1])
    return int(h*3600 + m*60 + s)

# ---------- local captions: SRT + SBV ----------
def local_caption_segments(video_id):
    patterns = [
        f"{SRT_DIR}/{video_id}*.srt", f"{SRT_DIR}/**/{video_id}*.srt",
        f"{SRT_DIR}/{video_id}*.sbv", f"{SRT_DIR}/**/{video_id}*.sbv",
    ]
    candidates = []
    for pat in patterns:
        candidates.extend(glob.glob(pat, recursive=True))
    if not candidates:
        return []

    def score(path: str):
        s = 0
        if re.search(r'(?:^|[._-])(en|en-US)(?:[._-]|\.(?:srt|sbv)$)', path, re.I):
            s += 2
        if path.lower().endswith('.srt'):
            s += 1
        return (s, path)

    # Prefer the best single file (we aggregate its lines)
    best = sorted(candidates, key=score, reverse=True)[0]
    try:
        with open(best, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
    except Exception:
        return []

    segs = []
    if best.lower().endswith(".srt"):
        try:
            subs = list(srt.parse(content))
        except Exception:
            subs = []
        for sub in subs:
            txt = normalize_text(sub.content or "")
            if not txt: continue
            segs.append({"start": int(sub.start.total_seconds()), "text": txt, "norm": txt.lower(), "src": "srt"})
    else:
        # SBV
        blocks = re.split(r'\r?\n\r?\n', content.strip())
        for block in blocks:
            lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
            if not lines: continue
            if not SBV_TIMECODE.match(lines[0]): continue
            start_tc = lines[0].split(',')[0].strip()
            start = _sbv_time_to_seconds(start_tc)
            text = normalize_text(' '.join(lines[1:]))
            if text:
                segs.append({"start": start, "text": text, "norm": text.lower(), "src": "srt"})
    return segs

# ---------- public timedtext captions ----------
def list_tracks_timedtext(video_id):
    r = requests.get("https://www.youtube.com/api/timedtext",
                     params={"type":"list","v":video_id,"hl":"en"}, timeout=20)
    if r.status_code != 200 or "<transcript_list" not in r.text:
        return []
    tracks = []
    for m in re.finditer(r'<track\b([^>]+)>', r.text):
        attrs = dict(re.findall(r'(\w+)="(.*?)"', m.group(1)))
        tracks.append({
            "lang": (attrs.get("lang_code") or "").lower(),
            "kind": attrs.get("kind",""),
            "name": attrs.get("name",""),
        })
    return tracks

def choose_track(tracks):
    for t in tracks:
        if t["kind"] != "asr" and any(t["lang"].startswith(x.split("-")[0]) for x in PREF_LANGS):
            return t
    if ALLOW_AUTO:
        for t in tracks:
            if t["kind"] == "asr" and any(t["lang"].startswith(x.split("-")[0]) for x in PREF_LANGS):
                return t
    for t in tracks:
        if t["kind"] != "asr":
            return t
    return tracks[0] if tracks else None

def fetch_track_vtt(video_id, track):
    params = {"v": video_id, "fmt": "vtt", "lang": track["lang"]}
    if track["kind"] == "asr": params["kind"] = "asr"
    elif track.get("name"):   params["name"] = track["name"]
    r = requests.get("https://www.youtube.com/api/timedtext", params=params, timeout=30)
    if r.status_code != 200 or "WEBVTT" not in r.text:
        return None
    return r.text

def parse_vtt(vtt_text):
    segs = []
    lines = [ln.rstrip("\n") for ln in vtt_text.splitlines()]
    i = 0
    while i < len(lines):
        ln = lines[i]
        if "-->" in ln:
            start = ln.split("-->")[0].strip()
            i += 1
            buf = []
            while i < len(lines) and lines[i].strip():
                if lines[i].strip().upper().startswith(("NOTE","STYLE")):
                    break
                buf.append(lines[i]); i += 1
            text = normalize_text(" ".join(buf))
            if text:
                segs.append({"start": vtt_to_seconds(start), "text": text, "norm": text.lower(), "src": "transcript"})
        i += 1
    return segs

# ---------- chapters + metadata ----------
def fetch_snippet(video_id):
    r = requests.get("https://www.googleapis.com/youtube/v3/videos",
        params={"part":"snippet","id":video_id,"key":API_KEY}, timeout=20)
    r.raise_for_status()
    items = r.json().get("items", [])
    if not items:
        return {"title":"", "description":""}
    sn = items[0]["snippet"]
    return {"title": sn.get("title") or "", "description": sn.get("description") or ""}

def chapters_from_description(desc):
    segs = []
    for line in (desc or "").splitlines():
        m = TIMESTAMP_RE.search(line)
        if not m: continue
        h, m_, s = m.groups()
        t = (int(h) if h else 0)*3600 + int(m_)*60 + int(s)
        text = TIMESTAMP_RE.sub("", line, count=1)
        text = normalize_text(text.strip(" -–—:·|"))
        if text:
            segs.append({"start": t, "text": text, "norm": text.lower(), "src": "chapters"})
    return segs

def metadata_segments(title, desc):
    out = []
    title = normalize_text(title)
    if title:
        out.append({"start": 0, "text": title, "norm": title.lower(), "src": "title"})
    desc = normalize_text(desc[:MAX_DESC_CHARS])
    if desc:
        out.append({"start": 0, "text": desc, "norm": desc.lower(), "src": "description"})
    return out

# ---------- merge + dedupe ----------
def dedupe_segments(segs):
    """
    Remove near-duplicate (start,text) within a video, preferring higher SOURCE_RANK.
    We round start to nearest second and compare lowercase text.
    """
    seen = {}
    for s in sorted(segs, key=lambda x: (-SOURCE_RANK.get(x["src"], 0), x["start"])):
        key = (int(round(s["start"])), s["text"].lower())
        if key in seen:
            continue
        seen[key] = s
    return list(seen.values())

# ---------- pipeline ----------
def process_video(v):
    all_segs = []

    # local captions
    all_segs += local_caption_segments(v["id"])

    # public captions
    tracks = list_tracks_timedtext(v["id"])
    chosen = choose_track(tracks)
    if chosen:
        vtt = fetch_track_vtt(v["id"], chosen)
        if vtt:
            all_segs += parse_vtt(vtt)

    # chapters + metadata
    sn = fetch_snippet(v["id"])
    all_segs += chapters_from_description(sn["description"])
    all_segs += metadata_segments(sn["title"] or v["title"] or "", sn["description"] or "")

    # finalize
    for s in all_segs:
        s["video_id"] = v["id"]
    return v, dedupe_segments(all_segs)

def main():
    pid = uploads_playlist_id()
    vids = list_uploads(pid)
    if LIMIT and LIMIT > 0:
        vids = vids[:LIMIT]
    total = len(vids)
    print(f"Discovered {total} uploads. Processing with WORKERS={WORKERS} LIMIT={LIMIT or 'none'}")

    videos, segments = [], []
    done = 0
    counts = {"srt":0,"transcript":0,"chapters":0,"title":0,"description":0}

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(process_video, v) for v in vids]
        for fut in as_completed(futs):
            v, segs = fut.result()
            if segs:
                videos.append(v)
                segments.extend(segs)
                # count sources present for logging
                seen_src = set(s["src"] for s in segs)
                for k in counts:
                    if k in seen_src: counts[k] += 1
            done += 1
            if done % PRINT_EVERY == 0 or done == total:
                print(f"Processed {done}/{total}… vids_with_data:{len(videos)}  segs:{len(segments)}  src_hits:{counts}")

    os.makedirs("public", exist_ok=True)
    with open("public/index.json","w",encoding="utf-8") as f:
        json.dump({
            "generated_at": int(time.time()),
            "channel": {"handle": HANDLE},
            "videos": videos,
            "segments": segments,     # each has video_id, start, text, norm, src
            "source_video_counts": counts,
            "config": {
                "allow_auto": ALLOW_AUTO,
                "pref_langs": PREF_LANGS,
                "srt_dir": SRT_DIR,
                "source_rank": SOURCE_RANK
            },
        }, f, ensure_ascii=False)
    print(f"✅ Done. Segments:{len(segments)} from Videos:{len(videos)} | per-source video counts: {counts}")

if __name__ == "__main__":
    main()
