# scripts/build_index_from_api_transcripts.py
# Builds public/index.json from (1) local SRTs, (2) public timedtext captions, (3) chapters, (4) title/description.
# No OAuth required. Local SRTs are preferred if present.

import os, json, time, re, glob, random
import requests
import srt
import glob
import re
import srt
from concurrent.futures import ThreadPoolExecutor, as_completed

API_KEY      = os.environ["YOUTUBE_API_KEY"]
HANDLE       = os.getenv("YOUTUBE_HANDLE", "@blackoutapp")
SRT_DIR      = os.getenv("SRT_DIR", "srt")     # where you drop your .srt files
PREF_LANGS   = ["en", "en-US", "en-GB", "en-CA", "en-AU"]  # preferred languages
ALLOW_AUTO   = True                             # allow auto captions if no manual track present
WORKERS      = int(os.getenv("WORKERS", "8"))
PRINT_EVERY  = int(os.getenv("PRINT_EVERY", "10"))
LIMIT        = int(os.getenv("LIMIT", "0"))
MAX_DESC_CHARS = 5000                           # limit indexed description size
TIMESTAMP_RE = re.compile(r'\b(?:(\d{1,2}):)?(\d{1,2}):(\d{2})\b')

SBV_TIMECODE = re.compile(r'^\s*(\d{1,2}:)?\d{1,2}:\d{2}(?:\.\d+)?\s*,\s*(\d{1,2}:)?\d{1,2}:\d{2}(?:\.\d+)?\s*$')

def _sbv_time_to_seconds(tc: str) -> int:
    parts = tc.strip().split(':')
    if len(parts) == 3:
        h, m, s = int(parts[0]), int(parts[1]), float(parts[2])
    else:
        h, m, s = 0, int(parts[0]), float(parts[1])
    return int(h*3600 + m*60 + s)

def _parse_sbv(content: str):
    segs = []
    # blocks separated by blank line
    blocks = re.split(r'\r?\n\r?\n', content.strip())
    for block in blocks:
        lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
        if not lines:
            continue
        # first line must be the time range "start,end"
        if not SBV_TIMECODE.match(lines[0]):
            continue
        start_tc = lines[0].split(',')[0].strip()
        start = _sbv_time_to_seconds(start_tc)
        text = re.sub(r'\s+', ' ', ' '.join(lines[1:])).strip()
        if text:
            segs.append({"start": start, "text": text, "norm": text.lower()})
    return segs

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

# ---------- Local SRT (preferred) ----------
def local_srt_segments(video_id):
    """
    Load local captions for a video, preferring English .srt, then other .srt,
    then English .sbv, then other .sbv. Returns list of segments or None.
    """
    candidates = []
    patterns = [
        f"{SRT_DIR}/{video_id}*.srt",
        f"{SRT_DIR}/**/{video_id}*.srt",
        f"{SRT_DIR}/{video_id}*.sbv",
        f"{SRT_DIR}/**/{video_id}*.sbv",
    ]
    for pat in patterns:
        candidates.extend(glob.glob(pat, recursive=True))
    if not candidates:
        return None

    def score(path: str):
        # prefer English-named files, prefer .srt over .sbv
        s = 0
        if re.search(r'(?:^|[._-])(en|en-US)(?:[._-]|\.(?:srt|sbv)$)', path, re.I):
            s += 2
        if path.lower().endswith('.srt'):
            s += 1
        return (s, path)

    best = sorted(candidates, key=score, reverse=True)[0]

    try:
        with open(best, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
    except Exception:
        return None

    if best.lower().endswith(".srt"):
        try:
            subs = list(srt.parse(content))
        except Exception:
            return None
        segs = []
        for sub in subs:
            txt = re.sub(r"\s+", " ", (sub.content or "")).strip()
            if not txt:
                continue
            start_sec = int(sub.start.total_seconds())
            segs.append({"video_id": video_id, "start": start_sec, "text": txt, "norm": txt.lower()})
        return segs

    # .sbv
    segs = _parse_sbv(content)
    if not segs:
        return None
    for s in segs:
        s["video_id"] = video_id
    return segs
# ---------- Public timedtext (manual > auto) ----------
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
    # Manual English
    for t in tracks:
        if t["kind"] != "asr" and any(t["lang"].startswith(x.split("-")[0]) for x in PREF_LANGS):
            return t
    # Auto English
    if ALLOW_AUTO:
        for t in tracks:
            if t["kind"] == "asr" and any(t["lang"].startswith(x.split("-")[0]) for x in PREF_LANGS):
                return t
    # Any manual
    for t in tracks:
        if t["kind"] != "asr":
            return t
    # Any track
    return tracks[0] if tracks else None

def fetch_track_vtt(video_id, track):
    params = {"v": video_id, "fmt": "vtt", "lang": track["lang"]}
    if track["kind"] == "asr":
        params["kind"] = "asr"
    elif track.get("name"):
        params["name"] = track["name"]
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
                # skip cues like NOTE/STYLE
                if lines[i].strip().upper().startswith(("NOTE","STYLE")):
                    break
                buf.append(lines[i])
                i += 1
            text = normalize_text(" ".join(buf))
            if text:
                segs.append({"start": vtt_to_seconds(start), "text": text, "norm": text.lower()})
        i += 1
    return segs

def vtt_to_seconds(ts):
    ts = ts.replace(",", ".")
    parts = ts.split(":")
    if len(parts) == 3:
        h, m, s = parts
    else:
        h, m, s = "0", parts[0], parts[1]
    return int(float(h)*3600 + float(m)*60 + float(s))

# ---------- Snippet / chapters / metadata ----------
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
        if not m: 
            continue
        h, m_, s = m.groups()
        t = (int(h) if h else 0)*3600 + int(m_)*60 + int(s)
        text = line[m.end():].strip(" -–—:·|")
        if not text:
            text = TIMESTAMP_RE.sub("", line).strip(" -–—:·|")
        text = normalize_text(text)
        if text:
            segs.append({"start": t, "text": text, "norm": text.lower()})
    return segs

def normalize_text(s):
    if not s: return ""
    s = re.sub(r"\s+", " ", s)
    s = s.replace("\u00A0", " ")  # nbsp
    return s.strip()

# ---------- Pipeline ----------
def process_video(v):
    # 0) Local SRT (highest quality)
    srt_segs = local_srt_segments(v["id"])
    if srt_segs:
        return (v, srt_segs, "srt")

    # 1) Public captions via timedtext
    tracks = list_tracks_timedtext(v["id"])
    chosen = choose_track(tracks)
    if chosen:
        vtt = fetch_track_vtt(v["id"], chosen)
        if vtt:
            parsed = parse_vtt(vtt)
            if parsed:
                segs = [{"video_id": v["id"], **s} for s in parsed]
                return (v, segs, "transcript")

    # 2) Chapters in description
    sn = fetch_snippet(v["id"])
    chs = chapters_from_description(sn["description"])
    if chs:
        segs = [{"video_id": v["id"], **s} for s in chs]
        return (v, segs, "chapters")

    # 3) Fallback: index title + (truncated) description at t=0
    fallback = []
    title = normalize_text(sn["title"] or v["title"] or "")
    if title:
        fallback.append({"video_id": v["id"], "start": 0, "text": title, "norm": title.lower()})
    desc = normalize_text((sn["description"] or "")[:MAX_DESC_CHARS])
    if desc:
        fallback.append({"video_id": v["id"], "start": 0, "text": desc, "norm": desc.lower()})
    return (v, fallback, "metadata" if fallback else "none")

def main():
    pid = uploads_playlist_id()
    vids = list_uploads(pid)
    if LIMIT and LIMIT > 0:
        vids = vids[:LIMIT]
    total = len(vids)
    print(f"Discovered {total} uploads. Processing with WORKERS={WORKERS} LIMIT={LIMIT or 'none'}")

    videos, segments = [], []
    done = 0; c_srt=c_tr=c_ch=c_meta=0

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(process_video, v) for v in vids]
        for fut in as_completed(futs):
            v, segs, src = fut.result()
            if segs:
                videos.append(v)
                segments.extend(segs)
                if src=="srt":        c_srt += 1
                elif src=="transcript": c_tr += 1
                elif src=="chapters": c_ch += 1
                elif src=="metadata": c_meta += 1
            done += 1
            if done % PRINT_EVERY == 0 or done == total:
                print(f"Processed {done}/{total}… vids_with_data:{len(videos)} segs:{len(segments)} "
                      f"(srt:{c_srt} tr:{c_tr} ch:{c_ch} meta:{c_meta})")

    os.makedirs("public", exist_ok=True)
    with open("public/index.json","w",encoding="utf-8") as f:
        json.dump({
            "generated_at": int(time.time()),
            "channel": {"handle": HANDLE},
            "videos": videos,
            "segments": segments,
            "source_counts": {"srt": c_srt, "transcript": c_tr, "chapters": c_ch, "metadata": c_meta},
            "config": {"allow_auto": ALLOW_AUTO, "pref_langs": PREF_LANGS, "srt_dir": SRT_DIR},
        }, f, ensure_ascii=False)
    print(f"✅ Done. Segments:{len(segments)} from Videos:{len(videos)} (srt:{c_srt} tr:{c_tr} ch:{c_ch} meta:{c_meta}).")

if __name__ == "__main__":
    main()
