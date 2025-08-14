import os, json, time, re, requests, random
from concurrent.futures import ThreadPoolExecutor, as_completed

API_KEY = os.environ["YOUTUBE_API_KEY"]
HANDLE  = os.getenv("YOUTUBE_HANDLE", "@blackoutapp")

LANGS   = ["en","en-US","en-GB","en-CA","en-AU"]   # language preference
ALLOW_AUTO = True

WORKERS = int(os.getenv("WORKERS", "8"))
PRINT_EVERY = int(os.getenv("PRINT_EVERY", "10"))
LIMIT = int(os.getenv("LIMIT", "0"))

TIMESTAMP_RE = re.compile(r'\b(?:(\d{1,2}):)?(\d{1,2}):(\d{2})\b')

def uploads_playlist_id():
    r = requests.get("https://www.googleapis.com/youtube/v3/channels",
        params={"part":"contentDetails","forHandle":HANDLE,"key":API_KEY}, timeout=30)
    r.raise_for_status()
    it = r.json().get("items", [])
    if not it:
        raise SystemExit(f"No channel for {HANDLE}")
    return it[0]["contentDetails"]["relatedPlaylists"]["uploads"]

def list_uploads(pid):
    out, page = [], None
    while True:
        r = requests.get("https://www.googleapis.com/youtube/v3/playlistItems",
            params={"part":"contentDetails,snippet","playlistId":pid,"maxResults":50,"pageToken":page,"key":API_KEY},
            timeout=30)
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

# ---------- timedtext helpers (public endpoint) ----------

def list_tracks_timedtext(video_id):
    # https://www.youtube.com/api/timedtext?type=list&v=VIDEO_ID&hl=en
    r = requests.get("https://www.youtube.com/api/timedtext",
                     params={"type":"list","v":video_id,"hl":"en"},
                     timeout=20)
    if r.status_code != 200 or "<transcript_list" not in r.text:
        return []
    tracks = []
    # very small XML parser (avoid adding deps)
    for m in re.finditer(r'<track\b([^>]+)>', r.text):
        attrs = dict(re.findall(r'(\w+)="(.*?)"', m.group(1)))
        # attrs: id, lang_code, kind="asr" for auto, name="..." maybe empty
        lang = attrs.get("lang_code","").lower()
        kind = attrs.get("kind","")
        name = attrs.get("name","")
        tracks.append({"lang": lang, "kind": kind, "name": name})
    return tracks

def fetch_track_vtt(video_id, track):
    # manual: v, lang[, name]
    # auto:   v, lang, kind=asr
    params = {"v": video_id, "fmt": "vtt", "lang": track["lang"]}
    if track["kind"] == "asr":
        params["kind"] = "asr"
    elif track.get("name"):
        params["name"] = track["name"]
    r = requests.get("https://www.youtube.com/api/timedtext",
                     params=params, timeout=30)
    if r.status_code != 200 or "WEBVTT" not in r.text:
        return None
    return r.text

def parse_vtt(vtt_text):
    # minimal VTT to segments
    segs = []
    lines = [ln.rstrip("\n") for ln in vtt_text.splitlines()]
    i = 0
    while i < len(lines):
        ln = lines[i]
        if "-->" in ln:
            # time line: 00:00:12.345 --> 00:00:14.000
            start = ln.split("-->")[0].strip()
            # collect following text lines until blank
            i += 1
            buf = []
            while i < len(lines) and lines[i].strip():
                if lines[i].strip().upper().startswith(("NOTE","STYLE")):
                    break
                buf.append(lines[i])
                i += 1
            text = " ".join(" ".join(buf).split())
            if text:
                segs.append({"start": vtt_to_seconds(start), "text": text, "norm": text.lower()})
        i += 1
    return segs

def vtt_to_seconds(ts):
    # HH:MM:SS.mmm or MM:SS.mmm
    parts = ts.replace(",", ".").split(":")
    parts = [p for p in parts if p]
    if len(parts) == 3:
        h, m, s = parts
    else:
        h, m, s = "0", parts[0], parts[1]
    s = float(s)
    return int(float(h)*3600 + float(m)*60 + s)

# ---------- description chapters fallback ----------

def chapters_from_description(video_id):
    r = requests.get("https://www.googleapis.com/youtube/v3/videos",
        params={"part":"snippet","id":video_id,"key":API_KEY}, timeout=20)
    if r.status_code != 200:
        return []
    items = r.json().get("items", [])
    if not items: return []
    desc = items[0]["snippet"].get("description") or ""
    segs = []
    for line in desc.splitlines():
        m = TIMESTAMP_RE.search(line)
        if not m: 
            continue
        h, m_, s = m.groups()
        t = (int(h) if h else 0)*3600 + int(m_)*60 + int(s)
        text = line[m.end():].strip(" -–—:·|")
        if not text:
            text = TIMESTAMP_RE.sub("", line).strip(" -–—:·|")
        if text:
            segs.append({"start": t, "text": text, "norm": text.lower()})
    return segs

# ---------- per-video pipeline ----------

def process_video(v):
    # 1) list tracks
    tracks = list_tracks_timedtext(v["id"])
    chosen = None

    # choose manual English first
    for t in tracks:
        if t["kind"] != "asr" and any(t["lang"].startswith(l.split("-")[0]) for l in LANGS):
            chosen = t; break
    # else auto English if allowed
    if not chosen and ALLOW_AUTO:
        for t in tracks:
            if t["kind"] == "asr" and any(t["lang"].startswith(l.split("-")[0]) for l in LANGS):
                chosen = t; break

    if chosen:
        vtt = fetch_track_vtt(v["id"], chosen)
        if vtt:
            segs = [{"video_id": v["id"], **s} for s in parse_vtt(vtt)]
            if segs:
                return (v, segs, "transcript")

    # 2) fallback: parse chapters from description
    chs = chapters_from_description(v["id"])
    if chs:
        segs = [{"video_id": v["id"], **s} for s in chs]
        return (v, segs, "chapters")

    return (v, [], "none")

def main():
    pid = uploads_playlist_id()
    vids = list_uploads(pid)
    if LIMIT and LIMIT > 0:
        vids = vids[:LIMIT]
    total = len(vids)
    print(f"Discovered {total} uploads. Processing with WORKERS={WORKERS} LIMIT={LIMIT or 'none'}")

    videos, segments = [], []
    done = 0; from_tr = 0; from_ch = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(process_video, v) for v in vids]
        for fut in as_completed(futs):
            v, segs, src = fut.result()
            if segs:
                videos.append(v)
                segments.extend(segs)
                if src == "transcript": from_tr += 1
                if src == "chapters": from_ch += 1
            done += 1
            if done % PRINT_EVERY == 0 or done == total:
                print(f"Processed {done}/{total}… (videos with data: {len(videos)} | transcript:{from_tr} chapters:{from_ch} | segments:{len(segments)})")

    os.makedirs("public", exist_ok=True)
    with open("public/index.json","w",encoding="utf-8") as f:
        json.dump({
            "generated_at": int(time.time()),
            "channel": {"handle": HANDLE},
            "videos": videos,
            "segments": segments,
            "source_counts": {"transcript": from_tr, "chapters": from_ch},
            "config": {"allow_auto": ALLOW_AUTO, "langs": LANGS},
        }, f, ensure_ascii=False)
    print(f"✅ Done. Indexed {len(segments)} segments from {len(videos)} videos (transcript:{from_tr} chapters:{from_ch}).")

if __name__ == "__main__":
    main()
