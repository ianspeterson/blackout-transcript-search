import os, json, time, re, requests, random
from concurrent.futures import ThreadPoolExecutor, as_completed

API_KEY = os.environ["YOUTUBE_API_KEY"]
HANDLE  = os.getenv("YOUTUBE_HANDLE", "@blackoutapp")

# Prefer English, but if no English track exists we'll accept ANY language
PREF_LANGS   = ["en","en-US","en-GB","en-CA","en-AU"]
ALLOW_AUTO   = True

WORKERS      = int(os.getenv("WORKERS", "8"))
PRINT_EVERY  = int(os.getenv("PRINT_EVERY", "10"))
LIMIT        = int(os.getenv("LIMIT", "0"))

TIMESTAMP_RE = re.compile(r'\b(?:(\d{1,2}):)?(\d{1,2}):(\d{2})\b')

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

# ---------- timedtext (public) ----------
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

def fetch_track_vtt(video_id, track):
    params = {"v": video_id, "fmt": "vtt", "lang": track["lang"]}
    if track["kind"] == "asr": params["kind"] = "asr"
    elif track.get("name"):   params["name"] = track["name"]
    r = requests.get("https://www.youtube.com/api/timedtext", params=params, timeout=30)
    if r.status_code != 200 or "WEBVTT" not in r.text:
        return None
    return r.text

def parse_vtt(vtt_text):
    segs, lines = [], [ln.rstrip("\n") for ln in vtt_text.splitlines()]
    i = 0
    while i < len(lines):
        ln = lines[i]
        if "-->" in ln:
            start = ln.split("-->")[0].strip()
            i += 1
            buf = []
            while i < len(lines) and lines[i].strip():
                if lines[i].strip().upper().startswith(("NOTE","STYLE")): break
                buf.append(lines[i]); i += 1
            text = " ".join(" ".join(buf).split())
            if text:
                segs.append({"start": vtt_to_seconds(start), "text": text, "norm": text.lower()})
        i += 1
    return segs

def vtt_to_seconds(ts):
    parts = ts.replace(",", ".").split(":")
    if len(parts) == 3: h,m,s = parts
    else:               h,m,s = "0", parts[0], parts[1]
    return int(float(h)*3600 + float(m)*60 + float(s))

# ---------- description/title helpers ----------
def fetch_snippet(video_id):
    r = requests.get("https://www.googleapis.com/youtube/v3/videos",
        params={"part":"snippet","id":video_id,"key":API_KEY}, timeout=20)
    r.raise_for_status()
    items = r.json().get("items", [])
    if not items: return {"title":"", "description":""}
    sn = items[0]["snippet"]
    return {"title": sn.get("title") or "", "description": sn.get("description") or ""}

def chapters_from_description(desc):
    segs = []
    for line in (desc or "").splitlines():
        m = TIMESTAMP_RE.search(line)
        if not m: continue
        h,m_,s = m.groups()
        t = (int(h) if h else 0)*3600 + int(m_)*60 + int(s)
        text = line[m.end():].strip(" -–—:·|")
        if not text:
            text = TIMESTAMP_RE.sub("", line).strip(" -–—:·|")
        if text:
            segs.append({"start": t, "text": text, "norm": text.lower()})
    return segs

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
    # ANY manual track (last resort)
    for t in tracks:
        if t["kind"] != "asr":
            return t
    # ANY auto track
    return tracks[0] if tracks else None

def process_video(v):
    # Fetch snippet once (title & description)
    sn = fetch_snippet(v["id"])

    # 1) timedtext captions (any language if needed)
    tracks = list_tracks_timedtext(v["id"])
    chosen = choose_track(tracks)
    if chosen:
        vtt = fetch_track_vtt(v["id"], chosen)
        if vtt:
            segs = [{"video_id": v["id"], **s} for s in parse_vtt(vtt)]
            if segs:
                return (v, segs, "transcript")

    # 2) chapters in description
    chs = chapters_from_description(sn["description"])
    if chs:
        return (v, [{"video_id": v["id"], **s} for s in chs], "chapters")

    # 3) FALLBACK: index title + description so queries still hit (jump to t=0)
    fallback = []
    title = (sn["title"] or v["title"] or "").strip()
    if title:
        fallback.append({"video_id": v["id"], "start": 0, "text": title, "norm": title.lower()})
    desc = (sn["description"] or "").strip()
    if desc:
        # Keep it single segment to avoid huge index
        fallback.append({"video_id": v["id"], "start": 0, "text": desc[:5000], "norm": desc[:5000].lower()})
    return (v, fallback, "metadata" if fallback else "none")

def main():
    pid = uploads_playlist_id()
    vids = list_uploads(pid)
    if LIMIT and LIMIT > 0: vids = vids[:LIMIT]
    total = len(vids)
    print(f"Discovered {total} uploads. Processing with WORKERS={WORKERS} LIMIT={LIMIT or 'none'}")

    videos, segments = [], []
    done = 0; c_tr=c_ch=c_meta=0

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(process_video, v) for v in vids]
        for fut in as_completed(futs):
            v, segs, src = fut.result()
            if segs:
                videos.append(v); segments.extend(segs)
                if src=="transcript": c_tr+=1
                elif src=="chapters": c_ch+=1
                elif src=="metadata": c_meta+=1
            done += 1
            if done % PRINT_EVERY == 0 or done == total:
                print(f"Processed {done}/{total}… vids_with_data:{len(videos)}  segs:{len(segments)}  by src -> transcript:{c_tr} chapters:{c_ch} metadata:{c_meta}")

    os.makedirs("public", exist_ok=True)
    with open("public/index.json","w",encoding="utf-8") as f:
        json.dump({
            "generated_at": int(time.time()),
            "channel": {"handle": HANDLE},
            "videos": videos,
            "segments": segments,
            "source_counts": {"transcript": c_tr, "chapters": c_ch, "metadata": c_meta},
            "config": {"allow_auto": ALLOW_AUTO, "pref_langs": PREF_LANGS},
        }, f, ensure_ascii=False)
    print(f"✅ Done. Segments:{len(segments)} from Videos:{len(videos)} (tr:{c_tr} ch:{c_ch} meta:{c_meta}).")

if __name__ == "__main__":
    main()
