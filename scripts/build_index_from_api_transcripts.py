import os, json, time, requests, random
from concurrent.futures import ThreadPoolExecutor, as_completed
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled

API_KEY = os.environ["YOUTUBE_API_KEY"]
HANDLE  = os.getenv("YOUTUBE_HANDLE", "@blackoutapp")

# Wider English set; change if needed
LANGS   = ["en","en-US","en-GB","en-CA","en-AU"]
ALLOW_AUTO = True

# Tuning knobs (optional via env)
WORKERS = int(os.getenv("WORKERS", "8"))          # parallelism
PRINT_EVERY = int(os.getenv("PRINT_EVERY", "10")) # progress cadence
LIMIT = int(os.getenv("LIMIT", "0"))              # 0 = no cap; set e.g. 20 for fast test

def uploads_playlist_id():
    r = requests.get("https://www.googleapis.com/youtube/v3/channels",
        params={"part":"contentDetails,snippet","forHandle":HANDLE,"key":API_KEY}, timeout=30)
    r.raise_for_status()
    items = r.json().get("items", [])
    if not items:
        raise SystemExit(f"No channel for handle {HANDLE}")
    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

def list_uploads(pid):
    vids = []
    page = None
    while True:
        r = requests.get("https://www.googleapis.com/youtube/v3/playlistItems",
            params={"part":"contentDetails,snippet","playlistId":pid,"maxResults":50,
                    "pageToken":page,"key":API_KEY}, timeout=30)
        r.raise_for_status()
        j = r.json()
        for it in j.get("items", []):
            vid = it["contentDetails"]["videoId"]
            vids.append({
                "id": vid,
                "title": it["snippet"]["title"],
                "published": it["contentDetails"].get("videoPublishedAt"),
                "url": f"https://youtu.be/{vid}",
            })
        page = j.get("nextPageToken")
        if not page:
            break
    return vids

def fetch_best_transcript(video_id):
    # Prefer manual English; then auto English if allowed. Retry gently.
    for attempt in range(3):
        try:
            tlist = YouTubeTranscriptApi.list_transcripts(video_id)
            # manual first
            for t in tlist:
                if not t.is_generated and any(t.language_code.startswith(l.split("-")[0]) for l in LANGS):
                    return t.fetch()
            if ALLOW_AUTO:
                for t in tlist:
                    if t.is_generated and any(t.language_code.startswith(l.split("-")[0]) for l in LANGS):
                        return t.fetch()
            return None
        except (NoTranscriptFound, TranscriptsDisabled):
            return None
        except Exception:
            time.sleep(0.7 + random.random())
    # last try
    try:
        return YouTubeTranscriptApi.get_transcript(video_id, languages=LANGS)
    except Exception:
        return None

def process_video(v):
    tr = fetch_best_transcript(v["id"])
    if not tr:
        return (v, [])
    segs = []
    for s in tr:
        txt = (s.get("text") or "").replace("\n"," ").strip()
        if not txt:
            continue
        segs.append({
            "video_id": v["id"],
            "start": int(s.get("start",0)),
            "text": txt,
            "norm": txt.lower(),
        })
    return (v, segs)

def main():
    pid = uploads_playlist_id()
    vids = list_uploads(pid)
    if LIMIT and LIMIT > 0:
        vids = vids[:LIMIT]
    total = len(vids)
    print(f"Discovered {total} uploads. Processing with WORKERS={WORKERS} LIMIT={LIMIT or 'none'}")

    videos, segments = [], []
    done = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(process_video, v) for v in vids]
        for fut in as_completed(futs):
            v, segs = fut.result()
            if segs:
                videos.append(v)
                segments.extend(segs)
            done += 1
            if done % PRINT_EVERY == 0 or done == total:
                print(f"Processed {done}/{total} videos… (so far: {len(segments)} segments from {len(videos)} videos)")

    os.makedirs("public", exist_ok=True)
    out = {
        "generated_at": int(time.time()),
        "channel": {"handle": HANDLE},
        "videos": videos,
        "segments": segments,
        "config": {"allow_auto": ALLOW_AUTO, "langs": LANGS},
    }
    with open("public/index.json","w",encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)
    print(f"✅ Done. Indexed {len(segments)} segments from {len(videos)} videos.")

if __name__ == "__main__":
    main()
