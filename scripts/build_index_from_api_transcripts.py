import os, json, time, requests, random
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled

API_KEY = os.environ["YOUTUBE_API_KEY"]
HANDLE  = os.getenv("YOUTUBE_HANDLE", "@blackoutapp")
LANGS   = ["en","en-US","en-GB","en-CA","en-AU"]   # widen English variants
ALLOW_AUTO = True                                   # allow auto-captions

def uploads_playlist_id():
    r = requests.get("https://www.googleapis.com/youtube/v3/channels",
        params={"part":"contentDetails,snippet","forHandle":HANDLE,"key":API_KEY}, timeout=30)
    r.raise_for_status()
    items = r.json().get("items", [])
    if not items: raise SystemExit(f"No channel for handle {HANDLE}")
    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

def iter_uploads(pid):
    page = None
    while True:
        r = requests.get("https://www.googleapis.com/youtube/v3/playlistItems",
            params={"part":"contentDetails,snippet","playlistId":pid,"maxResults":50,
                    "pageToken":page,"key":API_KEY}, timeout=30)
        r.raise_for_status()
        j = r.json()
        for it in j.get("items", []):
            vid = it["contentDetails"]["videoId"]
            yield {
                "id": vid,
                "title": it["snippet"]["title"],
                "published": it["contentDetails"].get("videoPublishedAt"),
                "url": f"https://youtu.be/{vid}",
            }
        page = j.get("nextPageToken")
        if not page: break

def fetch_best_transcript(video_id):
    # Prefer manual English; then auto English if allowed. Retry gently.
    for attempt in range(3):
        try:
            tlist = YouTubeTranscriptApi.list_transcripts(video_id)
            for t in tlist:
                if (not t.is_generated) and any(t.language_code.startswith(l.split("-")[0]) for l in LANGS):
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
    try:
        return YouTubeTranscriptApi.get_transcript(video_id, languages=LANGS)
    except Exception:
        return None

def main():
    pid = uploads_playlist_id()
    videos, segments = [], []
    for v in iter_uploads(pid):
        tr = fetch_best_transcript(v["id"])
        if not tr: continue
        videos.append(v)
        for s in tr:
            txt = (s.get("text") or "").replace("\n"," ").strip()
            if not txt: continue
            segments.append({
                "video_id": v["id"],
                "start": int(s.get("start",0)),
                "text": txt,
                "norm": txt.lower(),
            })
    os.makedirs("public", exist_ok=True)
    with open("public/index.json","w",encoding="utf-8") as f:
        json.dump({
            "generated_at": int(time.time()),
            "channel": {"handle": HANDLE},
            "videos": videos,
            "segments": segments,
            "config": {"allow_auto": ALLOW_AUTO, "langs": LANGS}
        }, f, ensure_ascii=False)
    print(f"Indexed {len(segments)} segments from {len(videos)} videos")

if __name__ == "__main__":
    main()
