import os, json, time, requests
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled

API_KEY   = os.environ["YOUTUBE_API_KEY"]
HANDLE    = os.getenv("YOUTUBE_HANDLE", "@blackoutapp")  # include @
LANGS     = ["en", "en-US"]

def get_uploads_playlist_id(handle: str):
    r = requests.get(
        "https://www.googleapis.com/youtube/v3/channels",
        params={"part": "contentDetails,snippet", "forHandle": handle, "key": API_KEY},
        timeout=30,
    )
    r.raise_for_status()
    items = r.json().get("items", [])
    if not items:
        raise SystemExit(f"No channel found for handle {handle}")
    ch = items[0]
    return {
        "channel_id": ch["id"],
        "channel_title": ch["snippet"]["title"],
        "uploads": ch["contentDetails"]["relatedPlaylists"]["uploads"],
    }

def iter_videos(uploads_playlist_id: str):
    page = None
    while True:
        r = requests.get(
            "https://www.googleapis.com/youtube/v3/playlistItems",
            params={
                "part": "contentDetails,snippet",
                "playlistId": uploads_playlist_id,
                "maxResults": 50,
                "pageToken": page,
                "key": API_KEY,
            },
            timeout=30,
        )
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
        if not page:
            break

def main():
    meta = get_uploads_playlist_id(HANDLE)
    videos, segments = [], []
    for v in iter_videos(meta["uploads"]):
        try:
            tr = YouTubeTranscriptApi.get_transcript(v["id"], languages=LANGS)
        except (NoTranscriptFound, TranscriptsDisabled, Exception):
            continue  # skip videos without transcripts
        videos.append(v)
        for s in tr:
            start = int(s.get("start", 0))
            text  = s.get("text", "").replace("\n"," ").strip()
            if not text:
                continue
            segments.append({
                "video_id": v["id"],
                "start": start,
                "text": text,
                "norm": text.lower(),
            })

    out = {
        "generated_at": int(time.time()),
        "channel": {"id": meta["channel_id"], "title": meta["channel_title"], "handle": HANDLE},
        "videos": videos,
        "segments": segments,
    }
    os.makedirs("public", exist_ok=True)
    with open("public/index.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)
    print(f"Wrote {len(segments)} segments from {len(videos)} videos")

if __name__ == "__main__":
    main()
