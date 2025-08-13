# builds public/index.json from SRTs downloaded into captions/
import json, os, glob, time
import srt

CHANNEL_HANDLE = os.getenv("YT_HANDLE", "@blackoutapp")

def load_titles():
    # created by the Action with: yt-dlp -J --flat-playlist <channel> > playlist.json
    titles = {}
    try:
        with open("playlist.json", "r", encoding="utf-8") as f:
            j = json.load(f)
        for e in j.get("entries", []):
            vid = e.get("id")
            title = (e.get("title") or vid) if vid else None
            if vid and title:
                titles[vid] = title
    except Exception:
        pass
    return titles

def main():
    titles = load_titles()
    segments = []
    have = set()
    for path in sorted(glob.glob("captions/*.srt")):
        base = os.path.basename(path)              # e.g. abc123.en.srt
        vid = base.split(".")[0]
        have.add(vid)
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            subs = list(srt.parse(f.read()))
        for sub in subs:
            start = int(sub.start.total_seconds())
            text = " ".join(str(sub.content).split())
            if text:
                segments.append({
                    "video_id": vid,
                    "start": start,
                    "text": text,
                    "norm": text.lower(),
                })

    videos = [{"id": v, "title": titles.get(v, v), "url": f"https://youtu.be/{v}"} for v in sorted(have)]
    os.makedirs("public", exist_ok=True)
    with open("public/index.json", "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": int(time.time()),
            "channel": {"handle": CHANNEL_HANDLE, "url": f"https://www.youtube.com/{CHANNEL_HANDLE}"},
            "videos": videos,
            "segments": segments,
        }, f, ensure_ascii=False)
    print(f"Indexed {len(segments)} segments from {len(videos)} videos")

if __name__ == "__main__":
    main()
