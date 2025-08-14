# scripts/build_index_from_srt.py
import json, os, glob, time
import srt

CHANNEL_HANDLE = os.getenv("YT_HANDLE", "@blackoutapp")

def load_titles():
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

def pick_caption_files():
    """Return dict video_id -> chosen srt path (prefer manual over auto)."""
    paths = glob.glob("captions/*.srt")
    manual = {}
    auto = {}
    for p in paths:
        base = os.path.basename(p)
        parts = base.split(".")
        vid = parts[0]
        is_auto = len(parts) >= 3 and parts[1] == "auto"  # abc123.auto.srt
        if is_auto:
            auto[vid] = p
        else:
            manual[vid] = p
    chosen = {}
    vids = set(list(auto.keys()) + list(manual.keys()))
    for vid in vids:
        chosen[vid] = manual.get(vid, auto.get(vid))
    return chosen

def main():
    titles = load_titles()
    chosen = pick_caption_files()
    segments = []
    for vid, path in sorted(chosen.items()):
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
    videos = [{"id": v, "title": titles.get(v, v), "url": f"https://youtu.be/{v}"} for v in sorted(chosen)]
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
