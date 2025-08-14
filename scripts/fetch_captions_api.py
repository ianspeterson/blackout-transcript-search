import os, json, time, requests, pathlib

API_KEY   = os.environ["YOUTUBE_API_KEY"]
CLIENT_ID = os.environ["YT_CLIENT_ID"]
CLIENT_SECRET = os.environ["YT_CLIENT_SECRET"]
REFRESH_TOKEN = os.environ["YT_REFRESH_TOKEN"]
HANDLE    = os.getenv("YOUTUBE_HANDLE", "@blackoutapp")
LANGS     = [l.strip().lower() for l in os.getenv("YOUTUBE_LANGS","en,en-US").split(",") if l.strip()]
OUTDIR    = pathlib.Path("captions"); OUTDIR.mkdir(parents=True, exist_ok=True)

def token():
    r = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN, "grant_type": "refresh_token",
    }, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]

def get_uploads_playlist_id():
    r = requests.get("https://www.googleapis.com/youtube/v3/channels", params={
        "part": "contentDetails,snippet", "forHandle": HANDLE, "key": API_KEY
    }, timeout=30)
    r.raise_for_status()
    it = r.json().get("items", [])
    if not it: raise SystemExit(f"No channel for {HANDLE}")
    ch = it[0]
    return ch["contentDetails"]["relatedPlaylists"]["uploads"]

def iter_videos(uploads_id):
    page = None
    while True:
        r = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", params={
            "part": "contentDetails,snippet", "playlistId": uploads_id,
            "maxResults": 50, "pageToken": page, "key": API_KEY
        }, timeout=30)
        r.raise_for_status()
        j = r.json()
        for it in j.get("items", []):
            vid = it["contentDetails"]["videoId"]
            title = it["snippet"]["title"]
            yield {"id": vid, "title": title}
        page = j.get("nextPageToken")
        if not page: break

def best_track(tracks):
    # Prefer manual (non-ASR) English; else allow ASR English
    def is_en(sn): 
        lang = (sn.get("language") or "").lower()
        return lang in LANGS or any(lang.startswith(l.split("-")[0]) for l in LANGS)
    manual = [t for t in tracks if is_en(t["snippet"]) and t["snippet"].get("trackKind") != "ASR"]
    auto   = [t for t in tracks if is_en(t["snippet"]) and t["snippet"].get("trackKind") == "ASR"]
    return (manual[0], False) if manual else ((auto[0], True) if auto else (None, False))

def main():
    access = token()
    headers = {"Authorization": f"Bearer {access}"}
    uploads = get_uploads_playlist_id()

    # Save a playlist map for titles (used by build_index_from_srt.py)
    with open("playlist.json","w",encoding="utf-8") as f:
        json.dump({"entries": []}, f)

    entries = []
    for v in iter_videos(uploads):
        vid = v["id"]; entries.append({"id": vid, "title": v["title"]})
        # list caption tracks
        r = requests.get("https://www.googleapis.com/youtube/v3/captions", params={
            "part": "snippet", "videoId": vid
        }, headers=headers, timeout=30)
        r.raise_for_status()
        items = r.json().get("items", [])
        chosen, is_auto = best_track(items)
        if not chosen:
            continue
        cap_id = chosen["id"]
        # download SRT
        r = requests.get(f"https://www.googleapis.com/youtube/v3/captions/{cap_id}",
                         params={"tfmt":"srt"}, headers=headers, timeout=60)
        if r.status_code == 200 and r.text.strip():
            suffix = ".auto.srt" if is_auto else ".srt"
            (OUTDIR / f"{vid}{suffix}").write_text(r.text, encoding="utf-8", errors="ignore")
    # write playlist map
    with open("playlist.json","w",encoding="utf-8") as f:
        json.dump({"entries": entries}, f, ensure_ascii=False)

if __name__ == "__main__":
    main()
