import json
import glob
import os
from collections import defaultdict, Counter
from datetime import datetime

DATA_FOLDER = "./spotify_data"
TOP_N = 10
SAVE_CSV = True
INCLUDE_PODCASTS_AUDIOBOOKS = False

def parse_dt(ts):
    ts = str(ts).strip()
    if ts.endswith("Z"):
        ts = ts[:-1]
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(ts, fmt)
            except Exception:
                pass
    return None

def safe_get(d, keys):
    for k in keys:
        if k in d and d[k] not in (None, "", "null"):
            return d[k]
    return None

def load_streaming_history(folder):
    paths = sorted(set(glob.glob(os.path.join(folder, "**", "*.json"), recursive=True)))
    if not paths:
        raise FileNotFoundError(f"No .json files found in {folder}.")
    records = []
    for p in paths:
        with open(p, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except Exception:
                continue
            if isinstance(data, dict) and "history" in data and isinstance(data["history"], list):
                data = data["history"]
            if isinstance(data, list):
                for r in data:
                    if isinstance(r, dict):
                        records.append(r)
    return records

def normalize_record(r):
    ts = safe_get(r, ["ts", "endTime", "timestamp", "played_at"])
    dt = parse_dt(ts) if ts is not None else None
    if dt is None:
        return None

    ms = safe_get(r, ["ms_played", "msPlayed", "play_duration_ms"])
    try:
        ms = int(ms) if ms is not None else 0
    except Exception:
        ms = 0
    if ms <= 0:
        return None

    track = safe_get(r, ["master_metadata_track_name", "trackName", "track", "track_name", "name"])
    artist = safe_get(r, ["master_metadata_album_artist_name", "artistName", "artist", "artist_name", "master_metadata_artist_name"])

    if track and artist:
        item_type = "song"
        item_key = f"{artist} - {track}"
        artist_key = str(artist).strip()
        return dt.year, item_type, artist_key, str(item_key).strip(), ms

    if INCLUDE_PODCASTS_AUDIOBOOKS:
        ep = safe_get(r, ["episode_name"])
        show = safe_get(r, ["episode_show_name"])
        if ep and show:
            item_type = "podcast"
            item_key = f"{show} - {ep}"
            artist_key = str(show).strip()
            return dt.year, item_type, artist_key, str(item_key).strip(), ms

        ab = safe_get(r, ["audiobook_title"])
        ch = safe_get(r, ["audiobook_chapter_title"])
        if ab and ch:
            item_type = "audiobook"
            item_key = f"{ab} - {ch}"
            artist_key = str(ab).strip()
            return dt.year, item_type, artist_key, str(item_key).strip(), ms

    return None

def ms_to_hours(ms):
    return ms / 1000 / 60 / 60

def save_counter_csv(path, counter, left, right):
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"{left},{right}\n")
        for k, v in counter.most_common():
            f.write(f"\"{k}\",{v}\n")

def main():
    records = load_streaming_history(DATA_FOLDER)

    artist_ms = defaultdict(Counter)
    track_ms = defaultdict(Counter)
    year_total_ms = Counter()
    year_streams = Counter()

    usable = 0
    for r in records:
        norm = normalize_record(r)
        if not norm:
            continue
        usable += 1
        year, item_type, artist_key, item_key, ms = norm

        artist_ms[year][artist_key] += ms
        track_ms[year][item_key] += ms
        year_total_ms[year] += ms
        year_streams[year] += 1

    if usable == 0:
        print("No usable streaming records found.")
        return

    years = sorted(year_total_ms.keys())
    print("\n===== Spotify Yearly Recap =====\n")

    if SAVE_CSV:
        os.makedirs("out", exist_ok=True)

    for y in years:
        print(f"--- {y} ---")
        print(f"Total listening: {ms_to_hours(year_total_ms[y]):.2f} hours")
        print(f"Total streams:   {year_streams[y]}")

        print(f"\nTop {TOP_N} Artists:")
        for a, ms in artist_ms[y].most_common(TOP_N):
            print(f"  {a:40} {ms_to_hours(ms):6.2f} hrs")

        print(f"\nTop {TOP_N} Songs:")
        for s, ms in track_ms[y].most_common(TOP_N):
            print(f"  {s:60} {ms_to_hours(ms):6.2f} hrs")
        print()

        if SAVE_CSV:
            save_counter_csv(f"out/{y}_top_artists.csv", artist_ms[y], "artist", "ms_listened")
            save_counter_csv(f"out/{y}_top_songs.csv", track_ms[y], "song", "ms_listened")

    if SAVE_CSV:
        with open("out/year_totals.csv", "w", encoding="utf-8") as f:
            f.write("year,total_ms,total_hours,total_streams\n")
            for y in years:
                f.write(f"{y},{year_total_ms[y]},{ms_to_hours(year_total_ms[y]):.4f},{year_streams[y]}\n")
        print("Saved CSVs in ./out")

if __name__ == "__main__":
    main()
