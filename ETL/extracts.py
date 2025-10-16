from datetime import date
from .markets import MARKETS
from .spotify_clients import SpotifyClient


# Spotify typically names playlists like: "Top 50 - India", owner "Spotify"
# We'll search per market by display name pattern.

def find_top50_playlist_for_market(sp, market_code):
    # Example names vary slightly; this search pattern works well:
    q = f"Top 50 - {market_code}"  # backup pattern if localized names fail
    data = sp.get("/search", params={"q": q, "type": "playlist", "limit": 10})
    items = data.get("playlists", {}).get("items", [])
    # prefer owner "Spotify" and 50 tracks
    for p in items:
        if not p:
            continue
        owner = (p.get("owner") or {}).get("display_name","").lower()
        name  = p.get("name","")
        if "spotify" in owner and "top 50" in name.lower():
            return p["id"], p["name"]
    # fallback: generic "Top 50 - <Country Name>" via broader search
    data2 = sp.get("/search", params={"q": "top 50", "type": "playlist", "limit": 20})
    for p in data2.get("playlists", {}).get("items", []):
        if not p:
            continue
        owner = (p.get("owner") or {}).get("display_name","").lower()
        if "spotify" in owner and market_code.lower() in (p.get("name","").lower()):
            return p["id"], p["name"]
    return None, None

def fetch_playlist_tracks(sp, playlist_id, market=None):
    # paginate through tracks
    out = []
    path = f"/playlists/{playlist_id}/tracks"
    params = {"limit": 100, "market": market} if market else {"limit": 100}
    while True:
        data = sp.get(path, params=params)
        items = data.get("items", [])
        out.extend(items)
        next_url = data.get("next")
        if not next_url:
            break
        # convert next_url to path/params
        path = next_url.replace("https://api.spotify.com/v1", "")
        params = None
    return out

def extract_daily_snapshots(sp):
    today = str(date.today())
    snapshots = []
    for m in MARKETS:
        pid, pname = find_top50_playlist_for_market(sp, m)
        if not pid:
            continue
        items = fetch_playlist_tracks(sp, pid, market=m)
        # Normalize rank 1..50
        rank = 1
        for it in items[:50]:
            tr = it.get("track") or {}
            snapshots.append({
                "snapshot_date": today,
                "market": m,
                "playlist_id": pid,
                "playlist_name": pname,
                "rank": rank,
                "track_id": tr.get("id"),
                "track_name": tr.get("name"),
                "artist_ids": [a.get("id") for a in (tr.get("artists") or [])],
                "artist_names": [a.get("name") for a in (tr.get("artists") or [])],
            })
            rank += 1
    return snapshots
