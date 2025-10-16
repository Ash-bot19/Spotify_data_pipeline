import os
import pandas as pd
from ETL.spotify_clients import SpotifyClient
from ETL.extracts import extract_daily_snapshots
from ETL.transform import to_artist_rankings

def main():
    sp = SpotifyClient()
    snapshots = extract_daily_snapshots(sp)
    bronze, per_market, global_daily = to_artist_rankings(snapshots)

    target = os.environ.get("TARGET","files")
    if target == "postgres":
        from .load_postgres import load_to_postgres
        load_to_postgres(bronze, per_market, global_daily)
    else:
        from .load_files import load_to_files
        load_to_files(bronze, per_market, global_daily)

if __name__ == "__main__":
    main()
