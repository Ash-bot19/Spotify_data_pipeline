import os
import sys

# Ensure package imports work when the script is executed as `python ETL/main.py`
if __package__ in (None, ""):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

import pandas as pd
from ETL.spotify_clients import SpotifyClient
from ETL.extracts import extract_daily_snapshots
from ETL.transform import to_artist_rankings


def main():
    sp = SpotifyClient()
    snapshots = extract_daily_snapshots(sp)
    bronze, per_market, global_daily = to_artist_rankings(snapshots)

    target = os.environ.get("TARGET", "files")
    if target == "postgres":
        from ETL.load_postgres import load_to_postgres
        load_to_postgres(bronze, per_market, global_daily)
    else:
        # Lazy import to avoid requiring this file when targeting Postgres
        from ETL.load_files import load_to_files
        load_to_files(bronze, per_market, global_daily)

if __name__ == "__main__":
    main()

