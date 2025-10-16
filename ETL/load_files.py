"""
Persist pipeline outputs to local files.

Default location is `ETL/outputs/`, override with `FILES_OUTPUT_DIR`.
"""

import os
from pathlib import Path
from typing import Optional

import pandas as pd


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    _ensure_parent(path)
    if df.empty:
        df.head(0).to_parquet(path, index=False)
    else:
        df.to_parquet(path, index=False)


def load_to_files(
    bronze: pd.DataFrame,
    per_market: pd.DataFrame,
    global_daily: pd.DataFrame,
    base_dir: Optional[str] = None,
) -> None:
    """Persist dataframes to parquet files for downstream analytics."""
    base_path = Path(
        base_dir
        or os.environ.get("FILES_OUTPUT_DIR")
        or Path(__file__).resolve().parent / "outputs"
    )
    base_path.mkdir(parents=True, exist_ok=True)

    outputs = {
        "bronze_daily_tracks.parquet": bronze,
        "silver_artist_market_daily.parquet": per_market,
        "gold_artist_global_daily.parquet": global_daily,
    }

    for filename, df in outputs.items():
        target = base_path / filename
        _write_parquet(df, target)
        print(f"Wrote {len(df)} rows to {target}")
