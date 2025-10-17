"""Storage utilities for writing ETL outputs to disk."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Dict

import pandas as pd


def write_parquet_snapshot(
    bronze_df: pd.DataFrame,
    silver_df: pd.DataFrame,
    gold_df: pd.DataFrame,
    output_dir: Path,
    snapshot_ts: datetime,
) -> Dict[str, Path]:
    """Persist ETL outputs to parquet files and return the resulting paths."""
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = snapshot_ts.strftime("%Y%m%dT%H%M%SZ")
    bronze_path = output_dir / f"bronze_daily_tracks_{timestamp}.parquet"
    silver_path = output_dir / f"silver_artist_market_daily_{timestamp}.parquet"
    gold_path = output_dir / f"gold_artist_global_daily_{timestamp}.parquet"

    bronze_df.to_parquet(bronze_path, index=False)
    silver_df.to_parquet(silver_path, index=False)
    gold_df.to_parquet(gold_path, index=False)

    return {
        "bronze_daily_tracks": bronze_path,
        "silver_artist_market_daily": silver_path,
        "gold_artist_global_daily": gold_path,
    }


__all__ = ["write_parquet_snapshot"]
