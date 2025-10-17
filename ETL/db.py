"""Database utilities for persisting Spotify ETL outputs."""

from __future__ import annotations

import logging
from datetime import date
from typing import Dict, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.types import Integer, Text
from sqlalchemy.dialects.postgresql import ARRAY, DATE as PGDATE

LOGGER = logging.getLogger(__name__)

BRONZE_TABLE = "bronze_daily_tracks"
SILVER_TABLE = "silver_artist_market_daily"
GOLD_TABLE = "gold_artist_global_daily"

BRONZE_DDL = f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
  snapshot_date date,
  market text,
  playlist_id text,
  playlist_name text,
  rank int,
  track_id text,
  track_name text,
  artist_ids text[],
  artist_names text[],
  score int,
  PRIMARY KEY (snapshot_date, market, rank)
);
"""

SILVER_DDL = f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
  snapshot_date date,
  market text,
  artist_id text,
  artist_name text,
  tracks int,
  total_score int,
  best_rank int,
  PRIMARY KEY (snapshot_date, market, artist_id)
);
"""

GOLD_DDL = f"""
CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
  snapshot_date date,
  artist_id text,
  artist_name text,
  markets int,
  total_score int,
  best_rank int,
  PRIMARY KEY (snapshot_date, artist_id)
);
"""

SILVER_INDEX = f"""
CREATE INDEX IF NOT EXISTS idx_silver_date_market
  ON {SILVER_TABLE} (snapshot_date, market);
"""

GOLD_INDEX = f"""
CREATE INDEX IF NOT EXISTS idx_gold_date
  ON {GOLD_TABLE} (snapshot_date);
"""


def initialise_database(engine: Engine) -> None:
    """Ensure required tables exist in the target database."""
    statements = [BRONZE_DDL, SILVER_DDL, GOLD_DDL, SILVER_INDEX, GOLD_INDEX]
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


def _delete_snapshot(engine: Engine, snapshot: date) -> None:
    """Remove existing rows for the given snapshot date to avoid duplicates."""
    delete_statements = [
        (BRONZE_TABLE, text(f"DELETE FROM {BRONZE_TABLE} WHERE snapshot_date = :dt")),
        (SILVER_TABLE, text(f"DELETE FROM {SILVER_TABLE} WHERE snapshot_date = :dt")),
        (GOLD_TABLE, text(f"DELETE FROM {GOLD_TABLE} WHERE snapshot_date = :dt")),
    ]
    with engine.begin() as connection:
        for table, statement in delete_statements:
            result = connection.execute(statement, {"dt": snapshot})
            LOGGER.debug(
                "Deleted %s existing rows for %s on %s", result.rowcount, table, snapshot
            )


def load_dataframes(
    bronze_df: pd.DataFrame,
    silver_df: pd.DataFrame,
    gold_df: pd.DataFrame,
    database_url: str,
    snapshot: Optional[date] = None,
) -> Dict[str, int]:
    """Append the ETL outputs into the target database."""
    engine = create_engine(database_url, pool_pre_ping=True)
    initialise_database(engine)

    if snapshot is None:
        for df in (bronze_df, silver_df, gold_df):
            if not df.empty and "snapshot_date" in df.columns:
                snapshot = df["snapshot_date"].iloc[0]
                break
    if snapshot is None:
        raise RuntimeError("Unable to determine snapshot date for database load.")

    _delete_snapshot(engine, snapshot)

    inserted: Dict[str, int] = {}

    if not bronze_df.empty:
        bronze_df = bronze_df.copy()
        with engine.begin() as connection:
            bronze_df.to_sql(
                BRONZE_TABLE,
                connection,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=500,
                dtype={
                    "snapshot_date": PGDATE(),
                    "market": Text(),
                    "playlist_id": Text(),
                    "playlist_name": Text(),
                    "rank": Integer(),
                    "track_id": Text(),
                    "track_name": Text(),
                    "artist_ids": ARRAY(Text()),
                    "artist_names": ARRAY(Text()),
                    "score": Integer(),
                },
            )
        inserted[BRONZE_TABLE] = len(bronze_df)
        LOGGER.info("Inserted %s rows into %s", len(bronze_df), BRONZE_TABLE)

    if not silver_df.empty:
        silver_df = silver_df.copy()
        with engine.begin() as connection:
            silver_df.to_sql(
                SILVER_TABLE,
                connection,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=500,
                dtype={
                    "snapshot_date": PGDATE(),
                    "market": Text(),
                    "artist_id": Text(),
                    "artist_name": Text(),
                    "tracks": Integer(),
                    "total_score": Integer(),
                    "best_rank": Integer(),
                },
            )
        inserted[SILVER_TABLE] = len(silver_df)
        LOGGER.info("Inserted %s rows into %s", len(silver_df), SILVER_TABLE)

    if not gold_df.empty:
        gold_df = gold_df.copy()
        with engine.begin() as connection:
            gold_df.to_sql(
                GOLD_TABLE,
                connection,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=500,
                dtype={
                    "snapshot_date": PGDATE(),
                    "artist_id": Text(),
                    "artist_name": Text(),
                    "markets": Integer(),
                    "total_score": Integer(),
                    "best_rank": Integer(),
                },
            )
        inserted[GOLD_TABLE] = len(gold_df)
        LOGGER.info("Inserted %s rows into %s", len(gold_df), GOLD_TABLE)

    return inserted


__all__ = [
    "load_dataframes",
    "initialise_database",
    "BRONZE_TABLE",
    "SILVER_TABLE",
    "GOLD_TABLE",
]
