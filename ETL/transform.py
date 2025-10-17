"""Transform Spotify playlist payloads into bronze/silver/gold tables."""

from __future__ import annotations

from datetime import date
from typing import Iterable, List, Mapping, MutableMapping, Sequence, Tuple

import pandas as pd


BRONZE_COLUMNS = [
    "snapshot_date",
    "market",
    "playlist_id",
    "playlist_name",
    "rank",
    "track_id",
    "track_name",
    "artist_ids",
    "artist_names",
    "score",
]

SILVER_COLUMNS = [
    "snapshot_date",
    "market",
    "artist_id",
    "artist_name",
    "tracks",
    "total_score",
    "best_rank",
]

GOLD_COLUMNS = [
    "snapshot_date",
    "artist_id",
    "artist_name",
    "markets",
    "total_score",
    "best_rank",
]


def playlist_to_bronze(
    items: Iterable[Mapping[str, object]],
    *,
    market: str,
    playlist_id: str,
    playlist_name: str,
    snapshot_date: date,
) -> pd.DataFrame:
    """Flatten a playlist payload into the bronze_daily_tracks structure."""
    rows: List[MutableMapping[str, object]] = []
    for rank, raw_item in enumerate(items, start=1):
        item = dict(raw_item or {})
        track = item.get("track") or {}
        if not track:
            continue

        track_id = track.get("id")
        if not track_id:
            continue

        artists: Sequence[Mapping[str, object]] = track.get("artists") or []
        artist_ids = [str(artist.get("id")) for artist in artists if artist.get("id")]
        artist_names = [
            str(artist.get("name"))
            for artist in artists
            if artist.get("name") is not None
        ]

        rows.append(
            {
                "snapshot_date": snapshot_date,
                "market": market,
                "playlist_id": playlist_id,
                "playlist_name": playlist_name,
                "rank": rank,
                "track_id": track_id,
                "track_name": track.get("name"),
                "artist_ids": artist_ids,
                "artist_names": artist_names,
                "score": track.get("popularity"),
            }
        )

    bronze = pd.DataFrame(rows, columns=BRONZE_COLUMNS)
    if bronze.empty:
        return bronze

    bronze["rank"] = pd.to_numeric(bronze["rank"], downcast="integer")
    bronze["score"] = pd.to_numeric(bronze["score"], errors="coerce").round().astype(
        "Int64"
    )
    bronze["snapshot_date"] = pd.to_datetime(bronze["snapshot_date"]).dt.date
    return bronze


def top_tracks_to_bronze(
    tracks: Iterable[Mapping[str, object]],
    *,
    market: str,
    playlist_id: str,
    playlist_name: str,
    snapshot_date: date,
) -> pd.DataFrame:
    """Convert artist top tracks response into bronze schema rows."""
    rows: List[MutableMapping[str, object]] = []
    for rank, track in enumerate(tracks, start=1):
        track_id = track.get("id")
        if not track_id:
            continue
        artists: Sequence[Mapping[str, object]] = track.get("artists") or []
        artist_ids = [str(artist.get("id")) for artist in artists if artist.get("id")]
        artist_names = [
            str(artist.get("name"))
            for artist in artists
            if artist.get("name") is not None
        ]
        rows.append(
            {
                "snapshot_date": snapshot_date,
                "market": market,
                "playlist_id": playlist_id,
                "playlist_name": playlist_name,
                "rank": rank,
                "track_id": track_id,
                "track_name": track.get("name"),
                "artist_ids": artist_ids,
                "artist_names": artist_names,
                "score": track.get("popularity"),
            }
        )

    bronze = pd.DataFrame(rows, columns=BRONZE_COLUMNS)
    if bronze.empty:
        return bronze

    bronze["rank"] = pd.to_numeric(bronze["rank"], downcast="integer")
    bronze["score"] = pd.to_numeric(bronze["score"], errors="coerce").round().astype(
        "Int64"
    )
    bronze["snapshot_date"] = pd.to_datetime(bronze["snapshot_date"]).dt.date
    return bronze


def bronze_to_silver(bronze_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate bronze tracks to market-level artist metrics."""
    if bronze_df.empty:
        return pd.DataFrame(columns=SILVER_COLUMNS)

    exploded_rows: List[Tuple] = []
    for _, row in bronze_df.iterrows():
        artist_pairs = list(zip(row["artist_ids"] or [], row["artist_names"] or []))
        if not artist_pairs:
            continue

        for artist_id, artist_name in artist_pairs:
            exploded_rows.append(
                (
                    row["snapshot_date"],
                    row["market"],
                    artist_id,
                    artist_name,
                    row["track_id"],
                    row["score"],
                    row["rank"],
                )
            )

    columns = [
        "snapshot_date",
        "market",
        "artist_id",
        "artist_name",
        "track_id",
        "score",
        "rank",
    ]
    exploded = pd.DataFrame(exploded_rows, columns=columns)
    if exploded.empty:
        return pd.DataFrame(columns=SILVER_COLUMNS)

    grouped = (
        exploded.groupby(
            ["snapshot_date", "market", "artist_id", "artist_name"], as_index=False
        ).agg(
            tracks=("track_id", "nunique"),
            total_score=("score", "sum"),
            best_rank=("rank", "min"),
        )
    )

    grouped["tracks"] = pd.to_numeric(grouped["tracks"], downcast="integer").astype(
        "Int64"
    )
    grouped["total_score"] = pd.to_numeric(
        grouped["total_score"], errors="coerce"
    ).astype("Int64")
    grouped["best_rank"] = pd.to_numeric(
        grouped["best_rank"], errors="coerce"
    ).astype("Int64")
    grouped = grouped[SILVER_COLUMNS]

    return grouped.sort_values(
        ["snapshot_date", "market", "best_rank", "total_score"], ascending=[True, True, True, False]
    )


def silver_to_gold(silver_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate market artist metrics into a global daily snapshot."""
    if silver_df.empty:
        return pd.DataFrame(columns=GOLD_COLUMNS)

    grouped = (
        silver_df.groupby(["snapshot_date", "artist_id", "artist_name"], as_index=False)
        .agg(
            markets=("market", "nunique"),
            total_score=("total_score", "sum"),
            best_rank=("best_rank", "min"),
        )
        .sort_values(["snapshot_date", "best_rank", "total_score"], ascending=[True, True, False])
    )

    grouped["markets"] = pd.to_numeric(
        grouped["markets"], downcast="integer"
    ).astype("Int64")
    grouped["total_score"] = pd.to_numeric(
        grouped["total_score"], errors="coerce"
    ).astype("Int64")
    grouped["best_rank"] = pd.to_numeric(
        grouped["best_rank"], errors="coerce"
    ).astype("Int64")

    return grouped[GOLD_COLUMNS]


__all__ = [
    "playlist_to_bronze",
    "top_tracks_to_bronze",
    "bronze_to_silver",
    "silver_to_gold",
    "BRONZE_COLUMNS",
    "SILVER_COLUMNS",
    "GOLD_COLUMNS",
]
