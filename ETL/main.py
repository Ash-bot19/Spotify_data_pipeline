"""Entrypoint for the Spotify Top Artists ETL pipeline."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, List

import pandas as pd
from requests import HTTPError

from .config import load_settings
from .db import load_dataframes
from .spotify import SpotifyClient
from .storage import write_parquet_snapshot
from .transform import bronze_to_silver, playlist_to_bronze, silver_to_gold

LOGGER = logging.getLogger(__name__)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def run() -> Dict[str, object]:
    """Execute the ETL pipeline."""
    configure_logging()
    settings = load_settings()
    playlist_targets = settings.playlists
    LOGGER.info(
        "Starting Spotify ETL pipeline for %s playlist(s)", len(playlist_targets)
    )

    client = SpotifyClient(settings.spotify_client_id, settings.spotify_client_secret)
    snapshot_ts = datetime.now(timezone.utc)
    snapshot_date = snapshot_ts.date()

    bronze_frames: List[pd.DataFrame] = []

    errors: List[str] = []

    for target in playlist_targets:
        try:
            playlist = client.fetch_playlist(
                target.playlist_id, market=target.api_market
            )
        except HTTPError as exc:
            response_text = ""
            if exc.response is not None:
                try:
                    response_text = f" | Spotify response: {exc.response.text}"
                except Exception:  # pragma: no cover - defensive
                    response_text = ""
            message = (
                f"Failed to fetch playlist '{target.playlist_id}' "
                f"for market '{target.market}'. Verify the playlist ID/URL is public, "
                "available in the requested market, and that the app has access."
                f"{response_text}"
            )
            LOGGER.error(message)
            errors.append(message)
            continue
        playlist_name = playlist.get("name") or target.playlist_id
        items = playlist.get("tracks", {}).get("items", [])
        LOGGER.info(
            "Normalising playlist '%s' for market %s (%s tracks)",
            playlist_name,
            target.market,
            len(items),
        )
        bronze_frame = playlist_to_bronze(
            items,
            market=target.dataset_market,
            playlist_id=target.playlist_id,
            playlist_name=playlist_name,
            snapshot_date=snapshot_date,
        )
        if bronze_frame.empty:
            LOGGER.warning(
                "Playlist %s returned no track rows; skipping market %s",
                target.playlist_id,
                target.market,
            )
            continue
        bronze_frames.append(bronze_frame)

    if not bronze_frames:
        error_message = (
            "; ".join(errors) if errors else "No playlist data retrieved; aborting run."
        )
        LOGGER.error(error_message)
        raise RuntimeError(error_message)

    bronze_df = pd.concat(bronze_frames, ignore_index=True)
    silver_df = bronze_to_silver(bronze_df)
    gold_df = silver_to_gold(silver_df)

    LOGGER.info(
        "Prepared %s bronze rows, %s silver rows, %s gold rows",
        len(bronze_df),
        len(silver_df),
        len(gold_df),
    )

    result: Dict[str, object]
    if settings.use_database:
        rows_inserted = load_dataframes(
            bronze_df, silver_df, gold_df, settings.database_url or "", snapshot_date
        )
        result = {
            "status": "loaded",
            "rows_inserted": rows_inserted,
            "snapshot_date": snapshot_date.isoformat(),
        }
    else:
        outputs = write_parquet_snapshot(
            bronze_df, silver_df, gold_df, settings.outputs_dir, snapshot_ts
        )
        result = {
            "status": "written",
            "files": {k: str(v) for k, v in outputs.items()},
            "snapshot_date": snapshot_date.isoformat(),
        }

    LOGGER.info("ETL completed successfully: %s", result)
    return result


if __name__ == "__main__":  # pragma: no cover
    run()
