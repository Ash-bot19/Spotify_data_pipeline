"""Configuration helpers for the Spotify ETL."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency during bootstrap
    load_dotenv = None  # type: ignore[assignment]


_ENV_LOADED = False


def _ensure_env_loaded() -> None:
    """Load environment variables from a `.env` file if python-dotenv is available."""
    global _ENV_LOADED
    if _ENV_LOADED:
        return

    if load_dotenv:
        candidate_paths = [
            Path.cwd() / ".env",
            Path(__file__).resolve().parent / ".env",
        ]
        for path in candidate_paths:
            if path.exists():
                load_dotenv(dotenv_path=path, override=False)
                break

    _ENV_LOADED = True


@dataclass(frozen=True)
class PlaylistTarget:
    """Represents a playlist to ingest for a particular market."""

    market: str
    playlist_id: str
    api_market_override: Optional[str] = None

    @property
    def dataset_market(self) -> str:
        """Market label used in downstream tables."""
        return self.market.upper()

    @property
    def api_market(self) -> Optional[str]:
        """Market code used when calling Spotify."""
        if self.api_market_override:
            return self.api_market_override.upper()

        if self.market.lower() == "global":
            return None

        return self.market.upper()


@dataclass(frozen=True)
class Settings:
    """Runtime configuration for the pipeline."""

    spotify_client_id: str
    spotify_client_secret: str
    target: str
    database_url: Optional[str]
    files_output_dir: Path
    playlists: Tuple[PlaylistTarget, ...]

    @property
    def outputs_dir(self) -> Path:
        """Return the absolute output directory for file targets."""
        if self.files_output_dir.is_absolute():
            return self.files_output_dir
        return (Path.cwd() / self.files_output_dir).resolve()

    @property
    def use_database(self) -> bool:
        return self.target == "postgres" and bool(self.database_url)


def _parse_playlists(raw_value: Optional[str]) -> Tuple[PlaylistTarget, ...]:
    """Parse playlist configuration from an environment variable."""
    if not raw_value:
        return (
            PlaylistTarget(
                market="us",
                playlist_id="37i9dQZEVXbLRQDuF5jeBp",
                api_market_override="US",
            ),
        )

    targets: List[PlaylistTarget] = []
    parts = [part.strip() for part in raw_value.split(",") if part.strip()]
    for part in parts:
        if ":" not in part:
            raise RuntimeError(
                "Invalid SPOTIFY_PLAYLIST_IDS entry. Use MARKET:PLAYLIST_ID format."
            )
        market, playlist_entry = [value.strip() for value in part.split(":", 1)]
        if not market or not playlist_entry:
            raise RuntimeError(
                "Invalid SPOTIFY_PLAYLIST_IDS entry. Use MARKET:PLAYLIST_ID format."
            )
        playlist_value, api_override = _split_playlist_entry(playlist_entry)
        targets.append(
            PlaylistTarget(
                market=market.lower(),
                playlist_id=_normalise_playlist_id(playlist_value),
                api_market_override=api_override,
            )
        )

    if not targets:
        raise RuntimeError("SPOTIFY_PLAYLIST_IDS did not contain any playlist entries.")
    return tuple(targets)


def _split_playlist_entry(entry: str) -> Tuple[str, Optional[str]]:
    """Split a playlist entry that may include an API market override (id@market)."""
    if "@" not in entry:
        return entry, None
    playlist_id, api_market = entry.split("@", 1)
    playlist_id = playlist_id.strip()
    api_market = api_market.strip()
    if not playlist_id:
        raise RuntimeError("Playlist entry is missing the playlist ID before '@'.")
    if not api_market:
        raise RuntimeError(
            "Playlist entry must include a market code after '@' when provided."
        )
    return playlist_id, api_market


def _normalise_playlist_id(value: str) -> str:
    """Extract the raw playlist ID from various formats (URL, URI, plain)."""
    value = value.strip()
    if value.startswith("spotify:"):
        parts = value.split(":")
        return parts[-1].strip()

    if "open.spotify.com" in value:
        # Handle URLs like https://open.spotify.com/playlist/{id}?si=...
        # and https://open.spotify.com/user/foo/playlist/{id}
        segment = value.split("playlist/", 1)[-1]
        segment = segment.split("?", 1)[0]
        segment = segment.split("&", 1)[0]
        return segment.strip("/").strip()

    return value


def load_settings() -> Settings:
    """Build a Settings instance from environment variables."""
    _ensure_env_loaded()

    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError(
            "Missing Spotify credentials. "
            "Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET."
        )

    target = os.getenv("TARGET", "files").strip().lower()
    if target not in {"files", "postgres"}:
        raise RuntimeError("TARGET must be either 'files' or 'postgres'.")

    database_url = os.getenv("SUPABASE_DATABASE_URL")
    files_output = os.getenv("FILES_OUTPUT_DIR") or str(Path("ETL") / "outputs")
    playlists = _parse_playlists(os.getenv("SPOTIFY_PLAYLIST_IDS"))

    return Settings(
        spotify_client_id=client_id,
        spotify_client_secret=client_secret,
        target=target,
        database_url=database_url,
        files_output_dir=Path(files_output),
        playlists=playlists,
    )


__all__ = ["Settings", "PlaylistTarget", "load_settings"]
