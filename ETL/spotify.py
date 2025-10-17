"""Spotify API client utilities."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

import requests

LOGGER = logging.getLogger(__name__)

TOKEN_URL = "https://accounts.spotify.com/api/token"
API_BASE_URL = "https://api.spotify.com/v1"


class SpotifyClient:
    """Minimal Spotify Web API client using the Client Credentials flow."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        *,
        timeout: float = 15.0,
        session: Optional[requests.Session] = None,
    ) -> None:
        self._client_id = client_id
        self._client_secret = client_secret
        self._timeout = timeout
        self._session = session or requests.Session()

        self._token: Optional[str] = None
        self._token_expiry: float = 0.0

    def fetch_playlist(
        self, playlist_id: str, *, market: Optional[str] = None
    ) -> Dict[str, Any]:
        """Return playlist metadata and all track items for the given playlist ID."""
        params = {"market": market} if market else None
        playlist = self._request(
            "GET", f"{API_BASE_URL}/playlists/{playlist_id}", params=params
        )
        tracks = playlist.get("tracks", {}) or {}
        items = tracks.get("items", []) or []
        next_url = tracks.get("next")

        while next_url:
            response_data = self._request("GET", next_url)
            batch = response_data.get("items", [])
            LOGGER.debug(
                "Fetched %s playlist records for playlist %s",
                len(batch),
                playlist_id,
            )
            items.extend(batch)
            next_url = response_data.get("next")

        tracks["items"] = items
        playlist["tracks"] = tracks
        LOGGER.info(
            "Fetched playlist '%s' with %s track entries",
            playlist.get("name"),
            len(items),
        )
        return playlist

    # Internal helpers -----------------------------------------------------

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Perform an HTTP request with automatic token management."""
        self._ensure_token()

        headers = {"Authorization": f"Bearer {self._token}"}
        while True:
            response = self._session.request(
                method,
                url,
                params=params,
                data=data,
                headers=headers,
                timeout=self._timeout,
            )

            if response.status_code == 401:
                LOGGER.info("Refreshing Spotify token and retrying %s", url)
                self._invalidate_token()
                self._ensure_token()
                headers["Authorization"] = f"Bearer {self._token}"
                continue

            if response.status_code == 429:
                retry_after = float(response.headers.get("Retry-After", "1"))
                LOGGER.warning("Rate limited by Spotify. Sleeping for %ss", retry_after)
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            return response.json()

    def _ensure_token(self) -> None:
        if self._token and time.time() < (self._token_expiry - 30):
            return

        LOGGER.debug("Requesting Spotify client credentials token")
        response = self._session.post(
            TOKEN_URL,
            data={"grant_type": "client_credentials"},
            timeout=self._timeout,
            auth=(self._client_id, self._client_secret),
        )
        response.raise_for_status()
        payload = response.json()
        self._token = payload["access_token"]
        expires_in = int(payload.get("expires_in", 3600))
        self._token_expiry = time.time() + expires_in

    def _invalidate_token(self) -> None:
        self._token = None
        self._token_expiry = 0.0


__all__ = ["SpotifyClient"]
