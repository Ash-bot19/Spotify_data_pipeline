import os, time
import requests
from tenacity import retry, wait_exponential, stop_after_attempt

from .config import require_env

TOKEN_URL = "https://accounts.spotify.com/api/token"
BASE_URL  = "https://api.spotify.com/v1"

class SpotifyClient:
    def __init__(self, client_id=None, client_secret=None):
        self.client_id = client_id or os.environ.get("SPOTIFY_CLIENT_ID")
        self.client_secret = client_secret or os.environ.get("SPOTIFY_CLIENT_SECRET")
        if not self.client_id:
            self.client_id = require_env("SPOTIFY_CLIENT_ID")
        if not self.client_secret:
            self.client_secret = require_env("SPOTIFY_CLIENT_SECRET")
        self.token = None
        self.token_expiry = 0

    def _refresh_token(self):
        resp = requests.post(
            TOKEN_URL,
            data={"grant_type":"client_credentials"},
            auth=(self.client_id, self.client_secret),
            timeout=20
        )
        resp.raise_for_status()
        data = resp.json()
        self.token = data["access_token"]
        self.token_expiry = time.time() + data["expires_in"] - 30

    def _headers(self):
        if not self.token or time.time() > self.token_expiry:
            self._refresh_token()
        return {"Authorization": f"Bearer {self.token}"}

    @retry(wait=wait_exponential(multiplier=1, min=1, max=30), stop=stop_after_attempt(5))
    def get(self, path, params=None):
        r = requests.get(f"{BASE_URL}{path}", headers=self._headers(), params=params or {}, timeout=30)
        if r.status_code == 429:  # rate limited
            retry_after = int(r.headers.get("Retry-After","1"))
            time.sleep(retry_after + 1)
            raise Exception("Rate limited, retrying")
        r.raise_for_status()
        return r.json()
