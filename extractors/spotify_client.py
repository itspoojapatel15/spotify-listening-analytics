"""Spotify OAuth 2.0 client with PKCE and token refresh."""

import base64
import hashlib
import json
import os
import secrets
import webbrowser
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any
from urllib.parse import urlencode, urlparse, parse_qs

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import get_settings

logger = structlog.get_logger(__name__)

TOKEN_FILE = ".spotify_token.json"
AUTH_URL = "https://accounts.spotify.com/authorize"
TOKEN_URL = "https://accounts.spotify.com/api/token"
API_BASE = "https://api.spotify.com/v1"

SCOPES = [
    "user-read-recently-played",
    "user-read-playback-state",
    "user-top-read",
    "playlist-read-private",
    "playlist-read-collaborative",
    "user-library-read",
]


class SpotifyClient:
    """Full Spotify API client with OAuth 2.0 PKCE, token refresh, and rate limiting."""

    def __init__(self):
        settings = get_settings()
        self._client_id = settings.spotify.client_id
        self._client_secret = settings.spotify.client_secret
        self._redirect_uri = settings.spotify.redirect_uri
        self._token_data: dict | None = None
        self._load_token()

    # === TOKEN MANAGEMENT ===

    def _load_token(self) -> None:
        """Load token from disk."""
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE) as f:
                self._token_data = json.load(f)
            logger.info("token_loaded")

    def _save_token(self, token_data: dict) -> None:
        """Save token to disk."""
        token_data["obtained_at"] = datetime.now(timezone.utc).isoformat()
        self._token_data = token_data
        with open(TOKEN_FILE, "w") as f:
            json.dump(token_data, f, indent=2)
        logger.info("token_saved")

    @property
    def _is_token_expired(self) -> bool:
        """Check if current token is expired."""
        if not self._token_data:
            return True
        obtained = datetime.fromisoformat(self._token_data.get("obtained_at", "2000-01-01"))
        expires_in = self._token_data.get("expires_in", 3600)
        return datetime.now(timezone.utc) > obtained + timedelta(seconds=expires_in - 60)

    @property
    def access_token(self) -> str:
        """Get valid access token, refreshing if needed."""
        if self._is_token_expired:
            self._refresh_token()
        return self._token_data["access_token"]

    def _refresh_token(self) -> None:
        """Refresh the access token."""
        if not self._token_data or "refresh_token" not in self._token_data:
            raise RuntimeError("No refresh token available. Run --auth first.")

        logger.info("refreshing_token")
        resp = httpx.post(
            TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": self._token_data["refresh_token"],
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
        )
        resp.raise_for_status()
        new_data = resp.json()

        # Preserve refresh token if not returned
        if "refresh_token" not in new_data:
            new_data["refresh_token"] = self._token_data["refresh_token"]

        self._save_token(new_data)
        logger.info("token_refreshed")

    # === OAUTH FLOW ===

    def authorize(self) -> None:
        """Run full OAuth 2.0 authorization code flow with PKCE."""
        # Generate PKCE challenge
        code_verifier = secrets.token_urlsafe(64)
        code_challenge = base64.urlsafe_b64encode(
            hashlib.sha256(code_verifier.encode()).digest()
        ).rstrip(b"=").decode()
        state = secrets.token_urlsafe(16)

        params = {
            "client_id": self._client_id,
            "response_type": "code",
            "redirect_uri": self._redirect_uri,
            "scope": " ".join(SCOPES),
            "state": state,
            "code_challenge_method": "S256",
            "code_challenge": code_challenge,
        }

        auth_url = f"{AUTH_URL}?{urlencode(params)}"
        print(f"\nOpening browser for Spotify authorization...\n{auth_url}\n")
        webbrowser.open(auth_url)

        # Start local server to receive callback
        auth_code = self._wait_for_callback(state)

        # Exchange code for token
        resp = httpx.post(
            TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": auth_code,
                "redirect_uri": self._redirect_uri,
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "code_verifier": code_verifier,
            },
        )
        resp.raise_for_status()
        self._save_token(resp.json())
        print("Authorization successful! Token saved.")

    def _wait_for_callback(self, expected_state: str) -> str:
        """Wait for OAuth callback on local server."""
        auth_code = None

        class CallbackHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                nonlocal auth_code
                query = parse_qs(urlparse(self.path).query)
                if query.get("state", [None])[0] != expected_state:
                    self.send_response(400)
                    self.end_headers()
                    self.wfile.write(b"State mismatch")
                    return
                auth_code = query.get("code", [None])[0]
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"<h1>Success! You can close this window.</h1>")

            def log_message(self, format, *args):
                pass

        parsed = urlparse(self._redirect_uri)
        server = HTTPServer((parsed.hostname or "localhost", parsed.port or 8888), CallbackHandler)
        server.handle_request()
        server.server_close()

        if not auth_code:
            raise RuntimeError("Failed to receive authorization code")
        return auth_code

    # === API REQUESTS ===

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=30),
        retry=retry_if_exception_type(httpx.HTTPStatusError),
    )
    def _get(self, endpoint: str, params: dict | None = None) -> dict[str, Any]:
        """Make authenticated GET request with retry and rate limit handling."""
        resp = httpx.get(
            f"{API_BASE}{endpoint}",
            params=params,
            headers={"Authorization": f"Bearer {self.access_token}"},
            timeout=30,
        )

        # Handle rate limiting (429)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 5))
            logger.warning("rate_limited", retry_after=retry_after)
            import time
            time.sleep(retry_after)
            resp = httpx.get(
                f"{API_BASE}{endpoint}",
                params=params,
                headers={"Authorization": f"Bearer {self.access_token}"},
                timeout=30,
            )

        resp.raise_for_status()
        return resp.json()

    def get_recently_played(self, limit: int = 50, before: int | None = None, after: int | None = None) -> dict:
        """Get recently played tracks."""
        params = {"limit": min(limit, 50)}
        if before:
            params["before"] = before
        if after:
            params["after"] = after
        return self._get("/me/player/recently-played", params)

    def get_audio_features(self, track_ids: list[str]) -> list[dict]:
        """Get audio features for multiple tracks (max 100)."""
        resp = self._get("/audio-features", {"ids": ",".join(track_ids[:100])})
        return [f for f in resp.get("audio_features", []) if f is not None]

    def get_current_user_playlists(self, limit: int = 50, offset: int = 0) -> dict:
        """Get current user's playlists."""
        return self._get("/me/playlists", {"limit": limit, "offset": offset})

    def get_playlist_tracks(self, playlist_id: str, limit: int = 100, offset: int = 0) -> dict:
        """Get tracks from a playlist."""
        return self._get(f"/playlists/{playlist_id}/tracks", {"limit": limit, "offset": offset})

    def get_top_artists(self, time_range: str = "medium_term", limit: int = 50) -> dict:
        """Get user's top artists."""
        return self._get("/me/top/artists", {"time_range": time_range, "limit": limit})

    def get_top_tracks(self, time_range: str = "medium_term", limit: int = 50) -> dict:
        """Get user's top tracks."""
        return self._get("/me/top/tracks", {"time_range": time_range, "limit": limit})


if __name__ == "__main__":
    import sys
    client = SpotifyClient()
    if "--auth" in sys.argv:
        client.authorize()
    else:
        print("Usage: python spotify_client.py --auth")
