"""Playlist and playlist tracks extractor."""

from datetime import datetime, timezone
from typing import Any

import structlog

from .spotify_client import SpotifyClient

logger = structlog.get_logger(__name__)


class PlaylistExtractor:
    """Extract user playlists and their tracks."""

    def __init__(self, client: SpotifyClient | None = None):
        self.client = client or SpotifyClient()
        self.extracted_at = datetime.now(timezone.utc).isoformat()

    def extract_playlists(self) -> list[dict[str, Any]]:
        """Extract all user playlists with pagination."""
        playlists = []
        offset = 0
        limit = 50

        while True:
            data = self.client.get_current_user_playlists(limit=limit, offset=offset)
            items = data.get("items", [])
            if not items:
                break

            for item in items:
                playlists.append({
                    "playlist_id": item["id"],
                    "name": item["name"],
                    "description": item.get("description", ""),
                    "owner_id": item["owner"]["id"],
                    "owner_name": item["owner"].get("display_name", ""),
                    "public": item.get("public", False),
                    "collaborative": item.get("collaborative", False),
                    "total_tracks": item["tracks"]["total"],
                    "snapshot_id": item.get("snapshot_id"),
                    "extracted_at": self.extracted_at,
                })

            offset += limit
            if offset >= data.get("total", 0):
                break

        logger.info("playlists_extracted", count=len(playlists))
        return playlists

    def extract_playlist_tracks(self, playlist_id: str) -> list[dict[str, Any]]:
        """Extract all tracks from a playlist."""
        tracks = []
        offset = 0

        while True:
            data = self.client.get_playlist_tracks(playlist_id, limit=100, offset=offset)
            items = data.get("items", [])
            if not items:
                break

            for i, item in enumerate(items):
                track = item.get("track")
                if not track or not track.get("id"):
                    continue
                artists = track.get("artists", [])

                tracks.append({
                    "playlist_id": playlist_id,
                    "track_id": track["id"],
                    "track_name": track["name"],
                    "artist_id": artists[0]["id"] if artists else None,
                    "artist_name": artists[0]["name"] if artists else None,
                    "album_name": track.get("album", {}).get("name"),
                    "duration_ms": track.get("duration_ms"),
                    "added_at": item.get("added_at"),
                    "added_by": item.get("added_by", {}).get("id"),
                    "position": offset + i,
                    "extracted_at": self.extracted_at,
                })

            offset += 100
            if offset >= data.get("total", 0):
                break

        return tracks
