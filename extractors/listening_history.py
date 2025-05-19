"""Listening history extractor with cursor-based pagination."""

from datetime import datetime, timezone
from typing import Any

import structlog

from .spotify_client import SpotifyClient

logger = structlog.get_logger(__name__)


class ListeningHistoryExtractor:
    """Extract recently played tracks using cursor-based pagination."""

    def __init__(self, client: SpotifyClient | None = None):
        self.client = client or SpotifyClient()
        self.extracted_at = datetime.now(timezone.utc).isoformat()

    def extract_recent_plays(self, max_items: int = 500) -> list[dict[str, Any]]:
        """Extract recently played tracks with cursor pagination.

        Spotify's recently-played endpoint returns max 50 at a time
        with 'before' cursor for pagination.

        Args:
            max_items: Maximum total items to extract

        Returns:
            List of listening event dictionaries
        """
        all_plays = []
        before_cursor = None

        while len(all_plays) < max_items:
            data = self.client.get_recently_played(limit=50, before=before_cursor)
            items = data.get("items", [])

            if not items:
                break

            for item in items:
                track = item.get("track", {})
                artists = track.get("artists", [])

                play = {
                    "played_at": item["played_at"],
                    "track_id": track.get("id"),
                    "track_name": track.get("name"),
                    "track_uri": track.get("uri"),
                    "duration_ms": track.get("duration_ms"),
                    "explicit": track.get("explicit", False),
                    "popularity": track.get("popularity", 0),
                    "album_id": track.get("album", {}).get("id"),
                    "album_name": track.get("album", {}).get("name"),
                    "album_release_date": track.get("album", {}).get("release_date"),
                    "artist_id": artists[0]["id"] if artists else None,
                    "artist_name": artists[0]["name"] if artists else None,
                    "artist_ids": [a["id"] for a in artists],
                    "artist_names": [a["name"] for a in artists],
                    "preview_url": track.get("preview_url"),
                    "extracted_at": self.extracted_at,
                }
                all_plays.append(play)

            # Get next cursor
            cursors = data.get("cursors", {})
            before_cursor = cursors.get("before")
            if not before_cursor:
                break

            logger.info("pagination_progress", collected=len(all_plays), cursor=before_cursor)

        logger.info("listening_history_extracted", total_plays=len(all_plays))
        return all_plays

    def extract_unique_track_ids(self, plays: list[dict]) -> list[str]:
        """Get unique track IDs from plays for audio feature extraction."""
        return list({p["track_id"] for p in plays if p.get("track_id")})

    def extract_unique_artist_ids(self, plays: list[dict]) -> list[str]:
        """Get unique artist IDs from plays."""
        artist_ids = set()
        for p in plays:
            if p.get("artist_ids"):
                artist_ids.update(p["artist_ids"])
        return list(artist_ids)
