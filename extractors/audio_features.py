"""Audio features batch extractor."""

from datetime import datetime, timezone
from typing import Any

import structlog

from .spotify_client import SpotifyClient

logger = structlog.get_logger(__name__)


class AudioFeaturesExtractor:
    """Extract audio features for tracks in batches of 100."""

    def __init__(self, client: SpotifyClient | None = None):
        self.client = client or SpotifyClient()
        self.extracted_at = datetime.now(timezone.utc).isoformat()

    def extract_features(self, track_ids: list[str]) -> list[dict[str, Any]]:
        """Extract audio features for a list of track IDs.

        Spotify allows max 100 IDs per request. This method handles batching.
        """
        all_features = []

        for i in range(0, len(track_ids), 100):
            batch = track_ids[i : i + 100]
            features = self.client.get_audio_features(batch)

            for f in features:
                all_features.append({
                    "track_id": f["id"],
                    "danceability": f["danceability"],
                    "energy": f["energy"],
                    "key": f["key"],
                    "loudness": f["loudness"],
                    "mode": f["mode"],
                    "speechiness": f["speechiness"],
                    "acousticness": f["acousticness"],
                    "instrumentalness": f["instrumentalness"],
                    "liveness": f["liveness"],
                    "valence": f["valence"],
                    "tempo": f["tempo"],
                    "duration_ms": f["duration_ms"],
                    "time_signature": f["time_signature"],
                    "extracted_at": self.extracted_at,
                })

            logger.info("audio_features_batch", batch_num=i // 100 + 1, count=len(features))

        logger.info("audio_features_complete", total=len(all_features))
        return all_features
