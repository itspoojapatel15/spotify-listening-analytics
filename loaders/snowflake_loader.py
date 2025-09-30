"""Snowflake loader for Spotify data."""

import json
from datetime import datetime, timezone
from typing import Any

import snowflake.connector
import structlog

from config import get_settings

logger = structlog.get_logger(__name__)


class SnowflakeLoader:
    """Loads Spotify data into Snowflake raw tables as VARIANT JSON."""

    def __init__(self):
        settings = get_settings()
        self.conn_params = {
            "account": settings.snowflake.account,
            "user": settings.snowflake.user,
            "password": settings.snowflake.password,
            "warehouse": settings.snowflake.warehouse,
            "database": settings.snowflake.database,
            "role": settings.snowflake.role,
        }

    def _get_connection(self):
        return snowflake.connector.connect(**self.conn_params)

    def load_records(self, records: list[dict[str, Any]], schema: str, table: str) -> int:
        """Load records as JSON VARIANT into a raw table."""
        if not records:
            return 0

        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    raw_data VARIANT,
                    loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                    batch_id VARCHAR(100)
                )
            """)

            batch_id = f"{table}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
            sql = f"""
                INSERT INTO {schema}.{table} (raw_data, loaded_at, batch_id)
                SELECT PARSE_JSON(%s), CURRENT_TIMESTAMP(), %s
            """

            rows = [(json.dumps(r, default=str), batch_id) for r in records]
            cursor.executemany(sql, rows)
            conn.commit()

            logger.info("loaded_to_snowflake", schema=schema, table=table, rows=len(records))
            return len(records)
        finally:
            conn.close()

    def load_listening_history(self, records: list[dict]) -> int:
        return self.load_records(records, "RAW_SPOTIFY", "LISTENING_HISTORY")

    def load_tracks(self, records: list[dict]) -> int:
        return self.load_records(records, "RAW_SPOTIFY", "TRACKS")

    def load_audio_features(self, records: list[dict]) -> int:
        return self.load_records(records, "RAW_SPOTIFY", "AUDIO_FEATURES")

    def load_playlists(self, records: list[dict]) -> int:
        return self.load_records(records, "RAW_SPOTIFY", "PLAYLISTS")

    def load_artists(self, records: list[dict]) -> int:
        return self.load_records(records, "RAW_SPOTIFY", "ARTISTS")
