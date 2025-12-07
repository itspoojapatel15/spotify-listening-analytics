"""Tests for Spotify listening analytics logic."""

import pytest
from datetime import datetime, timezone


class TestSpotifyOAuth:
    """Test OAuth 2.0 PKCE flow logic."""

    def test_token_expiry_check(self):
        obtained_at = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        expires_in = 3600
        now = datetime(2024, 1, 1, 12, 30, 0, tzinfo=timezone.utc)
        elapsed = (now - obtained_at).total_seconds()
        is_expired = elapsed >= expires_in
        assert is_expired is False

    def test_token_expired(self):
        obtained_at = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        expires_in = 3600
        now = datetime(2024, 1, 1, 13, 5, 0, tzinfo=timezone.utc)
        elapsed = (now - obtained_at).total_seconds()
        is_expired = elapsed >= expires_in
        assert is_expired is True

    def test_pkce_code_verifier_length(self):
        import secrets
        verifier = secrets.token_urlsafe(32)
        assert 43 <= len(verifier) <= 128


class TestListeningHistory:
    """Test listening history extraction and processing."""

    def test_cursor_pagination(self):
        response = {
            "items": [{"track": {"id": "t1"}}, {"track": {"id": "t2"}}],
            "cursors": {"before": "1700000000000"},
            "next": "https://api.spotify.com/v1/me/player/recently-played?before=1700000000000",
        }
        has_next = response.get("next") is not None
        assert has_next is True
        assert len(response["items"]) == 2

    def test_no_more_pages(self):
        response = {"items": [], "cursors": None, "next": None}
        has_next = response.get("next") is not None
        assert has_next is False


class TestAudioFeatures:
    """Test audio feature extraction and mood classification."""

    def test_mood_classification_happy(self):
        valence, energy = 0.8, 0.7
        if valence >= 0.6 and energy >= 0.6:
            mood = "happy"
        elif valence >= 0.6 and energy < 0.6:
            mood = "peaceful"
        elif valence < 0.4 and energy >= 0.6:
            mood = "angry"
        elif valence < 0.4 and energy < 0.4:
            mood = "sad"
        else:
            mood = "neutral"
        assert mood == "happy"

    def test_mood_classification_sad(self):
        valence, energy = 0.2, 0.3
        if valence >= 0.6 and energy >= 0.6:
            mood = "happy"
        elif valence >= 0.6 and energy < 0.6:
            mood = "peaceful"
        elif valence < 0.4 and energy >= 0.6:
            mood = "angry"
        elif valence < 0.4 and energy < 0.4:
            mood = "sad"
        else:
            mood = "neutral"
        assert mood == "sad"

    def test_mood_classification_angry(self):
        valence, energy = 0.2, 0.8
        if valence < 0.4 and energy >= 0.6:
            mood = "angry"
        else:
            mood = "other"
        assert mood == "angry"

    def test_batch_audio_features(self):
        track_ids = [f"track_{i}" for i in range(250)]
        batch_size = 100
        batches = [track_ids[i:i + batch_size] for i in range(0, len(track_ids), batch_size)]
        assert len(batches) == 3
        assert len(batches[0]) == 100
        assert len(batches[2]) == 50


class TestSessionization:
    """Test listening session detection logic."""

    def test_session_gap_detection(self):
        timestamps = [
            datetime(2024, 1, 1, 10, 0),
            datetime(2024, 1, 1, 10, 4),
            datetime(2024, 1, 1, 10, 8),
            datetime(2024, 1, 1, 11, 0),  # 52 min gap → new session
            datetime(2024, 1, 1, 11, 4),
        ]
        gap_threshold_min = 30
        sessions = []
        current_session = [timestamps[0]]
        for i in range(1, len(timestamps)):
            gap = (timestamps[i] - timestamps[i - 1]).total_seconds() / 60
            if gap > gap_threshold_min:
                sessions.append(current_session)
                current_session = [timestamps[i]]
            else:
                current_session.append(timestamps[i])
        sessions.append(current_session)

        assert len(sessions) == 2
        assert len(sessions[0]) == 3
        assert len(sessions[1]) == 2

    def test_single_track_session(self):
        sessions = [[datetime(2024, 1, 1, 10, 0)]]
        assert len(sessions) == 1
        assert len(sessions[0]) == 1


class TestRateLimitHandling:
    """Test 429 Retry-After handling."""

    def test_retry_after_parsing(self):
        headers = {"Retry-After": "5"}
        wait_seconds = int(headers["Retry-After"])
        assert wait_seconds == 5

    def test_exponential_backoff(self):
        base = 1.0
        delays = [min(base * (2 ** i), 60) for i in range(5)]
        assert delays == [1.0, 2.0, 4.0, 8.0, 16.0]
