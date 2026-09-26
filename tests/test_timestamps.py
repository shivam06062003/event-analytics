from datetime import UTC, datetime, timedelta

from app.services.ingest import corrected_timestamp

RECEIVED = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
TOLERANCE = timedelta(seconds=60)


def test_missing_timestamp_uses_receive_time() -> None:
    assert corrected_timestamp(None, None, RECEIVED, TOLERANCE) == RECEIVED


def test_fast_client_clock_is_corrected_using_sent_at() -> None:
    # Clock 2 hours ahead; event 30s before sending.
    sent_at = RECEIVED + timedelta(hours=2)
    event_ts = sent_at - timedelta(seconds=30)

    assert corrected_timestamp(event_ts, sent_at, RECEIVED, TOLERANCE) == RECEIVED - timedelta(
        seconds=30
    )


def test_without_sent_at_the_client_timestamp_is_trusted() -> None:
    event_ts = RECEIVED - timedelta(minutes=3)

    assert corrected_timestamp(event_ts, None, RECEIVED, TOLERANCE) == event_ts


def test_impossible_future_timestamps_are_clamped() -> None:
    future = RECEIVED + timedelta(days=1)

    assert corrected_timestamp(future, None, RECEIVED, TOLERANCE) == RECEIVED


def test_small_future_skew_within_tolerance_is_kept() -> None:
    slightly_ahead = RECEIVED + timedelta(seconds=10)

    assert corrected_timestamp(slightly_ahead, None, RECEIVED, TOLERANCE) == slightly_ahead
