import uuid
from datetime import UTC, datetime

import orjson

from app.processor.transform import DeadLetter, EventRow, parse


def message(**overrides: object) -> bytes:
    now = datetime.now(UTC).isoformat()
    body: dict[str, object] = {
        "schema_version": 1,
        "project_id": str(uuid.uuid4()),
        "event_id": str(uuid.uuid4()),
        "event": "signup",
        "distinct_id": "u-1",
        "user_id": "u-1",
        "anonymous_id": None,
        "timestamp": now,
        "client_timestamp": None,
        "sent_at": None,
        "received_at": now,
        "ip": "10.0.0.1",
        "properties": {"plan": "pro"},
        "context": {},
    }
    body.update(overrides)
    return orjson.dumps(body)


def test_valid_message_becomes_a_row_with_kafka_lineage() -> None:
    row = parse(message(), b"k", partition=2, offset=41)

    assert isinstance(row, EventRow)
    assert (row.event, row.distinct_id, row.kafka_partition, row.kafka_offset) == (
        "signup",
        "u-1",
        2,
        41,
    )
    assert orjson.loads(row.properties) == {"plan": "pro"}


def test_garbage_becomes_a_dead_letter_instead_of_crashing() -> None:
    letter = parse(b"\x00not json", None, 0, 7)

    assert isinstance(letter, DeadLetter)
    assert (letter.reason, letter.offset) == ("invalid_json", 7)


def test_unknown_schema_version_is_dead_lettered() -> None:
    letter = parse(message(schema_version=99), None, 0, 0)

    assert isinstance(letter, DeadLetter)
    assert letter.reason == "unsupported_schema_version:99"


def test_invalid_field_is_dead_lettered_with_the_reason() -> None:
    letter = parse(message(event_id="not-a-uuid"), None, 0, 0)

    assert isinstance(letter, DeadLetter)
    assert letter.reason.startswith("invalid_message:event_id:")


def test_json_that_is_not_an_object_is_dead_lettered() -> None:
    letter = parse(b"[1, 2, 3]", None, 0, 0)

    assert isinstance(letter, DeadLetter)
    assert letter.reason == "not_an_object"
