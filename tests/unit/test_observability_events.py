"""Phase 16: unit tests for the canonical event schema and redaction."""

from __future__ import annotations

import pytest

from mri.observability.events import FIELDS, PipelineEvent, redact


def _event(**overrides: object) -> PipelineEvent:
    kwargs: dict[str, object] = {
        "timestamp": "2026-10-09T12:00:00.000000Z",
        "run_id": "20261009T120000Z-abcdef01",
        "pipeline": "ingestion",
        "stage": "validate",
        "status": "success",
        "dag_run_id": "dag__1",
        "source": "sales",
        "batch_id": "sales",
        "duration_ms": 12,
        "records_read": 10,
        "message": "ok",
    }
    kwargs.update(overrides)
    return PipelineEvent(**kwargs)  # type: ignore[arg-type]


def test_fields_is_fixed_and_complete() -> None:
    assert isinstance(FIELDS, tuple)
    assert "timestamp" in FIELDS
    assert "run_id" in FIELDS
    assert "dag_run_id" in FIELDS
    assert "payload" in FIELDS
    # no accidental renames between the log and downstream consumers
    assert len(FIELDS) == len(set(FIELDS))


def test_as_dict_preserves_canonical_key_order() -> None:
    data = _event().as_dict()
    assert list(data.keys()) == list(FIELDS)


def test_as_dict_redacts_credentials_nested_in_payload(caplog: pytest.LogCaptureFixture) -> None:
    event = _event(
        payload={
            "conn": {"user": "loader", "password": "hunter2", "api_key": "abc"},
            "token": "xyz",
            "safe": 1,
        }
    )
    data = event.as_dict()
    assert data["payload"]["conn"]["password"] == "***"
    assert data["payload"]["conn"]["api_key"] == "***"
    assert data["payload"]["token"] == "***"
    assert data["payload"]["conn"]["user"] == "loader"
    assert data["payload"]["safe"] == 1


def test_redact_value_hits() -> None:
    assert redact({"secret": "s", "kind": "ok"})["secret"] == "***"
    assert redact({"access_key": "k"}) == {"access_key": "***"}


def test_to_json_is_one_compact_line() -> None:
    line = _event().to_json()
    assert "\n" not in line
    assert "hunter2" not in line


def test_event_hash_is_stable_and_unique() -> None:
    first = _event()
    twin = PipelineEvent.from_dict(first.as_dict())
    assert first.event_hash() == twin.event_hash()
    other = _event(message="different")
    assert first.event_hash() != other.event_hash()


def test_from_dict_round_trip() -> None:
    event = _event(payload={"partition": "year=2026"})
    restored = PipelineEvent.from_dict(event.as_dict())
    assert restored == event


def test_from_dict_tolerates_missing_and_odd_values() -> None:
    restored = PipelineEvent.from_dict({"run_id": "r1", "pipeline": "x"})
    assert restored.run_id == "r1"
    assert restored.pipeline == "x"
    assert restored.status == ""
    assert restored.timestamp  # fallback to now
    odd = PipelineEvent.from_dict({"run_id": 42, "payload": ["not", "a", "dict"]})
    assert odd.run_id == "42"
    assert odd.payload == {}


def test_from_dict_tolerates_numeric_counts() -> None:
    event = PipelineEvent.from_dict(
        {"records_read": "2000", "records_rejected": "3", "duration_ms": 15}
    )
    assert event.records_read == 2000
    assert event.records_rejected == 3
    assert event.duration_ms == 15


def test_from_json_line_round_trip() -> None:
    event = _event()
    restored = PipelineEvent.from_json_line(event.to_json())
    assert restored == event
