from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from media_catalog.broker import handle_movie_message, process_movie_delivery
from media_catalog.database import Database


def _message(name: str = "Synthetic Queue Entry") -> bytes:
    return json.dumps({"name": name, "genre": "Documentary", "synopsis": "Generated queue fixture.", "streamUrl": "https://media.example.invalid/queue-entry"}).encode()


def test_movie_message_uses_strict_schema_and_persistent_idempotency(tmp_path) -> None:
    database = Database(tmp_path / "movies.sqlite3", role="movie")
    database.initialize()
    saved = handle_movie_message(_message(), database, "rabbitmq:stable-message-id")
    assert saved["id"] == 1
    assert saved["streamUrl"] == "https://media.example.invalid/queue-entry"
    replay = handle_movie_message(_message(), database, "rabbitmq:stable-message-id")
    assert replay["id"] == 1
    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM movies").fetchone()[0] == 1


def test_invalid_movie_queue_payload_is_rejected_before_write(tmp_path) -> None:
    database = Database(tmp_path / "movies.sqlite3", role="movie")
    database.initialize()
    with pytest.raises(ValidationError):
        handle_movie_message(b'{"name": [], "genre": "Demo"}', database)
    with pytest.raises(ValidationError):
        handle_movie_message(b'{"name": "Old spelling", "ulrStream": "https://example.invalid"}', database)
    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM movies").fetchone()[0] == 0


class RecordingChannel:
    def __init__(self) -> None:
        self.acknowledged: list[int] = []
        self.rejected: list[tuple[int, bool]] = []

    def basic_ack(self, *, delivery_tag: int) -> None:
        self.acknowledged.append(delivery_tag)

    def basic_nack(self, *, delivery_tag: int, requeue: bool) -> None:
        self.rejected.append((delivery_tag, requeue))


def test_callback_acknowledges_after_commit_and_rejects_invalid_messages(tmp_path) -> None:
    database = Database(tmp_path / "movies.sqlite3", role="movie")
    database.initialize()
    channel = RecordingChannel()
    method = SimpleNamespace(delivery_tag=42)
    properties = SimpleNamespace(message_id="delivery-42")
    process_movie_delivery(channel, method, properties, _message(), database)
    assert channel.acknowledged == [42] and channel.rejected == []
    process_movie_delivery(channel, method, properties, _message("Changed body"), database)
    assert channel.rejected == [(42, False)]
    process_movie_delivery(channel, method, SimpleNamespace(message_id="bad-json"), b"{", database)
    assert channel.rejected[-1] == (42, False)
    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM movies").fetchone()[0] == 1


def test_callback_requeues_transient_database_errors(tmp_path, monkeypatch) -> None:
    database = Database(tmp_path / "movies.sqlite3", role="movie")
    database.initialize()

    @contextmanager
    def unavailable_transaction(*args, **kwargs):
        raise sqlite3.OperationalError("temporary database lock")
        yield

    monkeypatch.setattr(database, "transaction", unavailable_transaction)
    channel = RecordingChannel()
    process_movie_delivery(channel, SimpleNamespace(delivery_tag=9), SimpleNamespace(message_id="retry-me"), _message(), database)
    assert channel.acknowledged == []
    assert channel.rejected == [(9, True)]
