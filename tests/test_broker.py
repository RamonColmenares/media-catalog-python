from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from media_catalog.broker import handle_movie_message
from media_catalog.database import Database


def test_movie_queue_payload_is_persisted_with_wire_field_names(tmp_path) -> None:
    database = Database(tmp_path / "movies.sqlite3")
    database.initialize()
    saved = handle_movie_message(
        json.dumps(
            {
                "name": "Demo Queue Entry",
                "genre": "Documentary",
                "ulrStream": "https://media.example.invalid/demo-queue-entry",
            }
        ).encode(),
        database,
    )
    assert saved["id"] == 1
    assert saved["ulrStream"] == "https://media.example.invalid/demo-queue-entry"
    assert database.list_movies("Documentary") == [saved]


def test_invalid_movie_queue_payload_is_rejected(tmp_path) -> None:
    database = Database(tmp_path / "movies.sqlite3")
    database.initialize()
    with pytest.raises(ValidationError):
        handle_movie_message(b'{"name": [], "genre": "Demo"}', database)
    assert database.list_movies() == []
