from __future__ import annotations

import pytest

from media_catalog.catalog_repository import CatalogRepository
from media_catalog.database import Database
from media_catalog.errors import DomainError
from media_catalog.models import CollectionInput, ContentReference, LabelInput


def _item(kind: str, media_id: int, name: str, revision: int = 1) -> dict:
    return {
        "id": media_id,
        "kind": kind,
        "name": name,
        "synopsis": "A synthetic test item.",
        "genre": "Drama",
        "releaseYear": 2020,
        "language": "en",
        "externalId": None,
        "status": "draft",
        "version": revision,
        "sourceRevision": revision,
        "licenses": [],
        "assets": [],
    }


def _event(kind: str, sequence: int, revision: int, item: dict) -> dict:
    return {"kind": kind, "sequence": sequence, "revision": revision, "item": item, "eventType": "created"}


def test_change_cursor_advances_atomically_and_ignores_replayed_events(tmp_path) -> None:
    database = Database(tmp_path / "catalog.sqlite3", role="catalog")
    database.initialize()
    repository = CatalogRepository(database)
    first = _event("movie", 1, 1, _item("movie", 1, "First"))
    assert repository.apply_event_page("movie", [first], source_cursor=1, source_id="source-a")["cursor"] == 1
    replay = repository.apply_event_page("movie", [first], source_cursor=1, source_id="source-a")
    assert replay == {"events": 0, "changed": 0, "cursor": 1}

    with pytest.raises(DomainError) as error:
        repository.apply_event_page(
            "movie",
            [_event("movie", 3, 3, _item("movie", 3, "Skipped"))],
            source_cursor=3,
            source_id="source-a",
        )
    assert error.value.code == "source_feed_gap"
    assert repository.cursor_state()["movie"]["sequence"] == 1
    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM catalog_items WHERE media_id=3").fetchone()[0] == 0

    with pytest.raises(DomainError) as error:
        repository.apply_event_page(
            "movie",
            [_event("movie", 2, 2, _item("movie", 2, "Second")), _event("movie", 4, 4, _item("movie", 4, "Out of order"))],
            source_cursor=4,
            source_id="source-a",
        )
    assert error.value.code == "source_feed_gap"
    assert repository.cursor_state()["movie"]["sequence"] == 1


def test_snapshot_rebuild_preserves_catalog_labels_and_collections(tmp_path) -> None:
    database = Database(tmp_path / "catalog.sqlite3", role="catalog")
    database.initialize()
    repository = CatalogRepository(database)
    snapshots = {
        "movie": {"kind": "movie", "sourceId": "movie-source-a", "items": [_item("movie", 1, "Movie One", 8)], "highWaterMark": 8},
        "series": {"kind": "series", "sourceId": "series-source-a", "items": [_item("series", 1, "Series One")], "highWaterMark": 1},
    }
    assert repository.apply_snapshots(snapshots) == {"movie": 1, "series": 1}
    label = repository.create_label(LabelInput.model_validate({"name": "Research"}))
    repository.attach_label(label["id"], "movie", 1)
    collection = repository.create_collection(CollectionInput.model_validate({"name": "Both Kinds", "items": [{"kind": "movie", "id": 1}, {"kind": "series", "id": 1}]}))
    replacement = {
        "movie": {"kind": "movie", "sourceId": "movie-source-b", "items": [_item("movie", 1, "Replacement after reset", 1)], "highWaterMark": 0},
        "series": {"kind": "series", "sourceId": "series-source-a", "items": [], "highWaterMark": 1},
    }
    repository.apply_snapshots(replacement)
    assert repository.lookup("movie", 1)["name"] == "Replacement after reset"
    assert repository.cursor_state()["movie"]["sourceId"] == "movie-source-b"
    persisted_collection = repository.get_collection(collection["id"])
    assert len(persisted_collection["items"]) == 2
    assert persisted_collection["items"][1]["content"] is None
    quality_codes = {issue["code"] for issue in repository.quality_report()["issues"]}
    assert "dangling_label_reference" not in quality_codes
    assert persisted_collection["items"][0]["content"]["name"] == "Replacement after reset"


def test_source_identity_change_requires_explicit_rebuild(tmp_path) -> None:
    database = Database(tmp_path / "catalog.sqlite3", role="catalog")
    database.initialize()
    repository = CatalogRepository(database)
    repository.apply_event_page("movie", [_event("movie", 1, 1, _item("movie", 1, "Before reset"))], source_cursor=1, source_id="source-a")
    with pytest.raises(DomainError) as error:
        repository.apply_event_page("movie", [_event("movie", 1, 1, _item("movie", 1, "After reset"))], source_cursor=1, source_id="source-b")
    assert error.value.code == "source_reset_requires_rebuild"
    assert repository.lookup("movie", 1)["name"] == "Before reset"
    assert repository.cursor_state()["movie"]["sequence"] == 1
