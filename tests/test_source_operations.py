from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from media_catalog.database import Database
from media_catalog.movie_service import create_app as create_movie_app
from media_catalog.series_service import create_app as create_series_app


def _window(starts: datetime, ends: datetime, region: str = "WORLD") -> dict[str, str]:
    return {
        "region": region,
        "startsAt": starts.isoformat(),
        "endsAt": ends.isoformat(),
        "rightsHolder": "DUMMY Rights Holder",
    }


def test_movie_validation_versioned_lifecycle_rights_and_audit(tmp_path) -> None:
    client = TestClient(create_movie_app(tmp_path / "movie.sqlite3"))
    created = client.post(
        "/movies",
        json={"name": "Night Train", "synopsis": "A mystery on rails.", "streamUrl": "https://media.example.invalid/night-train"},
        headers={"Idempotency-Key": "movie-create-0001"},
    )
    assert created.status_code == 201
    movie = created.json()
    assert movie["streamUrl"].endswith("night-train")
    assert "ulrStream" not in movie and "urlStream" not in movie

    replay = client.post(
        "/movies",
        json={"name": "Night Train", "synopsis": "A mystery on rails.", "streamUrl": "https://media.example.invalid/night-train"},
        headers={"Idempotency-Key": "movie-create-0001"},
    )
    assert replay.json()["id"] == movie["id"]
    mismatch = client.post("/movies", json={"name": "Other"}, headers={"Idempotency-Key": "movie-create-0001"})
    assert mismatch.status_code == 409 and mismatch.json()["error"]["code"] == "idempotency_conflict"

    bad = client.post("/movies", json={"name": "Known", "unexpected": True})
    assert bad.status_code == 422 and bad.json()["error"]["code"] == "invalid_request"
    bad_url = client.post("/movies", json={"name": "Unsafe", "streamUrl": "file:///tmp/video"})
    assert bad_url.status_code == 422
    assert client.post("/movies/1/transition", json={"action": "publish", "expectedVersion": 1}).status_code == 409

    now = datetime.now(timezone.utc)
    first_start = now - timedelta(minutes=2)
    boundary = now + timedelta(hours=1)
    later_end = now + timedelta(hours=2)
    right = client.post("/movies/1/licenses", json=_window(first_start, boundary))
    assert right.status_code == 201
    adjacent = client.post("/movies/1/licenses", json=_window(boundary, later_end))
    assert adjacent.status_code == 201
    overlap = client.post("/movies/1/licenses", json=_window(now, later_end))
    assert overlap.status_code == 409 and overlap.json()["error"]["code"] == "license_overlap"

    readiness = client.get("/movies/1/readiness").json()
    assert readiness["ready"] is True
    published = client.post("/movies/1/transition", json={"action": "publish", "expectedVersion": readiness["version"]})
    assert published.status_code == 200 and published.json()["status"] == "published"
    remove_sole_active_right = client.delete(f"/movies/1/licenses/{right.json()['id']}")
    assert remove_sole_active_right.status_code == 409
    assert remove_sole_active_right.json()["error"]["code"] == "published_content_not_ready"
    assert len(client.get("/movies/1/licenses").json()["items"]) == 2
    published_version = published.json()["version"]
    unready_edit = client.put("/movies/1", json={
        "name": "Night Train", "synopsis": "", "genre": "Uncategorized", "language": "und",
        "streamUrl": None, "runtimeMinutes": 98, "expectedVersion": published_version,
    })
    assert unready_edit.status_code == 409
    assert unready_edit.json()["error"]["code"] == "published_content_not_ready"
    assert client.get("/movies/1").json()["status"] == "published"
    assert client.put("/movies/1", json={
        "name": "Night Train 2", "synopsis": "Updated.", "genre": "Uncategorized", "language": "und",
        "streamUrl": "https://media.example.invalid/night-train", "runtimeMinutes": 98,
        "expectedVersion": 1,
    }).status_code == 409
    at_start = client.get("/movies/1/availability", params={"region": "CO", "at": first_start.isoformat()}).json()
    assert at_start["available"] is True
    at_end = client.get("/movies/1/availability", params={"region": "CO", "at": later_end.isoformat()}).json()
    assert at_end["available"] is False
    audit = client.get("/movies/1/audit").json()["items"]
    assert [item["action"] for item in audit] == ["created", "license_added", "license_added", "publish"]
    unpublished = client.post("/movies/1/transition", json={"action": "unpublish", "expectedVersion": published.json()["version"]})
    assert unpublished.status_code == 200 and unpublished.json()["status"] == "draft"
    archived = client.post("/movies/1/transition", json={"action": "archive", "expectedVersion": unpublished.json()["version"]})
    assert archived.status_code == 200 and archived.json()["status"] == "archived"
    assert client.post("/movies/1/transition", json={"action": "publish", "expectedVersion": archived.json()["version"]}).status_code == 409
    archived_edit = client.put("/movies/1", json={
        "name": "Night Train", "synopsis": "Still unchanged.", "genre": "Uncategorized", "language": "und",
        "streamUrl": "https://media.example.invalid/night-train", "runtimeMinutes": 98,
        "expectedVersion": archived.json()["version"],
    })
    assert archived_edit.status_code == 409
    restored = client.post("/movies/1/transition", json={"action": "restore", "expectedVersion": archived.json()["version"]})
    assert restored.status_code == 200 and restored.json()["status"] == "draft"
    audit_after_restore = client.get("/movies/1/audit").json()["items"]
    assert [entry["action"] for entry in audit_after_restore[-3:]] == ["unpublish", "archive", "restore"]
    assert audit[-1]["actor"] == "local-operator"


def test_series_stable_hierarchy_and_atomic_reorder(tmp_path) -> None:
    client = TestClient(create_series_app(tmp_path / "series.sqlite3"))
    created = client.post(
        "/series",
        json={
            "name": "DUMMY Expedition",
            "synopsis": "An episodic demo.",
            "seasons": [{
                "seasonNumber": 1,
                "episodes": [
                    {"name": "Opening", "episodeNumber": 1, "runtimeMinutes": 20, "streamUrl": "https://media.example.invalid/e1"},
                    {"name": "Detour", "episodeNumber": 3, "runtimeMinutes": 21, "streamUrl": "https://media.example.invalid/e3"},
                ],
            }],
        },
    )
    assert created.status_code == 201
    series = created.json()
    season = series["seasons"][0]
    ids = [episode["id"] for episode in season["episodes"]]
    report = client.get("/series/1/structure-report").json()
    assert report["numberingGaps"] == [{"seasonNumber": 1, "missingEpisodeNumbers": [2]}]
    assert report["knownRuntimeMinutes"] == 41

    invalid_reorder = client.put(f"/series/1/seasons/{season['id']}/reorder", json={"episodeIds": [ids[0]]})
    assert invalid_reorder.status_code == 422
    assert [episode["id"] for episode in client.get("/series/1").json()["seasons"][0]["episodes"]] == ids
    reordered = client.put(f"/series/1/seasons/{season['id']}/reorder", json={"episodeIds": list(reversed(ids))})
    assert reordered.status_code == 200
    after = client.get("/series/1").json()["seasons"][0]["episodes"]
    assert [episode["id"] for episode in after] == list(reversed(ids))

    duplicated_numbers = client.post("/series", json={"name": "Bad", "seasons": [{"seasonNumber": 1}, {"seasonNumber": 1}]})
    assert duplicated_numbers.status_code == 422
    supplied_ids = client.post("/series", json={"name": "Client IDs", "seasons": [{"id": 100, "seasonNumber": 1}]})
    assert supplied_ids.status_code == 422


def test_series_individual_hierarchy_mutations_are_versioned_and_audited(tmp_path) -> None:
    client = TestClient(create_series_app(tmp_path / "individual-series.sqlite3"))
    parent = client.post("/series", json={"name": "DUMMY Atlas", "synopsis": "Operations fixture."}).json()
    season = client.post("/series/1/seasons", params={"expectedVersion": 1}, json={"seasonNumber": 1, "episodes": []})
    assert season.status_code == 201
    season_id = season.json()["seasons"][0]["id"]
    duplicate_season = client.post("/series/1/seasons", params={"expectedVersion": 2}, json={"seasonNumber": 1, "episodes": []})
    assert duplicate_season.status_code == 409

    episode = client.post("/series/1/seasons/1/episodes", params={"expectedVersion": 2}, json={"name": "First", "episodeNumber": 1, "runtimeMinutes": 10})
    assert episode.status_code == 201
    episode_id = episode.json()["seasons"][0]["episodes"][0]["id"]
    duplicate_episode = client.post("/series/1/seasons/1/episodes", params={"expectedVersion": 3}, json={"name": "Duplicate", "episodeNumber": 1})
    assert duplicate_episode.status_code == 409

    revised = client.put(
        f"/series/1/seasons/{season_id}/episodes/{episode_id}",
        params={"expectedVersion": 3},
        json={"id": episode_id, "name": "First Revised", "episodeNumber": 2, "runtimeMinutes": 12, "streamUrl": "https://media.example.invalid/first"},
    )
    assert revised.status_code == 200
    assert revised.json()["seasons"][0]["episodes"][0]["id"] == episode_id
    stale = client.delete(f"/series/1/seasons/{season_id}/episodes/{episode_id}", params={"expectedVersion": 3})
    assert stale.status_code == 409
    removed_episode = client.delete(f"/series/1/seasons/{season_id}/episodes/{episode_id}", params={"expectedVersion": 4})
    assert removed_episode.status_code == 200 and removed_episode.json()["seasons"][0]["episodes"] == []
    removed_season = client.delete(f"/series/1/seasons/{season_id}", params={"expectedVersion": 5})
    assert removed_season.status_code == 200 and removed_season.json()["seasons"] == []
    assert [entry["action"] for entry in client.get("/series/1/audit").json()["items"]] == ["created", "season_added", "episode_added", "episode_updated", "episode_removed", "season_removed"]
    assert parent["version"] == 1


def test_full_series_update_can_swap_unique_numbers_without_changing_ids(tmp_path) -> None:
    client = TestClient(create_series_app(tmp_path / "series-swaps.sqlite3"))
    created = client.post("/series", json={
        "name": "DUMMY Numbering",
        "synopsis": "Swap regression.",
        "seasons": [
            {"seasonNumber": 1, "episodes": [{"name": "A", "episodeNumber": 1}, {"name": "B", "episodeNumber": 2}]},
            {"seasonNumber": 2, "episodes": [{"name": "C", "episodeNumber": 1}, {"name": "D", "episodeNumber": 2}]},
        ],
    })
    assert created.status_code == 201
    original = created.json()
    first, second = original["seasons"]
    first_a, first_b = first["episodes"]
    second_c, second_d = second["episodes"]
    update = client.put("/series/1", json={
        "name": original["name"], "synopsis": original["synopsis"], "genre": original["genre"],
        "releaseYear": original["releaseYear"], "language": original["language"], "externalId": original["externalId"],
        "expectedVersion": original["version"],
        "seasons": [
            {"id": first["id"], "seasonNumber": 2, "episodes": [
                {"id": first_a["id"], "name": "A", "episodeNumber": 2},
                {"id": first_b["id"], "name": "B", "episodeNumber": 1},
            ]},
            {"id": second["id"], "seasonNumber": 1, "episodes": [
                {"id": second_c["id"], "name": "C", "episodeNumber": 2},
                {"id": second_d["id"], "name": "D", "episodeNumber": 1},
            ]},
        ],
    })
    assert update.status_code == 200
    revised = update.json()
    assert [(season["id"], season["seasonNumber"]) for season in revised["seasons"]] == [(first["id"], 2), (second["id"], 1)]
    assert [(episode["id"], episode["episodeNumber"]) for episode in revised["seasons"][0]["episodes"]] == [(first_a["id"], 2), (first_b["id"], 1)]
    assert [(episode["id"], episode["episodeNumber"]) for episode in revised["seasons"][1]["episodes"]] == [(second_c["id"], 2), (second_d["id"], 1)]


def test_asset_paths_are_metadata_only_and_inventory_reports_quality(tmp_path) -> None:
    client = TestClient(create_movie_app(tmp_path / "assets.sqlite3"))
    movie = client.post("/movies", json={"name": "Still Frame", "synopsis": "Assets only."}).json()
    checksum = "a" * 64
    bad = client.post("/movies/1/assets", json={"path": "../../outside", "role": "poster", "mimeType": "image/png", "sizeBytes": 5, "checksumSha256": checksum})
    assert bad.status_code == 422
    asset = client.post("/movies/1/assets", json={"path": "posters/still-frame.png", "role": "poster", "mimeType": "image/png", "sizeBytes": 0, "checksumSha256": checksum})
    assert asset.status_code == 201
    assert asset.json()["diskValidation"] == "not_inspected"
    assert client.post("/movies/1/assets", json={"path": "posters/still-frame.png", "role": "poster", "mimeType": "image/png", "sizeBytes": 5, "checksumSha256": checksum}).status_code == 409
    report = client.get("/movies/reports/assets").json()
    assert report["issues"][0]["code"] == "zero_sized_asset"
    assert report["diskInspection"].startswith("not_performed")
    second = client.post("/movies", json={"name": "Same checksum target"}).json()
    other_asset = client.post(f"/movies/{second['id']}/assets", json={"path": "posters/second.png", "role": "poster", "mimeType": "image/png", "sizeBytes": 10, "checksumSha256": checksum})
    assert other_asset.status_code == 201
    report = client.get("/movies/reports/assets").json()
    assert report["duplicateChecksums"][0]["checksumSha256"] == checksum
    removed = client.delete(f"/movies/{second['id']}/assets/{other_asset.json()['id']}")
    assert removed.status_code == 200 and removed.json()["deleted"] is True
    assert client.get("/movies/reports/assets").json()["duplicateChecksums"] == []
    assert client.get(f"/movies/{second['id']}/audit").json()["items"][-1]["action"] == "asset_removed"

    playback_movie = client.post("/movies", json={"name": "Video Asset Route", "synopsis": "Playback is asset metadata."}).json()
    playback_asset = client.post(f"/movies/{playback_movie['id']}/assets", json={"path": "masters/video.bin", "role": "video", "mimeType": "video/mp4", "sizeBytes": 100, "checksumSha256": "b" * 64}).json()
    now = datetime.now(timezone.utc)
    right = client.post(f"/movies/{playback_movie['id']}/licenses", json=_window(now - timedelta(minutes=1), now + timedelta(hours=2)))
    assert right.status_code == 201
    version = client.get(f"/movies/{playback_movie['id']}").json()["version"]
    assert client.post(f"/movies/{playback_movie['id']}/transition", json={"action": "publish", "expectedVersion": version}).status_code == 200
    before_failed_removal = client.get(f"/movies/{playback_movie['id']}").json()
    before_audit_count = len(client.get(f"/movies/{playback_movie['id']}/audit").json()["items"])
    before_feed_mark = client.get("/movies/changes").json()["highWaterMark"]
    remove_playback = client.delete(f"/movies/{playback_movie['id']}/assets/{playback_asset['id']}")
    assert remove_playback.status_code == 409
    assert client.get(f"/movies/{playback_movie['id']}/assets").json()["items"][0]["id"] == playback_asset["id"]
    assert client.get(f"/movies/{playback_movie['id']}").json()["version"] == before_failed_removal["version"]
    assert len(client.get(f"/movies/{playback_movie['id']}/audit").json()["items"]) == before_audit_count
    assert client.get("/movies/changes").json()["highWaterMark"] == before_feed_mark
    assert movie["id"] == 1


def test_duplicate_candidates_and_external_identifier_conflicts(tmp_path) -> None:
    client = TestClient(create_movie_app(tmp_path / "duplicates.sqlite3"))
    first = client.post("/movies", json={"name": "River Crossing", "releaseYear": 2021, "externalId": "DUMMY-RIVER-1"})
    assert first.status_code == 201
    collision = client.post("/movies", json={"name": "Different Entry", "releaseYear": 2021, "externalId": "DUMMY-RIVER-1"})
    assert collision.status_code == 409
    similar = client.post("/movies", json={"name": "River-Crossing", "releaseYear": 2021})
    assert similar.status_code == 201
    candidates = client.get("/movies/duplicates").json()["items"]
    assert candidates[0]["match"] == "normalized_name_year"
    assert {item["id"] for item in candidates[0]["items"]} == {first.json()["id"], similar.json()["id"]}


def test_legacy_database_migration_preserves_rows_nested_data_and_stream_typo(tmp_path) -> None:
    movie_path = tmp_path / "legacy-movies.sqlite3"
    with sqlite3.connect(movie_path) as connection:
        connection.execute("CREATE TABLE movies(id INTEGER PRIMARY KEY, name TEXT, genre TEXT, ulr_stream TEXT)")
        connection.execute("INSERT INTO movies VALUES (7, 'Legacy Feature', 'Drama', 'https://media.example.invalid/legacy')")
    movie_db = Database(movie_path, role="movie")
    movie_db.initialize()
    from media_catalog.content_repository import SourceRepository

    migrated_movie = SourceRepository(movie_db, "movie").get(7)
    assert migrated_movie["name"] == "Legacy Feature"
    assert migrated_movie["streamUrl"] == "https://media.example.invalid/legacy"

    series_path = tmp_path / "legacy-series.sqlite3"
    legacy_hierarchy = [{"id": 4, "seasonNumber": 2, "chapters": [{"id": 9, "name": "Old Episode", "number": 5, "urlStream": "https://media.example.invalid/old-episode"}]}]
    with sqlite3.connect(series_path) as connection:
        connection.execute("CREATE TABLE series(id INTEGER PRIMARY KEY, name TEXT, genre TEXT, seasons_json TEXT)")
        connection.execute("INSERT INTO series VALUES (?, ?, ?, ?)", (12, "Legacy Series", "Drama", json.dumps(legacy_hierarchy)))
    series_db = Database(series_path, role="series")
    series_db.initialize()
    migrated_series = SourceRepository(series_db, "series").get(12)
    assert migrated_series["seasons"][0]["id"] == 4
    assert migrated_series["seasons"][0]["episodes"][0]["id"] == 9
    assert migrated_series["seasons"][0]["episodes"][0]["episodeNumber"] == 5
    assert migrated_series["seasons"][0]["episodes"][0]["streamUrl"].endswith("old-episode")


def test_ambiguous_legacy_hierarchy_fails_without_partial_migration(tmp_path) -> None:
    path = tmp_path / "ambiguous-series.sqlite3"
    original = [{"seasonNumber": 1}, {"seasonNumber": 1}]
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE series(id INTEGER PRIMARY KEY, name TEXT, genre TEXT, seasons_json TEXT)")
        connection.execute("INSERT INTO series VALUES (1, 'Duplicate seasons', 'Drama', ?)", (json.dumps(original),))
    try:
        Database(path, role="series").initialize()
    except ValueError as error:
        assert "duplicate legacy seasonNumber" in str(error)
    else:
        raise AssertionError("ambiguous legacy hierarchy must stop migration")
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT seasons_json FROM series WHERE id=1").fetchone()[0] == json.dumps(original)
        assert connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_migrations'").fetchone() is None
