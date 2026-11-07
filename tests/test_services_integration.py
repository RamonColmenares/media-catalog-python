from __future__ import annotations

import io
import json
import sqlite3
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

from conftest import LiveStack


def _movie(name: str, *, synopsis: str = "A synthetic catalog entry.", external_id: str | None = None) -> dict:
    item = {"name": name, "synopsis": synopsis, "genre": "Adventure", "releaseYear": 2024, "language": "en", "streamUrl": "https://media.example.invalid/demo"}
    if external_id:
        item["externalId"] = external_id
    return item


def _series(name: str) -> dict:
    return {
        "name": name,
        "synopsis": "A synthetic episodic catalog entry.",
        "genre": "Adventure",
        "releaseYear": 2025,
        "language": "en",
        "seasons": [{"seasonNumber": 1, "episodes": [
            {"name": "First Signal", "episodeNumber": 1, "runtimeMinutes": 24, "streamUrl": "https://media.example.invalid/first-signal"},
            {"name": "Long Route", "episodeNumber": 3, "runtimeMinutes": 28, "streamUrl": "https://media.example.invalid/long-route"},
        ]}],
    }


def test_real_services_validate_version_lifecycle_gateway_and_source_feed(live_stack: LiveStack) -> None:
    with httpx.Client(timeout=5) as client:
        gateway = live_stack.url("gateway", "")
        movie = client.post(f"{gateway}/movies", json=_movie("Night Railway"), headers={"Idempotency-Key": "night-railway-create"})
        assert movie.status_code == 201
        item = movie.json()
        assert item["kind"] == "movie" and item["status"] == "draft" and item["version"] == 1
        assert "streamUrl" in item and "ulrStream" not in item
        replay = client.post(f"{gateway}/movies", json=_movie("Night Railway"), headers={"Idempotency-Key": "night-railway-create"})
        assert replay.json()["id"] == item["id"]
        mismatch = client.post(f"{gateway}/movies", json=_movie("Another Name"), headers={"Idempotency-Key": "night-railway-create"})
        assert mismatch.status_code == 409

        racing = client.post(f"{gateway}/movies", json=_movie("Race Entry")).json()
        update_body = {**_movie("Race Entry", synopsis="One writer wins."), "expectedVersion": racing["version"]}
        def update_from_another_request() -> int:
            with httpx.Client(timeout=5) as contender:
                return contender.put(f"{gateway}/movies/{racing['id']}", json=update_body).status_code
        with ThreadPoolExecutor(max_workers=2) as pool:
            statuses = list(pool.map(lambda _: update_from_another_request(), range(2)))
        assert sorted(statuses) == [200, 409]

        series_response = client.post(f"{gateway}/series", json=_series("Quiet Signals"))
        assert series_response.status_code == 201
        series = series_response.json()
        episode_ids = [episode["id"] for episode in series["seasons"][0]["episodes"]]
        assert all("episodes" in season and "chapters" not in season for season in series["seasons"])
        season_id = series["seasons"][0]["id"]
        incomplete = client.put(f"{gateway}/series/{series['id']}/seasons/{season_id}/reorder", json={"episodeIds": episode_ids[:1]})
        assert incomplete.status_code == 422
        reordered = client.put(f"{gateway}/series/{series['id']}/seasons/{season_id}/reorder", json={"episodeIds": list(reversed(episode_ids))})
        assert reordered.status_code == 200
        assert [ep["id"] for ep in client.get(f"{gateway}/series/{series['id']}").json()["seasons"][0]["episodes"]] == list(reversed(episode_ids))
        structure = client.get(f"{gateway}/series/{series['id']}/structure-report").json()
        assert structure["numberingGaps"] == [{"seasonNumber": 1, "missingEpisodeNumbers": [2]}]
        assert structure["knownRuntimeMinutes"] == 52

        invalid = client.post(f"{gateway}/movies", json={"name": "Bad Field", "urlStream": "https://media.example.invalid/old-name"})
        assert invalid.status_code == 422 and invalid.json()["error"]["code"] == "invalid_request"
        assert client.get(f"{gateway}/movies/987654").status_code == 404
        assert client.get(f"{gateway}/movies?limit=1&offset=0").json()["total"] == 2
        assert client.get(f"{gateway}/catalogs/search?text=one&text=two").status_code == 422
        assert client.get(f"{gateway}/unknown/path").status_code == 404

        now = datetime.now(timezone.utc)
        end = now + timedelta(hours=1)
        rights = client.post(f"{gateway}/movies/{item['id']}/licenses", json={"region": "WORLD", "startsAt": (now - timedelta(minutes=1)).isoformat(), "endsAt": end.isoformat(), "rightsHolder": "DUMMY Studio"})
        assert rights.status_code == 201
        current = client.get(f"{gateway}/movies/{item['id']}").json()
        ready = client.get(f"{gateway}/movies/{item['id']}/readiness").json()
        assert ready["ready"] is True
        published = client.post(f"{gateway}/movies/{item['id']}/transition", json={"action": "publish", "expectedVersion": current["version"]})
        assert published.status_code == 200 and published.json()["status"] == "published"
        invalid_edit = client.put(f"{gateway}/movies/{item['id']}", json={**_movie("Night Railway", synopsis=""), "expectedVersion": published.json()["version"]})
        assert invalid_edit.status_code == 409
        available = client.get(f"{gateway}/movies/{item['id']}/availability", params={"region": "CO", "at": now.isoformat()}).json()
        assert available["available"] is True
        expired = client.get(f"{gateway}/movies/{item['id']}/availability", params={"region": "CO", "at": end.isoformat()}).json()
        assert expired["available"] is False
        audit = client.get(f"{gateway}/movies/{item['id']}/audit").json()["items"]
        assert [entry["action"] for entry in audit] == ["created", "license_added", "publish"]

        feed = client.get(f"{live_stack.url('movie', '')}/movies/changes", params={"after": 0, "limit": 50}).json()
        assert feed["items"] and feed["nextCursor"] == feed["items"][-1]["sequence"]
        assert feed["items"][0]["item"]["sourceRevision"] >= 1
        snapshot = client.get(f"{live_stack.url('movie', '')}/movies/snapshot").json()
        assert snapshot["highWaterMark"] == feed["highWaterMark"]
        assert snapshot["items"][0]["streamUrl"] == item["streamUrl"]
        live_stack.restart_service("movie")
        restarted_movie = client.get(f"{live_stack.url('movie', '')}/movies/{item['id']}").json()
        assert restarted_movie["status"] == "published"
        assert len(restarted_movie["licenses"]) == 1
        assert len(client.get(f"{live_stack.url('movie', '')}/movies/{item['id']}/audit").json()["items"]) == 3
        live_stack.restart_service("series")
        restarted_series = client.get(f"{live_stack.url('series', '')}/series/{series['id']}").json()
        assert [episode["id"] for episode in restarted_series["seasons"][0]["episodes"]] == list(reversed(episode_ids))
        assert client.get(f"{live_stack.url('gateway', '')}/health/ready").json()["ready"] is True


def test_mixed_catalog_workflows_sync_rebuild_restart_and_source_outage(live_stack: LiveStack) -> None:
    with httpx.Client(timeout=5) as client:
        gateway = live_stack.url("gateway", "")
        movie = client.post(f"{gateway}/movies", json=_movie("Amber Coast", external_id="DUMMY-COAST-1")).json()
        series = client.post(f"{gateway}/series", json=_series("Amber Signals")).json()
        assert movie["id"] == series["id"] == 1
        sync = client.post(f"{gateway}/catalogs/sync").json()
        assert sync["run"]["status"] == "complete"
        assert sync["sources"]["movie"]["events"] >= 1
        assert sync["sources"]["series"]["events"] >= 1

        search = client.get(f"{gateway}/catalogs/search", params={"text": "amber", "sort": "name"}).json()
        assert {(item["kind"], item["id"]) for item in search["items"]} == {("movie", 1), ("series", 1)}
        assert client.get(f"{gateway}/catalogs/search", params={"kind": "movie", "genre": "adventure", "releaseYearFrom": 2020}).json()["total"] == 1
        detail = client.get(f"{gateway}/catalogs/items/movie/{movie['id']}").json()
        assert detail["name"] == "Amber Coast"
        assert client.get(f"{gateway}/catalogs/items/movie/{movie['id']}/availability", params={"region": "CO"}).json()["reasons"] == ["content_draft"]
        facets = client.get(f"{gateway}/catalogs/facets", params={"genre": "Adventure"}).json()
        assert {facet["value"] for facet in facets["kind"]} == {"movie", "series"}
        assert client.get(f"{gateway}/catalogs/search", params={"unknownFilter": "x"}).status_code == 422

        label = client.post(f"{gateway}/catalogs/labels", json={"name": "Featured", "color": "#ff7700"})
        assert label.status_code == 201
        assignment = client.post(f"{gateway}/catalogs/labels/{label.json()['id']}/items", json={"kind": "movie", "id": movie["id"]})
        assert assignment.status_code == 201
        assert client.get(f"{gateway}/catalogs/labels").json()["items"][0]["usageCount"] == 1
        assert client.get(f"{gateway}/catalogs/search", params={"label": "FEATURED"}).json()["total"] == 1
        assert client.post(f"{gateway}/catalogs/labels/{label.json()['id']}/items", json={"kind": "movie", "id": 999}).status_code == 404
        assert client.delete(f"{gateway}/catalogs/labels/{label.json()['id']}").status_code == 409

        collection = client.post(f"{gateway}/catalogs/collections", json={"name": "April slate", "description": "DUMMY mixed slate", "items": [{"kind": "movie", "id": movie["id"]}, {"kind": "series", "id": series["id"]}]})
        assert collection.status_code == 201
        collection_id = collection.json()["id"]
        changed_collection = client.put(f"{gateway}/catalogs/collections/{collection_id}", json={"name": "April slate revised", "description": "DUMMY mixed slate", "expectedVersion": 1})
        assert changed_collection.status_code == 200 and changed_collection.json()["version"] == 2
        stale_collection = client.put(f"{gateway}/catalogs/collections/{collection_id}", json={"name": "Stale slate", "description": "", "expectedVersion": 1})
        assert stale_collection.status_code == 409
        assert [(item["kind"], item["id"]) for item in collection.json()["items"]] == [("movie", 1), ("series", 1)]
        incomplete_order = client.put(f"{gateway}/catalogs/collections/{collection_id}/reorder", json={"items": [{"kind": "movie", "id": 1}]})
        assert incomplete_order.status_code == 422
        valid_order = client.put(f"{gateway}/catalogs/collections/{collection_id}/reorder", json={"items": [{"kind": "series", "id": 1}, {"kind": "movie", "id": 1}]})
        assert valid_order.status_code == 200
        validation = client.get(f"{gateway}/catalogs/collections/{collection_id}/validate", params={"region": "WORLD"}).json()
        assert validation["valid"] is False

        saved = client.post(f"{gateway}/catalogs/saved-searches", json={"name": "Adventure shelf", "query": {"genre": "Adventure", "sort": "name"}})
        assert saved.status_code == 201
        updated_saved = client.put(f"{gateway}/catalogs/saved-searches/1", json={"name": "Adventure shelf revised", "query": {"genre": "Adventure", "sort": "updatedAt"}, "expectedVersion": 1})
        assert updated_saved.status_code == 200 and updated_saved.json()["version"] == 2
        assert client.put(f"{gateway}/catalogs/saved-searches/1", json={"name": "Stale", "query": {}, "expectedVersion": 1}).status_code == 409
        assert client.post(f"{gateway}/catalogs/saved-searches/1/run").json()["result"]["total"] == 2
        bad_saved = client.post(f"{gateway}/catalogs/saved-searches", json={"name": "Invalid", "query": {"genre": "Adventure", "typo": 1}})
        assert bad_saved.status_code == 422
        assert client.get(f"{gateway}/catalogs/reports/statistics").json()["total"] == 2
        assert client.get(f"{gateway}/catalogs/reports/availability", params={"region": "WORLD"}).json()["total"] == 2
        assert client.get(f"{gateway}/catalogs/sync/runs").json()["items"]
        assert client.get(f"{gateway}/catalogs/reports/quality").json()["checkedItems"] == 2
        assert client.get(f"{gateway}/catalogs/reports/series-structure").json()["items"][0]["numberingGaps"][0]["missingEpisodeNumbers"] == [2]

        catalog_db = live_stack.db_paths["catalog"]
        before_cursor = client.get(f"{live_stack.url('catalog', '')}/catalogs/health").json()["projection"]["cursors"]["movie"]["sequence"]
        live_stack.restart_service("catalog")
        assert client.get(f"{live_stack.url('catalog', '')}/catalogs/search", params={"text": "Amber"}).json()["total"] == 2
        assert client.get(f"{live_stack.url('catalog', '')}/catalogs/collections/{collection_id}").json()["items"][0]["kind"] == "series"
        assert client.get(f"{live_stack.url('catalog', '')}/catalogs/collections/{collection_id}").json()["name"] == "April slate revised"
        assert client.get(f"{live_stack.url('catalog', '')}/catalogs/saved-searches").json()["items"][0]["version"] == 2
        assert client.get(f"{live_stack.url('catalog', '')}/catalogs/health").json()["projection"]["cursors"]["movie"]["sequence"] == before_cursor
        assert catalog_db.exists()

        extra = client.post(f"{gateway}/movies", json=_movie("Amber Dusk", external_id="DUMMY-COAST-2"))
        assert extra.status_code == 201
        movie_cursor = client.get(f"{gateway}/catalogs/health").json()["projection"]["cursors"]["movie"]["sequence"]
        live_stack.stop_service("movie")
        health = client.get(f"{gateway}/catalogs/health").json()
        assert health["ready"] is False and health["upstreams"]["movie"]["reachable"] is False
        failed_sync = client.post(f"{gateway}/catalogs/sync").json()
        assert failed_sync["run"]["status"] == "partial"
        assert failed_sync["cursors"]["movie"]["sequence"] == movie_cursor
        assert client.get(f"{gateway}/catalogs/search", params={"text": "Amber Coast"}).json()["total"] == 1
        assert client.get(f"{gateway}/movies").status_code == 503

        live_stack.restart_service("movie")
        recovered = client.post(f"{gateway}/catalogs/sync").json()
        assert recovered["run"]["status"] == "complete"
        assert client.get(f"{gateway}/catalogs/search", params={"text": "Amber Dusk"}).json()["total"] == 1
        rebuilt = client.post(f"{gateway}/catalogs/rebuild").json()
        assert rebuilt["projectionChanged"] is True
        assert client.get(f"{gateway}/catalogs/search", params={"text": "Amber"}).json()["total"] == 3
        assert client.get(f"{gateway}/catalogs/labels").json()["items"][0]["usageCount"] == 1
        assert client.get(f"{gateway}/catalogs/collections/{collection_id}").status_code == 200
        assert client.get(f"{gateway}/catalogs/rebuilds").json()["items"]


def test_durable_import_preview_resume_and_http_idempotency(live_stack: LiveStack) -> None:
    with httpx.Client(timeout=5) as client:
        gateway = live_stack.url("gateway", "")
        request = {
            "idempotencyKey": "import-batch-retry-0001",
            "rows": [{"kind": "movie", "item": _movie("Imported Harbor", external_id="DUMMY-IMPORT-1")}],
        }
        preview = client.post(f"{gateway}/catalogs/imports/preview", json={"rows": request["rows"]})
        assert preview.status_code == 200
        assert preview.json()["valid"] is True and preview.json()["rows"][0]["operation"] == "create"
        invalid_preview = client.post(f"{gateway}/catalogs/imports/preview", json={"rows": [{"kind": "movie", "item": {"name": "Bad", "unknown": "field"}}]})
        assert invalid_preview.status_code == 200 and invalid_preview.json()["invalidCount"] == 1
        duplicate_preview = client.post(f"{gateway}/catalogs/imports/preview", json={"rows": [{"kind": "movie", "item": _movie("Repeat", external_id="DUMMY-REPEAT")}, {"kind": "movie", "item": _movie("Repeat", external_id="DUMMY-REPEAT")}]})
        assert duplicate_preview.json()["valid"] is False
        assert duplicate_preview.json()["rows"][1]["errors"][0]["code"] == "duplicate_batch_row"

        job_response = client.post(f"{gateway}/catalogs/imports", json=request)
        assert job_response.status_code == 201
        job = job_response.json()
        same_key = client.post(f"{gateway}/catalogs/imports", json=request)
        assert same_key.status_code == 201 and same_key.json()["id"] == job["id"]
        conflict_body = {**request, "rows": [{"kind": "movie", "item": _movie("Changed", external_id="DUMMY-IMPORT-2")}]}
        assert client.post(f"{gateway}/catalogs/imports", json=conflict_body).status_code == 409

        live_stack.stop_service("movie")
        failed = client.post(f"{gateway}/catalogs/imports/{job['id']}/run").json()
        assert failed["state"] == "failed" and failed["rows"][0]["attempts"] == 1
        failed_jobs = client.get(f"{gateway}/catalogs/imports", params={"state": "failed"}).json()
        assert failed_jobs["total"] == 1 and failed_jobs["items"][0]["id"] == job["id"]
        live_stack.restart_service("movie")
        live_stack.restart_service("catalog")
        assert client.get(f"{gateway}/catalogs/imports", params={"state": "failed"}).json()["items"][0]["remainingRows"] == 1
        retry = client.post(f"{gateway}/catalogs/imports/{job['id']}/retry").json()
        assert retry["state"] == "completed"
        assert retry["rows"][0]["attempts"] == 2
        assert client.get(f"{gateway}/catalogs/imports", params={"state": "completed"}).json()["items"][0]["completedRows"] == 1
        imported = client.get(f"{gateway}/movies?genre=Adventure").json()
        assert imported["total"] == 1 and imported["items"][0]["name"] == "Imported Harbor"


def test_deterministic_metadata_zip_checksum_validation_and_limits(live_stack: LiveStack) -> None:
    with httpx.Client(timeout=5) as client:
        gateway = live_stack.url("gateway", "")
        movie = client.post(f"{gateway}/movies", json=_movie("Package Feature")).json()
        client.post(f"{gateway}/catalogs/sync")
        request = {"references": [{"kind": "movie", "id": movie["id"]}]}
        package_a = client.post(f"{gateway}/catalogs/exports", json=request)
        package_b = client.post(f"{gateway}/catalogs/exports", json=request)
        assert package_a.status_code == 200 and package_a.headers["content-type"] == "application/zip"
        assert package_a.content == package_b.content
        assert package_a.headers["x-package-sha256"] == package_b.headers["x-package-sha256"]
        assert client.get(f"{gateway}/catalogs/exports").json()["total"] == 1
        verified = client.post(f"{gateway}/catalogs/exports/validate", content=package_a.content, headers={"content-type": "application/zip"})
        assert verified.status_code == 200 and verified.json()["valid"] is True
        assert verified.json()["itemCount"] == 1

        with zipfile.ZipFile(io.BytesIO(package_a.content)) as archive:
            content = json.loads(archive.read("content.json"))
            manifest = json.loads(archive.read("manifest.json"))
        content["items"][0]["name"] = "Altered Package Feature"
        tampered = io.BytesIO()
        with zipfile.ZipFile(tampered, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("content.json", json.dumps(content))
            archive.writestr("manifest.json", json.dumps(manifest))
        tampered_result = client.post(f"{gateway}/catalogs/exports/validate", content=tampered.getvalue(), headers={"content-type": "application/zip"}).json()
        assert tampered_result["valid"] is False
        assert any(item["code"] == "checksum_mismatch" for item in tampered_result["findings"])
        malformed = client.post(f"{gateway}/catalogs/exports/validate", content=b"not a zip", headers={"content-type": "application/zip"}).json()
        assert malformed["findings"][0]["code"] == "invalid_zip"
        too_large = client.post(f"{gateway}/catalogs/exports/validate", content=b"0" * (8 * 1024 * 1024 + 1), headers={"content-type": "application/zip"})
        assert too_large.status_code == 413


def test_change_feed_pages_are_atomic_on_outage_and_resume_all_events(live_stack: LiveStack) -> None:
    with httpx.Client(timeout=5) as client:
        movie_service = live_stack.url("movie", "")
        gateway = live_stack.url("gateway", "")
        for index in range(205):
            response = client.post(f"{movie_service}/movies", json=_movie(f"Page Entry {index:03d}", external_id=f"DUMMY-PAGE-{index:03d}"))
            assert response.status_code == 201
        first_page = client.get(f"{movie_service}/movies/changes", params={"after": 0, "limit": 200}).json()
        assert len(first_page["items"]) == 200 and first_page["nextCursor"] == 200 and first_page["hasMore"] is True

        live_stack.stop_service("movie")
        failed = client.post(f"{gateway}/catalogs/sync").json()
        assert failed["run"]["status"] == "partial"
        assert failed["cursors"]["movie"]["sequence"] == 0
        assert client.get(f"{gateway}/catalogs/search", params={"text": "Page Entry"}).json()["total"] == 0

        live_stack.restart_service("movie")
        synced = client.post(f"{gateway}/catalogs/sync").json()
        assert synced["run"]["status"] == "complete"
        assert synced["sources"]["movie"]["events"] == 205
        assert synced["cursors"]["movie"]["sequence"] == 205
        assert client.get(f"{gateway}/catalogs/search", params={"text": "Page Entry", "limit": 200}).json()["total"] == 205


def test_source_database_reset_requires_rebuild_and_replaces_stale_projection(live_stack: LiveStack) -> None:
    with httpx.Client(timeout=5) as client:
        gateway = live_stack.url("gateway", "")
        client.post(f"{gateway}/movies", json=_movie("Before Reset"))
        client.post(f"{gateway}/catalogs/sync")
        state = client.get(f"{gateway}/catalogs/health").json()["projection"]["cursors"]["movie"]
        assert state["sequence"] == 1 and state["sourceId"]
        live_stack.stop_service("movie")
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(live_stack.db_paths["movie"]) + suffix)
            if path.exists():
                path.unlink()
        live_stack.restart_service("movie")

        reset_sync = client.post(f"{gateway}/catalogs/sync").json()
        assert reset_sync["run"]["status"] == "partial"
        assert reset_sync["cursors"]["movie"]["sequence"] == state["sequence"]
        assert "identity changed" in reset_sync["sources"]["movie"]["error"]
        assert client.get(f"{gateway}/catalogs/search", params={"text": "Before Reset"}).json()["total"] == 1

        rebuild = client.post(f"{gateway}/catalogs/rebuild").json()
        assert rebuild["projectionChanged"] is True
        assert client.get(f"{gateway}/catalogs/search", params={"text": "Before Reset"}).json()["total"] == 0
        cursor = client.get(f"{gateway}/catalogs/health").json()["projection"]["cursors"]["movie"]
        assert cursor["sequence"] == 0 and cursor["sourceId"] != state["sourceId"]
