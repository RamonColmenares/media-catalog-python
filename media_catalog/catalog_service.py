"""Catalog HTTP service backed by its own durable HTTP-derived projection."""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncIterator, Literal

import httpx
from fastapi import FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from pydantic import ValidationError

from media_catalog.catalog_repository import CatalogRepository
from media_catalog.content_repository import utc_now
from media_catalog.database import Database
from media_catalog.errors import DomainError, invalid, not_found
from media_catalog.exports import MAX_UPLOAD_BYTES, build_export, validate_export
from media_catalog.import_engine import ImportEngine
from media_catalog.models import CollectionInput, CollectionReorderInput, CollectionUpdate, ContentReference, ImportCommit, ImportPreview, LabelInput, SavedSearchInput, SavedSearchUpdate, SearchQuery


_SEARCH_KEYS = {"text", "kind", "genre", "status", "language", "releaseYearFrom", "releaseYearTo", "label", "sort", "direction", "limit", "offset"}


def create_app(database_path: str | None = None) -> FastAPI:
    database = Database(database_path or os.getenv("CATALOG_DB_PATH", "./data/catalog.sqlite3"), role="catalog")
    database.initialize()
    repository = CatalogRepository(database)
    imports = ImportEngine(database, repository)
    source_urls = {
        "movie": os.getenv("MOVIE_SERVICE_URL", "http://127.0.0.1:8102").rstrip("/"),
        "series": os.getenv("SERIES_SERVICE_URL", "http://127.0.0.1:8103").rstrip("/"),
    }
    request_timeout = float(os.getenv("SERVICE_REQUEST_TIMEOUT", "3"))

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        async with httpx.AsyncClient(timeout=request_timeout) as client:
            app.state.http_client = client
            yield

    app = FastAPI(title="Catalog service", version="2.0.0", lifespan=lifespan)
    app.state.database = database
    app.state.repository = repository

    @app.exception_handler(DomainError)
    async def handle_domain_error(request: Request, error: DomainError) -> JSONResponse:
        return JSONResponse(status_code=error.status_code, content={"error": {"code": error.code, "message": error.message, "details": error.details}})

    @app.exception_handler(RequestValidationError)
    async def handle_request_error(request: Request, error: RequestValidationError) -> JSONResponse:
        details = [{"path": ".".join(str(part) for part in item["loc"][1:]), "message": item["msg"], "type": item["type"]} for item in error.errors()]
        return JSONResponse(status_code=422, content={"error": {"code": "invalid_request", "message": "Request validation failed", "details": details}})

    def parse_search(request: Request) -> SearchQuery:
        unknown = sorted(set(request.query_params.keys()) - _SEARCH_KEYS)
        if unknown:
            raise invalid("unknown_search_fields", "Search contains unsupported query fields", {"fields": unknown})
        repeated = sorted(key for key in _SEARCH_KEYS if len(request.query_params.getlist(key)) > 1)
        if repeated:
            raise invalid("repeated_search_fields", "Search fields may appear only once", {"fields": repeated})
        values = {key: request.query_params.get(key) for key in _SEARCH_KEYS if key in request.query_params}
        try:
            return SearchQuery.model_validate(values)
        except ValidationError as error:
            raise invalid("invalid_search", "Search query validation failed", [{"path": ".".join(str(part) for part in item["loc"]), "message": item["msg"]} for item in error.errors()]) from error

    async def source_json(kind: str, path: str) -> dict[str, Any]:
        try:
            response = await app.state.http_client.get(f"{source_urls[kind]}{path}")
            response.raise_for_status()
            result = response.json()
            if not isinstance(result, dict):
                raise ValueError("source response was not an object")
            return result
        except httpx.HTTPStatusError as error:
            raise DomainError(503, "source_unavailable", f"{kind.title()} source returned HTTP {error.response.status_code}") from error
        except (httpx.HTTPError, ValueError) as error:
            raise DomainError(503, "source_unavailable", f"{kind.title()} source could not be reached") from error

    async def sync_sources() -> dict[str, Any]:
        run_id = repository.begin_sync({"movie": "configured", "series": "configured"})
        reports: dict[str, Any] = {}
        errors = []
        total_events = 0
        for kind in ("movie", "series"):
            current_cursor = repository.cursor_state()[kind]
            committed_cursor = current_cursor["sequence"]
            requested_cursor = committed_cursor
            source_id = current_cursor["sourceId"]
            event_batch = []
            try:
                for _ in range(100):
                    resource = "movies" if kind == "movie" else "series"
                    page = await source_json(kind, f"/{resource}/changes?after={requested_cursor}&limit=200")
                    events = page.get("items")
                    if not isinstance(events, list):
                        raise DomainError(502, "invalid_source_feed", f"{kind.title()} source returned an invalid change page")
                    page_source_id = page.get("sourceId")
                    if not isinstance(page_source_id, str) or not page_source_id:
                        raise DomainError(502, "invalid_source_feed", f"{kind.title()} source feed did not identify its database")
                    if source_id and page_source_id != source_id:
                        raise DomainError(409, "source_reset_requires_rebuild", f"{kind.title()} source database identity changed; rebuild the catalog projection")
                    source_id = page_source_id
                    next_cursor = page.get("nextCursor")
                    if not isinstance(next_cursor, int) or next_cursor < requested_cursor:
                        raise DomainError(502, "invalid_source_cursor", f"{kind.title()} source returned an invalid nextCursor")
                    if events:
                        if next_cursor != events[-1].get("sequence"):
                            raise DomainError(502, "invalid_source_cursor", f"{kind.title()} nextCursor does not match its page")
                        event_batch.extend(events)
                        if next_cursor <= requested_cursor:
                            raise DomainError(502, "stalled_source_feed", f"{kind.title()} source feed did not advance its cursor")
                        requested_cursor = next_cursor
                    elif next_cursor != requested_cursor:
                        raise DomainError(502, "invalid_source_cursor", f"{kind.title()} source advanced a cursor without returning events")
                    if not page.get("hasMore") or not events:
                        break
                else:
                    raise DomainError(503, "sync_page_limit", f"{kind.title()} source feed exceeded the per-run page limit")
                applied = repository.apply_event_page(kind, event_batch, source_cursor=requested_cursor, source_id=source_id)
                total_events += applied["events"]
                reports[kind] = {"status": "ok", "events": applied["events"], "cursor": applied["cursor"]}
            except DomainError as error:
                repository.mark_sync_error(kind, error.message)
                errors.append(f"{kind}: {error.message}")
                reports[kind] = {"status": "unavailable", "events": 0, "cursor": repository.cursor_state()[kind]["sequence"], "error": error.message}
            except Exception as error:
                message = f"source synchronization failed ({error.__class__.__name__})"
                repository.mark_sync_error(kind, message)
                errors.append(f"{kind}: {message}")
                reports[kind] = {"status": "unavailable", "events": 0, "cursor": repository.cursor_state()[kind]["sequence"], "error": message}
        status = "partial" if errors else "complete"
        run = repository.finish_sync(run_id, status, total_events, "; ".join(errors) if errors else None)
        return {"run": run, "sources": reports, "cursors": repository.cursor_state()}

    @app.get("/catalogs/health")
    async def health() -> dict[str, Any]:
        upstreams = {}
        for kind, url in source_urls.items():
            resource = "movies" if kind == "movie" else "series"
            try:
                response = await app.state.http_client.get(f"{url}/{resource}/health")
                upstreams[kind] = {"reachable": response.status_code == 200, "statusCode": response.status_code}
            except httpx.HTTPError:
                upstreams[kind] = {"reachable": False, "statusCode": None}
        state = repository.cursor_state()
        with database.connect() as connection:
            row = connection.execute("SELECT MAX(projected_at) FROM catalog_items").fetchone()
        return {"service": "catalog", "live": True, "database": database.health(), "ready": all(item["reachable"] for item in upstreams.values()), "upstreams": upstreams, "projection": {"lastProjectedAt": row[0], "cursors": state}}

    @app.get("/catalogs/ready")
    async def readiness() -> Response:
        status = await health()
        return JSONResponse(status_code=200 if status["ready"] and status["database"]["database"] else 503, content=status)

    @app.post("/catalogs/sync")
    async def sync() -> dict[str, Any]:
        return await sync_sources()

    @app.get("/catalogs/sync/runs")
    async def sync_runs(limit: int = Query(default=50, ge=1, le=200), offset: int = Query(default=0, ge=0, le=1_000_000)) -> dict[str, Any]:
        return repository.sync_history(limit, offset)

    @app.post("/catalogs/rebuild")
    async def rebuild() -> dict[str, Any]:
        rebuild_id = repository.begin_rebuild()
        try:
            results = await asyncio.gather(
                source_json("movie", "/movies/snapshot"),
                source_json("series", "/series/snapshot"),
                return_exceptions=True,
            )
            failures = [result for result in results if isinstance(result, Exception)]
            if failures:
                message = "; ".join(str(error) for error in failures)
                report = repository.finish_rebuild(rebuild_id, "failed", error=message)
                return {"rebuild": report, "projectionChanged": False, "error": message}
            snapshots = {"movie": results[0], "series": results[1]}
            applied = repository.apply_snapshots(snapshots)
            marks = {kind: snapshots[kind]["highWaterMark"] for kind in ("movie", "series")}
            catch_up = await sync_sources()
            report = repository.finish_rebuild(rebuild_id, "complete" if catch_up["run"]["status"] == "complete" else "partial", marks, catch_up["run"].get("error"))
            return {"rebuild": report, "projectionChanged": True, "snapshotItemsApplied": applied, "catchUp": catch_up}
        except DomainError as error:
            report = repository.finish_rebuild(rebuild_id, "failed", error=error.message)
            return {"rebuild": report, "projectionChanged": False, "error": error.message}

    @app.get("/catalogs/rebuilds")
    async def rebuild_history(limit: int = Query(default=50, ge=1, le=200), offset: int = Query(default=0, ge=0, le=1_000_000)) -> dict[str, Any]:
        return repository.rebuild_history(limit, offset)

    @app.get("/catalogs/items/{kind}/{media_id}")
    async def projected_item(kind: str, media_id: int) -> dict[str, Any]:
        if kind not in {"movie", "series"}:
            raise invalid("invalid_kind", "kind must be movie or series")
        return repository.lookup(kind, media_id)

    @app.get("/catalogs/items/{kind}/{media_id}/availability")
    async def projected_availability(kind: str, media_id: int, region: str = "WORLD", at: datetime | None = None) -> dict[str, Any]:
        if kind not in {"movie", "series"}:
            raise invalid("invalid_kind", "kind must be movie or series")
        if at is not None and (at.tzinfo is None or at.utcoffset() is None):
            raise invalid("invalid_time", "at must contain a timezone offset")
        return repository.availability_detail(kind, media_id, region, at)

    @app.get("/catalogs/search")
    async def search(request: Request) -> dict[str, Any]:
        return repository.search(parse_search(request))

    @app.post("/catalogs/search")
    async def search_body(query: SearchQuery) -> dict[str, Any]:
        return repository.search(query)

    @app.get("/catalogs/facets")
    async def facets(request: Request) -> dict[str, Any]:
        return repository.facets(parse_search(request))

    @app.get("/catalogs/labels")
    async def labels() -> dict[str, Any]:
        return {"items": repository.list_labels()}

    @app.post("/catalogs/labels", status_code=201)
    async def create_label(label: LabelInput) -> dict[str, Any]:
        return repository.create_label(label)

    @app.delete("/catalogs/labels/{label_id}")
    async def delete_label(label_id: int) -> dict[str, bool]:
        repository.delete_label(label_id)
        return {"deleted": True}

    @app.post("/catalogs/labels/{label_id}/items", status_code=201)
    async def attach_label(label_id: int, reference: ContentReference) -> dict[str, Any]:
        return repository.attach_label(label_id, reference.kind, reference.id)

    @app.delete("/catalogs/labels/{label_id}/items/{kind}/{media_id}")
    async def detach_label(label_id: int, kind: str, media_id: int) -> dict[str, bool]:
        if kind not in {"movie", "series"}:
            raise invalid("invalid_kind", "kind must be movie or series")
        repository.detach_label(label_id, kind, media_id)
        return {"deleted": True}

    @app.get("/catalogs/collections")
    async def collections() -> dict[str, Any]:
        return {"items": repository.list_collections()}

    @app.post("/catalogs/collections", status_code=201)
    async def create_collection(collection: CollectionInput) -> dict[str, Any]:
        return repository.create_collection(collection)

    @app.get("/catalogs/collections/{collection_id}")
    async def get_collection(collection_id: int) -> dict[str, Any]:
        return repository.get_collection(collection_id)

    @app.put("/catalogs/collections/{collection_id}")
    async def update_collection(collection_id: int, update: CollectionUpdate) -> dict[str, Any]:
        return repository.update_collection(collection_id, update)

    @app.delete("/catalogs/collections/{collection_id}")
    async def delete_collection(collection_id: int) -> dict[str, bool]:
        repository.delete_collection(collection_id)
        return {"deleted": True}

    @app.post("/catalogs/collections/{collection_id}/items", status_code=201)
    async def add_collection_item(collection_id: int, reference: ContentReference) -> dict[str, Any]:
        return repository.add_collection_member(collection_id, reference)

    @app.delete("/catalogs/collections/{collection_id}/items/{kind}/{media_id}")
    async def remove_collection_item(collection_id: int, kind: str, media_id: int) -> dict[str, Any]:
        if kind not in {"movie", "series"}:
            raise invalid("invalid_kind", "kind must be movie or series")
        return repository.remove_collection_member(collection_id, kind, media_id)

    @app.put("/catalogs/collections/{collection_id}/reorder")
    async def reorder_collection(collection_id: int, request: CollectionReorderInput) -> dict[str, Any]:
        return repository.reorder_collection(collection_id, request.items)

    @app.get("/catalogs/collections/{collection_id}/validate")
    async def validate_collection(collection_id: int, region: str = "WORLD", at: datetime | None = None) -> dict[str, Any]:
        if at is not None and (at.tzinfo is None or at.utcoffset() is None):
            raise invalid("invalid_time", "at must contain a timezone offset")
        return repository.validate_collection(collection_id, region, at)

    @app.get("/catalogs/saved-searches")
    async def saved_searches() -> dict[str, Any]:
        return {"items": repository.list_saved_searches()}

    @app.post("/catalogs/saved-searches", status_code=201)
    async def save_search(request: SavedSearchInput) -> dict[str, Any]:
        return repository.save_search(request.name, request.query)

    @app.put("/catalogs/saved-searches/{search_id}")
    async def update_search(search_id: int, update: SavedSearchUpdate) -> dict[str, Any]:
        return repository.update_saved_search(search_id, update)

    @app.delete("/catalogs/saved-searches/{search_id}")
    async def delete_search(search_id: int) -> dict[str, bool]:
        repository.delete_saved_search(search_id)
        return {"deleted": True}

    @app.post("/catalogs/saved-searches/{search_id}/run")
    async def run_saved_search(search_id: int) -> dict[str, Any]:
        return repository.run_saved_search(search_id)

    @app.get("/catalogs/reports/statistics")
    async def statistics(region: str = "WORLD", at: datetime | None = None) -> dict[str, Any]:
        if at is not None and (at.tzinfo is None or at.utcoffset() is None):
            raise invalid("invalid_time", "at must contain a timezone offset")
        return repository.statistics(at, region)

    @app.get("/catalogs/reports/quality")
    async def quality_report() -> dict[str, Any]:
        return repository.quality_report()

    @app.get("/catalogs/reports/duplicates")
    async def duplicate_report() -> dict[str, Any]:
        return {"items": repository.duplicate_candidates()}

    @app.get("/catalogs/reports/assets")
    async def asset_report() -> dict[str, Any]:
        return repository.asset_report()

    @app.get("/catalogs/reports/availability")
    async def availability_report(
        region: str = "WORLD",
        at: datetime | None = None,
        limit: int = Query(default=50, ge=1, le=200),
        offset: int = Query(default=0, ge=0, le=1_000_000),
    ) -> dict[str, Any]:
        if at is not None and (at.tzinfo is None or at.utcoffset() is None):
            raise invalid("invalid_time", "at must contain a timezone offset")
        return repository.availability_report(region, at, limit, offset)

    @app.get("/catalogs/reports/series-structure")
    async def series_structure_report() -> dict[str, Any]:
        with database.connect() as connection:
            series_items = [item for item in repository._all_items(connection) if item["kind"] == "series"]
        reports = []
        for item in series_items:
            seasons = item.get("seasons", [])
            gaps = []
            runtime = 0
            runtime_count = 0
            episode_count = 0
            for season in seasons:
                episodes = season.get("episodes", [])
                episode_count += len(episodes)
                numbers = {episode.get("episodeNumber") for episode in episodes}
                numbers.discard(None)
                if numbers:
                    missing = sorted(set(range(1, max(numbers) + 1)) - numbers)
                    if missing:
                        gaps.append({"seasonNumber": season.get("seasonNumber"), "missingEpisodeNumbers": missing})
                for episode in episodes:
                    if episode.get("runtimeMinutes") is not None:
                        runtime += episode["runtimeMinutes"]
                        runtime_count += 1
            reports.append({"seriesId": item["id"], "name": item["name"], "seasonCount": len(seasons), "episodeCount": episode_count, "knownRuntimeMinutes": runtime, "episodesWithRuntime": runtime_count, "numberingGaps": gaps})
        return {"items": reports, "total": len(reports)}

    @app.post("/catalogs/imports/preview")
    async def import_preview(request: ImportPreview) -> dict[str, Any]:
        return imports.preview(request)

    @app.post("/catalogs/imports", status_code=201)
    async def create_import(request: ImportCommit) -> dict[str, Any]:
        return imports.commit(request)

    @app.get("/catalogs/imports")
    async def list_imports(
        state: Literal["pending", "running", "completed", "failed"] | None = None,
        limit: int = Query(default=50, ge=1, le=200),
        offset: int = Query(default=0, ge=0, le=1_000_000),
    ) -> dict[str, Any]:
        return imports.list_jobs(state, limit, offset)

    @app.get("/catalogs/imports/{job_id}")
    async def get_import(job_id: str) -> dict[str, Any]:
        return imports.get(job_id)

    @app.post("/catalogs/imports/{job_id}/run")
    async def run_import(job_id: str) -> dict[str, Any]:
        return await imports.run(job_id, app.state.http_client, source_urls)

    @app.post("/catalogs/imports/{job_id}/retry")
    async def retry_import(job_id: str) -> dict[str, Any]:
        if not imports.retryable(job_id):
            current = imports.get(job_id)
            if current["state"] == "completed":
                return current
            raise DomainError(409, "import_not_retryable", "The import job has no failed or interrupted rows")
        return await imports.run(job_id, app.state.http_client, source_urls)

    @app.post("/catalogs/exports")
    async def create_export(request: dict[str, Any]) -> Response:
        if set(request) != {"references"} or not isinstance(request["references"], list) or not 1 <= len(request["references"]) <= 1000:
            raise invalid("invalid_export_request", "Body must contain 1 to 1000 content references")
        references = []
        for raw in request["references"]:
            try:
                ref = ContentReference.model_validate(raw)
            except ValidationError as error:
                raise invalid("invalid_export_request", "An export reference is malformed", error.errors()) from error
            references.append((ref.kind, ref.id))
        if len(references) != len(set(references)):
            raise invalid("duplicate_export_reference", "Export references must be unique")
        items = [repository.lookup(kind, media_id) for kind, media_id in references]
        package, _ = build_export(items)
        import hashlib
        package_hash = hashlib.sha256(package).hexdigest()
        with database.transaction() as connection:
            connection.execute("INSERT OR IGNORE INTO export_records(id, content_sha256, item_count, created_at) VALUES (?, ?, ?, ?)", (package_hash[:24], package_hash, len(items), utc_now()))
        return Response(content=package, media_type="application/zip", headers={"Content-Disposition": f'attachment; filename="media-catalog-{package_hash[:12]}.zip"', "X-Package-SHA256": package_hash})

    @app.get("/catalogs/exports")
    async def export_history(limit: int = Query(default=50, ge=1, le=200), offset: int = Query(default=0, ge=0, le=1_000_000)) -> dict[str, Any]:
        return repository.export_history(limit, offset)

    @app.post("/catalogs/exports/validate")
    async def validate_export_package(request: Request) -> dict[str, Any]:
        content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if content_type not in {"application/zip", "application/octet-stream"}:
            raise DomainError(415, "unsupported_media_type", "Send an application/zip request body")
        parts = []
        size = 0
        async for chunk in request.stream():
            size += len(chunk)
            if size > MAX_UPLOAD_BYTES:
                raise DomainError(413, "package_too_large", "ZIP package exceeds the 8 MiB upload limit")
            parts.append(chunk)
        return validate_export(b"".join(parts))

    return app


app = create_app()
