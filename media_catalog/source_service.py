"""HTTP route assembly shared by the independently owned movie and series services."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Literal

from fastapi import FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from media_catalog.content_repository import SourceRepository
from media_catalog.database import Database
from media_catalog.errors import DomainError
from media_catalog.models import AssetInput, EpisodeInput, LicenseInput, MovieCreate, MovieUpdate, SeasonInput, SeriesCreate, SeriesUpdate, TransitionRequest
from media_catalog.series_repository import SeriesRepository


def _error_response(error: DomainError) -> JSONResponse:
    return JSONResponse(status_code=error.status_code, content={"error": {"code": error.code, "message": error.message, "details": error.details}})


def create_source_app(kind: Literal["movie", "series"], database_path: str | None = None) -> FastAPI:
    table = "movies" if kind == "movie" else "series"
    path_env = "MOVIE_DB_PATH" if kind == "movie" else "SERIES_DB_PATH"
    default_path = "./data/movies.sqlite3" if kind == "movie" else "./data/series.sqlite3"
    database = Database(database_path or os.getenv(path_env, default_path), role=kind)
    database.initialize()
    repository = SourceRepository(database, kind)
    series_repository = SeriesRepository(repository) if kind == "series" else None
    app = FastAPI(title=f"{kind.title()} service", version="2.0.0")
    app.state.database = database
    app.state.repository = repository

    @app.exception_handler(DomainError)
    async def handle_domain_error(request: Request, error: DomainError) -> JSONResponse:
        return _error_response(error)

    @app.exception_handler(RequestValidationError)
    async def handle_request_error(request: Request, error: RequestValidationError) -> JSONResponse:
        details = [{"path": ".".join(str(part) for part in item["loc"][1:]), "message": item["msg"], "type": item["type"]} for item in error.errors()]
        return JSONResponse(status_code=422, content={"error": {"code": "invalid_request", "message": "Request validation failed", "details": details}})

    def audit_actor(x_audit_actor: str | None) -> str:
        return (x_audit_actor or "local-operator").strip()[:120] or "local-operator"

    @app.get(f"/{table}/health")
    async def health() -> dict[str, Any]:
        return {"service": kind, "live": True, **database.health()}

    @app.get(f"/{table}/changes")
    async def changes(after: int = Query(default=0, ge=0), limit: int = Query(default=100, ge=1, le=200)) -> dict[str, Any]:
        return repository.changes(after, limit)

    @app.get(f"/{table}/snapshot")
    async def snapshot() -> dict[str, Any]:
        return repository.snapshot()

    @app.get(f"/{table}")
    async def list_content(
        genre: str | None = None,
        status: Literal["draft", "published", "archived"] | None = None,
        limit: int = Query(default=50, ge=1, le=200),
        offset: int = Query(default=0, ge=0, le=1_000_000),
    ) -> dict[str, Any]:
        return repository.list(genre=genre, status=status, limit=limit, offset=offset)

    @app.get(f"/{table}/duplicates")
    async def duplicate_candidates() -> dict[str, Any]:
        return {"items": repository.duplicates()}

    @app.get(f"/{table}/reports/assets")
    async def asset_report() -> dict[str, Any]:
        return repository.asset_quality()

    if kind == "movie":
        @app.post("/movies", status_code=201)
        async def create_movie(
            content: MovieCreate,
            x_audit_actor: str | None = Header(default=None),
            idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", min_length=8, max_length=120),
        ) -> dict[str, Any]:
            return repository.create(content, actor=audit_actor(x_audit_actor), idempotency_key=idempotency_key)

        @app.put("/movies/{entity_id}")
        async def update_movie(
            entity_id: int,
            content: MovieUpdate,
            x_audit_actor: str | None = Header(default=None),
            idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", min_length=8, max_length=120),
        ) -> dict[str, Any]:
            return repository.update(entity_id, content, content.expected_version, actor=audit_actor(x_audit_actor), idempotency_key=idempotency_key)
    else:
        @app.post("/series", status_code=201)
        async def create_series(
            content: SeriesCreate,
            x_audit_actor: str | None = Header(default=None),
            idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", min_length=8, max_length=120),
        ) -> dict[str, Any]:
            return repository.create(content, actor=audit_actor(x_audit_actor), idempotency_key=idempotency_key)

        @app.put("/series/{entity_id}")
        async def update_series(
            entity_id: int,
            content: SeriesUpdate,
            x_audit_actor: str | None = Header(default=None),
            idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", min_length=8, max_length=120),
        ) -> dict[str, Any]:
            return repository.update(entity_id, content, content.expected_version, actor=audit_actor(x_audit_actor), idempotency_key=idempotency_key)

    @app.get(f"/{table}/{{entity_id}}")
    async def get_content(entity_id: int) -> dict[str, Any]:
        return repository.get(entity_id)

    @app.get(f"/{table}/{{entity_id}}/readiness")
    async def readiness(entity_id: int) -> dict[str, Any]:
        return repository.readiness(entity_id)

    @app.post(f"/{table}/{{entity_id}}/transition")
    async def transition(entity_id: int, request: TransitionRequest, x_audit_actor: str | None = Header(default=None)) -> dict[str, Any]:
        return repository.transition(entity_id, request.action, request.expected_version, actor=audit_actor(x_audit_actor), reason=request.reason)

    @app.get(f"/{table}/{{entity_id}}/licenses")
    async def list_licenses(entity_id: int) -> dict[str, Any]:
        return {"items": repository.list_licenses(entity_id)}

    @app.post(f"/{table}/{{entity_id}}/licenses", status_code=201)
    async def add_license(entity_id: int, license_input: LicenseInput, x_audit_actor: str | None = Header(default=None)) -> dict[str, Any]:
        return repository.add_license(entity_id, license_input, actor=audit_actor(x_audit_actor))

    @app.delete(f"/{table}/{{entity_id}}/licenses/{{license_id}}")
    async def remove_license(entity_id: int, license_id: int, x_audit_actor: str | None = Header(default=None)) -> dict[str, Any]:
        return repository.remove_license(entity_id, license_id, actor=audit_actor(x_audit_actor))

    @app.get(f"/{table}/{{entity_id}}/availability")
    async def availability(entity_id: int, region: str = Query(min_length=2, max_length=5), at: datetime = Query()) -> dict[str, Any]:
        if at.tzinfo is None:
            raise DomainError(422, "invalid_time", "at must contain a timezone offset")
        return repository.availability(entity_id, region, at)

    @app.get(f"/{table}/{{entity_id}}/assets")
    async def list_assets(entity_id: int) -> dict[str, Any]:
        return {"items": repository.list_assets(entity_id)}

    @app.post(f"/{table}/{{entity_id}}/assets", status_code=201)
    async def add_asset(entity_id: int, asset: AssetInput, x_audit_actor: str | None = Header(default=None)) -> dict[str, Any]:
        return repository.add_asset(entity_id, asset, actor=audit_actor(x_audit_actor))

    @app.delete(f"/{table}/{{entity_id}}/assets/{{asset_id}}")
    async def remove_asset(entity_id: int, asset_id: int, x_audit_actor: str | None = Header(default=None)) -> dict[str, Any]:
        return repository.remove_asset(entity_id, asset_id, actor=audit_actor(x_audit_actor))

    @app.get(f"/{table}/{{entity_id}}/audit")
    async def audit_history(entity_id: int) -> dict[str, Any]:
        return {"items": repository.audit(entity_id)}

    if kind == "series":
        @app.post("/series/{entity_id}/seasons", status_code=201)
        async def add_season(
            entity_id: int,
            season: SeasonInput,
            expected_version: int = Query(alias="expectedVersion", ge=1),
            x_audit_actor: str | None = Header(default=None),
        ) -> dict[str, Any]:
            return series_repository.add_season(entity_id, season, expected_version, audit_actor(x_audit_actor))

        @app.delete("/series/{entity_id}/seasons/{season_id}")
        async def remove_season(
            entity_id: int,
            season_id: int,
            expected_version: int = Query(alias="expectedVersion", ge=1),
            x_audit_actor: str | None = Header(default=None),
        ) -> dict[str, Any]:
            return series_repository.remove_season(entity_id, season_id, expected_version, audit_actor(x_audit_actor))

        @app.post("/series/{entity_id}/seasons/{season_id}/episodes", status_code=201)
        async def add_episode(
            entity_id: int,
            season_id: int,
            episode: EpisodeInput,
            expected_version: int = Query(alias="expectedVersion", ge=1),
            x_audit_actor: str | None = Header(default=None),
        ) -> dict[str, Any]:
            return series_repository.add_episode(entity_id, season_id, episode, expected_version, audit_actor(x_audit_actor))

        @app.put("/series/{entity_id}/seasons/{season_id}/episodes/{episode_id}")
        async def update_episode(
            entity_id: int,
            season_id: int,
            episode_id: int,
            episode: EpisodeInput,
            expected_version: int = Query(alias="expectedVersion", ge=1),
            x_audit_actor: str | None = Header(default=None),
        ) -> dict[str, Any]:
            return series_repository.update_episode(entity_id, season_id, episode_id, episode, expected_version, audit_actor(x_audit_actor))

        @app.delete("/series/{entity_id}/seasons/{season_id}/episodes/{episode_id}")
        async def remove_episode(
            entity_id: int,
            season_id: int,
            episode_id: int,
            expected_version: int = Query(alias="expectedVersion", ge=1),
            x_audit_actor: str | None = Header(default=None),
        ) -> dict[str, Any]:
            return series_repository.remove_episode(entity_id, season_id, episode_id, expected_version, audit_actor(x_audit_actor))

        @app.get("/series/{entity_id}/structure-report")
        async def structure_report(entity_id: int) -> dict[str, Any]:
            return repository.structure_report(entity_id)

        @app.put("/series/{entity_id}/seasons/{season_id}/reorder")
        async def reorder_episodes(entity_id: int, season_id: int, request: dict[str, list[int]], x_audit_actor: str | None = Header(default=None)) -> dict[str, Any]:
            if set(request) != {"episodeIds"} or not isinstance(request["episodeIds"], list) or any(not isinstance(item, int) for item in request["episodeIds"]):
                raise DomainError(422, "invalid_request", "Body must contain an episodeIds integer array")
            return repository.reorder_episodes(entity_id, season_id, request["episodeIds"], actor=audit_actor(x_audit_actor))

    return app
