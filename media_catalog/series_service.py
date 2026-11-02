"""Series HTTP service, including nested season and chapter data."""

from __future__ import annotations

import os

from fastapi import FastAPI
from starlette.responses import Response

from media_catalog.database import Database
from media_catalog.models import Series


def create_app(database_path: str | None = None) -> FastAPI:
    database = Database(database_path or os.getenv("SERIES_DB_PATH", "./data/series.sqlite3"))
    database.initialize()
    app = FastAPI(title="Series service", version="1.0.0")
    app.state.database = database

    @app.get("/series", response_model=list[Series])
    def series_list() -> list[dict] | Response:
        items = database.list_series()
        return items if items else Response(status_code=404)

    @app.get("/series/{genre}", response_model=list[Series])
    def series_list_by_genre(genre: str) -> list[dict] | Response:
        items = database.list_series(genre)
        return items if items else Response(status_code=404)

    @app.post("/series", response_model=Series)
    def series_save(series: Series) -> dict:
        return database.save_series(series)

    return app


app = create_app()
