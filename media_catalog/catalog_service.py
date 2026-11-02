"""Catalog HTTP service that queries and caches both upstream services."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import AsyncIterator
from urllib.parse import quote

import httpx
from fastapi import FastAPI, HTTPException

from media_catalog.database import Database
from media_catalog.models import Movie


def create_app(database_path: str | None = None) -> FastAPI:
    database = Database(database_path or os.getenv("CATALOG_DB_PATH", "./data/catalog.sqlite3"))
    database.initialize()
    movie_base_url = os.getenv("MOVIE_SERVICE_URL", "http://127.0.0.1:8102").rstrip("/")
    series_base_url = os.getenv("SERIES_SERVICE_URL", "http://127.0.0.1:8103").rstrip("/")
    request_timeout = float(os.getenv("SERVICE_REQUEST_TIMEOUT", "3"))

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        async with httpx.AsyncClient(timeout=request_timeout) as client:
            app.state.http_client = client
            yield

    app = FastAPI(title="Catalog service", version="1.0.0", lifespan=lifespan)
    app.state.database = database

    async def get_movies(genre: str) -> list[dict]:
        url = f"{movie_base_url}/movies/{quote(genre, safe='')}"
        try:
            response = await app.state.http_client.get(url)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as error:
            raise HTTPException(
                status_code=502,
                detail=f"Movie service returned HTTP {error.response.status_code}",
            ) from error
        except (httpx.RequestError, ValueError) as error:
            raise HTTPException(status_code=502, detail="Movie service request failed") from error

    async def get_series(genre: str) -> list[dict]:
        url = f"{series_base_url}/series/{quote(genre, safe='')}"
        try:
            response = await app.state.http_client.get(url)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as error:
            raise HTTPException(
                status_code=502,
                detail=f"Series service returned HTTP {error.response.status_code}",
            ) from error
        except (httpx.RequestError, ValueError) as error:
            raise HTTPException(status_code=502, detail="Series service request failed") from error

    @app.get("/catalogs/movies/{genre}", response_model=list[Movie])
    async def movies_by_genre(genre: str) -> list[dict]:
        return await get_movies(genre)

    @app.get("/catalogs/{genre}", response_model=list[Movie])
    async def catalog_by_genre(genre: str) -> list[dict]:
        movies = await get_movies(genre)
        database.cache_movies(movies)
        series = await get_series(genre)
        database.cache_series(series)
        # This is the existing catalog contract: cache movies and series, return movies.
        return movies

    return app


app = create_app()
