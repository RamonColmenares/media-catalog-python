"""Movie HTTP service."""

from __future__ import annotations

import os

from fastapi import FastAPI

from media_catalog.database import Database
from media_catalog.models import Movie


def create_app(database_path: str | None = None) -> FastAPI:
    database = Database(database_path or os.getenv("MOVIE_DB_PATH", "./data/movies.sqlite3"))
    database.initialize()
    app = FastAPI(title="Movie service", version="1.0.0")
    app.state.database = database

    @app.get("/movies", response_model=list[Movie])
    def movie_list() -> list[dict]:
        return database.list_movies()

    @app.post("/movies", response_model=Movie)
    def movie_save(movie: Movie) -> dict:
        return database.save_movie(movie)

    @app.get("/movies/{genre}", response_model=list[Movie])
    def movie_list_by_genre(genre: str) -> list[dict]:
        return database.list_movies(genre)

    return app


app = create_app()
