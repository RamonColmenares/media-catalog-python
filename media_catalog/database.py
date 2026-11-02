"""Small SQLite repositories used by the independently running services."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from media_catalog.models import Movie, Series


def _wire_movie(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "name": row["name"],
        "genre": row["genre"],
        "ulrStream": row["ulr_stream"],
    }


def _wire_series(row: sqlite3.Row) -> dict[str, Any]:
    value = {
        "id": row["id"],
        "name": row["name"],
        "genre": row["genre"],
        "seasons": json.loads(row["seasons_json"]) if row["seasons_json"] is not None else None,
    }
    return value


class Database:
    """A database file owned by one service."""

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)

    def connect(self) -> sqlite3.Connection:
        if self.path != ":memory:":
            Path(self.path).expanduser().parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS movies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    genre TEXT,
                    ulr_stream TEXT
                );
                CREATE INDEX IF NOT EXISTS movies_genre_idx ON movies(genre);

                CREATE TABLE IF NOT EXISTS series (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    genre TEXT,
                    seasons_json TEXT
                );
                CREATE INDEX IF NOT EXISTS series_genre_idx ON series(genre);

                CREATE TABLE IF NOT EXISTS catalog_movie_cache (
                    media_id INTEGER PRIMARY KEY,
                    genre TEXT,
                    payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS catalog_movie_cache_genre_idx
                    ON catalog_movie_cache(genre);

                CREATE TABLE IF NOT EXISTS catalog_series_cache (
                    media_id INTEGER PRIMARY KEY,
                    genre TEXT,
                    payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS catalog_series_cache_genre_idx
                    ON catalog_series_cache(genre);
                """
            )

    def save_movie(self, movie: Movie | dict[str, Any]) -> dict[str, Any]:
        data = movie.model_dump(by_alias=True) if isinstance(movie, Movie) else Movie.model_validate(movie).model_dump(by_alias=True)
        with self.connect() as connection:
            if data["id"] is None:
                cursor = connection.execute(
                    "INSERT INTO movies (name, genre, ulr_stream) VALUES (?, ?, ?)",
                    (data["name"], data["genre"], data["ulrStream"]),
                )
                movie_id = cursor.lastrowid
            else:
                connection.execute(
                    """INSERT INTO movies (id, name, genre, ulr_stream) VALUES (?, ?, ?, ?)
                       ON CONFLICT(id) DO UPDATE SET
                         name = excluded.name, genre = excluded.genre, ulr_stream = excluded.ulr_stream""",
                    (data["id"], data["name"], data["genre"], data["ulrStream"]),
                )
                movie_id = data["id"]
            row = connection.execute("SELECT * FROM movies WHERE id = ?", (movie_id,)).fetchone()
        return _wire_movie(row)

    def list_movies(self, genre: str | None = None) -> list[dict[str, Any]]:
        with self.connect() as connection:
            if genre is None:
                rows = connection.execute("SELECT * FROM movies ORDER BY id").fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM movies WHERE genre = ? ORDER BY id", (genre,)
                ).fetchall()
        return [_wire_movie(row) for row in rows]

    def save_series(self, series: Series | dict[str, Any]) -> dict[str, Any]:
        data = series.model_dump(by_alias=True) if isinstance(series, Series) else Series.model_validate(series).model_dump(by_alias=True)
        seasons_json = json.dumps(data["seasons"], separators=(",", ":")) if data["seasons"] is not None else None
        with self.connect() as connection:
            if data["id"] is None:
                cursor = connection.execute(
                    "INSERT INTO series (name, genre, seasons_json) VALUES (?, ?, ?)",
                    (data["name"], data["genre"], seasons_json),
                )
                series_id = cursor.lastrowid
            else:
                connection.execute(
                    """INSERT INTO series (id, name, genre, seasons_json) VALUES (?, ?, ?, ?)
                       ON CONFLICT(id) DO UPDATE SET
                         name = excluded.name, genre = excluded.genre, seasons_json = excluded.seasons_json""",
                    (data["id"], data["name"], data["genre"], seasons_json),
                )
                series_id = data["id"]
            row = connection.execute("SELECT * FROM series WHERE id = ?", (series_id,)).fetchone()
        return _wire_series(row)

    def list_series(self, genre: str | None = None) -> list[dict[str, Any]]:
        with self.connect() as connection:
            if genre is None:
                rows = connection.execute("SELECT * FROM series ORDER BY id").fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM series WHERE genre = ? ORDER BY id", (genre,)
                ).fetchall()
        return [_wire_series(row) for row in rows]

    def cache_movies(self, movies: list[dict[str, Any]]) -> None:
        with self.connect() as connection:
            for movie in movies:
                if movie.get("id") is None:
                    continue
                connection.execute(
                    """INSERT INTO catalog_movie_cache (media_id, genre, payload_json)
                       VALUES (?, ?, ?) ON CONFLICT(media_id) DO UPDATE SET
                         genre = excluded.genre, payload_json = excluded.payload_json""",
                    (movie["id"], movie.get("genre"), json.dumps(movie, separators=(",", ":"))),
                )

    def cache_series(self, series: list[dict[str, Any]]) -> None:
        with self.connect() as connection:
            for item in series:
                if item.get("id") is None:
                    continue
                connection.execute(
                    """INSERT INTO catalog_series_cache (media_id, genre, payload_json)
                       VALUES (?, ?, ?) ON CONFLICT(media_id) DO UPDATE SET
                         genre = excluded.genre, payload_json = excluded.payload_json""",
                    (item["id"], item.get("genre"), json.dumps(item, separators=(",", ":"))),
                )

    def cached_items(self, kind: str, genre: str) -> list[dict[str, Any]]:
        table = {"movies": "catalog_movie_cache", "series": "catalog_series_cache"}.get(kind)
        if table is None:
            raise ValueError(f"Unsupported cache kind: {kind}")
        with self.connect() as connection:
            rows = connection.execute(
                f"SELECT payload_json FROM {table} WHERE genre = ? ORDER BY media_id", (genre,)
            ).fetchall()
        return [json.loads(row["payload_json"]) for row in rows]
