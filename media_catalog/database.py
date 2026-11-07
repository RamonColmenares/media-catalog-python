"""SQLite connection management and role-scoped schema initialization."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Literal

from media_catalog.migrations import migrate_catalog, migrate_source

DatabaseRole = Literal["movie", "series", "catalog"]


class ClosingConnection(sqlite3.Connection):
    """Give SQLite's context manager deterministic close semantics for reads."""

    def __exit__(self, exception_type, exception, traceback):
        try:
            return super().__exit__(exception_type, exception, traceback)
        finally:
            self.close()


class Database:
    """A single-process database handle; each service owns one file and schema."""

    def __init__(self, path: str | Path, role: DatabaseRole = "movie") -> None:
        if role not in {"movie", "series", "catalog"}:
            raise ValueError("database role must be movie, series, or catalog")
        self.path = str(path)
        self.role = role

    def connect(self) -> sqlite3.Connection:
        if self.path != ":memory:":
            Path(self.path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=10, isolation_level="DEFERRED", factory=ClosingConnection)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 10000")
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            if self.role == "catalog":
                migrate_catalog(connection)
            else:
                migrate_source(connection, self.role)

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        """Run a short write transaction, rolling back on every exception."""
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def health(self) -> dict[str, str | bool]:
        try:
            with self.connect() as connection:
                result = connection.execute("PRAGMA quick_check").fetchone()[0]
                version = connection.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0]
            return {"database": True, "integrity": result, "schemaVersion": int(version or 0)}
        except sqlite3.Error:
            return {"database": False, "integrity": "unavailable", "schemaVersion": 0}
