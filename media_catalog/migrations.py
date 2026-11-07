"""Versioned migrations for the three independently owned SQLite schemas."""

from __future__ import annotations

import json
import sqlite3
import uuid
from typing import Literal


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in connection.execute(f"PRAGMA table_info({table})")}


def _add_column(connection: sqlite3.Connection, table: str, name: str, definition: str) -> None:
    if name not in _columns(connection, table):
        connection.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")


def _stamp(connection: sqlite3.Connection, version: int, role: str) -> None:
    connection.execute(
        "INSERT INTO schema_migrations(version, role, applied_at) VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
        (version, role),
    )


def migrate_source(connection: sqlite3.Connection, role: Literal["movie", "series"]) -> None:
    """Bring an old movie/series database forward without discarding its rows."""
    connection.execute("BEGIN IMMEDIATE")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER NOT NULL, role TEXT NOT NULL, applied_at TEXT NOT NULL, PRIMARY KEY(version, role))"
    )
    current = connection.execute(
        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations WHERE role = ?", (role,)
    ).fetchone()[0]
    table = "movies" if role == "movie" else "series"
    if current < 1:
        if role == "movie":
            connection.execute(
                "CREATE TABLE IF NOT EXISTS movies (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, genre TEXT, ulr_stream TEXT)"
            )
            if "ulr_stream" in _columns(connection, table) and "stream_url" not in _columns(connection, table):
                connection.execute("ALTER TABLE movies RENAME COLUMN ulr_stream TO stream_url")
        else:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS series (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, genre TEXT, seasons_json TEXT)"
            )
            _add_column(connection, table, "seasons_json", "TEXT")
        _add_column(connection, table, "synopsis", "TEXT NOT NULL DEFAULT ''")
        _add_column(connection, table, "release_year", "INTEGER")
        _add_column(connection, table, "language", "TEXT NOT NULL DEFAULT 'und'")
        _add_column(connection, table, "external_id", "TEXT")
        _add_column(connection, table, "status", "TEXT NOT NULL DEFAULT 'draft'")
        _add_column(connection, table, "version", "INTEGER NOT NULL DEFAULT 1")
        _add_column(connection, table, "created_at", "TEXT NOT NULL DEFAULT ''")
        _add_column(connection, table, "updated_at", "TEXT NOT NULL DEFAULT ''")
        if role == "movie":
            _add_column(connection, table, "runtime_minutes", "INTEGER")
            _add_column(connection, table, "stream_url", "TEXT")
        else:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS seasons (id INTEGER PRIMARY KEY AUTOINCREMENT, series_id INTEGER NOT NULL REFERENCES series(id) ON DELETE CASCADE, season_number INTEGER NOT NULL, position INTEGER NOT NULL DEFAULT 0, UNIQUE(series_id, season_number))"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS episodes (id INTEGER PRIMARY KEY AUTOINCREMENT, season_id INTEGER NOT NULL REFERENCES seasons(id) ON DELETE CASCADE, name TEXT NOT NULL, episode_number INTEGER NOT NULL, synopsis TEXT NOT NULL DEFAULT '', runtime_minutes INTEGER, stream_url TEXT, position INTEGER NOT NULL DEFAULT 0, UNIQUE(season_id, episode_number))"
            )
            legacy_rows = connection.execute(
                "SELECT id, seasons_json FROM series WHERE seasons_json IS NOT NULL ORDER BY id"
            ).fetchall()
            for legacy in legacy_rows:
                if connection.execute("SELECT 1 FROM seasons WHERE series_id = ? LIMIT 1", (legacy["id"],)).fetchone():
                    continue
                try:
                    old_seasons = json.loads(legacy["seasons_json"])
                except (TypeError, json.JSONDecodeError) as error:
                    raise ValueError(f"Series {legacy['id']} has malformed legacy seasons JSON") from error
                if not isinstance(old_seasons, list):
                    raise ValueError(f"Series {legacy['id']} legacy seasons must be a JSON array")
                seen_seasons: set[int] = set()
                for season_position, season in enumerate(old_seasons):
                    if not isinstance(season, dict):
                        raise ValueError(f"Series {legacy['id']} contains a non-object legacy season")
                    season_number = season.get("seasonNumber", 0)
                    if not isinstance(season_number, int) or season_number < 1:
                        season_number = season_position + 1
                    if season_number in seen_seasons:
                        raise ValueError(f"Series {legacy['id']} contains duplicate legacy seasonNumber {season_number}")
                    seen_seasons.add(season_number)
                    legacy_season_id = season.get("id")
                    if legacy_season_id is not None:
                        if not isinstance(legacy_season_id, int) or legacy_season_id < 1 or connection.execute("SELECT 1 FROM seasons WHERE id=?", (legacy_season_id,)).fetchone():
                            raise ValueError(f"Series {legacy['id']} has an invalid or duplicate legacy season ID")
                        connection.execute(
                            "INSERT INTO seasons(id, series_id, season_number, position) VALUES (?, ?, ?, ?)",
                            (legacy_season_id, legacy["id"], season_number, season_position),
                        )
                    else:
                        connection.execute(
                            "INSERT INTO seasons(series_id, season_number, position) VALUES (?, ?, ?)",
                            (legacy["id"], season_number, season_position),
                        )
                    season_id = connection.execute(
                        "SELECT id FROM seasons WHERE series_id = ? AND season_number = ?",
                        (legacy["id"], season_number),
                    ).fetchone()[0]
                    episodes = season.get("episodes", season.get("chapters", []))
                    if not isinstance(episodes, list):
                        raise ValueError(f"Series {legacy['id']} season {season_number} episodes must be a JSON array")
                    seen_episodes: set[int] = set()
                    for episode_position, episode in enumerate(episodes):
                        if not isinstance(episode, dict):
                            raise ValueError(f"Series {legacy['id']} season {season_number} contains a non-object legacy episode")
                        episode_number = episode.get("episodeNumber", episode.get("number", episode_position + 1))
                        if not isinstance(episode_number, int) or episode_number < 1:
                            episode_number = episode_position + 1
                        if episode_number in seen_episodes:
                            raise ValueError(f"Series {legacy['id']} season {season_number} contains duplicate legacy episodeNumber {episode_number}")
                        seen_episodes.add(episode_number)
                        fields = (season_id, str(episode.get("name") or f"Episode {episode_number}"), episode_number, "", None, episode.get("streamUrl", episode.get("urlStream")), episode_position)
                        legacy_episode_id = episode.get("id")
                        if legacy_episode_id is not None:
                            if not isinstance(legacy_episode_id, int) or legacy_episode_id < 1 or connection.execute("SELECT 1 FROM episodes WHERE id=?", (legacy_episode_id,)).fetchone():
                                raise ValueError(f"Series {legacy['id']} has an invalid or duplicate legacy episode ID")
                            connection.execute(
                                "INSERT INTO episodes(id, season_id, name, episode_number, synopsis, runtime_minutes, stream_url, position) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                (legacy_episode_id, *fields),
                            )
                        else:
                            connection.execute(
                                "INSERT INTO episodes(season_id, name, episode_number, synopsis, runtime_minutes, stream_url, position) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                fields,
                            )
            _add_column(connection, "episodes", "runtime_minutes", "INTEGER")
            connection.execute("CREATE INDEX IF NOT EXISTS episodes_season_position_idx ON episodes(season_id, position, id)")
            connection.execute("CREATE INDEX IF NOT EXISTS seasons_series_position_idx ON seasons(series_id, position, id)")
            connection.execute("UPDATE series SET seasons_json=NULL WHERE seasons_json IS NOT NULL")
        connection.execute(f"UPDATE {table} SET created_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE created_at = ''")
        connection.execute(f"UPDATE {table} SET updated_at = created_at WHERE updated_at = ''")
        connection.execute(f"CREATE INDEX IF NOT EXISTS {table}_genre_idx ON {table}(genre)")
        connection.execute(f"CREATE INDEX IF NOT EXISTS {table}_status_idx ON {table}(status)")
        connection.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {table}_external_id_idx ON {table}(external_id) WHERE external_id IS NOT NULL")
        _stamp(connection, 1, role)
        current = 1
    if current < 2:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS source_state (id INTEGER PRIMARY KEY CHECK(id = 1), revision INTEGER NOT NULL DEFAULT 0)"
        )
        connection.execute("INSERT OR IGNORE INTO source_state(id, revision) VALUES (1, 0)")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS source_outbox (sequence INTEGER PRIMARY KEY AUTOINCREMENT, entity_kind TEXT NOT NULL, entity_id INTEGER NOT NULL, revision INTEGER NOT NULL, event_type TEXT NOT NULL, payload_json TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
        connection.execute("CREATE INDEX IF NOT EXISTS source_outbox_entity_idx ON source_outbox(entity_kind, entity_id, revision)")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, entity_kind TEXT NOT NULL, entity_id INTEGER NOT NULL, action TEXT NOT NULL, actor TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '', version INTEGER NOT NULL, summary_json TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
        connection.execute("CREATE INDEX IF NOT EXISTS audit_entity_idx ON audit_log(entity_kind, entity_id, id)")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS idempotency_keys (key TEXT PRIMARY KEY, payload_hash TEXT NOT NULL, response_json TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS licenses (id INTEGER PRIMARY KEY AUTOINCREMENT, entity_kind TEXT NOT NULL, entity_id INTEGER NOT NULL, region TEXT NOT NULL, starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, rights_holder TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(entity_kind, entity_id, region, starts_at, ends_at))"
        )
        connection.execute("CREATE INDEX IF NOT EXISTS licenses_entity_region_idx ON licenses(entity_kind, entity_id, region, starts_at, ends_at)")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS assets (id INTEGER PRIMARY KEY AUTOINCREMENT, entity_kind TEXT NOT NULL, entity_id INTEGER NOT NULL, path TEXT NOT NULL, role TEXT NOT NULL, mime_type TEXT NOT NULL, size_bytes INTEGER NOT NULL, checksum_sha256 TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(entity_kind, entity_id, path))"
        )
        connection.execute("CREATE INDEX IF NOT EXISTS assets_checksum_idx ON assets(checksum_sha256)")
        _stamp(connection, 2, role)
    if current < 3:
        _add_column(connection, "source_state", "generation", "TEXT")
        state = connection.execute("SELECT generation FROM source_state WHERE id=1").fetchone()
        if state is None:
            connection.execute("INSERT INTO source_state(id, revision, generation) VALUES (1, 0, ?)", (str(uuid.uuid4()),))
        elif not state["generation"]:
            connection.execute("UPDATE source_state SET generation=? WHERE id=1", (str(uuid.uuid4()),))
        content_revision = connection.execute(f"SELECT COALESCE(MAX(version), 0) FROM {table}").fetchone()[0]
        connection.execute("UPDATE source_state SET revision=MAX(revision, ?) WHERE id=1", (content_revision,))
        _stamp(connection, 3, role)
    connection.commit()


def migrate_catalog(connection: sqlite3.Connection) -> None:
    """Create only catalog-owned projection, workflow, and operator tables."""
    connection.execute("BEGIN IMMEDIATE")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER NOT NULL, role TEXT NOT NULL, applied_at TEXT NOT NULL, PRIMARY KEY(version, role))"
    )
    current = connection.execute(
        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations WHERE role = 'catalog'"
    ).fetchone()[0]
    if current < 1:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS catalog_items (kind TEXT NOT NULL CHECK(kind IN ('movie','series')), media_id INTEGER NOT NULL, revision INTEGER NOT NULL, payload_json TEXT NOT NULL, projected_at TEXT NOT NULL, PRIMARY KEY(kind, media_id))"
        )
        connection.execute("CREATE INDEX IF NOT EXISTS catalog_items_kind_idx ON catalog_items(kind, media_id)")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS sync_cursors (kind TEXT PRIMARY KEY CHECK(kind IN ('movie','series')), sequence INTEGER NOT NULL DEFAULT 0, high_water_mark INTEGER NOT NULL DEFAULT 0, last_success_at TEXT, last_error TEXT)"
        )
        connection.execute("INSERT OR IGNORE INTO sync_cursors(kind) VALUES ('movie'), ('series')")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS labels (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, color TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS content_labels (label_id INTEGER NOT NULL REFERENCES labels(id) ON DELETE CASCADE, kind TEXT NOT NULL, media_id INTEGER NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(label_id, kind, media_id))"
        )
        connection.execute("CREATE INDEX IF NOT EXISTS content_labels_item_idx ON content_labels(kind, media_id)")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS collections (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE COLLATE NOCASE, description TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS collection_members (collection_id INTEGER NOT NULL REFERENCES collections(id) ON DELETE CASCADE, position INTEGER NOT NULL, kind TEXT NOT NULL, media_id INTEGER NOT NULL, PRIMARY KEY(collection_id, position), UNIQUE(collection_id, kind, media_id))"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS saved_searches (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE COLLATE NOCASE, query_json TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS import_jobs (id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE, payload_hash TEXT NOT NULL, state TEXT NOT NULL, row_count INTEGER NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, error TEXT)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS import_rows (job_id TEXT NOT NULL REFERENCES import_jobs(id) ON DELETE CASCADE, row_number INTEGER NOT NULL, kind TEXT NOT NULL, payload_json TEXT NOT NULL, operation_key TEXT NOT NULL, state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, result_json TEXT, error TEXT, updated_at TEXT NOT NULL, PRIMARY KEY(job_id, row_number), UNIQUE(operation_key))"
        )
        connection.execute("CREATE INDEX IF NOT EXISTS import_rows_state_idx ON import_rows(job_id, state, row_number)")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS export_records (id TEXT PRIMARY KEY, content_sha256 TEXT NOT NULL, item_count INTEGER NOT NULL, created_at TEXT NOT NULL)"
        )
        _stamp(connection, 1, "catalog")
    if current < 2:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS catalog_sync_runs (id INTEGER PRIMARY KEY AUTOINCREMENT, started_at TEXT NOT NULL, finished_at TEXT, status TEXT NOT NULL, sources_json TEXT NOT NULL, applied_events INTEGER NOT NULL DEFAULT 0, error TEXT)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS projection_rebuilds (id INTEGER PRIMARY KEY AUTOINCREMENT, started_at TEXT NOT NULL, finished_at TEXT, status TEXT NOT NULL, movie_high_water INTEGER, series_high_water INTEGER, error TEXT)"
        )
        _stamp(connection, 2, "catalog")
    if current < 3:
        _add_column(connection, "sync_cursors", "source_id", "TEXT")
        _stamp(connection, 3, "catalog")
    if current < 4:
        _add_column(connection, "collections", "version", "INTEGER NOT NULL DEFAULT 1")
        _add_column(connection, "saved_searches", "version", "INTEGER NOT NULL DEFAULT 1")
        _stamp(connection, 4, "catalog")
    if current < 5:
        _add_column(connection, "labels", "version", "INTEGER NOT NULL DEFAULT 1")
        _stamp(connection, 5, "catalog")
    connection.commit()
