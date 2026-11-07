"""Focused hierarchy mutations for series with parent-version concurrency."""

from __future__ import annotations

import sqlite3
from typing import Any

from media_catalog.content_repository import SourceRepository
from media_catalog.errors import conflict, invalid, not_found
from media_catalog.models import EpisodeInput, SeasonInput


class SeriesRepository:
    """Mutate individual seasons and episodes without replacing the full series."""

    def __init__(self, content: SourceRepository) -> None:
        if content.kind != "series":
            raise ValueError("SeriesRepository requires a series source repository")
        self.content = content
        self.database = content.database

    @staticmethod
    def _season(connection: sqlite3.Connection, series_id: int, season_id: int) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM seasons WHERE id=? AND series_id=?", (season_id, series_id)).fetchone()
        if row is None:
            raise not_found("season", season_id)
        return row

    def _finish(self, connection: sqlite3.Connection, series_id: int, action: str, actor: str, summary: dict[str, Any]) -> dict[str, Any]:
        self.content._touch(connection, series_id)
        parent = connection.execute("SELECT status FROM series WHERE id=?", (series_id,)).fetchone()
        if parent["status"] == "published":
            readiness = self.content._readiness(connection, series_id)
            if not readiness["ready"]:
                raise conflict("published_content_not_ready", "This hierarchy change would make published content fail readiness", {"issues": readiness["issues"]})
        return self.content._record(connection, series_id, action, actor, summary=summary)

    def add_season(self, series_id: int, season: SeasonInput, expected_version: int, actor: str = "local-operator") -> dict[str, Any]:
        data = season.model_dump(by_alias=True)
        if data.get("id") is not None or any(episode.get("id") is not None for episode in data["episodes"]):
            raise invalid("generated_id", "Season and episode IDs are assigned by the service")
        try:
            with self.database.transaction() as connection:
                self.content._assert_editable(connection, series_id, expected_version)
                position = connection.execute("SELECT COALESCE(MAX(position), -1)+1 FROM seasons WHERE series_id=?", (series_id,)).fetchone()[0]
                season_id = int(connection.execute("INSERT INTO seasons(series_id, season_number, position) VALUES (?, ?, ?)", (series_id, data["seasonNumber"], position)).lastrowid)
                for episode_position, episode in enumerate(data["episodes"]):
                    self._insert_episode(connection, season_id, episode, episode_position)
                return self._finish(connection, series_id, "season_added", actor, {"seasonId": season_id, "seasonNumber": data["seasonNumber"], "episodeCount": len(data["episodes"])})
        except sqlite3.IntegrityError as error:
            raise conflict("duplicate_hierarchy_number", "A season with that number already exists in this series") from error

    def remove_season(self, series_id: int, season_id: int, expected_version: int, actor: str = "local-operator") -> dict[str, Any]:
        with self.database.transaction() as connection:
            self.content._assert_editable(connection, series_id, expected_version)
            season = self._season(connection, series_id, season_id)
            connection.execute("DELETE FROM seasons WHERE id=?", (season_id,))
            remaining = connection.execute("SELECT id FROM seasons WHERE series_id=? ORDER BY position, season_number, id", (series_id,)).fetchall()
            for position, row in enumerate(remaining):
                connection.execute("UPDATE seasons SET position=? WHERE id=?", (position, row["id"]))
            return self._finish(connection, series_id, "season_removed", actor, {"seasonId": season_id, "seasonNumber": season["season_number"]})

    @staticmethod
    def _insert_episode(connection: sqlite3.Connection, season_id: int, data: dict[str, Any], position: int) -> int:
        return int(connection.execute(
            "INSERT INTO episodes(season_id, name, episode_number, synopsis, runtime_minutes, stream_url, position) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (season_id, data["name"], data["episodeNumber"], data["synopsis"], data["runtimeMinutes"], data["streamUrl"], position),
        ).lastrowid)

    def add_episode(self, series_id: int, season_id: int, episode: EpisodeInput, expected_version: int, actor: str = "local-operator") -> dict[str, Any]:
        data = episode.model_dump(by_alias=True)
        if data.get("id") is not None:
            raise invalid("generated_id", "Episode IDs are assigned by the service")
        try:
            with self.database.transaction() as connection:
                self.content._assert_editable(connection, series_id, expected_version)
                self._season(connection, series_id, season_id)
                position = connection.execute("SELECT COALESCE(MAX(position), -1)+1 FROM episodes WHERE season_id=?", (season_id,)).fetchone()[0]
                episode_id = self._insert_episode(connection, season_id, data, position)
                return self._finish(connection, series_id, "episode_added", actor, {"seasonId": season_id, "episodeId": episode_id, "episodeNumber": data["episodeNumber"]})
        except sqlite3.IntegrityError as error:
            raise conflict("duplicate_hierarchy_number", "An episode with that number already exists in this season") from error

    def update_episode(self, series_id: int, season_id: int, episode_id: int, episode: EpisodeInput, expected_version: int, actor: str = "local-operator") -> dict[str, Any]:
        data = episode.model_dump(by_alias=True)
        if data.get("id") is not None and data["id"] != episode_id:
            raise invalid("path_id_mismatch", "The body episode ID must match the episode ID in the path")
        try:
            with self.database.transaction() as connection:
                self.content._assert_editable(connection, series_id, expected_version)
                self._season(connection, series_id, season_id)
                row = connection.execute("SELECT position FROM episodes WHERE id=? AND season_id=?", (episode_id, season_id)).fetchone()
                if row is None:
                    raise not_found("episode", episode_id)
                connection.execute(
                    "UPDATE episodes SET name=?, episode_number=?, synopsis=?, runtime_minutes=?, stream_url=? WHERE id=?",
                    (data["name"], data["episodeNumber"], data["synopsis"], data["runtimeMinutes"], data["streamUrl"], episode_id),
                )
                return self._finish(connection, series_id, "episode_updated", actor, {"seasonId": season_id, "episodeId": episode_id, "episodeNumber": data["episodeNumber"]})
        except sqlite3.IntegrityError as error:
            raise conflict("duplicate_hierarchy_number", "An episode with that number already exists in this season") from error

    def remove_episode(self, series_id: int, season_id: int, episode_id: int, expected_version: int, actor: str = "local-operator") -> dict[str, Any]:
        with self.database.transaction() as connection:
            self.content._assert_editable(connection, series_id, expected_version)
            self._season(connection, series_id, season_id)
            row = connection.execute("SELECT episode_number FROM episodes WHERE id=? AND season_id=?", (episode_id, season_id)).fetchone()
            if row is None:
                raise not_found("episode", episode_id)
            connection.execute("DELETE FROM episodes WHERE id=?", (episode_id,))
            remaining = connection.execute("SELECT id FROM episodes WHERE season_id=? ORDER BY position, episode_number, id", (season_id,)).fetchall()
            for position, item in enumerate(remaining):
                connection.execute("UPDATE episodes SET position=? WHERE id=?", (position, item["id"]))
            return self._finish(connection, series_id, "episode_removed", actor, {"seasonId": season_id, "episodeId": episode_id, "episodeNumber": row["episode_number"]})
