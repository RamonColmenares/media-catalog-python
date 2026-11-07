"""Transactional source repositories for movie and series content."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any, Literal

from media_catalog.database import Database
from media_catalog.errors import DomainError, conflict, invalid, not_found
from media_catalog.models import AssetInput, EpisodeInput, LicenseInput, MovieCreate, SeriesCreate

Kind = Literal["movie", "series"]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def payload_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _mapping(model: Any) -> dict[str, Any]:
    data = model.model_dump(by_alias=True)
    data.pop("expectedVersion", None)
    return data


class SourceRepository:
    """Owns one source kind and commits content, audit, and feed events together."""

    def __init__(self, database: Database, kind: Kind) -> None:
        if database.role != kind:
            raise ValueError(f"{kind} repository requires a {kind} database")
        self.database = database
        self.kind = kind
        self.table = "movies" if kind == "movie" else "series"

    def _entity(self, connection: sqlite3.Connection, entity_id: int) -> dict[str, Any]:
        row = connection.execute(f"SELECT * FROM {self.table} WHERE id = ?", (entity_id,)).fetchone()
        if row is None:
            raise not_found(self.kind, entity_id)
        item: dict[str, Any] = {
            "id": row["id"],
            "kind": self.kind,
            "name": row["name"],
            "synopsis": row["synopsis"],
            "genre": row["genre"],
            "releaseYear": row["release_year"],
            "language": row["language"],
            "externalId": row["external_id"],
            "status": row["status"],
            "version": row["version"],
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
        }
        if self.kind == "movie":
            item["runtimeMinutes"] = row["runtime_minutes"]
            item["streamUrl"] = row["stream_url"]
        else:
            item["seasons"] = self._seasons(connection, entity_id)
        licenses = connection.execute(
            "SELECT id FROM licenses WHERE entity_kind=? AND entity_id=? ORDER BY starts_at, region, id",
            (self.kind, entity_id),
        ).fetchall()
        assets = connection.execute(
            "SELECT id FROM assets WHERE entity_kind=? AND entity_id=? ORDER BY role, id",
            (self.kind, entity_id),
        ).fetchall()
        item["licenses"] = [self._license(connection, row["id"]) for row in licenses]
        item["assets"] = [self._asset(connection, row["id"]) for row in assets]
        return item

    @staticmethod
    def _seasons(connection: sqlite3.Connection, series_id: int) -> list[dict[str, Any]]:
        seasons = connection.execute(
            "SELECT id, season_number, position FROM seasons WHERE series_id = ? ORDER BY position, season_number, id",
            (series_id,),
        ).fetchall()
        result = []
        for season in seasons:
            episodes = connection.execute(
                "SELECT id, name, episode_number, synopsis, runtime_minutes, stream_url, position FROM episodes WHERE season_id = ? ORDER BY position, episode_number, id",
                (season["id"],),
            ).fetchall()
            result.append(
                {
                    "id": season["id"],
                    "seasonNumber": season["season_number"],
                    "episodes": [
                        {
                            "id": episode["id"],
                            "name": episode["name"],
                            "episodeNumber": episode["episode_number"],
                            "synopsis": episode["synopsis"],
                            "runtimeMinutes": episode["runtime_minutes"],
                            "streamUrl": episode["stream_url"],
                        }
                        for episode in episodes
                    ],
                }
            )
        return result

    def _assert_idempotency(self, connection: sqlite3.Connection, key: str | None, body_hash: str) -> dict[str, Any] | None:
        if not key:
            return None
        row = connection.execute(
            "SELECT payload_hash, response_json FROM idempotency_keys WHERE key = ?", (key,)
        ).fetchone()
        if row is None:
            return None
        if row["payload_hash"] != body_hash:
            raise conflict("idempotency_conflict", "The idempotency key was already used with a different payload")
        return json.loads(row["response_json"])

    @staticmethod
    def _save_idempotency(connection: sqlite3.Connection, key: str | None, body_hash: str, response: dict[str, Any]) -> None:
        if key:
            connection.execute(
                "INSERT INTO idempotency_keys(key, payload_hash, response_json, created_at) VALUES (?, ?, ?, ?)",
                (key, body_hash, canonical_json(response), utc_now()),
            )

    def _touch(self, connection: sqlite3.Connection, entity_id: int) -> None:
        connection.execute(
            f"UPDATE {self.table} SET version = version + 1, updated_at = ? WHERE id = ?",
            (utc_now(), entity_id),
        )

    def _assert_published_ready(self, connection: sqlite3.Connection, entity_id: int) -> None:
        status = connection.execute(f"SELECT status FROM {self.table} WHERE id=?", (entity_id,)).fetchone()["status"]
        if status == "published":
            readiness = self._readiness(connection, entity_id)
            if not readiness["ready"]:
                raise conflict("published_content_not_ready", "This change would make published content fail readiness; unpublish it first", {"issues": readiness["issues"]})

    def _record(self, connection: sqlite3.Connection, entity_id: int, action: str, actor: str, reason: str = "", summary: Any = None) -> dict[str, Any]:
        item = self._entity(connection, entity_id)
        changed = connection.execute("UPDATE source_state SET revision = revision + 1 WHERE id = 1 RETURNING revision").fetchone()[0]
        item["sourceRevision"] = changed
        at = utc_now()
        connection.execute(
            "INSERT INTO audit_log(entity_kind, entity_id, action, actor, reason, version, summary_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (self.kind, entity_id, action, actor[:120], reason[:500], item["version"], canonical_json(summary or {}), at),
        )
        connection.execute(
            "INSERT INTO source_outbox(entity_kind, entity_id, revision, event_type, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (self.kind, entity_id, changed, action, canonical_json(item), at),
        )
        item.pop("sourceRevision", None)
        return item

    def _assert_editable(self, connection: sqlite3.Connection, entity_id: int, expected_version: int | None = None) -> sqlite3.Row:
        row = connection.execute(f"SELECT * FROM {self.table} WHERE id = ?", (entity_id,)).fetchone()
        if row is None:
            raise not_found(self.kind, entity_id)
        if row["status"] == "archived":
            raise conflict("archived_content", "Archived content must be restored before it can be edited")
        if expected_version is not None and row["version"] != expected_version:
            raise conflict(
                "stale_version",
                "The content changed since it was read",
                {"expectedVersion": expected_version, "currentVersion": row["version"]},
            )
        return row

    def create(self, model: MovieCreate | SeriesCreate, *, actor: str = "local-operator", idempotency_key: str | None = None) -> dict[str, Any]:
        data = _mapping(model)
        body_hash = payload_hash({"kind": self.kind, "item": data})
        now = utc_now()
        try:
            with self.database.transaction() as connection:
                previous = self._assert_idempotency(connection, idempotency_key, body_hash)
                if previous is not None:
                    return previous
                if self.kind == "movie":
                    cursor = connection.execute(
                        "INSERT INTO movies(name, synopsis, genre, release_year, language, external_id, runtime_minutes, stream_url, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?)",
                        (data["name"], data["synopsis"], data["genre"], data["releaseYear"], data["language"], data["externalId"], data["runtimeMinutes"], data["streamUrl"], now, now),
                    )
                else:
                    cursor = connection.execute(
                        "INSERT INTO series(name, synopsis, genre, release_year, language, external_id, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, 'draft', ?, ?)",
                        (data["name"], data["synopsis"], data["genre"], data["releaseYear"], data["language"], data["externalId"], now, now),
                    )
                entity_id = int(cursor.lastrowid)
                if self.kind == "series":
                    self._replace_hierarchy(connection, entity_id, data["seasons"], is_create=True)
                result = self._record(connection, entity_id, "created", actor, summary={"fields": sorted(data)})
                self._save_idempotency(connection, idempotency_key, body_hash, result)
                return result
        except sqlite3.IntegrityError as error:
            raise conflict("unique_identifier", "The externalId or hierarchy number is already in use") from error

    def update(self, entity_id: int, model: MovieCreate | SeriesCreate, expected_version: int, *, actor: str = "local-operator", reason: str = "", idempotency_key: str | None = None) -> dict[str, Any]:
        data = _mapping(model)
        body_hash = payload_hash({"kind": self.kind, "id": entity_id, "expectedVersion": expected_version, "item": data})
        try:
            with self.database.transaction() as connection:
                previous = self._assert_idempotency(connection, idempotency_key, body_hash)
                if previous is not None:
                    return previous
                self._assert_editable(connection, entity_id, expected_version)
                if self.kind == "movie":
                    connection.execute(
                        "UPDATE movies SET name=?, synopsis=?, genre=?, release_year=?, language=?, external_id=?, runtime_minutes=?, stream_url=? WHERE id=?",
                        (data["name"], data["synopsis"], data["genre"], data["releaseYear"], data["language"], data["externalId"], data["runtimeMinutes"], data["streamUrl"], entity_id),
                    )
                else:
                    connection.execute(
                        "UPDATE series SET name=?, synopsis=?, genre=?, release_year=?, language=?, external_id=? WHERE id=?",
                        (data["name"], data["synopsis"], data["genre"], data["releaseYear"], data["language"], data["externalId"], entity_id),
                    )
                    self._replace_hierarchy(connection, entity_id, data["seasons"], is_create=False)
                self._touch(connection, entity_id)
                self._assert_published_ready(connection, entity_id)
                result = self._record(connection, entity_id, "updated", actor, reason, {"fields": sorted(data)})
                self._save_idempotency(connection, idempotency_key, body_hash, result)
                return result
        except sqlite3.IntegrityError as error:
            raise conflict("unique_identifier", "The externalId or hierarchy number is already in use") from error

    def _replace_hierarchy(self, connection: sqlite3.Connection, series_id: int, seasons: list[dict[str, Any]], *, is_create: bool) -> None:
        current_seasons = connection.execute("SELECT id, season_number FROM seasons WHERE series_id = ?", (series_id,)).fetchall()
        current_by_id = {row["id"]: row for row in current_seasons}
        current_by_number = {row["season_number"]: row for row in current_seasons}
        temporary_number = max([0, *(row["season_number"] for row in current_seasons), *(season.get("seasonNumber", 0) for season in seasons)]) + len(current_seasons) + 1
        for offset, row in enumerate(current_seasons):
            connection.execute("UPDATE seasons SET season_number=? WHERE id=?", (temporary_number + offset, row["id"]))
        retained: set[int] = set()
        for season_position, season in enumerate(seasons):
            season_data = season.model_dump(by_alias=True) if hasattr(season, "model_dump") else season
            supplied_id = season_data.get("id")
            if is_create and (supplied_id is not None or any(episode.get("id") is not None for episode in season_data["episodes"])):
                raise invalid("generated_id", "Season and episode IDs are assigned by the service")
            matched = current_by_id.get(supplied_id) if supplied_id is not None else current_by_number.get(season_data["seasonNumber"])
            if supplied_id is not None and matched is None:
                raise conflict("unknown_season", "A season ID does not belong to this series", {"seasonId": supplied_id})
            if matched:
                season_id = matched["id"]
                connection.execute("UPDATE seasons SET season_number=?, position=? WHERE id=?", (season_data["seasonNumber"], season_position, season_id))
            else:
                season_id = connection.execute(
                    "INSERT INTO seasons(series_id, season_number, position) VALUES (?, ?, ?)",
                    (series_id, season_data["seasonNumber"], season_position),
                ).lastrowid
            retained.add(int(season_id))
            self._replace_episodes(connection, int(season_id), season_data["episodes"])
        for row in current_seasons:
            if row["id"] not in retained:
                connection.execute("DELETE FROM seasons WHERE id = ?", (row["id"],))

    @staticmethod
    def _replace_episodes(connection: sqlite3.Connection, season_id: int, episodes: list[dict[str, Any]]) -> None:
        current = connection.execute("SELECT id, episode_number FROM episodes WHERE season_id = ?", (season_id,)).fetchall()
        by_id = {row["id"]: row for row in current}
        by_number = {row["episode_number"]: row for row in current}
        temporary_number = max([0, *(row["episode_number"] for row in current), *(episode.get("episodeNumber", episode.get("number", 0)) for episode in episodes)]) + len(current) + 1
        for offset, row in enumerate(current):
            connection.execute("UPDATE episodes SET episode_number=? WHERE id=?", (temporary_number + offset, row["id"]))
        retained: set[int] = set()
        for position, episode in enumerate(episodes):
            data = episode.model_dump(by_alias=True) if hasattr(episode, "model_dump") else episode
            supplied_id = data.get("id")
            matched = by_id.get(supplied_id) if supplied_id is not None else by_number.get(data["episodeNumber"])
            if supplied_id is not None and matched is None:
                raise conflict("unknown_episode", "An episode ID does not belong to this season", {"episodeId": supplied_id})
            values = (data["name"], data.get("episodeNumber", data.get("number")), data.get("synopsis", ""), data.get("runtimeMinutes"), data.get("streamUrl"), position)
            if matched:
                episode_id = int(matched["id"])
                connection.execute("UPDATE episodes SET name=?, episode_number=?, synopsis=?, runtime_minutes=?, stream_url=?, position=? WHERE id=?", (*values, episode_id))
            else:
                episode_id = int(connection.execute(
                    "INSERT INTO episodes(season_id, name, episode_number, synopsis, runtime_minutes, stream_url, position) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (season_id, *values),
                ).lastrowid)
            retained.add(episode_id)
        if retained:
            placeholders = ",".join("?" for _ in retained)
            connection.execute(f"DELETE FROM episodes WHERE season_id=? AND id NOT IN ({placeholders})", (season_id, *sorted(retained)))
        else:
            connection.execute("DELETE FROM episodes WHERE season_id=?", (season_id,))

    def get(self, entity_id: int) -> dict[str, Any]:
        with self.database.connect() as connection:
            return self._entity(connection, entity_id)

    def list(self, *, genre: str | None = None, status: str | None = None, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        where = []
        values: list[Any] = []
        if genre:
            where.append("genre = ? COLLATE NOCASE")
            values.append(genre)
        if status:
            where.append("status = ?")
            values.append(status)
        clause = " WHERE " + " AND ".join(where) if where else ""
        with self.database.connect() as connection:
            total = connection.execute(f"SELECT COUNT(*) FROM {self.table}{clause}", values).fetchone()[0]
            rows = connection.execute(f"SELECT id FROM {self.table}{clause} ORDER BY id LIMIT ? OFFSET ?", (*values, limit, offset)).fetchall()
            items = [self._entity(connection, row["id"]) for row in rows]
        return {"items": items, "total": total, "limit": limit, "offset": offset}

    def transition(self, entity_id: int, action: str, expected_version: int, *, actor: str = "local-operator", reason: str = "") -> dict[str, Any]:
        with self.database.transaction() as connection:
            row = connection.execute(f"SELECT status, version FROM {self.table} WHERE id=?", (entity_id,)).fetchone()
            if row is None:
                raise not_found(self.kind, entity_id)
            if row["version"] != expected_version:
                raise conflict("stale_version", "The content changed since it was read", {"expectedVersion": expected_version, "currentVersion": row["version"]})
            current = row["status"]
            allowed = {"publish": {"draft"}, "unpublish": {"published"}, "archive": {"draft", "published"}, "restore": {"archived"}}
            if action not in allowed or current not in allowed[action]:
                raise conflict("invalid_transition", f"Cannot {action} content in {current} state", {"status": current, "action": action})
            if action == "publish":
                readiness = self._readiness(connection, entity_id)
                if not readiness["ready"]:
                    raise conflict("not_ready", "Content has unresolved publication issues", {"issues": readiness["issues"]})
            next_status = {"publish": "published", "unpublish": "draft", "archive": "archived", "restore": "draft"}[action]
            connection.execute(f"UPDATE {self.table} SET status=?, version=version+1, updated_at=? WHERE id=?", (next_status, utc_now(), entity_id))
            return self._record(connection, entity_id, action, actor, reason, {"from": current, "to": next_status})

    def readiness(self, entity_id: int) -> dict[str, Any]:
        with self.database.connect() as connection:
            return self._readiness(connection, entity_id)

    def _readiness(self, connection: sqlite3.Connection, entity_id: int) -> dict[str, Any]:
        item = self._entity(connection, entity_id)
        issues: list[dict[str, Any]] = []
        if not item["synopsis"].strip():
            issues.append({"code": "missing_synopsis", "field": "synopsis", "message": "Add a synopsis before publishing"})
        if self.kind == "movie":
            if not item["streamUrl"]:
                has_playback_asset = connection.execute("SELECT 1 FROM assets WHERE entity_kind=? AND entity_id=? AND role IN ('video','audio') LIMIT 1", (self.kind, entity_id)).fetchone()
                if not has_playback_asset:
                    issues.append({"code": "missing_playback", "field": "streamUrl", "message": "Add a stream URL or asset metadata"})
        else:
            episodes = [episode for season in item["seasons"] for episode in season["episodes"]]
            if not episodes:
                issues.append({"code": "no_episodes", "field": "seasons", "message": "Add at least one episode before publishing"})
            for episode in episodes:
                if not episode["streamUrl"]:
                    issues.append({"code": "missing_episode_stream", "field": f"episodes.{episode['id']}.streamUrl", "message": "Add a stream URL for every episode"})
        now = utc_now()
        license_row = connection.execute(
            "SELECT 1 FROM licenses WHERE entity_kind=? AND entity_id=? AND region='WORLD' AND starts_at<=? AND ends_at>? LIMIT 1",
            (self.kind, entity_id, now, now),
        ).fetchone()
        if not license_row:
            issues.append({"code": "missing_worldwide_rights", "field": "licenses", "message": "Add an active WORLD rights window before publishing"})
        return {"kind": self.kind, "id": entity_id, "status": item["status"], "version": item["version"], "ready": not issues, "issues": issues}

    def add_license(self, entity_id: int, model: LicenseInput, *, actor: str = "local-operator") -> dict[str, Any]:
        data = model.model_dump(by_alias=True)
        start = data["startsAt"].isoformat(timespec="microseconds").replace("+00:00", "Z")
        end = data["endsAt"].isoformat(timespec="microseconds").replace("+00:00", "Z")
        try:
            with self.database.transaction() as connection:
                self._assert_editable(connection, entity_id)
                overlap = connection.execute(
                    "SELECT id, starts_at, ends_at FROM licenses WHERE entity_kind=? AND entity_id=? AND region=? AND starts_at < ? AND ends_at > ? LIMIT 1",
                    (self.kind, entity_id, data["region"], end, start),
                ).fetchone()
                if overlap:
                    raise conflict("license_overlap", "The rights window overlaps an existing window", {"licenseId": overlap["id"]})
                license_id = connection.execute(
                    "INSERT INTO licenses(entity_kind, entity_id, region, starts_at, ends_at, rights_holder, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (self.kind, entity_id, data["region"], start, end, data["rightsHolder"], utc_now()),
                ).lastrowid
                self._touch(connection, entity_id)
                self._record(connection, entity_id, "license_added", actor, summary={"licenseId": license_id, "region": data["region"]})
                return self._license(connection, int(license_id))
        except sqlite3.IntegrityError as error:
            raise conflict("license_conflict", "The rights window conflicts with stored licensing data") from error

    @staticmethod
    def _license(connection: sqlite3.Connection, license_id: int) -> dict[str, Any]:
        row = connection.execute("SELECT * FROM licenses WHERE id=?", (license_id,)).fetchone()
        return {"id": row["id"], "kind": row["entity_kind"], "contentId": row["entity_id"], "region": row["region"], "startsAt": row["starts_at"], "endsAt": row["ends_at"], "rightsHolder": row["rights_holder"], "createdAt": row["created_at"]}

    def list_licenses(self, entity_id: int) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            self._entity(connection, entity_id)
            rows = connection.execute("SELECT id FROM licenses WHERE entity_kind=? AND entity_id=? ORDER BY starts_at, region, id", (self.kind, entity_id)).fetchall()
            return [self._license(connection, row["id"]) for row in rows]

    def remove_license(self, entity_id: int, license_id: int, *, actor: str = "local-operator") -> dict[str, Any]:
        with self.database.transaction() as connection:
            self._assert_editable(connection, entity_id)
            row = connection.execute("SELECT id FROM licenses WHERE id=? AND entity_kind=? AND entity_id=?", (license_id, self.kind, entity_id)).fetchone()
            if row is None:
                raise not_found("license", license_id)
            connection.execute("DELETE FROM licenses WHERE id=?", (license_id,))
            self._touch(connection, entity_id)
            self._assert_published_ready(connection, entity_id)
            return self._record(connection, entity_id, "license_removed", actor, summary={"licenseId": license_id})

    def availability(self, entity_id: int, region: str, at: datetime) -> dict[str, Any]:
        item = self.get(entity_id)
        normalized = region.upper()
        if normalized != "WORLD" and not re.fullmatch(r"[A-Z]{2}", normalized):
            raise invalid("invalid_region", "region must be WORLD or a two-letter country code")
        when = at.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
        with self.database.connect() as connection:
            rights = connection.execute(
                "SELECT region, starts_at, ends_at FROM licenses WHERE entity_kind=? AND entity_id=? AND region IN (?, 'WORLD') AND starts_at<=? AND ends_at>? ORDER BY CASE region WHEN ? THEN 0 ELSE 1 END, starts_at DESC LIMIT 1",
                (self.kind, entity_id, normalized, when, when, normalized),
            ).fetchone()
        reasons = []
        if item["status"] != "published":
            reasons.append(f"content_{item['status']}")
        if rights is None:
            reasons.append("no_active_license")
        return {"kind": self.kind, "id": entity_id, "region": normalized, "at": when, "available": not reasons, "reasons": reasons, "license": dict(rights) if rights else None}

    def add_asset(self, entity_id: int, model: AssetInput, *, actor: str = "local-operator") -> dict[str, Any]:
        data = model.model_dump(by_alias=True)
        try:
            with self.database.transaction() as connection:
                self._assert_editable(connection, entity_id)
                asset_id = connection.execute(
                    "INSERT INTO assets(entity_kind, entity_id, path, role, mime_type, size_bytes, checksum_sha256, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (self.kind, entity_id, data["path"], data["role"], data["mimeType"], data["sizeBytes"], data["checksumSha256"], utc_now()),
                ).lastrowid
                self._touch(connection, entity_id)
                self._record(connection, entity_id, "asset_registered", actor, summary={"assetId": asset_id, "role": data["role"]})
                return self._asset(connection, int(asset_id))
        except sqlite3.IntegrityError as error:
            raise conflict("duplicate_asset_path", "The content already has an asset at that relative path") from error

    @staticmethod
    def _asset(connection: sqlite3.Connection, asset_id: int) -> dict[str, Any]:
        row = connection.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
        return {"id": row["id"], "kind": row["entity_kind"], "contentId": row["entity_id"], "path": row["path"], "role": row["role"], "mimeType": row["mime_type"], "sizeBytes": row["size_bytes"], "checksumSha256": row["checksum_sha256"], "createdAt": row["created_at"], "diskValidation": "not_inspected"}

    def list_assets(self, entity_id: int) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            self._entity(connection, entity_id)
            rows = connection.execute("SELECT id FROM assets WHERE entity_kind=? AND entity_id=? ORDER BY role, id", (self.kind, entity_id)).fetchall()
            return [self._asset(connection, row["id"]) for row in rows]

    def remove_asset(self, entity_id: int, asset_id: int, *, actor: str = "local-operator") -> dict[str, Any]:
        with self.database.transaction() as connection:
            self._assert_editable(connection, entity_id)
            row = connection.execute("SELECT id FROM assets WHERE id=? AND entity_kind=? AND entity_id=?", (asset_id, self.kind, entity_id)).fetchone()
            if row is None:
                raise not_found("asset", asset_id)
            asset = self._asset(connection, asset_id)
            connection.execute("DELETE FROM assets WHERE id=?", (asset_id,))
            self._touch(connection, entity_id)
            self._assert_published_ready(connection, entity_id)
            self._record(connection, entity_id, "asset_removed", actor, summary={"assetId": asset_id, "role": asset["role"]})
            return {"deleted": True, "asset": asset}

    def audit(self, entity_id: int) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            self._entity(connection, entity_id)
            rows = connection.execute("SELECT * FROM audit_log WHERE entity_kind=? AND entity_id=? ORDER BY id", (self.kind, entity_id)).fetchall()
            return [{"id": row["id"], "kind": row["entity_kind"], "contentId": row["entity_id"], "action": row["action"], "actor": row["actor"], "reason": row["reason"], "version": row["version"], "summary": json.loads(row["summary_json"]), "createdAt": row["created_at"]} for row in rows]

    def changes(self, after: int = 0, limit: int = 200) -> dict[str, Any]:
        with self.database.connect() as connection:
            high_water = connection.execute("SELECT COALESCE(MAX(sequence), 0) FROM source_outbox").fetchone()[0]
            source_id = connection.execute("SELECT generation FROM source_state WHERE id=1").fetchone()[0]
            rows = connection.execute("SELECT * FROM source_outbox WHERE sequence>? ORDER BY sequence LIMIT ?", (after, limit)).fetchall()
            events = [{"sequence": row["sequence"], "kind": row["entity_kind"], "id": row["entity_id"], "revision": row["revision"], "eventType": row["event_type"], "item": json.loads(row["payload_json"]), "createdAt": row["created_at"]} for row in rows]
            return {"sourceId": source_id, "items": events, "nextCursor": events[-1]["sequence"] if events else after, "highWaterMark": high_water, "hasMore": bool(events and events[-1]["sequence"] < high_water)}

    def snapshot(self) -> dict[str, Any]:
        with self.database.transaction(immediate=False) as connection:
            items = [self._entity(connection, row["id"]) for row in connection.execute(f"SELECT id FROM {self.table} ORDER BY id")]
            for item in items:
                latest = connection.execute(
                    "SELECT MAX(revision) FROM source_outbox WHERE entity_kind=? AND entity_id=?",
                    (self.kind, item["id"]),
                ).fetchone()[0]
                item["sourceRevision"] = int(latest or 1)
            watermark = connection.execute("SELECT COALESCE(MAX(sequence), 0) FROM source_outbox").fetchone()[0]
            source_id = connection.execute("SELECT generation FROM source_state WHERE id=1").fetchone()[0]
            return {"kind": self.kind, "sourceId": source_id, "items": items, "highWaterMark": watermark, "capturedAt": utc_now()}

    def reorder_episodes(self, series_id: int, season_id: int, ids: list[int], *, actor: str = "local-operator") -> dict[str, Any]:
        with self.database.transaction() as connection:
            self._assert_editable(connection, series_id)
            season = connection.execute("SELECT id FROM seasons WHERE id=? AND series_id=?", (season_id, series_id)).fetchone()
            if season is None:
                raise not_found("season", season_id)
            actual = [row["id"] for row in connection.execute("SELECT id FROM episodes WHERE season_id=? ORDER BY position, episode_number, id", (season_id,))]
            if len(ids) != len(actual) or set(ids) != set(actual):
                raise invalid("invalid_permutation", "Episode reorder must contain every episode ID exactly once", {"expectedIds": actual})
            for position, episode_id in enumerate(ids):
                connection.execute("UPDATE episodes SET position=? WHERE id=?", (position, episode_id))
            self._touch(connection, series_id)
            return self._record(connection, series_id, "episodes_reordered", actor, summary={"seasonId": season_id, "episodeIds": ids})

    def structure_report(self, series_id: int) -> dict[str, Any]:
        item = self.get(series_id)
        seasons = item["seasons"]
        gaps = []
        total_runtime = 0
        runtime_count = 0
        for season in seasons:
            episodes = season["episodes"]
            numbers = {episode["episodeNumber"] for episode in episodes}
            if numbers:
                missing = sorted(set(range(1, max(numbers) + 1)) - numbers)
                if missing:
                    gaps.append({"seasonNumber": season["seasonNumber"], "missingEpisodeNumbers": missing})
            for episode in episodes:
                if episode["runtimeMinutes"] is not None:
                    total_runtime += episode["runtimeMinutes"]
                    runtime_count += 1
        return {"seriesId": series_id, "seasonCount": len(seasons), "episodeCount": sum(len(s["episodes"]) for s in seasons), "knownRuntimeMinutes": total_runtime, "episodesWithRuntime": runtime_count, "numberingGaps": gaps}

    def asset_quality(self) -> dict[str, Any]:
        with self.database.connect() as connection:
            items = connection.execute(f"SELECT id, name FROM {self.table} ORDER BY id").fetchall()
            assets = connection.execute("SELECT * FROM assets WHERE entity_kind=? ORDER BY id", (self.kind,)).fetchall()
            by_checksum: dict[str, list[dict[str, Any]]] = {}
            by_content: dict[int, list[sqlite3.Row]] = {}
            for row in assets:
                by_checksum.setdefault(row["checksum_sha256"], []).append({"id": row["id"], "contentId": row["entity_id"], "path": row["path"]})
                by_content.setdefault(row["entity_id"], []).append(row)
            issues = []
            for item in items:
                content_assets = by_content.get(item["id"], [])
                if not any(asset["role"] == "poster" for asset in content_assets):
                    issues.append({"kind": self.kind, "id": item["id"], "code": "missing_poster", "message": "Register poster asset metadata"})
                for asset in content_assets:
                    if asset["size_bytes"] == 0:
                        issues.append({"kind": self.kind, "id": item["id"], "assetId": asset["id"], "code": "zero_sized_asset", "message": "Asset metadata reports a zero byte size"})
            duplicates = [{"checksumSha256": checksum, "references": references} for checksum, references in sorted(by_checksum.items()) if len(references) > 1]
            return {"kind": self.kind, "checkedItems": len(items), "registeredAssets": len(assets), "issues": issues, "duplicateChecksums": duplicates, "diskInspection": "not_performed; asset records describe metadata only"}

    def duplicates(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute(f"SELECT id, name, release_year, external_id FROM {self.table} ORDER BY id").fetchall()
        normalized: dict[tuple[str, int | None], list[dict[str, Any]]] = {}
        for row in rows:
            key = (re.sub(r"[^a-z0-9]+", " ", row["name"].casefold()).strip(), row["release_year"])
            normalized.setdefault(key, []).append({"kind": self.kind, "id": row["id"], "name": row["name"], "releaseYear": row["release_year"], "externalId": row["external_id"]})
        return [{"match": "normalized_name_year", "items": group} for group in normalized.values() if len(group) > 1]

    def exists(self, entity_id: int) -> bool:
        with self.database.connect() as connection:
            return connection.execute(f"SELECT 1 FROM {self.table} WHERE id=?", (entity_id,)).fetchone() is not None
