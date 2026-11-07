"""Durable catalog projection and editorial workflows over source HTTP events."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable

from media_catalog.content_repository import canonical_json, utc_now
from media_catalog.database import Database
from media_catalog.errors import conflict, invalid, not_found
from media_catalog.models import CollectionInput, CollectionUpdate, ContentReference, LabelInput, LabelUpdate, SavedSearchUpdate, SearchQuery


def _reference(value: ContentReference | dict[str, Any]) -> tuple[str, int]:
    data = value.model_dump() if hasattr(value, "model_dump") else value
    return data["kind"], int(data["id"])


def _normalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()


def _when(value: datetime | None = None) -> str:
    return (value or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


class CatalogRepository:
    """Owns projections and operator-created catalog organization data."""

    def __init__(self, database: Database) -> None:
        if database.role != "catalog":
            raise ValueError("catalog repository requires a catalog database")
        self.database = database

    @staticmethod
    def _project(connection: sqlite3.Connection, kind: str, item: dict[str, Any], revision: int, projected_at: str) -> bool:
        if kind not in {"movie", "series"} or int(item.get("id", 0)) < 1 or revision < 1:
            raise invalid("invalid_source_event", "Source event has an invalid entity key or revision")
        existing = connection.execute("SELECT revision FROM catalog_items WHERE kind=? AND media_id=?", (kind, item["id"])).fetchone()
        if existing and existing["revision"] >= revision:
            return False
        connection.execute(
            "INSERT INTO catalog_items(kind, media_id, revision, payload_json, projected_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(kind, media_id) DO UPDATE SET revision=excluded.revision, payload_json=excluded.payload_json, projected_at=excluded.projected_at",
            (kind, item["id"], revision, canonical_json(item), projected_at),
        )
        return True

    def apply_event_page(self, kind: str, events: list[dict[str, Any]], *, source_cursor: int | None = None, source_id: str | None = None) -> dict[str, int]:
        if kind not in {"movie", "series"}:
            raise ValueError("kind must be movie or series")
        with self.database.transaction() as connection:
            cursor_row = connection.execute("SELECT sequence, source_id FROM sync_cursors WHERE kind=?", (kind,)).fetchone()
            cursor = int(cursor_row["sequence"])
            if not source_id:
                raise invalid("missing_source_id", "Source change pages must include a sourceId")
            if cursor_row["source_id"] and cursor_row["source_id"] != source_id:
                raise conflict("source_reset_requires_rebuild", "The source database identity changed; rebuild the projection from snapshots")
            applied = changed = 0
            last_sequence = cursor
            for event in events:
                sequence = event.get("sequence")
                if not isinstance(sequence, int) or sequence <= 0 or event.get("kind") != kind:
                    raise invalid("invalid_source_event", "Source feed returned an invalid sequence or kind")
                if sequence <= cursor:
                    last_sequence = max(last_sequence, sequence)
                    continue
                if sequence <= last_sequence:
                    raise invalid("out_of_order_source_event", "Source events must be in strictly increasing sequence order")
                if sequence != last_sequence + 1:
                    raise invalid("source_feed_gap", "Source feed skipped a sequence before the next event")
                item = event.get("item")
                revision = event.get("revision")
                if not isinstance(item, dict) or not isinstance(revision, int):
                    raise invalid("invalid_source_event", "Source event is missing its full item snapshot")
                changed += int(self._project(connection, kind, item, revision, utc_now()))
                applied += 1
                last_sequence = sequence
            if source_cursor is not None:
                if source_cursor != last_sequence:
                    raise invalid("invalid_source_cursor", "The nextCursor must match the last event applied")
                last_sequence = source_cursor
            connection.execute(
                "UPDATE sync_cursors SET sequence=?, source_id=?, last_success_at=?, last_error=NULL WHERE kind=?",
                (last_sequence, source_id, utc_now(), kind),
            )
            return {"events": applied, "changed": changed, "cursor": last_sequence}

    def apply_snapshots(self, snapshots: dict[str, dict[str, Any]]) -> dict[str, int]:
        if set(snapshots) != {"movie", "series"}:
            raise invalid("invalid_snapshot_set", "A projection rebuild requires movie and series snapshots")
        report: dict[str, int] = {}
        with self.database.transaction() as connection:
            for kind in ("movie", "series"):
                snapshot = snapshots[kind]
                if snapshot.get("kind") != kind or not isinstance(snapshot.get("items"), list):
                    raise invalid("invalid_snapshot", f"The {kind} snapshot is malformed")
                high_water = snapshot.get("highWaterMark")
                if not isinstance(high_water, int) or high_water < 0:
                    raise invalid("invalid_snapshot", f"The {kind} snapshot is missing a valid highWaterMark")
                source_id = snapshot.get("sourceId")
                if not isinstance(source_id, str) or not source_id:
                    raise invalid("invalid_snapshot", f"The {kind} snapshot is missing a sourceId")
                previous_source_id = connection.execute("SELECT source_id FROM sync_cursors WHERE kind=?", (kind,)).fetchone()[0]
                if previous_source_id and previous_source_id != source_id:
                    connection.execute("DELETE FROM catalog_items WHERE kind=?", (kind,))
                seen: set[int] = set()
                changed = 0
                for item in snapshot["items"]:
                    if not isinstance(item, dict) or not isinstance(item.get("id"), int) or item["id"] in seen:
                        raise invalid("invalid_snapshot", f"The {kind} snapshot contains an invalid or duplicate ID")
                    seen.add(item["id"])
                    revision = item.get("sourceRevision")
                    if not isinstance(revision, int) or revision < 1:
                        revision = max(1, int(item.get("version", 1)))
                    changed += int(self._project(connection, kind, item, revision, utc_now()))
                if seen:
                    placeholders = ",".join("?" for _ in seen)
                    connection.execute(f"DELETE FROM catalog_items WHERE kind=? AND media_id NOT IN ({placeholders})", (kind, *sorted(seen)))
                else:
                    connection.execute("DELETE FROM catalog_items WHERE kind=?", (kind,))
                connection.execute(
                    "UPDATE sync_cursors SET sequence=?, source_id=?, high_water_mark=?, last_success_at=?, last_error=NULL WHERE kind=?",
                    (high_water, source_id, high_water, utc_now(), kind),
                )
                report[kind] = changed
        return report

    def mark_sync_error(self, kind: str, error: str) -> None:
        with self.database.transaction() as connection:
            connection.execute("UPDATE sync_cursors SET last_error=? WHERE kind=?", (error[:500], kind))

    def cursor_state(self) -> dict[str, dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute("SELECT * FROM sync_cursors ORDER BY kind").fetchall()
            return {row["kind"]: {"sequence": row["sequence"], "sourceId": row["source_id"], "highWaterMark": row["high_water_mark"], "lastSuccessAt": row["last_success_at"], "lastError": row["last_error"]} for row in rows}

    def sync_history(self, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        with self.database.connect() as connection:
            total = connection.execute("SELECT COUNT(*) FROM catalog_sync_runs").fetchone()[0]
            rows = connection.execute("SELECT * FROM catalog_sync_runs ORDER BY id DESC LIMIT ? OFFSET ?", (limit, offset)).fetchall()
            items = [{"id": row["id"], "status": row["status"], "startedAt": row["started_at"], "finishedAt": row["finished_at"], "appliedEvents": row["applied_events"], "error": row["error"], "sources": json.loads(row["sources_json"])} for row in rows]
            return {"items": items, "total": total, "limit": limit, "offset": offset}

    def rebuild_history(self, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        with self.database.connect() as connection:
            total = connection.execute("SELECT COUNT(*) FROM projection_rebuilds").fetchone()[0]
            rows = connection.execute("SELECT * FROM projection_rebuilds ORDER BY id DESC LIMIT ? OFFSET ?", (limit, offset)).fetchall()
            items = [{"id": row["id"], "status": row["status"], "startedAt": row["started_at"], "finishedAt": row["finished_at"], "movieHighWater": row["movie_high_water"], "seriesHighWater": row["series_high_water"], "error": row["error"]} for row in rows]
            return {"items": items, "total": total, "limit": limit, "offset": offset}

    def export_history(self, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        with self.database.connect() as connection:
            total = connection.execute("SELECT COUNT(*) FROM export_records").fetchone()[0]
            rows = connection.execute("SELECT * FROM export_records ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?", (limit, offset)).fetchall()
            items = [{"id": row["id"], "sha256": row["content_sha256"], "itemCount": row["item_count"], "createdAt": row["created_at"]} for row in rows]
            return {"items": items, "total": total, "limit": limit, "offset": offset}

    def begin_sync(self, sources: dict[str, Any]) -> int:
        with self.database.transaction() as connection:
            return int(connection.execute(
                "INSERT INTO catalog_sync_runs(started_at, status, sources_json) VALUES (?, 'running', ?)",
                (utc_now(), canonical_json(sources)),
            ).lastrowid)

    def finish_sync(self, run_id: int, status: str, applied: int, error: str | None = None) -> dict[str, Any]:
        with self.database.transaction() as connection:
            connection.execute("UPDATE catalog_sync_runs SET finished_at=?, status=?, applied_events=?, error=? WHERE id=?", (utc_now(), status, applied, error, run_id))
            row = connection.execute("SELECT * FROM catalog_sync_runs WHERE id=?", (run_id,)).fetchone()
            return {"id": row["id"], "status": row["status"], "appliedEvents": row["applied_events"], "error": row["error"], "startedAt": row["started_at"], "finishedAt": row["finished_at"], "sources": json.loads(row["sources_json"])}

    def begin_rebuild(self) -> int:
        with self.database.transaction() as connection:
            return int(connection.execute("INSERT INTO projection_rebuilds(started_at, status) VALUES (?, 'running')", (utc_now(),)).lastrowid)

    def finish_rebuild(self, rebuild_id: int, status: str, high_water: dict[str, int] | None = None, error: str | None = None) -> dict[str, Any]:
        marks = high_water or {}
        with self.database.transaction() as connection:
            connection.execute("UPDATE projection_rebuilds SET finished_at=?, status=?, movie_high_water=?, series_high_water=?, error=? WHERE id=?", (utc_now(), status, marks.get("movie"), marks.get("series"), error, rebuild_id))
            row = connection.execute("SELECT * FROM projection_rebuilds WHERE id=?", (rebuild_id,)).fetchone()
            return {"id": row["id"], "status": row["status"], "movieHighWater": row["movie_high_water"], "seriesHighWater": row["series_high_water"], "error": row["error"], "startedAt": row["started_at"], "finishedAt": row["finished_at"]}

    def _all_items(self, connection: sqlite3.Connection) -> list[dict[str, Any]]:
        rows = connection.execute("SELECT kind, media_id, revision, payload_json, projected_at FROM catalog_items ORDER BY kind, media_id").fetchall()
        output = []
        for row in rows:
            item = json.loads(row["payload_json"])
            item["kind"] = row["kind"]
            item["projection"] = {"revision": row["revision"], "projectedAt": row["projected_at"]}
            output.append(item)
        return output

    @staticmethod
    def _label_map(connection: sqlite3.Connection, refs: Iterable[tuple[str, int]]) -> dict[tuple[str, int], list[dict[str, Any]]]:
        keys = list(refs)
        if not keys:
            return {}
        map_result: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
        for kind, media_id in keys:
            rows = connection.execute(
                "SELECT l.id, l.name, l.color FROM content_labels cl JOIN labels l ON l.id=cl.label_id WHERE cl.kind=? AND cl.media_id=? ORDER BY l.name",
                (kind, media_id),
            ).fetchall()
            map_result[(kind, media_id)] = [{"id": row["id"], "name": row["name"], "color": row["color"]} for row in rows]
        return map_result

    def _matching_items(self, connection: sqlite3.Connection, query: SearchQuery) -> list[dict[str, Any]]:
        items = self._all_items(connection)
        label_map = self._label_map(connection, ((item["kind"], item["id"]) for item in items))
        filtered = []
        for item in items:
            if query.kind and item["kind"] != query.kind:
                continue
            if query.status and item.get("status") != query.status:
                continue
            if query.genre and item.get("genre", "").casefold() != query.genre.casefold():
                continue
            if query.language and item.get("language", "").casefold() != query.language.casefold():
                continue
            year = item.get("releaseYear")
            if query.release_year_from and (year is None or year < query.release_year_from):
                continue
            if query.release_year_to and (year is None or year > query.release_year_to):
                continue
            labels = label_map.get((item["kind"], item["id"]), [])
            item["labels"] = labels
            if query.label and not any(label["name"] == query.label.casefold() for label in labels):
                continue
            text = query.text.casefold().strip() if query.text else None
            if text and text not in f"{item.get('name', '')} {item.get('synopsis', '')}".casefold():
                continue
            filtered.append(item)
        sort_field = query.sort
        def sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
            value = item.get({"releaseYear": "releaseYear", "updatedAt": "updatedAt", "name": "name", "kind": "kind"}[sort_field])
            if sort_field == "releaseYear":
                value = value if value is not None else 0
            elif value is None:
                value = ""
            else:
                value = str(value).casefold()
            return (value, item["kind"], item["id"])
        filtered.sort(key=sort_key, reverse=query.direction == "desc")
        return filtered

    def search(self, query: SearchQuery) -> dict[str, Any]:
        with self.database.connect() as connection:
            filtered = self._matching_items(connection, query)
            return {"items": filtered[query.offset:query.offset + query.limit], "total": len(filtered), "limit": query.limit, "offset": query.offset}

    def facets(self, query: SearchQuery | None = None) -> dict[str, Any]:
        with self.database.connect() as connection:
            selected = self._matching_items(connection, query or SearchQuery())
        counts = {"kind": Counter(item["kind"] for item in selected), "status": Counter(item.get("status", "unknown") for item in selected), "genre": Counter(item.get("genre", "Uncategorized") for item in selected), "language": Counter(item.get("language", "und") for item in selected), "releaseYear": Counter(str(item["releaseYear"]) for item in selected if item.get("releaseYear") is not None)}
        counts["label"] = Counter(label["name"] for item in selected for label in item.get("labels", []))
        return {name: [{"value": value, "count": count} for value, count in sorted(counter.items())] for name, counter in counts.items()}

    def list_labels(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute("SELECT l.id, l.name, l.color, l.version, l.created_at, COUNT(cl.media_id) AS usage_count FROM labels l LEFT JOIN content_labels cl ON cl.label_id=l.id GROUP BY l.id ORDER BY l.name").fetchall()
            return [{"id": row["id"], "name": row["name"], "color": row["color"], "version": row["version"], "usageCount": row["usage_count"], "createdAt": row["created_at"]} for row in rows]

    def create_label(self, model: LabelInput) -> dict[str, Any]:
        data = model.model_dump(by_alias=True)
        try:
            with self.database.transaction() as connection:
                label_id = connection.execute("INSERT INTO labels(name, color, created_at) VALUES (?, ?, ?)", (data["name"], data["color"], utc_now())).lastrowid
                return self._label(connection, int(label_id))
        except sqlite3.IntegrityError as error:
            raise conflict("duplicate_label", "A label with that name already exists") from error

    @staticmethod
    def _label(connection: sqlite3.Connection, label_id: int) -> dict[str, Any]:
        row = connection.execute("SELECT id, name, color, version, created_at FROM labels WHERE id=?", (label_id,)).fetchone()
        if row is None:
            raise not_found("label", label_id)
        return {"id": row["id"], "name": row["name"], "color": row["color"], "version": row["version"], "createdAt": row["created_at"]}

    def update_label(self, label_id: int, update: LabelUpdate) -> dict[str, Any]:
        data = update.model_dump(by_alias=True)
        try:
            with self.database.transaction() as connection:
                row = connection.execute("SELECT version FROM labels WHERE id=?", (label_id,)).fetchone()
                if row is None:
                    raise not_found("label", label_id)
                if row["version"] != data["expectedVersion"]:
                    raise conflict("stale_version", "The label changed since it was read", {"expectedVersion": data["expectedVersion"], "currentVersion": row["version"]})
                connection.execute("UPDATE labels SET name=?, color=?, version=version+1 WHERE id=?", (data["name"], data["color"], label_id))
                return self._label(connection, label_id)
        except sqlite3.IntegrityError as error:
            raise conflict("duplicate_label", "A label with that name already exists") from error

    def attach_label(self, label_id: int, kind: str, media_id: int) -> dict[str, Any]:
        with self.database.transaction() as connection:
            label = self._label(connection, label_id)
            if connection.execute("SELECT 1 FROM catalog_items WHERE kind=? AND media_id=?", (kind, media_id)).fetchone() is None:
                raise not_found(kind, media_id)
            try:
                connection.execute("INSERT INTO content_labels(label_id, kind, media_id, created_at) VALUES (?, ?, ?, ?)", (label_id, kind, media_id, utc_now()))
            except sqlite3.IntegrityError as error:
                raise conflict("label_already_attached", "The label is already attached to this content") from error
            return {"label": label, "kind": kind, "contentId": media_id}

    def detach_label(self, label_id: int, kind: str, media_id: int) -> None:
        with self.database.transaction() as connection:
            self._label(connection, label_id)
            cursor = connection.execute("DELETE FROM content_labels WHERE label_id=? AND kind=? AND media_id=?", (label_id, kind, media_id))
            if cursor.rowcount == 0:
                raise not_found("label assignment", f"{label_id}:{kind}:{media_id}")

    def delete_label(self, label_id: int) -> None:
        with self.database.transaction() as connection:
            self._label(connection, label_id)
            count = connection.execute("SELECT COUNT(*) FROM content_labels WHERE label_id=?", (label_id,)).fetchone()[0]
            if count:
                raise conflict("label_in_use", "Remove this label from all content before deleting it", {"usageCount": count})
            connection.execute("DELETE FROM labels WHERE id=?", (label_id,))

    def create_collection(self, model: CollectionInput) -> dict[str, Any]:
        data = model.model_dump(by_alias=True)
        try:
            with self.database.transaction() as connection:
                refs = [_reference(item) for item in model.items]
                self._require_references(connection, refs)
                now = utc_now()
                collection_id = connection.execute("INSERT INTO collections(name, description, created_at, updated_at) VALUES (?, ?, ?, ?)", (data["name"], data["description"], now, now)).lastrowid
                for position, (kind, media_id) in enumerate(refs):
                    connection.execute("INSERT INTO collection_members(collection_id, position, kind, media_id) VALUES (?, ?, ?, ?)", (collection_id, position, kind, media_id))
                return self._collection(connection, int(collection_id))
        except sqlite3.IntegrityError as error:
            raise conflict("duplicate_collection", "A collection with that name already exists") from error

    @staticmethod
    def _require_references(connection: sqlite3.Connection, refs: Iterable[tuple[str, int]]) -> None:
        for kind, media_id in refs:
            if kind not in {"movie", "series"} or connection.execute("SELECT 1 FROM catalog_items WHERE kind=? AND media_id=?", (kind, media_id)).fetchone() is None:
                raise not_found(kind, media_id)

    def _collection(self, connection: sqlite3.Connection, collection_id: int) -> dict[str, Any]:
        row = connection.execute("SELECT * FROM collections WHERE id=?", (collection_id,)).fetchone()
        if row is None:
            raise not_found("collection", collection_id)
        members = connection.execute("SELECT position, kind, media_id FROM collection_members WHERE collection_id=? ORDER BY position", (collection_id,)).fetchall()
        content_map = {(item["kind"], item["id"]): item for item in self._all_items(connection)}
        items = []
        for member in members:
            content = content_map.get((member["kind"], member["media_id"]))
            items.append({"position": member["position"], "kind": member["kind"], "id": member["media_id"], "content": content})
        return {"id": row["id"], "name": row["name"], "description": row["description"], "version": row["version"], "items": items, "createdAt": row["created_at"], "updatedAt": row["updated_at"]}

    def list_collections(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute("SELECT id FROM collections ORDER BY name COLLATE NOCASE, id").fetchall()
            return [self._collection(connection, row["id"]) for row in rows]

    def get_collection(self, collection_id: int) -> dict[str, Any]:
        with self.database.connect() as connection:
            return self._collection(connection, collection_id)

    def update_collection(self, collection_id: int, update: CollectionUpdate) -> dict[str, Any]:
        data = update.model_dump(by_alias=True)
        try:
            with self.database.transaction() as connection:
                row = connection.execute("SELECT version FROM collections WHERE id=?", (collection_id,)).fetchone()
                if row is None:
                    raise not_found("collection", collection_id)
                if row["version"] != data["expectedVersion"]:
                    raise conflict("stale_version", "The collection changed since it was read", {"expectedVersion": data["expectedVersion"], "currentVersion": row["version"]})
                connection.execute("UPDATE collections SET name=?, description=?, version=version+1, updated_at=? WHERE id=?", (data["name"], data["description"], utc_now(), collection_id))
                return self._collection(connection, collection_id)
        except sqlite3.IntegrityError as error:
            raise conflict("duplicate_collection", "A collection with that name already exists") from error

    def delete_collection(self, collection_id: int) -> None:
        with self.database.transaction() as connection:
            cursor = connection.execute("DELETE FROM collections WHERE id=?", (collection_id,))
            if cursor.rowcount == 0:
                raise not_found("collection", collection_id)

    def add_collection_member(self, collection_id: int, ref: ContentReference) -> dict[str, Any]:
        kind, media_id = _reference(ref)
        with self.database.transaction() as connection:
            self._collection(connection, collection_id)
            self._require_references(connection, [(kind, media_id)])
            position = connection.execute("SELECT COALESCE(MAX(position), -1)+1 FROM collection_members WHERE collection_id=?", (collection_id,)).fetchone()[0]
            try:
                connection.execute("INSERT INTO collection_members(collection_id, position, kind, media_id) VALUES (?, ?, ?, ?)", (collection_id, position, kind, media_id))
            except sqlite3.IntegrityError as error:
                raise conflict("duplicate_member", "The content is already in this collection") from error
            connection.execute("UPDATE collections SET updated_at=? WHERE id=?", (utc_now(), collection_id))
            return self._collection(connection, collection_id)

    def remove_collection_member(self, collection_id: int, kind: str, media_id: int) -> dict[str, Any]:
        with self.database.transaction() as connection:
            self._collection(connection, collection_id)
            cursor = connection.execute("DELETE FROM collection_members WHERE collection_id=? AND kind=? AND media_id=?", (collection_id, kind, media_id))
            if not cursor.rowcount:
                raise not_found("collection member", f"{kind}:{media_id}")
            self._compact_collection(connection, collection_id)
            connection.execute("UPDATE collections SET updated_at=? WHERE id=?", (utc_now(), collection_id))
            return self._collection(connection, collection_id)

    @staticmethod
    def _compact_collection(connection: sqlite3.Connection, collection_id: int) -> None:
        rows = connection.execute("SELECT kind, media_id FROM collection_members WHERE collection_id=? ORDER BY position", (collection_id,)).fetchall()
        connection.execute("DELETE FROM collection_members WHERE collection_id=?", (collection_id,))
        connection.executemany("INSERT INTO collection_members(collection_id, position, kind, media_id) VALUES (?, ?, ?, ?)", ((collection_id, position, row["kind"], row["media_id"]) for position, row in enumerate(rows)))

    def reorder_collection(self, collection_id: int, refs: list[ContentReference]) -> dict[str, Any]:
        supplied = [_reference(ref) for ref in refs]
        if len(supplied) != len(set(supplied)):
            raise invalid("invalid_permutation", "Collection reorder references must be unique")
        with self.database.transaction() as connection:
            collection = self._collection(connection, collection_id)
            current = [(item["kind"], item["id"]) for item in collection["items"]]
            if len(supplied) != len(current) or set(supplied) != set(current):
                raise invalid("invalid_permutation", "Reorder must contain every current member exactly once", {"currentItems": [{"kind": kind, "id": media_id} for kind, media_id in current]})
            connection.execute("DELETE FROM collection_members WHERE collection_id=?", (collection_id,))
            connection.executemany("INSERT INTO collection_members(collection_id, position, kind, media_id) VALUES (?, ?, ?, ?)", ((collection_id, position, kind, media_id) for position, (kind, media_id) in enumerate(supplied)))
            connection.execute("UPDATE collections SET updated_at=? WHERE id=?", (utc_now(), collection_id))
            return self._collection(connection, collection_id)

    def _is_available(self, item: dict[str, Any], region: str, at: str) -> tuple[bool, str | None]:
        if item.get("status") != "published":
            return False, f"content_{item.get('status', 'unknown')}"
        if self._matching_license(item, region, at) is None:
            return False, "no_active_license"
        return True, None

    @staticmethod
    def _matching_license(item: dict[str, Any], region: str, at: str) -> dict[str, Any] | None:
        candidates = [
            license for license in item.get("licenses", [])
            if license.get("region") in {region, "WORLD"}
            and license.get("startsAt", "") <= at < license.get("endsAt", "")
        ]
        candidates.sort(key=lambda license: (license.get("region") != region, license.get("startsAt", "")), reverse=False)
        return candidates[0] if candidates else None

    def availability_detail(self, kind: str, media_id: int, region: str = "WORLD", at: datetime | None = None) -> dict[str, Any]:
        normalized = region.upper()
        if normalized != "WORLD" and not re.fullmatch(r"[A-Z]{2}", normalized):
            raise invalid("invalid_region", "region must be WORLD or a two-letter country code")
        when = _when(at)
        item = self.lookup(kind, media_id)
        available, reason = self._is_available(item, normalized, when)
        selected = self._matching_license(item, normalized, when)
        return {"kind": kind, "id": media_id, "name": item.get("name"), "status": item.get("status"), "region": normalized, "at": when, "available": available, "reasons": [] if available else [reason], "license": selected}

    def validate_collection(self, collection_id: int, region: str, at: datetime | None = None) -> dict[str, Any]:
        normalized = region.upper()
        if normalized != "WORLD" and not re.fullmatch(r"[A-Z]{2}", normalized):
            raise invalid("invalid_region", "region must be WORLD or a two-letter country code")
        when = _when(at)
        collection = self.get_collection(collection_id)
        issues = []
        for member in collection["items"]:
            content = member["content"]
            if content is None:
                issues.append({"kind": member["kind"], "id": member["id"], "code": "dangling_reference", "message": "The projected content no longer exists"})
                continue
            available, reason = self._is_available(content, normalized, when)
            if not available:
                issues.append({"kind": member["kind"], "id": member["id"], "code": reason, "message": "Content is not available for this distribution window"})
        return {"collectionId": collection_id, "region": normalized, "at": when, "valid": not issues, "issues": issues}

    def save_search(self, name: str, query: SearchQuery) -> dict[str, Any]:
        clean_name = name.strip()
        if not clean_name or len(clean_name) > 180:
            raise invalid("invalid_search_name", "Saved search name must contain 1 to 180 characters")
        payload = canonical_json(query.model_dump(by_alias=True))
        now = utc_now()
        try:
            with self.database.transaction() as connection:
                search_id = connection.execute("INSERT INTO saved_searches(name, query_json, created_at, updated_at) VALUES (?, ?, ?, ?)", (clean_name, payload, now, now)).lastrowid
                return self._saved_search(connection, int(search_id))
        except sqlite3.IntegrityError as error:
            raise conflict("duplicate_saved_search", "A saved search with that name already exists") from error

    @staticmethod
    def _saved_search(connection: sqlite3.Connection, search_id: int) -> dict[str, Any]:
        row = connection.execute("SELECT * FROM saved_searches WHERE id=?", (search_id,)).fetchone()
        if row is None:
            raise not_found("saved search", search_id)
        return {"id": row["id"], "name": row["name"], "query": json.loads(row["query_json"]), "version": row["version"], "createdAt": row["created_at"], "updatedAt": row["updated_at"]}

    def list_saved_searches(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute("SELECT id FROM saved_searches ORDER BY name COLLATE NOCASE, id").fetchall()
            return [self._saved_search(connection, row["id"]) for row in rows]

    def update_saved_search(self, search_id: int, update: SavedSearchUpdate) -> dict[str, Any]:
        data = update.model_dump(by_alias=True)
        payload = canonical_json(update.query.model_dump(by_alias=True))
        try:
            with self.database.transaction() as connection:
                row = connection.execute("SELECT version FROM saved_searches WHERE id=?", (search_id,)).fetchone()
                if row is None:
                    raise not_found("saved search", search_id)
                if row["version"] != data["expectedVersion"]:
                    raise conflict("stale_version", "The saved search changed since it was read", {"expectedVersion": data["expectedVersion"], "currentVersion": row["version"]})
                connection.execute("UPDATE saved_searches SET name=?, query_json=?, version=version+1, updated_at=? WHERE id=?", (data["name"].strip(), payload, utc_now(), search_id))
                return self._saved_search(connection, search_id)
        except sqlite3.IntegrityError as error:
            raise conflict("duplicate_saved_search", "A saved search with that name already exists") from error

    def delete_saved_search(self, search_id: int) -> None:
        with self.database.transaction() as connection:
            cursor = connection.execute("DELETE FROM saved_searches WHERE id=?", (search_id,))
            if cursor.rowcount == 0:
                raise not_found("saved search", search_id)

    def run_saved_search(self, search_id: int) -> dict[str, Any]:
        with self.database.connect() as connection:
            search = self._saved_search(connection, search_id)
        return {"search": search, "result": self.search(SearchQuery.model_validate(search["query"]))}

    def statistics(self, at: datetime | None = None, region: str = "WORLD") -> dict[str, Any]:
        when = _when(at)
        normalized = region.upper()
        if normalized != "WORLD" and not re.fullmatch(r"[A-Z]{2}", normalized):
            raise invalid("invalid_region", "region must be WORLD or a two-letter country code")
        with self.database.connect() as connection:
            items = self._all_items(connection)
        grouping_fields = ("kind", "status", "genre", "language")
        groups = {}
        for field in grouping_fields:
            counter = Counter(str(item.get(field) or "unknown") for item in items)
            groups[field] = [{"value": key, "count": value} for key, value in sorted(counter.items())]
        availability = Counter("available" if self._is_available(item, normalized, when)[0] else "unavailable" for item in items)
        return {"total": len(items), "groups": groups, "availability": [{"value": key, "count": value} for key, value in sorted(availability.items())], "region": normalized, "at": when, "generatedAt": utc_now()}

    def quality_report(self) -> dict[str, Any]:
        with self.database.connect() as connection:
            items = self._all_items(connection)
            dangling = connection.execute("SELECT cl.kind, cl.media_id, l.name FROM content_labels cl JOIN labels l ON l.id=cl.label_id LEFT JOIN catalog_items ci ON ci.kind=cl.kind AND ci.media_id=cl.media_id WHERE ci.media_id IS NULL ORDER BY cl.kind, cl.media_id").fetchall()
        issues: list[dict[str, Any]] = []
        for item in items:
            if not str(item.get("synopsis", "")).strip():
                issues.append({"kind": item["kind"], "id": item["id"], "code": "missing_synopsis", "message": "Add a synopsis"})
            if item["kind"] == "movie" and not item.get("streamUrl") and not any(asset.get("role") in {"video", "audio"} for asset in item.get("assets", [])):
                issues.append({"kind": item["kind"], "id": item["id"], "code": "missing_playback", "message": "Add a stream URL or asset record"})
            if item["kind"] == "series":
                episodes = [episode for season in item.get("seasons", []) for episode in season.get("episodes", [])]
                if not episodes:
                    issues.append({"kind": item["kind"], "id": item["id"], "code": "no_episodes", "message": "Add at least one episode"})
                for episode in episodes:
                    if not episode.get("streamUrl"):
                        issues.append({"kind": item["kind"], "id": item["id"], "episodeId": episode.get("id"), "code": "missing_episode_stream", "message": "Add an episode stream URL"})
            available, reason = self._is_available(item, "WORLD", utc_now())
            if item.get("status") == "published" and not available:
                issues.append({"kind": item["kind"], "id": item["id"], "code": reason or "unavailable", "message": "Published content is not currently available"})
        for row in dangling:
            issues.append({"kind": row["kind"], "id": row["media_id"], "code": "dangling_label_reference", "label": row["name"], "message": "The label points to content missing from the current projection"})
        return {"checkedItems": len(items), "issueCount": len(issues), "issues": issues, "generatedAt": utc_now()}

    def duplicate_candidates(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            items = self._all_items(connection)
        name_groups: dict[tuple[str, int | None], list[dict[str, Any]]] = defaultdict(list)
        external_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in items:
            normalized = _normalize_name(item.get("name", ""))
            name_groups[(normalized, item.get("releaseYear"))].append({"kind": item["kind"], "id": item["id"], "name": item.get("name"), "releaseYear": item.get("releaseYear"), "externalId": item.get("externalId")})
            if item.get("externalId"):
                external_groups[item["externalId"].casefold()].append({"kind": item["kind"], "id": item["id"], "name": item.get("name"), "releaseYear": item.get("releaseYear"), "externalId": item.get("externalId")})
        output = [{"match": "normalized_name_year", "items": group} for group in name_groups.values() if len(group) > 1]
        output.extend({"match": "external_id", "items": group} for group in external_groups.values() if len(group) > 1)
        return output

    def asset_report(self) -> dict[str, Any]:
        with self.database.connect() as connection:
            items = self._all_items(connection)
        checksums: dict[str, list[dict[str, Any]]] = defaultdict(list)
        issues = []
        total = 0
        for item in items:
            assets = item.get("assets", [])
            total += len(assets)
            if not any(asset.get("role") == "poster" for asset in assets):
                issues.append({"kind": item["kind"], "id": item["id"], "code": "missing_poster", "message": "Register poster asset metadata"})
            for asset in assets:
                if asset.get("sizeBytes") == 0:
                    issues.append({"kind": item["kind"], "id": item["id"], "assetId": asset.get("id"), "code": "zero_sized_asset", "message": "Asset metadata reports zero bytes"})
                checksum = asset.get("checksumSha256")
                if checksum:
                    checksums[checksum].append({"kind": item["kind"], "id": item["id"], "assetId": asset.get("id"), "path": asset.get("path")})
        duplicates = [{"checksumSha256": checksum, "references": refs} for checksum, refs in sorted(checksums.items()) if len(refs) > 1]
        return {"checkedItems": len(items), "registeredAssets": total, "issues": issues, "duplicateChecksums": duplicates, "diskInspection": "not_performed; asset records describe metadata only"}

    def availability_report(self, region: str = "WORLD", at: datetime | None = None, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        normalized = region.upper()
        if normalized != "WORLD" and not re.fullmatch(r"[A-Z]{2}", normalized):
            raise invalid("invalid_region", "region must be WORLD or a two-letter country code")
        when = _when(at)
        with self.database.connect() as connection:
            items = self._all_items(connection)
        results = []
        for item in items:
            available, reason = self._is_available(item, normalized, when)
            selected = self._matching_license(item, normalized, when)
            results.append({"kind": item["kind"], "id": item["id"], "name": item.get("name"), "status": item.get("status"), "available": available, "reasons": [] if available else [reason], "license": selected})
        return {"items": results[offset:offset + limit], "total": len(results), "limit": limit, "offset": offset, "region": normalized, "at": when}

    def lookup(self, kind: str, media_id: int) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute("SELECT payload_json FROM catalog_items WHERE kind=? AND media_id=?", (kind, media_id)).fetchone()
            if row is None:
                raise not_found(kind, media_id)
            item = json.loads(row["payload_json"])
            item["labels"] = self._label_map(connection, [(kind, media_id)]).get((kind, media_id), [])
            return item
