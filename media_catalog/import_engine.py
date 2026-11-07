"""Validated, durable catalog import previews and resumable HTTP jobs."""

from __future__ import annotations

import json
import sqlite3
import uuid
from typing import Any

import httpx
from pydantic import ValidationError

from media_catalog.catalog_repository import CatalogRepository
from media_catalog.content_repository import canonical_json, payload_hash, utc_now
from media_catalog.database import Database
from media_catalog.errors import DomainError, conflict, invalid, not_found
from media_catalog.models import ImportCommit, ImportPreview, MovieCreate, MovieUpdate, SeriesCreate, SeriesUpdate


def _candidate_key(kind: str, item: dict[str, Any]) -> tuple[str, str]:
    external_id = item.get("externalId")
    if external_id:
        return kind, "external:" + str(external_id).casefold()
    name = " ".join(str(item.get("name", "")).casefold().split())
    return kind, "name:" + name + ":" + str(item.get("releaseYear") or "")


class ImportEngine:
    def __init__(self, database: Database, repository: CatalogRepository) -> None:
        if database.role != "catalog":
            raise ValueError("imports require a catalog database")
        self.database = database
        self.repository = repository

    def preview(self, request: ImportPreview) -> dict[str, Any]:
        with self.database.connect() as connection:
            existing_items = self.repository._all_items(connection)
        external_index = {
            (item["kind"], str(item.get("externalId")).casefold()): item
            for item in existing_items if item.get("externalId")
        }
        previous_rows: dict[tuple[str, str], int] = {}
        results = []
        for row_number, row in enumerate(request.rows, start=1):
            raw = dict(row.item)
            model_type = MovieCreate if row.kind == "movie" else SeriesCreate
            try:
                model = model_type.model_validate(raw)
                normalized = model.model_dump(by_alias=True)
            except ValidationError as error:
                results.append({"rowNumber": row_number, "kind": row.kind, "valid": False, "operation": None, "errors": [{"path": ".".join(str(part) for part in item["loc"]), "message": item["msg"], "type": item["type"]} for item in error.errors()], "warnings": [], "item": raw})
                continue
            key = _candidate_key(row.kind, normalized)
            warnings = []
            duplicate_row = previous_rows.get(key)
            if duplicate_row is not None:
                results.append({"rowNumber": row_number, "kind": row.kind, "valid": False, "operation": None, "errors": [{"code": "duplicate_batch_row", "message": f"Duplicates row {duplicate_row} in this batch"}], "warnings": [], "item": normalized})
                continue
            previous_rows[key] = row_number
            for other in existing_items:
                if other["kind"] == row.kind and _candidate_key(row.kind, other) == key and not normalized.get("externalId"):
                    warnings.append({"code": "possible_duplicate", "message": "A similar normalized name and release year already exists", "contentId": other["id"]})
            match = external_index.get((row.kind, str(normalized.get("externalId")).casefold())) if normalized.get("externalId") else None
            operation = "update" if match else "create"
            result_item = normalized
            if match:
                result_item = {**normalized, "expectedVersion": match["version"]}
            results.append({"rowNumber": row_number, "kind": row.kind, "valid": True, "operation": operation, "contentId": match["id"] if match else None, "expectedVersion": match["version"] if match else None, "errors": [], "warnings": warnings, "item": result_item})
        valid_count = sum(bool(row["valid"]) for row in results)
        return {"valid": valid_count == len(results), "rowCount": len(results), "validCount": valid_count, "invalidCount": len(results) - valid_count, "rows": results}

    def commit(self, request: ImportCommit) -> dict[str, Any]:
        payload = {"rows": [row.model_dump(by_alias=True) for row in request.rows]}
        request_hash = payload_hash(payload)
        preview = self.preview(request)
        if not preview["valid"]:
            raise invalid("import_validation_failed", "Import contains invalid or duplicate rows", {"preview": preview})
        try:
            with self.database.transaction() as connection:
                existing = connection.execute("SELECT id, payload_hash FROM import_jobs WHERE idempotency_key=?", (request.idempotency_key,)).fetchone()
                if existing:
                    if existing["payload_hash"] != request_hash:
                        raise conflict("idempotency_conflict", "The import idempotency key was used with a different payload")
                    return self._job(connection, existing["id"])
                job_id = str(uuid.uuid4())
                now = utc_now()
                connection.execute("INSERT INTO import_jobs(id, idempotency_key, payload_hash, state, row_count, created_at, updated_at) VALUES (?, ?, ?, 'pending', ?, ?, ?)", (job_id, request.idempotency_key, request_hash, len(preview["rows"]), now, now))
                for row in preview["rows"]:
                    operation_key = f"import:{job_id}:{row['rowNumber']}:{payload_hash(row['item'])[:16]}"
                    connection.execute("INSERT INTO import_rows(job_id, row_number, kind, payload_json, operation_key, state, updated_at) VALUES (?, ?, ?, ?, ?, 'pending', ?)", (job_id, row["rowNumber"], row["kind"], canonical_json({"operation": row["operation"], "contentId": row["contentId"], "item": row["item"]}), operation_key, now))
                return self._job(connection, job_id)
        except sqlite3.IntegrityError as error:
            raise conflict("import_conflict", "The import job conflicts with stored job identifiers") from error

    @staticmethod
    def _job(connection: Any, job_id: str) -> dict[str, Any]:
        row = connection.execute("SELECT * FROM import_jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise not_found("import job", job_id)
        rows = connection.execute("SELECT * FROM import_rows WHERE job_id=? ORDER BY row_number", (job_id,)).fetchall()
        return {"id": row["id"], "state": row["state"], "rowCount": row["row_count"], "idempotencyKey": row["idempotency_key"], "error": row["error"], "createdAt": row["created_at"], "updatedAt": row["updated_at"], "rows": [{"rowNumber": item["row_number"], "kind": item["kind"], "state": item["state"], "attempts": item["attempts"], "result": json.loads(item["result_json"]) if item["result_json"] else None, "error": item["error"]} for item in rows]}

    def get(self, job_id: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            return self._job(connection, job_id)

    def list_jobs(self, state: str | None = None, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        if state is not None and state not in {"pending", "running", "completed", "failed"}:
            raise invalid("invalid_import_state", "state must be pending, running, completed, or failed")
        where = " WHERE state=?" if state else ""
        values = [state] if state else []
        with self.database.connect() as connection:
            total = connection.execute(f"SELECT COUNT(*) FROM import_jobs{where}", values).fetchone()[0]
            rows = connection.execute(
                f"""SELECT j.id, j.state, j.row_count, j.error, j.created_at, j.updated_at,
                           SUM(CASE WHEN r.state='completed' THEN 1 ELSE 0 END) AS completed_rows,
                           SUM(CASE WHEN r.state='failed' THEN 1 ELSE 0 END) AS failed_rows,
                           SUM(CASE WHEN r.state!='completed' THEN 1 ELSE 0 END) AS remaining_rows
                    FROM import_jobs j LEFT JOIN import_rows r ON r.job_id=j.id{where.replace('state', 'j.state')}
                    GROUP BY j.id ORDER BY j.created_at DESC, j.id DESC LIMIT ? OFFSET ?""",
                (*values, limit, offset),
            ).fetchall()
            items = [{"id": row["id"], "state": row["state"], "rowCount": row["row_count"], "completedRows": row["completed_rows"] or 0, "failedRows": row["failed_rows"] or 0, "remainingRows": row["remaining_rows"] or 0, "error": row["error"], "createdAt": row["created_at"], "updatedAt": row["updated_at"]} for row in rows]
            return {"items": items, "total": total, "limit": limit, "offset": offset}

    def _set_job_state(self, job_id: str, state: str, error: str | None = None) -> None:
        with self.database.transaction() as connection:
            cursor = connection.execute("UPDATE import_jobs SET state=?, error=?, updated_at=? WHERE id=?", (state, error[:500] if error else None, utc_now(), job_id))
            if cursor.rowcount == 0:
                raise not_found("import job", job_id)

    def _finalize(self, job_id: str) -> None:
        with self.database.transaction() as connection:
            job = connection.execute("SELECT id FROM import_jobs WHERE id=?", (job_id,)).fetchone()
            if job is None:
                raise not_found("import job", job_id)
            rows = connection.execute("SELECT state, error FROM import_rows WHERE job_id=? ORDER BY row_number", (job_id,)).fetchall()
            states = [row["state"] for row in rows]
            if states and all(state == "completed" for state in states):
                state, error = "completed", None
            elif any(value == "running" for value in states):
                state, error = "running", None
            else:
                state = "failed"
                error = "; ".join(row["error"] for row in rows if row["state"] == "failed" and row["error"])
                error = error[:500] or "Some rows remain incomplete"
            connection.execute("UPDATE import_jobs SET state=?, error=?, updated_at=? WHERE id=?", (state, error, utc_now(), job_id))

    async def run(self, job_id: str, client: httpx.AsyncClient, source_urls: dict[str, str]) -> dict[str, Any]:
        with self.database.transaction() as connection:
            job = connection.execute("SELECT state FROM import_jobs WHERE id=?", (job_id,)).fetchone()
            if job is None:
                raise not_found("import job", job_id)
            if job["state"] == "completed":
                return self._job(connection, job_id)
            connection.execute("UPDATE import_jobs SET state='running', error=NULL, updated_at=? WHERE id=?", (utc_now(), job_id))
        rows = self.database.connect()
        try:
            pending = rows.execute("SELECT * FROM import_rows WHERE job_id=? AND state!='completed' ORDER BY row_number", (job_id,)).fetchall()
        finally:
            rows.close()
        for row in pending:
            with self.database.transaction() as connection:
                current = connection.execute("SELECT state FROM import_rows WHERE job_id=? AND row_number=?", (job_id, row["row_number"])).fetchone()
                if current["state"] == "completed":
                    continue
                connection.execute("UPDATE import_rows SET state='running', attempts=attempts+1, error=NULL, updated_at=? WHERE job_id=? AND row_number=? AND state!='completed'", (utc_now(), job_id, row["row_number"]))
            data = json.loads(row["payload_json"])
            resource = "movies" if row["kind"] == "movie" else "series"
            url = f"{source_urls[row['kind']]}/{resource}"
            headers = {"Idempotency-Key": row["operation_key"], "X-Audit-Actor": "catalog-import"}
            if data["operation"] == "update":
                url += f"/{data['contentId']}"
                method = "PUT"
            else:
                method = "POST"
            try:
                response = await client.request(method, url, headers=headers, json=data["item"])
                if response.status_code >= 500 or response.status_code in {408, 429}:
                    raise httpx.HTTPStatusError("upstream temporary failure", request=response.request, response=response)
                if response.status_code >= 400:
                    try:
                        error_data = response.json()
                    except ValueError:
                        error_data = {"status": response.status_code}
                    message = error_data.get("error", {}).get("message", f"Source service returned HTTP {response.status_code}")
                    with self.database.transaction() as connection:
                        connection.execute("UPDATE import_rows SET state='failed', error=?, updated_at=? WHERE job_id=? AND row_number=? AND state!='completed'", (str(message)[:500], utc_now(), job_id, row["row_number"]))
                    continue
                body = response.json()
                with self.database.transaction() as connection:
                    connection.execute("UPDATE import_rows SET state='completed', result_json=?, error=NULL, updated_at=? WHERE job_id=? AND row_number=?", (canonical_json(body), utc_now(), job_id, row["row_number"]))
            except (httpx.HTTPError, ValueError, KeyError) as error:
                message = f"{error.__class__.__name__}: source request failed"
                with self.database.transaction() as connection:
                    connection.execute("UPDATE import_rows SET state='failed', error=?, updated_at=? WHERE job_id=? AND row_number=? AND state!='completed'", (message, utc_now(), job_id, row["row_number"]))
        self._finalize(job_id)
        return self.get(job_id)

    def retryable(self, job_id: str) -> bool:
        job = self.get(job_id)
        return job["state"] in {"pending", "running", "failed"} and any(row["state"] != "completed" for row in job["rows"])
