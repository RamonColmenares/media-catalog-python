from __future__ import annotations

import asyncio

import httpx

from media_catalog.catalog_repository import CatalogRepository
from media_catalog.database import Database
from media_catalog.import_engine import ImportEngine
from media_catalog.models import ImportCommit


def test_late_worker_timeout_cannot_overwrite_completed_import_row(tmp_path) -> None:
    async def scenario() -> None:
        database = Database(tmp_path / "catalog.sqlite3", role="catalog")
        database.initialize()
        repository = CatalogRepository(database)
        engine = ImportEngine(database, repository)
        request = ImportCommit.model_validate({
            "idempotencyKey": "parallel-import-0001",
            "rows": [{"kind": "movie", "item": {"name": "Concurrent DUMMY Import", "synopsis": "Race case", "externalId": "DUMMY-RACE-1"}}],
        })
        job = engine.commit(request)
        first_request_started = asyncio.Event()
        release_first_failure = asyncio.Event()
        calls = 0

        class ConcurrentClient:
            async def request(self, method: str, url: str, **kwargs) -> httpx.Response:
                nonlocal calls
                calls += 1
                outbound = httpx.Request(method, url, json=kwargs.get("json"))
                if calls == 1:
                    first_request_started.set()
                    await release_first_failure.wait()
                    raise httpx.ReadTimeout("The first operator request timed out", request=outbound)
                return httpx.Response(201, json={"id": 1, "name": "Concurrent DUMMY Import"}, request=outbound)

        client = ConcurrentClient()
        source_urls = {"movie": "http://movie.invalid", "series": "http://series.invalid"}
        late_timeout = asyncio.create_task(engine.run(job["id"], client, source_urls))
        await first_request_started.wait()
        completed = await engine.run(job["id"], client, source_urls)
        assert completed["state"] == "completed"
        assert completed["rows"][0]["state"] == "completed"
        release_first_failure.set()
        after_late_failure = await late_timeout
        assert after_late_failure["state"] == "completed"
        assert after_late_failure["rows"][0]["state"] == "completed"
        assert after_late_failure["rows"][0]["attempts"] == 2

    asyncio.run(scenario())
