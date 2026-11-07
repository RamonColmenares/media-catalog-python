from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import httpx

from conftest import LiveStack, PROJECT_ROOT


def _operator(stack: LiveStack, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "media_catalog", "operator", "--base-url", stack.url("gateway", ""), *arguments],
        cwd=PROJECT_ROOT,
        env=os.environ.copy(),
        text=True,
        capture_output=True,
        timeout=15,
        check=False,
    )


def test_operator_cli_search_import_job_lifecycle_and_local_package_io(live_stack: LiveStack, tmp_path: Path) -> None:
    with httpx.Client(timeout=5) as client:
        gateway = live_stack.url("gateway", "")
        movie = client.post(f"{gateway}/movies", json={
            "name": "DUMMY CLI Search Feature",
            "synopsis": "A local command example.",
            "genre": "Documentary",
            "streamUrl": "https://media.example.invalid/cli-search",
        }).json()
        client.post(f"{gateway}/catalogs/sync")

        found = _operator(live_stack, "search", "--text", "CLI Search")
        assert found.returncode == 0, found.stderr
        search_result = json.loads(found.stdout)
        assert search_result["total"] == 1 and search_result["items"][0]["id"] == movie["id"]

        batch_path = tmp_path / "batch.json"
        batch_path.write_text(json.dumps({"rows": [{"kind": "movie", "item": {
            "name": "DUMMY CLI Imported Feature",
            "synopsis": "A local import example.",
            "genre": "Documentary",
            "externalId": "DUMMY-CLI-IMPORT-1",
            "streamUrl": "https://media.example.invalid/cli-import",
        }}]}))
        preview = _operator(live_stack, "import", "preview", str(batch_path))
        assert preview.returncode == 0, preview.stderr
        assert json.loads(preview.stdout)["valid"] is True

        committed = _operator(live_stack, "import", "commit", str(batch_path), "--idempotency-key", "operator-import-0001")
        assert committed.returncode == 0, committed.stderr
        job = json.loads(committed.stdout)
        assert job["state"] == "pending"
        listed = _operator(live_stack, "import", "list", "--state", "pending")
        assert listed.returncode == 0 and json.loads(listed.stdout)["items"][0]["id"] == job["id"]

        run = _operator(live_stack, "import", "run", job["id"])
        assert run.returncode == 0, run.stderr
        completed_job = json.loads(run.stdout)
        assert completed_job["state"] == "completed"
        status = _operator(live_stack, "import", "status", job["id"])
        assert status.returncode == 0 and json.loads(status.stdout)["state"] == "completed"
        assert client.post(f"{gateway}/catalogs/sync").json()["run"]["status"] == "complete"

        references_path = tmp_path / "selection.json"
        references_path.write_text(json.dumps({"references": [{"kind": "movie", "id": completed_job["rows"][0]["result"]["id"]}]}))
        package_path = tmp_path / "operator-export.zip"
        exported = _operator(live_stack, "export", str(references_path), "--output", str(package_path))
        assert exported.returncode == 0, exported.stderr
        assert package_path.is_file() and package_path.stat().st_size > 0
        verified = _operator(live_stack, "validate", str(package_path))
        assert verified.returncode == 0, verified.stderr
        assert json.loads(verified.stdout)["valid"] is True

        already_exists = _operator(live_stack, "export", str(references_path), "--output", str(package_path))
        assert already_exists.returncode == 2
        assert json.loads(already_exists.stderr)["error"]["code"] == "output_exists"
        missing_reference_path = tmp_path / "missing-reference.json"
        missing_reference_path.write_text(json.dumps({"references": [{"kind": "movie", "id": 987654}]}))
        missing_export = _operator(live_stack, "export", str(missing_reference_path), "--output", str(tmp_path / "missing.zip"))
        assert missing_export.returncode == 4
        assert json.loads(missing_export.stderr)["error"]["code"] == "not_found"
        assert not (tmp_path / "missing.zip.partial").exists()
