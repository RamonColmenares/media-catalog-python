from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterator

import httpx
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class LiveStack:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.ports = {name: _free_port() for name in ("movie", "series", "catalog", "gateway")}
        self.db_paths = {
            "movie": data_dir / "movies.sqlite3",
            "series": data_dir / "series.sqlite3",
            "catalog": data_dir / "catalog.sqlite3",
        }
        env = os.environ.copy()
        env.update(
            {
                "PYTHONPATH": str(PROJECT_ROOT),
                "MOVIE_DB_PATH": str(self.db_paths["movie"]),
                "SERIES_DB_PATH": str(self.db_paths["series"]),
                "CATALOG_DB_PATH": str(self.db_paths["catalog"]),
                "MOVIE_PORT": str(self.ports["movie"]),
                "SERIES_PORT": str(self.ports["series"]),
                "CATALOG_PORT": str(self.ports["catalog"]),
                "GATEWAY_PORT": str(self.ports["gateway"]),
                "MOVIE_SERVICE_URL": f"http://127.0.0.1:{self.ports['movie']}",
                "SERIES_SERVICE_URL": f"http://127.0.0.1:{self.ports['series']}",
                "CATALOG_SERVICE_URL": f"http://127.0.0.1:{self.ports['catalog']}",
                "SERVICE_REQUEST_TIMEOUT": "0.8",
                "GATEWAY_REQUEST_TIMEOUT": "0.8",
                "LOG_LEVEL": "warning",
            }
        )
        self.env = env
        self.processes: dict[str, subprocess.Popen] = {}
        self.log_handles = {}

    def start_service(self, name: str) -> None:
        port = self.ports[name]
        log_path = self.data_dir / f"{name}.log"
        handle = log_path.open("ab")
        self.log_handles[name] = handle
        self.processes[name] = subprocess.Popen(
            [sys.executable, "-m", "media_catalog", "serve", name, "--host", "127.0.0.1", "--port", str(port)],
            cwd=PROJECT_ROOT,
            env=self.env,
            stdout=handle,
            stderr=subprocess.STDOUT,
        )
        self._wait_ready(name)

    def _wait_ready(self, name: str) -> None:
        paths = {"movie": "/movies", "series": "/series", "catalog": "/catalogs/movies/Probe", "gateway": "/movies"}
        url = f"http://127.0.0.1:{self.ports[name]}{paths[name]}"
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            process = self.processes[name]
            if process.poll() is not None:
                raise AssertionError(f"{name} process exited ({process.returncode}):\n{self._logs(name)}")
            try:
                response = httpx.get(url, timeout=0.4)
                if response.status_code < 500:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.1)
        raise AssertionError(f"{name} did not become ready:\n{self._logs(name)}")

    def _logs(self, name: str) -> str:
        path = self.data_dir / f"{name}.log"
        return path.read_text(errors="replace") if path.exists() else ""

    def start(self) -> None:
        for name in ("movie", "series", "catalog", "gateway"):
            self.start_service(name)

    def stop_service(self, name: str) -> None:
        process = self.processes.get(name)
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

    def restart_service(self, name: str) -> None:
        self.stop_service(name)
        old_handle = self.log_handles.pop(name, None)
        if old_handle:
            old_handle.close()
        self.start_service(name)

    def close(self) -> None:
        for name in tuple(self.processes):
            self.stop_service(name)
        for handle in self.log_handles.values():
            handle.close()

    def url(self, service: str, path: str) -> str:
        return f"http://127.0.0.1:{self.ports[service]}{path}"


@pytest.fixture
def live_stack(tmp_path: Path) -> Iterator[LiveStack]:
    stack = LiveStack(tmp_path)
    try:
        stack.start()
        yield stack
    finally:
        stack.close()
