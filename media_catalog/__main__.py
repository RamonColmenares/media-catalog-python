"""Command line entry point for one service, the local stack, or queue intake."""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
from pathlib import Path


SERVICES = {
    "movie": ("media_catalog.movie_service", "MOVIE_PORT", "8102"),
    "series": ("media_catalog.series_service", "SERIES_PORT", "8103"),
    "catalog": ("media_catalog.catalog_service", "CATALOG_PORT", "8104"),
    "gateway": ("media_catalog.gateway", "GATEWAY_PORT", "8080"),
}


def _serve(name: str, host: str, port: int) -> int:
    import uvicorn

    module = SERVICES[name][0]
    uvicorn.run(f"{module}:app", host=host, port=port, log_level=os.getenv("LOG_LEVEL", "info"))
    return 0


def _local(host: str, data_dir: str) -> int:
    data_path = Path(data_dir).expanduser().resolve()
    data_path.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.setdefault("MOVIE_DB_PATH", str(data_path / "movies.sqlite3"))
    env.setdefault("SERIES_DB_PATH", str(data_path / "series.sqlite3"))
    env.setdefault("CATALOG_DB_PATH", str(data_path / "catalog.sqlite3"))
    env.setdefault("MOVIE_SERVICE_URL", f"http://127.0.0.1:{env.get('MOVIE_PORT', '8102')}")
    env.setdefault("SERIES_SERVICE_URL", f"http://127.0.0.1:{env.get('SERIES_PORT', '8103')}")
    env.setdefault("CATALOG_SERVICE_URL", f"http://127.0.0.1:{env.get('CATALOG_PORT', '8104')}")

    children: list[subprocess.Popen] = []
    stopping = False

    def request_stop(signum: int, frame: object) -> None:
        nonlocal stopping
        stopping = True

    previous_handlers = {
        signal.SIGINT: signal.signal(signal.SIGINT, request_stop),
        signal.SIGTERM: signal.signal(signal.SIGTERM, request_stop),
    }
    try:
        for name, (_, port_env, default_port) in SERVICES.items():
            port = int(env.get(port_env, default_port))
            children.append(
                subprocess.Popen(
                    [sys.executable, "-m", "media_catalog", "serve", name, "--host", host, "--port", str(port)],
                    env=env,
                )
            )
        print("Local services are starting; use Ctrl-C to stop the stack.", flush=True)
        readiness_paths = {
            "movie": "/movies/health",
            "series": "/series/health",
            "catalog": "/catalogs/ready",
            "gateway": "/health/ready",
        }
        ready_services: set[str] = set()
        startup_deadline = time.monotonic() + 30
        while not stopping and len(ready_services) < len(SERVICES):
            for name, (_, port_env, default_port) in SERVICES.items():
                child = children[list(SERVICES).index(name)]
                code = child.poll()
                if code is not None:
                    print(f"{name} service exited during startup with code {code}.", file=sys.stderr, flush=True)
                    return code or 1
                if name in ready_services:
                    continue
                port = int(env.get(port_env, default_port))
                try:
                    with urlopen(f"http://127.0.0.1:{port}{readiness_paths[name]}", timeout=0.4) as response:
                        if response.status == 200:
                            ready_services.add(name)
                except HTTPError:
                    pass
                except (URLError, TimeoutError, OSError):
                    pass
            if time.monotonic() >= startup_deadline:
                waiting = ", ".join(sorted(set(SERVICES) - ready_services))
                print(f"Local services did not become ready within 30 seconds: {waiting}.", file=sys.stderr, flush=True)
                return 1
            time.sleep(0.15)
        if stopping:
            return 0
        print("All local services are ready.", flush=True)
        while not stopping:
            for name, child in zip(SERVICES, children):
                code = child.poll()
                if code is not None:
                    print(f"{name} service exited unexpectedly with code {code}.", file=sys.stderr, flush=True)
                    return code or 1
            time.sleep(0.2)
        return 0
    finally:
        for child in children:
            if child.poll() is None:
                child.terminate()
        for child in children:
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=5)
        for signum, old_handler in previous_handlers.items():
            signal.signal(signum, old_handler)


def main() -> None:
    parser = argparse.ArgumentParser(prog="media-catalog")
    commands = parser.add_subparsers(dest="command", required=True)

    serve = commands.add_parser("serve", help="Run one HTTP service")
    serve.add_argument("service", choices=SERVICES)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int)

    local = commands.add_parser("local", help="Run all four local HTTP services")
    local.add_argument("--host", default="127.0.0.1")
    local.add_argument("--data-dir", default="./data")

    consumer = commands.add_parser("consume-movies", help="Consume the optional RabbitMQ Movie queue")
    consumer.add_argument("--db")
    consumer.add_argument("--queue")

    from media_catalog.operator_cli import add_operator_arguments

    add_operator_arguments(commands)

    args = parser.parse_args()
    if args.command == "serve":
        default_port = int(os.getenv(SERVICES[args.service][1], SERVICES[args.service][2]))
        raise SystemExit(_serve(args.service, args.host, args.port or default_port))
    if args.command == "local":
        raise SystemExit(_local(args.host, args.data_dir))
    if args.command == "consume-movies":
        from media_catalog.broker import consume_movie_queue

        consume_movie_queue(database_path=args.db, queue_name=args.queue)
    if args.command == "operator":
        from media_catalog.operator_cli import run_operator

        raise SystemExit(run_operator(args))


if __name__ == "__main__":
    main()
