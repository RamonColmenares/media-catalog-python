"""Command line entry point for one service, the local stack, or queue intake."""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
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
        while not stopping:
            for child in children:
                code = child.poll()
                if code is not None:
                    return code or 0
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

    args = parser.parse_args()
    if args.command == "serve":
        default_port = int(os.getenv(SERVICES[args.service][1], SERVICES[args.service][2]))
        raise SystemExit(_serve(args.service, args.host, args.port or default_port))
    if args.command == "local":
        raise SystemExit(_local(args.host, args.data_dir))
    if args.command == "consume-movies":
        from media_catalog.broker import consume_movie_queue

        consume_movie_queue(database_path=args.db, queue_name=args.queue)


if __name__ == "__main__":
    main()
