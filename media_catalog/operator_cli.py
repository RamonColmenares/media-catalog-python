"""HTTP operator commands for discovery, import jobs, and metadata packages."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

import httpx

MAX_INPUT_BYTES = 2 * 1024 * 1024
MAX_PACKAGE_BYTES = 8 * 1024 * 1024
MAX_EXPORT_BYTES = 50 * 1024 * 1024


class OperatorError(Exception):
    def __init__(self, message: str, exit_code: int = 2, code: str = "operator_error", details: Any = None) -> None:
        super().__init__(message)
        self.exit_code = exit_code
        self.code = code
        self.details = details


def _positive_timeout(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("timeout must be a number from 0.1 to 60 seconds") from error
    if not 0.1 <= parsed <= 60:
        raise argparse.ArgumentTypeError("timeout must be a number from 0.1 to 60 seconds")
    return parsed


def add_operator_arguments(commands: argparse._SubParsersAction) -> None:
    operator = commands.add_parser("operator", help="Use the local catalog over HTTP")
    operator.add_argument("--base-url", default=os.getenv("MEDIA_CATALOG_GATEWAY_URL", "http://127.0.0.1:8080"))
    operator.add_argument("--timeout", type=_positive_timeout, default=5.0)
    operations = operator.add_subparsers(dest="operator_command", required=True)

    search = operations.add_parser("search", help="Search the projected mixed catalog")
    search.add_argument("--text")
    search.add_argument("--kind", choices=("movie", "series"))
    search.add_argument("--genre")
    search.add_argument("--status", choices=("draft", "published", "archived"))
    search.add_argument("--language")
    search.add_argument("--release-year-from", type=int)
    search.add_argument("--release-year-to", type=int)
    search.add_argument("--label")
    search.add_argument("--sort", choices=("name", "releaseYear", "updatedAt", "kind"), default="name")
    search.add_argument("--direction", choices=("asc", "desc"), default="asc")
    search.add_argument("--limit", type=int, default=50)
    search.add_argument("--offset", type=int, default=0)

    import_parser = operations.add_parser("import", help="Preview, create, inspect, and resume JSON import jobs")
    import_commands = import_parser.add_subparsers(dest="import_command", required=True)
    preview = import_commands.add_parser("preview", help="Validate a JSON batch without writing content")
    preview.add_argument("input", type=Path)
    commit = import_commands.add_parser("commit", help="Create a durable job from a JSON batch")
    commit.add_argument("input", type=Path)
    commit.add_argument("--idempotency-key")
    run = import_commands.add_parser("run", help="Run a pending or interrupted import job")
    run.add_argument("job_id")
    retry = import_commands.add_parser("retry", help="Retry failed or interrupted rows in a job")
    retry.add_argument("job_id")
    status = import_commands.add_parser("status", help="Read a durable import job by its ID")
    status.add_argument("job_id")
    listing = import_commands.add_parser("list", help="Find recent import jobs after a restart")
    listing.add_argument("--state", choices=("pending", "running", "completed", "failed"))
    listing.add_argument("--limit", type=int, default=50)
    listing.add_argument("--offset", type=int, default=0)

    export = operations.add_parser("export", help="Write selected metadata to a local ZIP package")
    export.add_argument("references", type=Path, help="JSON object with a references array")
    export.add_argument("--output", required=True, type=Path)
    export.add_argument("--overwrite", action="store_true")

    validate = operations.add_parser("validate", help="Validate a local metadata ZIP through the catalog API")
    validate.add_argument("package", type=Path)


def _base_url(value: str) -> str:
    try:
        parts = urlsplit(value)
        parts.port
    except ValueError as error:
        raise OperatorError("base URL must be a valid HTTP or HTTPS URL", code="invalid_base_url") from error
    if parts.scheme not in {"http", "https"} or not parts.hostname or parts.username or parts.password:
        raise OperatorError("base URL must be an absolute HTTP or HTTPS URL without embedded credentials", code="invalid_base_url")
    return value.rstrip("/")


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        size = path.stat().st_size
        if size > MAX_INPUT_BYTES:
            raise OperatorError("JSON input exceeds the 2 MiB limit", code="input_too_large")
        data = path.read_bytes()
    except OSError as error:
        raise OperatorError(f"cannot read {path}: {error.strerror or error.__class__.__name__}", code="input_unavailable") from error
    if len(data) > MAX_INPUT_BYTES:
        raise OperatorError("JSON input exceeds the 2 MiB limit", code="input_too_large")
    try:
        value = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise OperatorError(f"JSON input is invalid: {error}", code="invalid_json") from error
    if not isinstance(value, dict):
        raise OperatorError("JSON input must contain an object", code="invalid_json_shape")
    return value


def _print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _response_json(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return {"message": response.text[:1000]}


def _stream_error_json(response: httpx.Response) -> Any:
    body = bytearray()
    for chunk in response.iter_bytes():
        body.extend(chunk)
        if len(body) >= 64 * 1024:
            break
    try:
        return json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {"message": bytes(body[:1000]).decode("utf-8", errors="replace")}


def _request(client: httpx.Client, method: str, path: str, *, params: dict[str, Any] | None = None, body: Any = None, headers: dict[str, str] | None = None) -> Any:
    try:
        response = client.request(method, path, params=params, json=body, headers=headers)
    except httpx.TimeoutException as error:
        raise OperatorError("catalog request timed out", 3, "request_timeout") from error
    except httpx.HTTPError as error:
        raise OperatorError("catalog service could not be reached", 3, "service_unavailable", {"type": error.__class__.__name__}) from error
    result = _response_json(response)
    if response.status_code >= 400:
        error = result.get("error", {}) if isinstance(result, dict) else {}
        message = error.get("message", f"catalog returned HTTP {response.status_code}")
        raise OperatorError(str(message), 4, error.get("code", "http_error"), error.get("details"))
    return result


def _search(client: httpx.Client, args: argparse.Namespace) -> int:
    params = {
        "text": args.text,
        "kind": args.kind,
        "genre": args.genre,
        "status": args.status,
        "language": args.language,
        "releaseYearFrom": args.release_year_from,
        "releaseYearTo": args.release_year_to,
        "label": args.label,
        "sort": args.sort,
        "direction": args.direction,
        "limit": args.limit,
        "offset": args.offset,
    }
    _print_json(_request(client, "GET", "/catalogs/search", params={key: value for key, value in params.items() if value is not None}))
    return 0


def _import_command(client: httpx.Client, args: argparse.Namespace) -> int:
    command = args.import_command
    if command in {"preview", "commit"}:
        payload = _read_json_object(args.input)
        if command == "preview":
            rows = payload.get("rows")
            if not isinstance(rows, list):
                raise OperatorError("import input must contain a rows array", code="invalid_import_input")
            result = _request(client, "POST", "/catalogs/imports/preview", body={"rows": rows})
        else:
            if args.idempotency_key:
                payload["idempotencyKey"] = args.idempotency_key
            if not isinstance(payload.get("idempotencyKey"), str) or not isinstance(payload.get("rows"), list):
                raise OperatorError("commit input must contain rows and an idempotencyKey, or pass --idempotency-key", code="invalid_import_input")
            result = _request(client, "POST", "/catalogs/imports", body=payload)
        _print_json(result)
        return 0 if command == "commit" or result.get("valid") else 5
    if command == "list":
        params = {"limit": args.limit, "offset": args.offset}
        if args.state:
            params["state"] = args.state
        _print_json(_request(client, "GET", "/catalogs/imports", params=params))
        return 0
    if command == "status":
        _print_json(_request(client, "GET", _job_path(args.job_id)))
        return 0
    result = _request(client, "POST", _job_path(args.job_id, command))
    _print_json(result)
    return 0 if result.get("state") == "completed" else 5


def _job_path(job_id: str, action: str | None = None) -> str:
    path = f"/catalogs/imports/{quote(job_id, safe='')}"
    return f"{path}/{action}" if action else path


def _export(client: httpx.Client, args: argparse.Namespace) -> int:
    payload = _read_json_object(args.references)
    if not isinstance(payload.get("references"), list):
        raise OperatorError("export selection must contain a references array", code="invalid_export_input")
    output = args.output.expanduser()
    if output.exists() and not args.overwrite:
        raise OperatorError(f"output already exists: {output}; pass --overwrite to replace it", code="output_exists")
    if not output.parent.exists() or not output.parent.is_dir():
        raise OperatorError("output directory does not exist", code="output_directory_missing")
    partial = output.with_name(output.name + ".partial")
    if partial.exists():
        raise OperatorError(f"temporary output already exists: {partial}", code="partial_output_exists")
    try:
        with client.stream("POST", "/catalogs/exports", json=payload) as response:
            if response.status_code >= 400:
                error = _stream_error_json(response)
                details = error.get("error", {}) if isinstance(error, dict) else {}
                raise OperatorError(details.get("message", f"catalog returned HTTP {response.status_code}"), 4, details.get("code", "http_error"), details.get("details"))
            declared = response.headers.get("content-length")
            if declared:
                try:
                    declared_size = int(declared)
                except ValueError:
                    declared_size = None
                if declared_size is not None and declared_size > MAX_EXPORT_BYTES:
                    raise OperatorError("export exceeds the 50 MiB local write limit", code="export_too_large")
            size = 0
            with partial.open("xb") as handle:
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > MAX_EXPORT_BYTES:
                        raise OperatorError("export exceeds the 50 MiB local write limit", code="export_too_large")
                    handle.write(chunk)
                handle.flush()
                os.fsync(handle.fileno())
            if output.exists() and not args.overwrite:
                raise OperatorError(f"output already exists: {output}; pass --overwrite to replace it", code="output_exists")
            partial.replace(output)
            _print_json({"output": str(output), "bytes": size, "sha256": response.headers.get("x-package-sha256")})
            return 0
    except OperatorError:
        if partial.exists():
            partial.unlink()
        raise
    except (OSError, httpx.HTTPError) as error:
        if partial.exists():
            partial.unlink()
        if isinstance(error, httpx.HTTPError):
            raise OperatorError("catalog service could not be reached while streaming export", 3, "service_unavailable") from error
        raise OperatorError(f"cannot write export: {error.strerror or error.__class__.__name__}", 5, "output_unavailable") from error


def _validate_package(client: httpx.Client, path: Path) -> int:
    package = path.expanduser()
    try:
        size = package.stat().st_size
        if size > MAX_PACKAGE_BYTES:
            raise OperatorError("package exceeds the 8 MiB validation limit", code="package_too_large")
        data = package.read_bytes()
    except OSError as error:
        raise OperatorError(f"cannot read {package}: {error.strerror or error.__class__.__name__}", code="package_unavailable") from error
    if len(data) > MAX_PACKAGE_BYTES:
        raise OperatorError("package exceeds the 8 MiB validation limit", code="package_too_large")
    try:
        response = client.post("/catalogs/exports/validate", content=data, headers={"Content-Type": "application/zip"})
    except httpx.TimeoutException as error:
        raise OperatorError("catalog request timed out", 3, "request_timeout") from error
    except httpx.HTTPError as error:
        raise OperatorError("catalog service could not be reached", 3, "service_unavailable", {"type": error.__class__.__name__}) from error
    result = _response_json(response)
    if response.status_code >= 400:
        error = result.get("error", {}) if isinstance(result, dict) else {}
        raise OperatorError(error.get("message", f"catalog returned HTTP {response.status_code}"), 4, error.get("code", "http_error"), error.get("details"))
    _print_json(result)
    return 0 if result.get("valid") else 5


def run_operator(args: argparse.Namespace) -> int:
    try:
        base_url = _base_url(args.base_url)
        with httpx.Client(base_url=base_url, timeout=args.timeout, follow_redirects=False) as client:
            if args.operator_command == "search":
                return _search(client, args)
            if args.operator_command == "import":
                return _import_command(client, args)
            if args.operator_command == "export":
                return _export(client, args)
            if args.operator_command == "validate":
                return _validate_package(client, args.package)
            raise OperatorError("unsupported operator command", code="unknown_command")
    except OperatorError as error:
        print(json.dumps({"error": {"code": error.code, "message": str(error), "details": error.details}}, ensure_ascii=False), file=sys.stderr)
        return error.exit_code
