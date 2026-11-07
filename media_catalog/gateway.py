"""HTTP gateway that forwards domain paths and reports upstream readiness."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

import httpx
from fastapi import FastAPI, Request
from starlette.responses import JSONResponse, Response

logger = logging.getLogger("media_catalog.gateway")
_SERVICES = {"movies": "movie", "series": "series", "catalogs": "catalog"}
_HOP_BY_HOP = {
    b"connection", b"content-length", b"host", b"keep-alive",
    b"proxy-authenticate", b"proxy-authorization", b"te", b"trailer", b"transfer-encoding", b"upgrade",
}


def create_app() -> FastAPI:
    service_urls = {
        "movies": os.getenv("MOVIE_SERVICE_URL", "http://127.0.0.1:8102").rstrip("/"),
        "series": os.getenv("SERIES_SERVICE_URL", "http://127.0.0.1:8103").rstrip("/"),
        "catalogs": os.getenv("CATALOG_SERVICE_URL", "http://127.0.0.1:8104").rstrip("/"),
    }
    request_timeout = float(os.getenv("GATEWAY_REQUEST_TIMEOUT", "2"))

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        async with httpx.AsyncClient(timeout=request_timeout, follow_redirects=False) as client:
            app.state.http_client = client
            yield

    app = FastAPI(title="Media API gateway", version="2.0.0", lifespan=lifespan)

    @app.get("/health/live")
    async def live() -> dict[str, bool]:
        return {"live": True}

    @app.get("/health/ready")
    async def ready() -> Response:
        states = {}
        for prefix, url in service_urls.items():
            name = _SERVICES[prefix]
            health_path = "/catalogs/health" if name == "catalog" else f"/{prefix}/health"
            try:
                response = await app.state.http_client.get(f"{url}{health_path}")
                states[name] = {"reachable": response.status_code == 200, "statusCode": response.status_code}
            except httpx.HTTPError:
                states[name] = {"reachable": False, "statusCode": None}
        is_ready = all(state["reachable"] for state in states.values())
        return JSONResponse(status_code=200 if is_ready else 503, content={"ready": is_ready, "upstreams": states})

    async def forward(service: str, subpath: str, request: Request) -> Response:
        if service not in service_urls:
            return JSONResponse(status_code=404, content={"error": {"code": "unknown_service", "message": "No service handles this route", "details": {"service": service}}})
        route = f"/{service}" + (f"/{subpath}" if subpath else "")
        headers = [(key.decode("latin-1"), value.decode("latin-1")) for key, value in request.headers.raw if key.lower() not in _HOP_BY_HOP]
        try:
            upstream = await app.state.http_client.request(
                request.method,
                f"{service_urls[service]}{route}",
                params=list(request.query_params.multi_items()),
                headers=headers,
                content=await request.body(),
            )
        except httpx.HTTPError as error:
            logger.info("Upstream request failed service=%s path=%s type=%s", service, request.url.path, error.__class__.__name__)
            return JSONResponse(status_code=503, content={"error": {"code": "upstream_unavailable", "message": f"The {service} service is unavailable", "details": {"service": service}}})
        forwarded = Response(content=upstream.content, status_code=upstream.status_code)
        kept = [(key, value) for key, value in upstream.headers.raw if key.lower() not in _HOP_BY_HOP]
        retained_names = {key.lower() for key, _ in kept}
        generated = [(key, value) for key, value in forwarded.raw_headers if key.lower() not in retained_names]
        forwarded.raw_headers = [(key, value) for key, value in kept if key.lower() != b"content-encoding"] + generated
        return forwarded

    methods = ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]

    @app.api_route("/{service}", methods=methods)
    async def forward_root(service: str, request: Request) -> Response:
        return await forward(service, "", request)

    @app.api_route("/{service}/{subpath:path}", methods=methods)
    async def forward_path(service: str, subpath: str, request: Request) -> Response:
        return await forward(service, subpath, request)

    return app


app = create_app()
