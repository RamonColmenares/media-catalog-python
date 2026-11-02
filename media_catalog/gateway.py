"""HTTP gateway for the movie, series, and catalog services."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

import httpx
from fastapi import FastAPI, Request
from starlette.responses import PlainTextResponse, Response


logger = logging.getLogger("media_catalog.gateway")
_SERVICE_NAMES = {"movies": "movie", "series": "series", "catalogs": "catalogs"}
_FALLBACKS = {
    "movies": "Servidor de movies no está disponible actualmente",
    "series": "Servidor de series no está disponible actualmente",
    "catalogs": "Servidor de catalogos no está disponible actualmente",
}
_HOP_BY_HOP_HEADERS = {
    b"connection",
    b"content-encoding",
    b"content-length",
    b"host",
    b"keep-alive",
    b"proxy-authenticate",
    b"proxy-authorization",
    b"te",
    b"trailer",
    b"transfer-encoding",
    b"upgrade",
}


def create_app() -> FastAPI:
    service_urls = {
        "movies": os.getenv("MOVIE_SERVICE_URL", "http://127.0.0.1:8102").rstrip("/"),
        "series": os.getenv("SERIES_SERVICE_URL", "http://127.0.0.1:8103").rstrip("/"),
        "catalogs": os.getenv("CATALOG_SERVICE_URL", "http://127.0.0.1:8104").rstrip("/"),
    }
    request_timeout = float(os.getenv("GATEWAY_REQUEST_TIMEOUT", "1.5"))

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        async with httpx.AsyncClient(timeout=request_timeout) as client:
            app.state.http_client = client
            yield

    app = FastAPI(title="Media API gateway", version="1.0.0", lifespan=lifespan)

    @app.get("/fallback/movies")
    async def movie_fallback() -> PlainTextResponse:
        return PlainTextResponse(_FALLBACKS["movies"], status_code=500)

    @app.get("/fallback/series")
    async def series_fallback() -> PlainTextResponse:
        return PlainTextResponse(_FALLBACKS["series"], status_code=500)

    @app.get("/fallback/catalogs")
    async def catalog_fallback() -> PlainTextResponse:
        return PlainTextResponse(_FALLBACKS["catalogs"], status_code=500)

    async def forward(service: str, subpath: str, request: Request) -> Response:
        if service not in service_urls:
            return PlainTextResponse("Not Found", status_code=404)
        logger.info("Forwarding request path=%s", request.url.path)
        url_path = f"/{service}"
        if subpath:
            url_path += f"/{subpath}"
        headers = {
            key.decode("latin-1"): value.decode("latin-1")
            for key, value in request.headers.raw
            if key.lower() not in _HOP_BY_HOP_HEADERS
        }
        try:
            upstream = await app.state.http_client.request(
                request.method,
                f"{service_urls[service]}{url_path}",
                params=list(request.query_params.multi_items()),
                headers=headers,
                content=await request.body(),
            )
        except httpx.HTTPError:
            return PlainTextResponse(_FALLBACKS[service], status_code=500)

        response_headers = {
            key: value
            for key, value in upstream.headers.items()
            if key.lower().encode("latin-1") not in _HOP_BY_HOP_HEADERS
        }
        return Response(
            content=upstream.content,
            status_code=upstream.status_code,
            headers=response_headers,
        )

    methods = ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]

    @app.api_route("/{service}", methods=methods)
    async def forward_root(service: str, request: Request) -> Response:
        return await forward(service, "", request)

    @app.api_route("/{service}/{subpath:path}", methods=methods)
    async def forward_path(service: str, subpath: str, request: Request) -> Response:
        return await forward(service, subpath, request)

    return app


app = create_app()
