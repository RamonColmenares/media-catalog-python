from __future__ import annotations

import json
import sqlite3

import httpx

from conftest import LiveStack


def test_movie_and_series_routes_validate_and_preserve_nested_data(live_stack: LiveStack) -> None:
    client = httpx.Client(timeout=3)
    movie_service = live_stack.url("movie", "")
    series_service = live_stack.url("series", "")
    try:
        assert client.get(f"{movie_service}/movies").json() == []
        created_movie = client.post(
            f"{movie_service}/movies",
            json={
                "name": "Demo Feature",
                "genre": "Adventure",
                "ulrStream": "https://media.example.invalid/demo-feature",
            },
        )
        assert created_movie.status_code == 200
        movie = created_movie.json()
        assert movie == {
            "id": 1,
            "name": "Demo Feature",
            "genre": "Adventure",
            "ulrStream": "https://media.example.invalid/demo-feature",
        }
        assert client.get(f"{movie_service}/movies/Adventure").json() == [movie]
        assert client.get(f"{movie_service}/movies/adventure").json() == []

        empty_series = client.get(f"{series_service}/series")
        assert empty_series.status_code == 404
        assert empty_series.content == b""
        created_series = client.post(
            f"{series_service}/series",
            json={
                "name": "Demo Quest",
                "genre": "Adventure",
                "seasons": [
                    {
                        "seasonNumber": 1,
                        "chapters": [
                            {
                                "name": "The First Trail",
                                "number": 1,
                                "urlStream": "https://media.example.invalid/demo-quest-ep1",
                            }
                        ],
                    }
                ],
            },
        )
        assert created_series.status_code == 200
        series = created_series.json()
        assert series["id"] == 1
        assert series["seasons"][0]["seasonNumber"] == 1
        assert series["seasons"][0]["chapters"][0] == {
            "id": None,
            "name": "The First Trail",
            "number": 1,
            "urlStream": "https://media.example.invalid/demo-quest-ep1",
        }
        assert client.get(f"{series_service}/series/Adventure").json() == [series]
        no_match = client.get(f"{series_service}/series/Comedy")
        assert no_match.status_code == 404 and no_match.content == b""

        invalid_movie = client.post(
            f"{movie_service}/movies",
            content=b"{",
            headers={"content-type": "application/json"},
        )
        assert invalid_movie.status_code == 422
        invalid_series = client.post(
            f"{series_service}/series",
            json={"name": "Broken Demo", "seasons": [{"seasonNumber": "one"}]},
        )
        assert invalid_series.status_code == 422
        assert client.get(f"{movie_service}/movies/withErrors/Adventure").status_code == 404
        assert client.get(f"{series_service}/series/Adventure/1").status_code == 404
    finally:
        client.close()


def test_catalog_calls_both_services_caches_both_and_returns_movies(live_stack: LiveStack) -> None:
    client = httpx.Client(timeout=3)
    gateway = live_stack.url("gateway", "")
    catalog_service = live_stack.url("catalog", "")
    try:
        movie = client.post(
            f"{gateway}/movies",
            json={
                "name": "Demo Harbor",
                "genre": "Mystery",
                "ulrStream": "https://media.example.invalid/demo-harbor",
            },
        ).json()
        series = client.post(
            f"{gateway}/series",
            json={
                "name": "Demo Signal",
                "genre": "Mystery",
                "seasons": [
                    {
                        "seasonNumber": 2,
                        "chapters": [
                            {"name": "Static", "number": 4, "urlStream": "https://media.example.invalid/demo-signal-4"}
                        ],
                    }
                ],
            },
        ).json()

        movie_query = client.get(f"{catalog_service}/catalogs/movies/Mystery")
        assert movie_query.status_code == 200 and movie_query.json() == [movie]
        catalog_query = client.get(f"{gateway}/catalogs/Mystery")
        assert catalog_query.status_code == 200
        assert catalog_query.json() == [movie]

        with sqlite3.connect(live_stack.db_paths["catalog"]) as connection:
            cached_movie = connection.execute(
                "SELECT payload_json FROM catalog_movie_cache WHERE media_id = ?", (movie["id"],)
            ).fetchone()
            cached_series = connection.execute(
                "SELECT payload_json FROM catalog_series_cache WHERE media_id = ?", (series["id"],)
            ).fetchone()
        assert cached_movie is not None and json.loads(cached_movie[0]) == movie
        assert cached_series is not None and json.loads(cached_series[0]) == series

        assert client.get(f"{gateway}/unknown/path").status_code == 404
        # The generic /catalogs/{genre} route treats `movies` as a genre here;
        # its series upstream returns 404 because the demo data has no such genre.
        assert client.get(f"{gateway}/catalogs/movies").status_code == 502
        assert client.get(f"{gateway}/fallback/movies").status_code == 500
    finally:
        client.close()


def test_service_restart_keeps_data_and_gateway_returns_fallbacks(live_stack: LiveStack) -> None:
    client = httpx.Client(timeout=3)
    gateway = live_stack.url("gateway", "")
    try:
        movie = client.post(
            f"{gateway}/movies",
            json={"name": "Demo Station", "genre": "Drama"},
        ).json()
        series = client.post(
            f"{gateway}/series",
            json={"name": "Demo Weather", "genre": "Drama", "seasons": []},
        ).json()
        assert client.get(f"{gateway}/movies/Drama").json() == [movie]
        assert client.get(f"{gateway}/series/Drama").json() == [series]

        live_stack.restart_service("movie")
        client.close()
        client = httpx.Client(timeout=3)
        assert client.get(f"{live_stack.url('movie', '/movies/Drama')}").json() == [movie]

        live_stack.stop_service("movie")
        unavailable_movie = client.get(f"{gateway}/movies")
        assert unavailable_movie.status_code == 500
        assert unavailable_movie.text == "Servidor de movies no está disponible actualmente"

        live_stack.stop_service("series")
        unavailable_series = client.get(f"{gateway}/series")
        assert unavailable_series.status_code == 500
        assert unavailable_series.text == "Servidor de series no está disponible actualmente"

        live_stack.stop_service("catalog")
        unavailable_catalog = client.get(f"{gateway}/catalogs/Drama")
        assert unavailable_catalog.status_code == 500
        assert unavailable_catalog.text == "Servidor de catalogos no está disponible actualmente"
    finally:
        client.close()
