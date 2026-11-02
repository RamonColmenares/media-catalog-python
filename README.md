# Media Catalog Python

Four Python HTTP services for movie and series data, a catalog service, and an API gateway. The local profile uses separate SQLite databases and real HTTP between processes, so it runs without Docker or an account with an external service.

## Start the local services

Requirements: Python 3.11 or newer and [uv](https://docs.astral.sh/uv/).

```sh
uv sync --locked
./scripts/start-local.sh
```

The supervisor starts four independent processes on loopback:

| Service | Port | SQLite file |
| --- | ---: | --- |
| Movie | 8102 | `data/movies.sqlite3` |
| Series | 8103 | `data/series.sqlite3` |
| Catalog | 8104 | `data/catalog.sqlite3` |
| Gateway | 8080 | none |

Use Ctrl-C to stop the supervisor and its child services. You can also run a service in its own terminal with `uv run python -m media_catalog serve movie`; the service choices are `movie`, `series`, `catalog`, and `gateway`.

The `.env.example` file lists supported environment variable names. The app reads values from the process environment; it does not load a `.env` file. Set `MOVIE_DB_PATH`, `SERIES_DB_PATH`, or `CATALOG_DB_PATH` to use a different SQLite file. Set service URL variables when running the services on different hosts or ports.

## HTTP routes

The gateway forwards the same paths to their corresponding service. Service ports can also be called directly.

| Method | Path | Behavior |
| --- | --- | --- |
| `GET` | `/movies` | List movies; an empty collection returns `200 []`. |
| `POST` | `/movies` | Save a movie and return it with its generated ID. |
| `GET` | `/movies/{genre}` | List movies with an exact genre match. |
| `GET` | `/series` | List series; an empty collection returns `404` with no body. |
| `GET` | `/series/{genre}` | List matching series; no matches return `404` with no body. |
| `POST` | `/series` | Save a series, including nested seasons and chapters. |
| `GET` | `/catalogs/movies/{genre}` | Query the movie service by genre. |
| `GET` | `/catalogs/{genre}` | Query movies and series, cache both result sets by ID, and return the movie list. |

For example, `POST /movies` accepts:

```json
{
  "name": "Demo Feature",
  "genre": "Adventure",
  "ulrStream": "https://media.example.invalid/demo-feature"
}
```

The `ulrStream` key keeps the spelling used by the existing movie API. Series retain `seasons`, `seasonNumber`, `chapters`, `number`, and `urlStream` as nested JSON fields. Demo titles and `.invalid` stream addresses are synthetic placeholders, not playable media.

The catalog response deliberately matches the current service contract: `/catalogs/{genre}` stores the movie and series results locally, but returns only movies. The catalog Mongo repository in the source was not used by that route; the Python implementation keeps the fetched data in its own two SQLite cache tables. This endpoint is not a combined movie-and-series response.

If a gateway upstream cannot be reached, the gateway returns HTTP 500 with the same Spanish fallback text used by the source gateway. If the catalog cannot fetch an upstream response, it returns HTTP 502. Unknown paths return `404`.

## Optional RabbitMQ movie intake

The movie service supports `POST /movies` and the original durable RabbitMQ queue named `Movie`. The consumer is a separate process, disabled by default. To connect it to a real broker, install the optional dependency and supply a broker URL through the environment:

```sh
uv sync --locked --extra rabbitmq
RABBITMQ_URL="<broker URL supplied by the operator>" uv run python -m media_catalog consume-movies
```

The consumer reads JSON movie payloads and writes to the configured movie SQLite file. It acknowledges valid messages after persistence, rejects malformed payloads, and requeues unexpected persistence failures. The offline verification suite tests the message decoder and database write path; it does not claim to emulate or verify a live RabbitMQ broker. Supply the same `MOVIE_DB_PATH` to the HTTP service and consumer if you override the default.

## Verification

Run the reproducible verification command from the repository root:

```sh
./scripts/verify.sh
```

It checks the Python version, syncs dependencies from `uv.lock`, and runs pytest. Integration tests start the actual movie, series, catalog, and gateway HTTP processes on temporary ports with isolated SQLite files. They exercise supported routes, nested data, catalog caching, gateway proxying and failures, validation errors, and persistence after a service restart.

## Runtime scope

The Python services preserve the HTTP behavior described above and keep movie, series, and catalog storage in separate SQLite files. The original deployment infrastructure is not included: this package does not run Eureka discovery, Spring Config Server, Zipkin tracing, Redis, MySQL, or MongoDB. Service locations are configured with environment variables. RabbitMQ remains an optional real broker adapter, but live broker operation is outside the default local profile and is not covered by an end-to-end broker test.

## License

MIT. See [LICENSE](LICENSE).
