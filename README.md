# Media Catalog Python

Media Catalog Python is a local operations API for descriptive movie and series metadata. It runs four independent HTTP processes: movie, series, catalog, and gateway. The movie and series services own their SQLite files. The catalog service builds a durable projection by calling the source HTTP APIs; it does not open their databases. The gateway forwards domain requests to those services.

The local profile needs Python 3.11 or newer and [uv](https://docs.astral.sh/uv/). It does not require Docker, a cloud account, a media provider API, or a broker for the normal workflow.

## Start the services

```sh
uv sync --locked
./scripts/start-local.sh
```

The supervisor starts all four processes on loopback, waits for their health checks, and exits with an error if a child process stops unexpectedly. Use Ctrl-C to stop the stack. To run one service separately:

```sh
uv run python -m media_catalog serve movie
```

The service choices are `movie`, `series`, `catalog`, and `gateway`. The `local` command accepts `--data-dir` and `--host`; loopback is the default host.

The `operator` command uses the HTTP gateway for daily workflows. It defaults to `http://127.0.0.1:8080`; set `MEDIA_CATALOG_GATEWAY_URL` or pass `--base-url` after `operator` to use another gateway. Requests use a five-second timeout by default; `--timeout` accepts 0.1–60 seconds.

```sh
uv run python -m media_catalog operator search --text harbor --kind movie
uv run python -m media_catalog operator import preview ./batch.json
uv run python -m media_catalog operator import commit ./batch.json --idempotency-key import-batch-demo-01
uv run python -m media_catalog operator import list --state failed
uv run python -m media_catalog operator import retry JOB_ID
uv run python -m media_catalog operator export ./selection.json --output ./selection.zip
uv run python -m media_catalog operator validate ./selection.zip
```

The import JSON file contains a `rows` array with `{ "kind": "movie"|"series", "item": {...} }` records; commit accepts or generates its JSON `idempotencyKey` from `--idempotency-key`. JSON input is capped at 2 MiB. Export requires an explicit output path in an existing directory, does not overwrite an existing file unless `--overwrite` is supplied, writes through a temporary sibling, and caps package output at 50 MiB. Local package validation reads at most 8 MiB and asks the catalog service to verify the package without extracting it. Exit codes are 0 for a completed command, 2 for local input/argument errors, 3 for connection or timeout errors, 4 for an HTTP error, and 5 for invalid preview/package results or incomplete import runs.

| Service | Default port | Owned storage |
| --- | ---: | --- |
| Movie | 8102 | `data/movies.sqlite3` |
| Series | 8103 | `data/series.sqlite3` |
| Catalog | 8104 | `data/catalog.sqlite3` |
| Gateway | 8080 | None |

Set `MOVIE_DB_PATH`, `SERIES_DB_PATH`, and `CATALOG_DB_PATH` to select other SQLite files. `MOVIE_SERVICE_URL`, `SERIES_SERVICE_URL`, and `CATALOG_SERVICE_URL` configure HTTP upstreams. `.env.example` lists variable names with local demo values; the application reads process environment variables and does not load a `.env` file.

## Content model and API behavior

Movie and series records include an integer `id`, `name`, `synopsis`, `genre`, optional `releaseYear` and `externalId`, `language`, `status`, `version`, `createdAt`, and `updatedAt`. Movie records also have optional `runtimeMinutes` and `streamUrl`. Series contain `seasons`, and each season contains ordered `episodes` with a stable integer ID, `episodeNumber`, synopsis, optional runtime, and optional `streamUrl`. A stream URL is stored as metadata only; the services never fetch it.

All request models reject unknown fields. Names and genres must contain non-whitespace text. Years must be plausible and no more than five years in the future; runtimes and hierarchy numbers must be positive. Stream URLs must be absolute HTTP or HTTPS URLs without embedded user credentials. The current API uses English identifiers and payloads, including `streamUrl` and `episodes`.

Lists use an envelope such as `{"items": [], "total": 0, "limit": 50, "offset": 0}`. The default page size is 50 and the maximum is 200. Lists have stable ordering. Missing records return 404, invalid structures return 422, and state or uniqueness conflicts return 409. Errors use `{"error":{"code":"...","message":"...","details":...}}`.

| Service | Method and path | Behavior |
| --- | --- | --- |
| Movie | `GET /movies` | Filter by exact case-insensitive `genre` and/or `status`; paginate. |
| Movie | `POST /movies` | Create a draft movie; returns 201 and its assigned ID. |
| Movie | `GET /movies/{id}` | Read movie metadata, license windows, and registered asset metadata. |
| Movie | `PUT /movies/{id}` | Replace descriptive fields with an `expectedVersion`; reject stale edits. |
| Series | `GET /series`, `POST /series`, `GET /series/{id}`, `PUT /series/{id}` | List, create, read, and version-update series with nested seasons and episodes. A series update supplies the complete `seasons` array. Existing child IDs are retained when matching children are updated. |
| Series | `PUT /series/{id}/seasons/{seasonId}/reorder` | Replace episode order with `{"episodeIds":[...]}`; the body must be a complete permutation of the season's episode IDs. |
| Movie and series | `GET /{resource}/{id}/readiness` | Return structured publication issues. |
| Movie and series | `POST /{resource}/{id}/transition` | Apply `publish`, `unpublish`, `archive`, or `restore` with `expectedVersion`. Publishing requires a synopsis, playback metadata, and an active WORLD license; series also require a stream URL for every episode. |
| Movie and series | `GET`, `POST`, `DELETE /{resource}/{id}/licenses...` | Manage UTC rights windows by region. Start is inclusive, end is exclusive; overlapping windows for the same content and region are rejected. Availability checks prefer a matching country license and fall back to WORLD. |
| Movie and series | `GET`, `POST`, `DELETE /{resource}/{id}/assets...` | Manage safe relative-path asset metadata. No arbitrary file path is opened or inspected. |
| Movie and series | `GET /{resource}/{id}/audit` | Read actor context, reason, action, version, and mutation summary. `X-Audit-Actor` is attribution only, not authentication. |
| Movie and series | `GET /{resource}/changes`, `GET /{resource}/snapshot` | Read the transactional outbox feed and consistent snapshot with a high-water mark. Change feeds use monotonically increasing integer sequences. |
| Movie and series | `GET /{resource}/duplicates`, `GET /{resource}/reports/assets`, `GET /{resource}/reports/series-structure` | Review normalized name/year duplicates, asset metadata quality, and series numbering/runtime summaries. The structure report applies to series. |
| Catalog | `POST /catalogs/sync`, `POST /catalogs/rebuild` | Apply source changes after persisted cursors, or rebuild from both HTTP snapshots and catch up after their watermarks. |
| Catalog | `GET /catalogs/items/{kind}/{id}`, `GET /catalogs/items/{kind}/{id}/availability` | Read one projected mixed-catalog item and evaluate its current region/time availability without contacting a source process. |
| Catalog | `GET`, `POST /catalogs/search`, `GET /catalogs/facets` | Discover projected movie and series records by text, kind, genre, status, language, release-year range, and label. Search supports stable sort and pagination. |
| Catalog | `/catalogs/labels...` | Create labels, attach them to projected content, inspect usage counts, and remove assignments. Labels cannot be deleted while attached. |
| Catalog | `/catalogs/collections...` | Create mixed ordered collections, add or remove members, reorder by a complete `(kind, id)` permutation, validate distribution availability, and update collection metadata with an optimistic version. |
| Catalog | `/catalogs/saved-searches...` | Save validated search queries, run them against the current projection, and update them with an optimistic version. |
| Catalog | `/catalogs/reports/...` | Read statistics, availability, metadata quality, duplicate candidates, asset inventory, and projected series structure. |
| Catalog | `/catalogs/imports/preview`, `/catalogs/imports`, `/catalogs/imports/{jobId}/run`, `/retry` | Validate JSON batches, create durable idempotent jobs, list jobs by state, run them through source HTTP endpoints, inspect row outcomes, and retry failed or interrupted rows. |
| Catalog | `POST /catalogs/exports`, `POST /catalogs/exports/validate` | Download a deterministic metadata-only ZIP and validate package structure and checksums. `GET /catalogs/exports` lists package records. |
| Catalog | `GET /catalogs/health`, `GET /catalogs/ready`, `GET /catalogs/sync/runs`, `GET /catalogs/rebuilds` | Inspect database integrity, upstream readiness, projection cursors, and recorded sync/rebuild runs. |
| Gateway | `/{movies,series,catalogs}/...` | Forward the service path, repeated query parameters, status, and end-to-end headers. `GET /health/live` and `GET /health/ready` report gateway health. |

The movie and series services also expose an availability endpoint at `GET /{resource}/{id}/availability?region=CO&at=2026-01-15T12:00:00Z`. The supplied time must include a timezone. A published item without an active matching license returns `available: false` with a reason.

## Editing and lifecycle

Every source content update and lifecycle transition includes `expectedVersion`. A stale version returns a 409 `stale_version` error. Source changes, version increments, audit records, and full outbox events commit in the same SQLite transaction. Published content cannot be edited into a state that fails readiness; unpublish it first. Archived content must be restored before editing.

SQLite files use foreign keys, WAL mode, a 10-second busy timeout, role-specific versioned migrations, and explicit transactions. A movie process only creates and migrates the movie schema; series and catalog storage are separate. Migrations rename the old movie stream typo and normalize legacy nested chapter data while preserving entity and child IDs where possible. If legacy JSON is malformed or ambiguous numbering/IDs cannot be represented, migration raises an error and rolls back instead of silently dropping hierarchy rows.

## Catalog synchronization and outage behavior

The catalog service reads source records only through `/movies` and `/series` HTTP feeds and snapshots. Each source database has an identity as well as a sequence cursor. Synchronization buffers the pages for a source and applies its events and cursor in one catalog transaction only after the source pass completes; a failed request does not advance that source cursor. Replayed events are idempotent, out-of-order gaps are rejected, and older entity revisions do not overwrite newer projections.

Each source snapshot reads items and its high-water mark in one SQLite read transaction. Rebuild applies both snapshots without deleting catalog labels, collections, or saved searches, then synchronizes events newer than each snapshot boundary. If a source database is replaced, ordinary sync reports the identity change and retains the last projection until an operator runs a snapshot rebuild. During source outages, catalog search continues to use its last successful projection; health reports the upstream failure and the age/cursor of projected data.

## Labels, collections, and reports

Labels are case-normalized and can only attach to an existing projected `(kind, id)` key, so a movie ID and series ID may be equal without colliding. If a later snapshot removes an item, associated labels and collection memberships remain stored; quality and collection validation report dangling references.

Collection member order is explicit and deterministic. Reorder requests must contain every current member exactly once. Availability validation reports draft, archived, missing-rights, and dangling references for the requested region and time. Search facets are computed from all matching projected records, not only the current result page.

Asset records describe a relative path, role, MIME type, byte size, and SHA-256 checksum. Inventory reports missing poster metadata, zero-byte records, and duplicate checksum references. Disk inspection is deliberately not performed; registering a path does not download media or grant access to the filesystem.

## JSON import jobs

Preview accepts 1–500 rows and performs strict per-kind model validation without writing to source services. It reports row numbers, validation errors, duplicate batch identifiers, similar-name warnings, and create/update plans. A matching `externalId` plans an update using the current projected version.

Commit revalidates the batch before creating a durable job. Supply the 8–120 character `idempotencyKey` in the JSON body:

```json
{
  "idempotencyKey": "import-batch-demo-01",
  "rows": [
    {
      "kind": "movie",
      "item": {
        "name": "DUMMY Harbor Feature",
        "synopsis": "A synthetic import example.",
        "streamUrl": "https://media.example.invalid/harbor-feature"
      }
    }
  ]
}
```

Repeating the same key with the same canonical payload returns the original job; using that key with different content returns 409. Jobs begin in `pending` state and only run when `/run` is called. `GET /catalogs/imports` lists jobs newest first and can filter by `pending`, `running`, `completed`, or `failed`. Rows are sent to the movie or series process over HTTP with persisted operation keys. Successful rows are not repeated during retry. If the catalog stops after the source has committed but before the row result is saved, the source's idempotency record returns the original response on retry. A job can finish with partial failures; inspect row outcomes before retrying.

The separate source service `POST` and `PUT` endpoints accept an optional `Idempotency-Key` HTTP header of 8–120 characters. Import jobs generate and persist that source operation key per row.

The API accepts JSON batches only. It does not read arbitrary files or import uploaded ZIP archives.

## Metadata export packages

`POST /catalogs/exports` accepts selected movie/series references and returns a deterministic ZIP with `manifest.json` and `content.json`. The package includes descriptive metadata and recorded license/asset metadata; it does not include media files. ZIP entries have fixed timestamps and stable ordering, so the same projection and selection produce the same bytes and SHA-256 value.

`POST /catalogs/exports/validate` accepts raw ZIP bytes with `application/zip` or `application/octet-stream`. Uploads are capped at 8 MiB, expanded content at 24 MiB, and archives at eight entries. Only the two supported root entries are accepted. The validator checks manifest version, member names, item count, and content checksum without extracting files. It reports malformed, missing, tampered, encrypted, and unsupported-compression packages as findings.

## Optional RabbitMQ intake

RabbitMQ is a separate optional consumer of the `Movie` queue. Install the extra and configure the broker URL through process environment:

```sh
uv sync --locked --extra rabbitmq
RABBITMQ_URL="<broker URL supplied by the operator>" uv run python -m media_catalog consume-movies
```

The consumer uses the movie model and transactional repository, stores idempotency by message ID (or by canonical payload when no message ID is supplied), acknowledges only after persistence, rejects malformed/permanent conflicts without requeue, and requeues transient database failures. Its offline tests exercise decoding, persistence, and acknowledgment decisions through callback doubles. A live RabbitMQ server has not been verified as part of the local suite.

## Verification

```sh
./scripts/verify.sh
```

The script installs the locked development dependencies and runs pytest. Integration tests launch the four actual HTTP processes with temporary ports and separate temporary SQLite files. The suite covers source migrations, validation and state transitions, version races, hierarchy IDs and ordering, audit/outbox consistency, gateway forwarding, multi-page sync and cursor rollback, source/database restart and reset behavior, projected discovery during outages, catalog curation, import retry and concurrent workers, deterministic ZIP export, package validation, and broker callback decisions.

Tests use generated synthetic records and reserved `.invalid` URLs. The runtime creates no demo users, media files, API keys, or credentials.

## License

MIT. See [LICENSE](LICENSE).
