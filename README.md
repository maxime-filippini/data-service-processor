# Market data processor

Python processor for frozen processing jobs owned by the data-service Worker.
The Worker keeps D1 state and exposes create/read/claim/complete/fail routes;
this service calls those routes rather than maintaining another control plane.

## Run

Install with `uv sync`. Configuration uses `pydantic-settings`: set the environment
variables below or put them in a local `.env` file. Environment variables override
`.env` values. URLs must be HTTP(S), credentials must be nonempty, and all
processor settings are required before job execution. HTTP authentication also
requires `PROCESSOR_API_TOKEN`; the CLI does not. Secret values are masked in
settings representations. Never bake secrets into the image.

| Variable | Purpose |
| --- | --- |
| `PROCESSING_API_URL` | Worker base URL, including any route prefix before `/processing-jobs` |
| `PROCESSING_API_TOKEN` | Worker control-plane bearer token |
| `PROCESSOR_API_TOKEN` | Separate bearer token for this container's execution endpoint |
| `R2_ENDPOINT_URL` | `https://<account-id>.r2.cloudflarestorage.com` |
| `R2_ACCESS_KEY_ID` | R2 S3 access key |
| `R2_SECRET_ACCESS_KEY` | R2 S3 secret key |

R2 credentials need read access to `raw-market-data` and read/write access to
`processed-market-data`. The processor never lists buckets or accesses D1.

One-shot execution:

```sh
uv run processor --job-id <job-id>
```

HTTP execution:

```sh
uv run uvicorn data_service_processor.app:app --host 0.0.0.0 --port 8080
curl -X POST http://localhost:8080/processing-jobs/<job-id>/execute \
  -H "Authorization: Bearer $PROCESSOR_API_TOKEN"
```

`GET /health` is an unauthenticated liveness probe. Execution is synchronous:
the response is the completed Worker job, not an acknowledgement for a detached
background task. Claim conflicts return 409; missing jobs return 404.
Processing failures are reported to the Worker and return an error. Inspect
Worker job state before retrying an ambiguous or interrupted request.

## Container

```sh
docker build --platform linux/amd64 -t market-data-processor .
docker run --rm -p 8080:8080 --env-file .env market-data-processor
# The same image supports the one-shot command:
docker run --rm --env-file .env market-data-processor processor --job-id <job-id>
```

The image uses locked dependencies, runs as a non-root user, and listens on
`0.0.0.0:8080`. Cloudflare requires a `linux/amd64` image. Configure a container
Durable Object in the routing Worker with port 8080, outbound Internet access,
and the environment variables above. Configure its lifecycle so it stays alive
for the whole synchronous processing request. This repository supplies the
Python image; Worker binding, routing, queue delivery, and deployment remain
separate integration work.

## Transformation v1

Create jobs with `transformVersion: "v1"`. Only the EODHD version-1 envelope is
supported. The processor validates run identity, symbol, requested range, row
count, finite prices, nonnegative integer volume, and exact object ETags.
Parquet columns are `date` (Date), `open`, `close`, `high`, `low`,
`adjusted_close` (Float64), and `volume` (Int64). Rows are sorted by date;
later entries and later manifest runs replace earlier rows on duplicate dates.

Rebuild uses only frozen raw inputs. Merge reads the canonical object only
when `expectedBaseEtag` exists and conditionally replaces that version; an
initial merge uses `IfNoneMatch: *`. Unsupported versions fail the claimed job.
`completeThrough` is omitted because the document does not define a coverage
rule; the latest observed date alone does not establish complete coverage.

Datasets are processed in memory. A crash after claim leaves the job processing,
as in the current Worker contract. Object upload and D1 completion are separate
operations: if completion fails after upload, canonical bytes may already have
changed while metadata still references the previous ETag. Such failures need
operator reconciliation; there is no automatic transaction or recovery policy.

## Verification

```sh
uv run pytest
uv run ruff check src tests
uv run ruff format --check src tests
```

Tests exercise the HTTP client against a simulated Worker and R2 storage,
including actual Parquet serialization, merge/rebuild, precedence, ETag checks,
failure reporting, and HTTP authentication. A live Worker/D1/R2 end-to-end run
requires a deployed control plane and seeded raw ingestion runs.
