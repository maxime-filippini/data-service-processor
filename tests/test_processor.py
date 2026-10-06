import io
import json
from contextlib import contextmanager

import httpx
import polars as pl
import pytest
from fastapi.testclient import TestClient

from data_service_processor import app as app_module
from data_service_processor.client import ControlPlane
from data_service_processor.processor import SCHEMA, Processor


def entry(close=10, day="2024-01-01"):
    return {
        "date": day,
        "open": 10,
        "close": close,
        "high": 12,
        "low": 9,
        "adjusted_close": close,
        "volume": 100,
    }


def manifest(mode="rebuild"):
    return {
        "jobId": "job-1",
        "symbol": "AAPL.US",
        "mode": mode,
        "status": "processing",
        "transformVersion": "v1",
        "canonicalKey": "dataset=prices_eod/symbol=AAPL.US/data.parquet",
        "runs": [
            {
                "runId": "run-1",
                "provider": "eodhd",
                "precedence": 0,
                "requestedRange": {"from": "2024-01-01", "to": "2024-01-31"},
                "objects": [{"key": "raw-1", "etag": "raw-etag", "rowCount": 1}],
            }
        ],
    }


class Storage:
    def __init__(self, job):
        run = job["runs"][0]
        self.raw = json.dumps(
            {
                "version": 1,
                "runId": run["runId"],
                "provider": "eodhd",
                "symbol": job["symbol"],
                "requestedRange": run["requestedRange"],
                "entries": [entry()],
            }
        ).encode()
        self.reads = []
        self.writes = []

    def get_object(self, **kwargs):
        self.reads.append(kwargs)
        if kwargs["Bucket"] == "raw-market-data":
            return {"ETag": '"raw-etag"', "Body": io.BytesIO(self.raw)}
        frame = (
            pl.DataFrame([entry(5), entry(6, "2024-01-02")])
            .with_columns(pl.col("date").str.to_date())
            .cast(SCHEMA)
        )
        output = io.BytesIO()
        frame.write_parquet(output)
        return {"ETag": '"base-etag"', "Body": io.BytesIO(output.getvalue())}

    def put_object(self, **kwargs):
        self.writes.append(kwargs)
        return {"ETag": '"output-etag"'}


@pytest.mark.parametrize("mode", ["merge", "rebuild"])
def test_pipeline_uses_frozen_inputs_and_completes_after_parquet_write(mode):
    job = manifest(mode)
    if mode == "merge":
        job["expectedBaseEtag"] = "base-etag"
    storage = Storage(job)
    requests = []

    def handler(request):
        assert request.headers["Authorization"] == "Bearer test-token"
        requests.append(request)
        if request.url.path.endswith("/claim"):
            assert storage.reads == []
            return httpx.Response(200, json=job)
        assert request.url.path.endswith("/complete")
        assert storage.writes
        assert json.loads(request.content) == {"outputEtag": "output-etag"}
        return httpx.Response(200, json={**job, "status": "completed"})

    with httpx.Client(
        base_url="https://worker.test/",
        transport=httpx.MockTransport(handler),
        headers={"Authorization": "Bearer test-token"},
    ) as client:
        result = Processor(ControlPlane(client), storage).execute("job-1")
    assert result["status"] == "completed"
    output = storage.writes[0]
    assert output["Key"] == job["canonicalKey"]
    frame = pl.read_parquet(io.BytesIO(output["Body"]))
    assert frame["close"].to_list() == ([10, 6] if mode == "merge" else [10])
    assert dict(frame.schema) == SCHEMA
    assert all(read["IfMatch"] for read in storage.reads)
    if mode == "merge":
        assert output["IfMatch"] == '"base-etag"'
    else:
        assert len(storage.reads) == 1


def test_merge_without_base_protects_against_unexpected_existing_object():
    job = manifest("merge")
    storage = Storage(job)
    with httpx.Client(
        base_url="https://worker.test/",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=job)),
    ) as client:
        Processor(ControlPlane(client), storage).execute("job-1")
    assert storage.writes[0]["IfNoneMatch"] == "*"


@pytest.mark.parametrize(
    "failure", ["etag", "envelope", "transform", "put", "complete"]
)
def test_post_claim_errors_report_failure(failure):
    job = manifest()
    storage = Storage(job)
    calls = []
    if failure == "etag":
        job["runs"][0]["objects"][0]["etag"] = "wrong"
    if failure == "envelope":
        storage.raw = storage.raw.replace(b"AAPL.US", b"MSFT.US")
    if failure == "transform":
        job["transformVersion"] = "unknown"
    if failure == "put":

        def broken_put(**kwargs):
            raise OSError("storage unavailable")

        storage.put_object = broken_put

    def handler(request):
        calls.append(request.url.path)
        if request.url.path.endswith("/complete"):
            return httpx.Response(500, json={"error": "unavailable"})
        return httpx.Response(200, json=job)

    with (
        httpx.Client(
            base_url="https://worker.test/", transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises((ValueError, OSError, httpx.HTTPStatusError)),
    ):
        Processor(ControlPlane(client), storage).execute("job-1")
    assert calls[-1] == "/processing-jobs/job-1/fail"
    if failure in {"etag", "envelope", "transform"}:
        assert storage.writes == []


def test_conflicting_claim_does_not_read_write_or_fail():
    calls = []
    job = manifest()
    storage = Storage(job)

    def handler(request):
        calls.append(request.url.path)
        return httpx.Response(409, json={"error": "not claimable"})

    with (
        httpx.Client(
            base_url="https://worker.test/", transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(httpx.HTTPStatusError),
    ):
        Processor(ControlPlane(client), storage).execute("job-1")
    assert calls == ["/processing-jobs/job-1/claim"]
    assert not storage.reads and not storage.writes


def test_later_manifest_run_wins_duplicate_dates():
    job = manifest()
    job["runs"].append({**job["runs"][0], "precedence": 1})
    storage = Storage(job)
    original_get = storage.get_object
    count = 0

    def get(**kwargs):
        nonlocal count
        count += 1
        if count == 2:
            storage.raw = storage.raw.replace(b'"close": 10', b'"close": 11')
        return original_get(**kwargs)

    storage.get_object = get
    with httpx.Client(
        base_url="https://worker.test/",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=job)),
    ) as client:
        Processor(ControlPlane(client), storage).execute("job-1")
    assert pl.read_parquet(io.BytesIO(storage.writes[0]["Body"]))[
        "close"
    ].to_list() == [11]


def test_all_control_plane_endpoint_bodies():
    requests = []

    def handler(request):
        requests.append(
            (
                request.method,
                request.url.path,
                json.loads(request.content) if request.content else None,
            )
        )
        return httpx.Response(200, json={})

    with httpx.Client(
        base_url="https://worker.test/", transport=httpx.MockTransport(handler)
    ) as client:
        api = ControlPlane(client)
        api.create("AAPL.US")
        api.read("job-1")
        api.claim("job-1")
        api.complete("job-1", "etag", "2024-01-31")
        api.fail("job-1", "failure")
    assert requests == [
        (
            "POST",
            "/processing-jobs",
            {"symbol": "AAPL.US", "mode": "merge", "transformVersion": "v1"},
        ),
        ("GET", "/processing-jobs/job-1", None),
        ("POST", "/processing-jobs/job-1/claim", None),
        (
            "POST",
            "/processing-jobs/job-1/complete",
            {"outputEtag": "etag", "completeThrough": "2024-01-31"},
        ),
        ("POST", "/processing-jobs/job-1/fail", {"message": "failure"}),
    ]


def test_http_auth_health_and_execution(monkeypatch):
    monkeypatch.setenv("PROCESSOR_API_TOKEN", "secret")

    @contextmanager
    def runtime():
        class FakeProcessor:
            def execute(self, job_id):
                return {"jobId": job_id, "status": "completed"}

        yield FakeProcessor()

    monkeypatch.setattr(app_module, "processor_runtime", runtime)
    with TestClient(app_module.app) as client:
        assert client.get("/health").status_code == 200
        assert client.post("/processing-jobs/job-1/execute").status_code == 401
        result = client.post(
            "/processing-jobs/job-1/execute", headers={"Authorization": "Bearer secret"}
        )
        assert result.json() == {"jobId": "job-1", "status": "completed"}
        monkeypatch.delenv("PROCESSOR_API_TOKEN")
        assert client.post("/processing-jobs/job-1/execute").status_code == 503
