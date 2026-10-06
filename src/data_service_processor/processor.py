import io
import logging

import polars as pl

from .models import Envelope, Job

SCHEMA = {
    "date": pl.Date,
    "open": pl.Float64,
    "close": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "adjusted_close": pl.Float64,
    "volume": pl.Int64,
}
logger = logging.getLogger(__name__)


class Processor:
    def __init__(
        self,
        control_plane,
        s3,
        raw_bucket="raw-market-data",
        processed_bucket="processed-market-data",
    ):
        self.control_plane = control_plane
        self.s3 = s3
        self.raw_bucket = raw_bucket
        self.processed_bucket = processed_bucket

    def execute(self, job_id):
        manifest = self.control_plane.claim(job_id)
        # A rejected claim must never trigger fail and release another owner's lock.
        try:
            job = Job.model_validate(manifest)
            if job.job_id != job_id or job.status != "processing":
                raise ValueError("Claim returned an unexpected job or status")
            if (
                job.canonical_key
                != f"dataset=prices_eod/symbol={job.symbol}/data.parquet"
            ):
                raise ValueError("Unexpected canonical target key")
            frame = self._transform(job)
            output = io.BytesIO()
            frame.write_parquet(output)
            options = {}
            if job.mode == "merge":
                options = (
                    {"IfMatch": self._quoted(job.expected_base_etag)}
                    if job.expected_base_etag
                    else {"IfNoneMatch": "*"}
                )
            result = self.s3.put_object(
                Bucket=self.processed_bucket,
                Key=job.canonical_key,
                Body=output.getvalue(),
                ContentType="application/vnd.apache.parquet",
                **options,
            )
            # completeThrough is coverage metadata, not merely the latest row date.
            return self.control_plane.complete(job_id, result["ETag"].strip('"'))
        except Exception as error:
            # Avoid sending credentials, response bodies, or raw rows in errors.
            message = f"Processing failed ({type(error).__name__})"
            try:
                self.control_plane.fail(job_id, message)
            except Exception:
                logger.exception("Could not report failure for job %s", job_id)
            raise

    @staticmethod
    def _quoted(etag):
        return '"' + etag.strip('"') + '"'

    def _read(self, bucket, key, etag):
        result = self.s3.get_object(Bucket=bucket, Key=key, IfMatch=self._quoted(etag))
        body = result["Body"]
        try:
            if result["ETag"].strip('"') != etag.strip('"'):
                raise ValueError("Object ETag differs from frozen manifest")
            return body.read()
        finally:
            body.close()

    def _transform(self, job):
        frames = []
        if job.mode == "merge" and job.expected_base_etag:
            base = pl.read_parquet(
                io.BytesIO(
                    self._read(
                        self.processed_bucket,
                        job.canonical_key,
                        job.expected_base_etag,
                    )
                )
            )
            if dict(base.schema) != SCHEMA:
                raise ValueError("Canonical Parquet schema does not match v1")
            # Validate existing rows as strictly as newly ingested entries.
            from .models import Entry

            for row in base.iter_rows(named=True):
                Entry.model_validate(row)
            frames.append(base)
        precedences = [run.precedence for run in job.runs]
        if precedences != sorted(set(precedences)):
            raise ValueError("Manifest precedence must be unique and ascending")
        for run in job.runs:
            for obj in run.objects:
                raw = Envelope.model_validate_json(
                    self._read(self.raw_bucket, obj.key, obj.etag)
                )
                if (
                    raw.run_id != run.run_id
                    or raw.symbol != job.symbol
                    or raw.provider != run.provider
                    or raw.requested_range != run.requested_range
                    or len(raw.entries) != obj.row_count
                ):
                    raise ValueError("Raw envelope differs from frozen manifest")
                if any(
                    not run.requested_range.start <= row.date <= run.requested_range.end
                    for row in raw.entries
                ):
                    raise ValueError("Raw entry lies outside requested range")
                frames.append(
                    pl.DataFrame(
                        [row.model_dump() for row in raw.entries], schema=SCHEMA
                    )
                )
        return (
            pl.concat(frames)
            .unique(subset=["date"], keep="last", maintain_order=True)
            .sort("date")
        )
