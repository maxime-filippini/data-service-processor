from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, StrictInt


class Contract(BaseModel):
    model_config = ConfigDict(populate_by_name=True)


class DateRange(Contract):
    start: date = Field(alias="from")
    end: date = Field(alias="to")


class RawObject(Contract):
    key: str = Field(min_length=1)
    etag: str = Field(min_length=1)
    row_count: int = Field(alias="rowCount", ge=0)


class Run(Contract):
    run_id: str = Field(alias="runId")
    provider: Literal["eodhd"]
    requested_range: DateRange = Field(alias="requestedRange")
    precedence: int
    objects: list[RawObject] = Field(min_length=1)


class Job(Contract):
    job_id: str = Field(alias="jobId")
    symbol: str
    mode: Literal["merge", "rebuild"]
    canonical_key: str = Field(alias="canonicalKey")
    expected_base_etag: str | None = Field(default=None, alias="expectedBaseEtag")
    transform_version: Literal["v1"] = Field(alias="transformVersion")
    status: Literal["queued", "processing", "completed", "failed"]
    runs: list[Run] = Field(min_length=1)


class Entry(Contract):
    date: date
    open: FiniteFloat
    close: FiniteFloat
    high: FiniteFloat
    low: FiniteFloat
    adjusted_close: FiniteFloat
    volume: StrictInt = Field(ge=0)


class Envelope(Contract):
    version: Literal[1]
    run_id: str = Field(alias="runId")
    provider: Literal["eodhd"]
    symbol: str
    requested_range: DateRange = Field(alias="requestedRange")
    entries: list[Entry]
