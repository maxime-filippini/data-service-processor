from contextlib import contextmanager

import boto3
import httpx
from botocore.config import Config

from .client import ControlPlane
from .processor import Processor
from .settings import ProcessorSettings


@contextmanager
def processor_runtime(settings: ProcessorSettings | None = None):
    settings = settings if settings is not None else ProcessorSettings()
    s3 = boto3.client(
        "s3",
        endpoint_url=str(settings.r2_endpoint_url),
        region_name="auto",
        aws_access_key_id=settings.r2_access_key_id.get_secret_value(),
        aws_secret_access_key=settings.r2_secret_access_key.get_secret_value(),
        config=Config(
            connect_timeout=10, read_timeout=120, retries={"max_attempts": 2}
        ),
    )
    try:
        with httpx.Client(
            base_url=str(settings.processing_api_url).rstrip("/") + "/",
            headers={
                "Authorization": "Bearer "
                + settings.processing_api_token.get_secret_value()
            },
            timeout=30,
        ) as client:
            yield Processor(ControlPlane(client), s3)
    finally:
        s3.close()
