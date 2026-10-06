import os
from contextlib import contextmanager

import boto3
import httpx
from botocore.config import Config

from .client import ControlPlane
from .processor import Processor


@contextmanager
def processor_runtime():
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT_URL"],
        region_name="auto",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        config=Config(
            connect_timeout=10, read_timeout=120, retries={"max_attempts": 2}
        ),
    )
    try:
        with httpx.Client(
            base_url=os.environ["PROCESSING_API_URL"].rstrip("/") + "/",
            headers={"Authorization": "Bearer " + os.environ["PROCESSING_API_TOKEN"]},
            timeout=30,
        ) as client:
            yield Processor(ControlPlane(client), s3)
    finally:
        s3.close()
