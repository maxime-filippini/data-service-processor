import os
import secrets
from typing import Annotated

import httpx
from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .runtime import processor_runtime

app = FastAPI()
bearer = HTTPBearer(auto_error=False)


def authorize(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
):
    token = os.environ.get("PROCESSOR_API_TOKEN")
    if not token:
        raise HTTPException(503, "Processor authentication is not configured")
    if credentials is None or not secrets.compare_digest(
        credentials.credentials, token
    ):
        raise HTTPException(401, "Unauthorized", headers={"WWW-Authenticate": "Bearer"})


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/processing-jobs/{job_id}/execute", dependencies=[Depends(authorize)])
def execute(job_id: str):
    try:
        with processor_runtime() as processor:
            return processor.execute(job_id)
    except httpx.HTTPStatusError as error:
        status = error.response.status_code
        raise HTTPException(
            status if status in {404, 409} else 502, "Control-plane request failed"
        ) from error
    except Exception as error:
        raise HTTPException(500, "Processing failed; inspect the job state") from error
