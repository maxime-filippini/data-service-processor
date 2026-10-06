from urllib.parse import quote

import httpx


class ControlPlane:
    """No automatic retries: claim and terminal transitions are not idempotent."""

    def __init__(self, client: httpx.Client):
        self.client = client

    def _request(self, method, path, body=None):
        response = self.client.request(method, path, json=body)
        response.raise_for_status()
        return response.json()

    def create(self, symbol, mode="merge", transform_version="v1"):
        return self._request(
            "POST",
            "processing-jobs",
            {
                "symbol": symbol,
                "mode": mode,
                "transformVersion": transform_version,
            },
        )

    def read(self, job_id):
        return self._request("GET", self._path(job_id))

    def claim(self, job_id):
        return self._request("POST", self._path(job_id) + "/claim")

    def complete(self, job_id, output_etag, complete_through=None):
        body = {"outputEtag": output_etag}
        if complete_through is not None:
            body["completeThrough"] = complete_through
        return self._request("POST", self._path(job_id) + "/complete", body)

    def fail(self, job_id, message):
        return self._request("POST", self._path(job_id) + "/fail", {"message": message})

    @staticmethod
    def _path(job_id):
        return "processing-jobs/" + quote(job_id, safe="")
