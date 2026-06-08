from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlencode
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from .signing import HuaweiSigner


class HuaweiApiError(RuntimeError):
    pass


class HuaweiClient:
    def __init__(self, ak: str, sk: str, endpoint: str, workspace_id: str | None = None) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.workspace_id = workspace_id
        self.signer = HuaweiSigner(ak, sk)

    def get(self, path: str, query: dict[str, Any] | None = None) -> Any:
        return self.request("GET", path, query=query)

    def request(
        self,
        method: str,
        path: str,
        query: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> Any:
        url = f"{self.endpoint}{path}"
        if query:
            clean_query = {key: value for key, value in query.items() if value is not None}
            if clean_query:
                url = f"{url}?{urlencode(clean_query, doseq=True)}"

        headers = {"Content-Type": "application/json"}
        if self.workspace_id:
            headers["workspace"] = self.workspace_id

        raw_body = b"" if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
        signed = self.signer.sign(method, url, headers=headers, body=raw_body)
        request = Request(signed.url, data=signed.body or None, method=signed.method)
        for key, value in signed.headers.items():
            request.add_header(key, value)

        try:
            with urlopen(request, timeout=60) as response:
                payload = response.read()
                if not payload:
                    return None
                return json.loads(payload.decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise HuaweiApiError(f"{method} {url} failed: HTTP {exc.code}: {detail}") from exc
        except Exception as exc:
            raise HuaweiApiError(f"{method} {url} failed: {exc}") from exc
