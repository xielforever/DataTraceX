from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import parse_qsl, quote, urlparse


ALGORITHM = "SDK-HMAC-SHA256"


@dataclass(frozen=True, slots=True)
class SignedRequest:
    method: str
    url: str
    headers: dict[str, str]
    body: bytes


class HuaweiSigner:
    def __init__(self, ak: str, sk: str) -> None:
        self.ak = ak
        self.sk = sk.encode("utf-8")

    def sign(
        self,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        body: bytes | str | None = None,
        now: datetime | None = None,
    ) -> SignedRequest:
        payload = _body_bytes(body)
        parsed = urlparse(url)
        signed_headers = {k.lower(): v.strip() for k, v in (headers or {}).items()}
        signed_headers["host"] = parsed.netloc
        signed_headers["x-sdk-date"] = _format_sdk_date(now or datetime.now(timezone.utc))

        canonical_request = "\n".join(
            [
                method.upper(),
                _canonical_uri(parsed.path),
                _canonical_query(parsed.query),
                _canonical_headers(signed_headers),
                _signed_header_names(signed_headers),
                _sha256_hex(payload),
            ]
        )
        string_to_sign = "\n".join(
            [
                ALGORITHM,
                signed_headers["x-sdk-date"],
                _sha256_hex(canonical_request.encode("utf-8")),
            ]
        )
        signature = hmac.new(self.sk, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        signed_headers["authorization"] = (
            f"{ALGORITHM} Access={self.ak}, "
            f"SignedHeaders={_signed_header_names(signed_headers)}, "
            f"Signature={signature}"
        )
        return SignedRequest(method.upper(), url, signed_headers, payload)


def _body_bytes(body: bytes | str | None) -> bytes:
    if body is None:
        return b""
    if isinstance(body, bytes):
        return body
    return body.encode("utf-8")


def _format_sdk_date(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _canonical_uri(path: str) -> str:
    if not path:
        return "/"
    encoded = quote(path, safe="/~%")
    return encoded if encoded.endswith("/") else f"{encoded}/"


def _canonical_query(query: str) -> str:
    pairs = parse_qsl(query, keep_blank_values=True)
    encoded = [
        (quote(key, safe="-_.~"), quote(value, safe="-_.~"))
        for key, value in pairs
    ]
    return "&".join(f"{key}={value}" for key, value in sorted(encoded))


def _canonical_headers(headers: dict[str, str]) -> str:
    return "".join(f"{key}:{headers[key]}\n" for key in sorted(headers))


def _signed_header_names(headers: dict[str, str]) -> str:
    return ";".join(sorted(headers))


def _sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()
