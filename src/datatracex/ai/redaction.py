from __future__ import annotations

import re
from dataclasses import dataclass


SECRET_PATTERNS = [
    re.compile(r"(?i)\b(access[_-]?key|secret[_-]?key|password|passwd|pwd|token|ak|sk)\b\s*[:=]\s*['\"]?[^'\"\s,;]+"),
    re.compile(r"(?i)(jdbc:[^\s'\"]+://[^'\"]*?):([^:@/'\"]+)@"),
    re.compile(r"(?i)(postgresql://[^:/\s]+:)([^@\s]+)(@)"),
    re.compile(r"(?i)(mysql://[^:/\s]+:)([^@\s]+)(@)"),
]


@dataclass(frozen=True, slots=True)
class RedactionResult:
    text: str
    replacements: int


def redact_sensitive_text(text: str) -> RedactionResult:
    redacted = text
    replacements = 0

    def replace_key_value(match: re.Match[str]) -> str:
        nonlocal replacements
        replacements += 1
        key = match.group(1)
        return f"{key}=***"

    redacted = SECRET_PATTERNS[0].sub(replace_key_value, redacted)

    def replace_url_password(match: re.Match[str]) -> str:
        nonlocal replacements
        replacements += 1
        return f"{match.group(1)}***{match.group(3) if match.lastindex and match.lastindex >= 3 else '@'}"

    for pattern in SECRET_PATTERNS[1:]:
        redacted = pattern.sub(replace_url_password, redacted)

    return RedactionResult(redacted, replacements)
