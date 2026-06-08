from __future__ import annotations

import json
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any
from urllib.request import Request, urlopen


@dataclass(frozen=True, slots=True)
class AIRequest:
    system_prompt: str
    user_prompt: str
    model: str
    max_tokens: int = 2000
    timeout_seconds: int = 60


@dataclass(frozen=True, slots=True)
class AIResponse:
    text: str
    provider: str
    model: str
    raw: dict[str, Any] | None = None


class AIProvider(ABC):
    @abstractmethod
    def complete(self, request: AIRequest) -> AIResponse:
        raise NotImplementedError


class MockAIProvider(AIProvider):
    def __init__(self, response_text: str | list[str]) -> None:
        self.response_texts = [response_text] if isinstance(response_text, str) else list(response_text)
        self.calls = 0

    def complete(self, request: AIRequest) -> AIResponse:
        index = min(self.calls, len(self.response_texts) - 1)
        self.calls += 1
        return AIResponse(self.response_texts[index], provider="mock", model=request.model, raw={"mock": True})


class OpenAICompatibleProvider(AIProvider):
    def __init__(self, api_key: str | None = None, base_url: str | None = None, retries: int = 2) -> None:
        self.api_key = api_key or os.getenv("DATATRACEX_AI_API_KEY", "")
        self.base_url = (base_url or os.getenv("DATATRACEX_AI_BASE_URL", "https://api.openai.com/v1")).rstrip("/")
        self.retries = retries
        if not self.api_key:
            raise RuntimeError("missing DATATRACEX_AI_API_KEY")

    def complete(self, request: AIRequest) -> AIResponse:
        payload = {
            "model": request.model,
            "messages": [
                {"role": "system", "content": request.system_prompt},
                {"role": "user", "content": request.user_prompt},
            ],
            "max_tokens": request.max_tokens,
            "temperature": 0,
        }
        body = json.dumps(payload).encode("utf-8")
        for attempt in range(self.retries + 1):
            req = Request(
                f"{self.base_url}/chat/completions",
                data=body,
                method="POST",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
            )
            try:
                with urlopen(req, timeout=request.timeout_seconds) as response:
                    raw = json.loads(response.read().decode("utf-8"))
                text = raw["choices"][0]["message"]["content"]
                return AIResponse(text=text, provider="openai_compatible", model=request.model, raw=raw)
            except Exception:
                if attempt >= self.retries:
                    raise
                time.sleep(1.5 * (attempt + 1))
        raise RuntimeError("unreachable")
