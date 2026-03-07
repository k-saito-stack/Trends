"""MiniMax provider adapter using an OpenAI-compatible chat endpoint."""

from __future__ import annotations

import os
from typing import Any

import requests

from packages.llm.providers.base import BaseLLMProvider

MINIMAX_API_URL = os.environ.get("MINIMAX_API_URL", "https://api.minimax.chat/v1/chat/completions")


class MiniMaxProvider(BaseLLMProvider):
    provider_name = "minimax"

    def __init__(
        self,
        *,
        api_key: str | None = None,
        model: str = "MiniMax-M1",
        timeout: int = 60,
    ) -> None:
        self.api_key = api_key or os.environ.get("MINIMAX_API_KEY", "")
        self.model = model
        self.timeout = timeout

    @property
    def available(self) -> bool:
        return bool(self.api_key)

    def chat(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0,
        tools: list[dict[str, Any]] | None = None,
        max_tokens: int = 500,
    ) -> str | None:
        if not self.api_key:
            return None
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if tools:
            payload["tools"] = tools
        try:
            resp = requests.post(
                MINIMAX_API_URL,
                json=payload,
                headers=headers,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException:
            return None
        choices = data.get("choices", [])
        if not choices:
            return None
        content = choices[0].get("message", {}).get("content")
        return content if isinstance(content, str) else None
