"""xAI provider adapter."""

from __future__ import annotations

import os
from typing import Any

import requests

from packages.llm.providers.base import BaseLLMProvider

XAI_API_URL = "https://api.x.ai/v1/chat/completions"
XAI_RESPONSES_URL = "https://api.x.ai/v1/responses"


class XAIProvider(BaseLLMProvider):
    provider_name = "xai"

    def __init__(
        self,
        *,
        api_key: str | None = None,
        model: str = "grok-4-1-fast-non-reasoning",
        timeout: int = 60,
    ) -> None:
        self.api_key = api_key or os.environ.get("XAI_API_KEY", "")
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
            resp = requests.post(XAI_API_URL, json=payload, headers=headers, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException:
            return None
        choices = data.get("choices", [])
        if not choices:
            return None
        content = choices[0].get("message", {}).get("content")
        return content if isinstance(content, str) else None

    def responses_text(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0,
        tools: list[dict[str, Any]] | None = None,
        max_output_tokens: int = 500,
    ) -> str | None:
        if not self.api_key:
            return None
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload: dict[str, Any] = {
            "model": self.model,
            "input": messages,
            "temperature": temperature,
            "max_output_tokens": max_output_tokens,
        }
        if tools:
            payload["tools"] = tools
        try:
            resp = requests.post(
                XAI_RESPONSES_URL,
                json=payload,
                headers=headers,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException:
            return None
        for output in data.get("output", []):
            if output.get("type") != "message":
                continue
            for content in output.get("content", []):
                if content.get("type") == "output_text" and isinstance(content.get("text"), str):
                    return str(content["text"])
        return None
