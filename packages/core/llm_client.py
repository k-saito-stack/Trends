"""LLM client with backwards-compatible xAI defaults and optional provider abstraction."""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

import requests

from packages.llm.providers.base import BaseLLMProvider, build_provider

logger = logging.getLogger(__name__)

XAI_API_URL = "https://api.x.ai/v1/chat/completions"
XAI_RESPONSES_URL = "https://api.x.ai/v1/responses"


class LLMClient:
    """Thin client for summary/evidence LLM calls.

    Default behavior remains xAI-compatible so existing tests and x_search keep working.
    Alternate providers can be selected via `provider_name` or `LLM_PROVIDER`.
    """

    def __init__(
        self,
        api_key: str | None = None,
        model: str = "grok-4-1-fast-non-reasoning",
        timeout: int = 60,
        provider_name: str | None = None,
    ) -> None:
        self.provider_name = (provider_name or os.environ.get("LLM_PROVIDER") or "xai").strip()
        self.model = model
        self.timeout = timeout
        self.api_key = api_key or self._resolve_api_key(self.provider_name)
        self._provider: BaseLLMProvider | None = None
        if self.provider_name.lower() != "xai":
            self._provider = build_provider(
                self.provider_name,
                api_key=self.api_key,
                model=model,
                timeout=timeout,
            )

    @staticmethod
    def _resolve_api_key(provider_name: str) -> str:
        normalized = provider_name.lower()
        if normalized == "kimi":
            return os.environ.get("KIMI_API_KEY", "")
        if normalized == "minimax":
            return os.environ.get("MINIMAX_API_KEY", "")
        return os.environ.get("XAI_API_KEY", "")

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
            logger.warning("%s API key not set", self.provider_name.upper())
            return None

        if self._provider is not None:
            return self._provider.chat(
                messages,
                temperature=temperature,
                tools=tools,
                max_tokens=max_tokens,
            )

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
        except requests.RequestException as exc:
            logger.warning("xAI API call failed: %s", exc)
            return None

        choices = data.get("choices", [])
        if not choices:
            return None
        content = choices[0].get("message", {}).get("content")
        return content if isinstance(content, str) else None

    def chat_json(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0,
        tools: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any] | list[Any] | None:
        if self._provider is not None:
            return self._provider.chat_json(
                messages,
                temperature=temperature,
                tools=tools,
            )

        content = self.chat(messages, temperature=temperature, tools=tools)
        if content is None:
            return None
        try:
            return json.loads(content)  # type: ignore[no-any-return]
        except json.JSONDecodeError:
            pass
        json_match = re.search(r"```(?:json)?\s*\n?([\s\S]*?)\n?```", content)
        if json_match:
            try:
                return json.loads(json_match.group(1))  # type: ignore[no-any-return]
            except json.JSONDecodeError:
                pass
        logger.warning("Failed to parse JSON from LLM response")
        return None

    def responses_text(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0,
        tools: list[dict[str, Any]] | None = None,
        max_output_tokens: int = 500,
    ) -> str | None:
        if not self.api_key:
            logger.warning("%s API key not set", self.provider_name.upper())
            return None

        if self._provider is not None:
            return self._provider.responses_text(
                messages,
                temperature=temperature,
                tools=tools,
                max_output_tokens=max_output_tokens,
            )

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
        except requests.RequestException as exc:
            response = getattr(exc, "response", None)
            status_code = getattr(response, "status_code", "unknown")
            request_id = ""
            response_headers = getattr(response, "headers", None)
            if response_headers is not None:
                request_id = response_headers.get("x-request-id", "") or response_headers.get(
                    "request-id", ""
                )
            logger.warning(
                "xAI Responses API call failed: status=%s request_id=%s error=%s",
                status_code,
                request_id,
                exc,
            )
            return None

        for output in data.get("output", []):
            if output.get("type") != "message":
                continue
            for content in output.get("content", []):
                if content.get("type") == "output_text":
                    text = content.get("text")
                    if isinstance(text, str):
                        return text
        return None
