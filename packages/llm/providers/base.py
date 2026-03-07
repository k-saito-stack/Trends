"""Provider abstraction for external LLM services."""

from __future__ import annotations

import json
import re
from abc import ABC, abstractmethod
from typing import Any


class BaseLLMProvider(ABC):
    provider_name = "base"

    @property
    @abstractmethod
    def available(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def chat(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0,
        tools: list[dict[str, Any]] | None = None,
        max_tokens: int = 500,
    ) -> str | None:
        raise NotImplementedError

    def chat_json(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0,
        tools: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any] | list[Any] | None:
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
                return None
        return None

    def responses_text(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0,
        tools: list[dict[str, Any]] | None = None,
        max_output_tokens: int = 500,
    ) -> str | None:
        return self.chat(
            messages,
            temperature=temperature,
            tools=tools,
            max_tokens=max_output_tokens,
        )


def build_provider(
    provider_name: str,
    *,
    api_key: str | None,
    model: str,
    timeout: int,
) -> BaseLLMProvider:
    normalized = (provider_name or "xai").strip().lower()
    if normalized == "minimax":
        from packages.llm.providers.minimax_provider import MiniMaxProvider

        return MiniMaxProvider(api_key=api_key, model=model, timeout=timeout)
    if normalized == "kimi":
        from packages.llm.providers.kimi_provider import KimiProvider

        return KimiProvider(api_key=api_key, model=model, timeout=timeout)
    from packages.llm.providers.xai_provider import XAIProvider

    return XAIProvider(api_key=api_key, model=model, timeout=timeout)
