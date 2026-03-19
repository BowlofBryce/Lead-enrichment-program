from __future__ import annotations

import json
from dataclasses import dataclass

import requests

from app.settings import settings


@dataclass
class OllamaResult:
    ok: bool
    raw_text: str
    data: dict
    error: str


def _extract_json_object(text: str) -> dict:
    text = text.strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        snippet = text[start : end + 1]
        try:
            return json.loads(snippet)
        except json.JSONDecodeError:
            return {}
    return {}


def generate_json(prompt: str, retries: int = 2) -> OllamaResult:
    last_error = ""
    for _ in range(retries + 1):
        try:
            resp = requests.post(
                f"{settings.ollama_base_url}/api/generate",
                json={
                    "model": settings.ollama_model,
                    "prompt": prompt,
                    "stream": False,
                    "format": "json",
                },
                timeout=settings.ollama_timeout_seconds,
            )
            resp.raise_for_status()
            payload = resp.json()
            raw = payload.get("response", "")
            parsed = _extract_json_object(raw)
            if parsed:
                return OllamaResult(ok=True, raw_text=raw, data=parsed, error="")
            last_error = "invalid_json"
        except Exception as exc:
            last_error = str(exc)
    return OllamaResult(ok=False, raw_text="", data={}, error=last_error)
