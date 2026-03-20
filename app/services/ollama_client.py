from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

import requests

from app.services.logging_utils import get_logger
from app.settings import settings

logger = get_logger(__name__)


@dataclass
class OllamaResult:
    ok: bool
    raw_text: str
    data: dict
    error: str
    parse_error: str
    repaired_response: str
    duration_ms: int
    request_payload: dict[str, Any]
    raw_payload: dict[str, Any]


@dataclass
class OllamaModelInfo:
    name: str
    size: int | None = None
    modified_at: str = ""


def _extract_json_object(text: str) -> tuple[dict, str, str]:
    text = text.strip()
    if not text:
        return {}, "empty_response", ""
    try:
        return json.loads(text), "", ""
    except json.JSONDecodeError as exc:
        parse_error = str(exc)

    start = text.find("{")
    end = text.rfind("}")
    repaired = ""
    if start != -1 and end != -1 and end > start:
        snippet = text[start : end + 1]
        repaired = snippet
        try:
            return json.loads(snippet), "", repaired
        except json.JSONDecodeError as exc:
            parse_error = str(exc)
    return {}, parse_error, repaired


def list_models() -> list[OllamaModelInfo]:
    resp = requests.get(f"{settings.ollama_base_url}/api/tags", timeout=8)
    resp.raise_for_status()
    payload = resp.json()
    models: list[OllamaModelInfo] = []
    for item in payload.get("models", []):
        name = str(item.get("name", "") or "").strip()
        if not name:
            continue
        size = item.get("size")
        size_value = int(size) if isinstance(size, (int, float)) else None
        models.append(
            OllamaModelInfo(
                name=name,
                size=size_value,
                modified_at=str(item.get("modified_at", "") or ""),
            )
        )
    return sorted(models, key=lambda m: m.name.lower())


def pull_model(model_name: str) -> dict[str, Any]:
    payload = {"name": model_name, "stream": False}
    resp = requests.post(
        f"{settings.ollama_base_url}/api/pull",
        json=payload,
        timeout=settings.ollama_timeout_seconds,
    )
    resp.raise_for_status()
    return resp.json() if resp.content else {"status": "ok"}


def create_model_preset(base_model: str, preset_name: str, system_prompt: str) -> dict[str, Any]:
    clean_prompt = system_prompt.strip().replace("\r\n", "\n")
    modelfile = f'FROM {base_model}\n\nSYSTEM """\n{clean_prompt}\n"""\n'
    payload = {"name": preset_name, "modelfile": modelfile, "stream": False}
    resp = requests.post(
        f"{settings.ollama_base_url}/api/create",
        json=payload,
        timeout=settings.ollama_timeout_seconds,
    )
    if not resp.ok:
        detail = ""
        try:
            detail = str(resp.json().get("error", "") or "")
        except Exception:
            detail = resp.text[:300]
        raise requests.HTTPError(
            f"{resp.status_code} {resp.reason}: {detail or 'unknown error'}",
            response=resp,
        )
    return resp.json() if resp.content else {"status": "ok"}


def check_ollama_health() -> dict[str, Any]:
    out = {"reachable": False, "model_available": False, "error": "", "models": []}
    try:
        models = [m.name for m in list_models()]
        out["reachable"] = True
        out["models"] = models
        out["model_available"] = any(m == settings.ollama_model or m.startswith(f"{settings.ollama_model}:") for m in models)
        return out
    except Exception as exc:
        out["error"] = str(exc)
        return out

def generate(
    prompt: str,
    retries: int = 2,
    system: str = "",
    temperature: float | None = None,
    max_tokens: int | None = None,
    expect_json: bool = False,
    model: str | None = None,
) -> OllamaResult:
    last_error = ""
    last_parse_error = ""
    last_raw = ""
    repaired = ""
    last_payload: dict[str, Any] = {}

    model_name = (model or settings.ollama_model).strip()
    request_payload: dict[str, Any] = {
        "model": model_name,
        "prompt": prompt,
        "stream": False,
    }
    if expect_json:
        request_payload["format"] = "json"
    if system:
        request_payload["system"] = system
    options: dict[str, Any] = {}
    if temperature is not None:
        options["temperature"] = temperature
    if max_tokens is not None:
        options["num_predict"] = max_tokens
    if options:
        request_payload["options"] = options

    for _ in range(retries + 1):
        start = time.perf_counter()
        try:
            logger.info(
                "ollama.request.started",
                extra_fields={"model": model_name, "prompt_len": len(prompt), "expect_json": expect_json},
            )
            resp = requests.post(
                f"{settings.ollama_base_url}/api/generate",
                json=request_payload,
                timeout=settings.ollama_timeout_seconds,
            )
            resp.raise_for_status()
            duration_ms = int((time.perf_counter() - start) * 1000)
            payload = resp.json()
            last_payload = payload

            error_message = str(payload.get("error", "") or "")
            raw = str(payload.get("response", ""))
            if not raw and error_message:
                raw = json.dumps(payload)
            last_raw = raw

            if error_message:
                last_error = error_message
                logger.warning("ollama.request.error_payload", extra_fields={"error": error_message[:180]})
                continue

            if not expect_json:
                logger.info("ollama.request.completed", extra_fields={"duration_ms": duration_ms, "ok": True})
                return OllamaResult(True, raw, {}, "", "", "", duration_ms, request_payload, payload)

            parsed, parse_error, repaired = _extract_json_object(raw)
            if parsed:
                logger.info("ollama.request.completed", extra_fields={"duration_ms": duration_ms, "ok": True})
                return OllamaResult(True, raw, parsed, "", "", repaired, duration_ms, request_payload, payload)
            last_error = "invalid_json"
            last_parse_error = parse_error
            logger.warning("ollama.invalid_json", extra_fields={"parse_error": parse_error[:180], "duration_ms": duration_ms})
        except Exception as exc:
            last_error = str(exc)
            logger.exception("ollama.request.failed")
    return OllamaResult(False, last_raw, {}, last_error, last_parse_error, repaired, 0, request_payload, last_payload)


def generate_json(
    prompt: str,
    retries: int = 2,
    system: str = "",
    temperature: float | None = None,
    max_tokens: int | None = None,
    model: str | None = None,
) -> OllamaResult:
    return generate(
        prompt=prompt,
        retries=retries,
        system=system,
        temperature=temperature,
        max_tokens=max_tokens,
        expect_json=True,
        model=model,
    )
