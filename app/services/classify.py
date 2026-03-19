from __future__ import annotations

from dataclasses import dataclass

from app.services.ollama_client import OllamaResult, generate_json
from app.settings import settings


@dataclass
class ClassificationResult:
    model_name: str
    prompt_version: str
    raw_response: str
    business_type: str
    services: list[str]
    short_summary: str
    has_online_booking: bool
    has_contact_form: bool
    has_chat_widget: bool
    mentions_financing: bool
    likely_decision_maker_names: list[str]
    fit_reason: str
    confidence: float
    error: str


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "yes", "1"}
    return False


def classify_business(crawled_text: str, fallback_has_contact_form: bool) -> ClassificationResult:
    prompt = f"""
You are enriching a lead from public website content.
Use ONLY the provided content. Do not invent facts.
If unsupported by content, return empty string, empty list, false, or low confidence.
Return strict JSON with fields:
business_type (string), services (array of strings), short_summary (string),
has_online_booking (boolean), has_contact_form (boolean), has_chat_widget (boolean),
mentions_financing (boolean), likely_decision_maker_names (array of strings),
fit_reason (string), confidence (number between 0 and 1).

Website content:
\"\"\"{crawled_text[:16000]}\"\"\"
""".strip()

    result: OllamaResult = generate_json(prompt=prompt, retries=2)
    if not result.ok:
        return ClassificationResult(
            model_name="",
            prompt_version="v1",
            raw_response="",
            business_type="",
            services=[],
            short_summary="",
            has_online_booking=False,
            has_contact_form=fallback_has_contact_form,
            has_chat_widget=False,
            mentions_financing=False,
            likely_decision_maker_names=[],
            fit_reason="",
            confidence=0.0,
            error=result.error or "ollama_unavailable",
        )

    data = result.data
    services = data.get("services", [])
    names = data.get("likely_decision_maker_names", [])
    return ClassificationResult(
        model_name=settings.ollama_model,
        prompt_version="v1",
        raw_response=result.raw_text,
        business_type=str(data.get("business_type", "") or ""),
        services=services if isinstance(services, list) else [],
        short_summary=str(data.get("short_summary", "") or ""),
        has_online_booking=_as_bool(data.get("has_online_booking")),
        has_contact_form=_as_bool(data.get("has_contact_form")) or fallback_has_contact_form,
        has_chat_widget=_as_bool(data.get("has_chat_widget")),
        mentions_financing=_as_bool(data.get("mentions_financing")),
        likely_decision_maker_names=names if isinstance(names, list) else [],
        fit_reason=str(data.get("fit_reason", "") or ""),
        confidence=float(data.get("confidence", 0.0) or 0.0),
        error="",
    )
