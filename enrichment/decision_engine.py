from __future__ import annotations

import json
import re
from dataclasses import dataclass

from enrichment.contact_extractor import ContactExtractionResult, ContactItem
from app.services.ollama_client import generate_json

PHONE_WEIGHTS = {
    "team": 0.9,
    "about": 0.7,
    "contact": 0.5,
    "homepage": 0.4,
    "footer": 0.2,
}

PRIORITY_ROLES = ["owner", "founder", "ceo", "partner", "director", "manager"]

ROLE_HINTS = {
    "owner": ["owner"],
    "founder": ["founder", "co-founder"],
    "ceo": ["ceo", "chief executive"],
    "partner": ["partner"],
    "director": ["director"],
    "manager": ["manager", "general manager"],
}


@dataclass
class DecisionEngineOutput:
    decision_maker_name: str
    decision_maker_role: str
    decision_maker_phone: str
    decision_maker_email: str
    confidence: float
    source: str
    llm_input: dict[str, object]
    llm_output: dict[str, object]
    phone_weight: float
    llm_error: str = ""
    llm_timed_out: bool = False


def _classify_phone_weight(phone: ContactItem) -> float:
    snippet = (phone.raw_text or "").lower()
    if "footer" in snippet:
        return PHONE_WEIGHTS["footer"]
    return PHONE_WEIGHTS.get(phone.source_page, PHONE_WEIGHTS["homepage"])


def _email_matches_name(email: str, name: str) -> bool:
    if not email or not name:
        return False
    local = email.split("@", 1)[0].lower()
    parts = [p.lower() for p in re.findall(r"[A-Za-z]+", name)]
    if not parts:
        return False
    patterns = {
        "".join(parts),
        ".".join(parts),
        f"{parts[0][0]}{parts[-1]}",
        f"{parts[0]}.{parts[-1]}",
        f"{parts[0]}{parts[-1][0]}",
    }
    return any(p in local for p in patterns if p)


def _guess_role(raw_text: str) -> str:
    lower = raw_text.lower()
    for role, hints in ROLE_HINTS.items():
        if any(h in lower for h in hints):
            return role
    return "manager"


def _best_phone(phones: list[ContactItem]) -> tuple[str, float]:
    best = ("", 0.0)
    for phone in phones:
        weight = _classify_phone_weight(phone)
        if weight > best[1]:
            best = (phone.value, weight)
    return best


def _heuristic_decision(extracted: ContactExtractionResult) -> DecisionEngineOutput:
    names = extracted.names
    emails = extracted.emails
    phones = extracted.phones

    selected_name = names[0].value if names else ""
    selected_role = _guess_role(names[0].raw_text if names else "") if selected_name else ""

    selected_email = ""
    if selected_name:
        matching = [e.value for e in emails if _email_matches_name(e.value, selected_name)]
        selected_email = matching[0] if matching else (emails[0].value if emails else "")
    elif emails:
        selected_email = emails[0].value

    selected_phone, phone_weight = _best_phone(phones)

    return DecisionEngineOutput(
        decision_maker_name=selected_name,
        decision_maker_role=selected_role,
        decision_maker_phone=selected_phone,
        decision_maker_email=selected_email,
        confidence=0.0,
        source="heuristic_fallback",
        llm_input={},
        llm_output={},
        phone_weight=phone_weight,
        llm_error="",
        llm_timed_out=False,
    )


def _score(output: DecisionEngineOutput, *, has_only_generic: bool) -> float:
    score = 0.0
    role = (output.decision_maker_role or "").lower().strip()
    if role in PRIORITY_ROLES:
        score += 0.4
    if output.phone_weight >= 0.7:
        score += 0.3
    if _email_matches_name(output.decision_maker_email, output.decision_maker_name):
        score += 0.2
    if has_only_generic:
        score -= 0.3
    return max(0.0, min(1.0, round(score, 3)))


def _llm_prompt(extracted: ContactExtractionResult) -> tuple[str, dict[str, object]]:
    names = [n.to_dict() for n in extracted.names]
    phones = [{**p.to_dict(), "weight": _classify_phone_weight(p)} for p in extracted.phones]
    emails = [e.to_dict() for e in extracted.emails]
    snippets = [i.to_dict() for i in extracted.items[:40]]
    payload = {"names": names, "phones": phones, "emails": emails, "snippets": snippets}
    prompt = (
        "You are selecting one business decision maker and their best direct contact. "
        "Prefer owner/founder/ceo/partner/director/manager roles. "
        "Avoid generic company numbers unless there is no better option. "
        "Return strict JSON only with keys: "
        "decision_maker_name, decision_maker_role, decision_maker_phone, decision_maker_email, confidence. "
        f"DATA: {json.dumps(payload)}"
    )
    return prompt, payload


def _parse_llm_payload(payload: dict[str, object], phone_weight: float) -> DecisionEngineOutput:
    return DecisionEngineOutput(
        decision_maker_name=str(payload.get("decision_maker_name", "") or "").strip(),
        decision_maker_role=str(payload.get("decision_maker_role", "") or "").strip().lower(),
        decision_maker_phone=str(payload.get("decision_maker_phone", "") or "").strip(),
        decision_maker_email=str(payload.get("decision_maker_email", "") or "").strip(),
        confidence=float(payload.get("confidence", 0.0) or 0.0),
        source="llm",
        llm_input={},
        llm_output=payload,
        phone_weight=phone_weight,
        llm_error="",
        llm_timed_out=False,
    )


def run_decision_engine(extracted: ContactExtractionResult, *, model_name: str) -> DecisionEngineOutput:
    heuristic = _heuristic_decision(extracted)
    if not extracted.items:
        heuristic.confidence = _score(heuristic, has_only_generic=True)
        return heuristic

    prompt, llm_input = _llm_prompt(extracted)
    llm_result = generate_json(
        prompt=prompt,
        retries=0,
        temperature=0.0,
        max_tokens=320,
        model=model_name,
        stage="decision_maker_selection",
    )

    parsed = llm_result.data if llm_result.ok and llm_result.data else {}
    required = {"decision_maker_name", "decision_maker_role", "decision_maker_phone", "decision_maker_email", "confidence"}

    if not required.issubset(set(parsed.keys())):
        llm_result_retry = generate_json(
            prompt=prompt,
            retries=0,
            temperature=0.0,
            max_tokens=320,
            model=model_name,
            stage="decision_maker_selection_retry",
        )
        parsed = llm_result_retry.data if llm_result_retry.ok and llm_result_retry.data else {}
        if getattr(llm_result_retry, "error", ""):
            llm_result = llm_result_retry

    if required.issubset(set(parsed.keys())):
        best_phone_value, phone_weight = _best_phone(extracted.phones)
        output = _parse_llm_payload(parsed, phone_weight=phone_weight)
        if not output.decision_maker_phone:
            output.decision_maker_phone = best_phone_value
        output.llm_input = llm_input
        output.llm_output = parsed
    else:
        output = heuristic
        output.llm_input = llm_input
        output.llm_output = parsed
        output.llm_error = getattr(llm_result, "error", "") or ""
        output.llm_timed_out = output.llm_error == "ollama_timeout"

    generic = bool(output.decision_maker_phone) and output.phone_weight < 0.7
    output.confidence = _score(output, has_only_generic=generic)

    if output.confidence < 0.25:
        previous_error = output.llm_error
        previous_timed_out = output.llm_timed_out
        output = heuristic
        output.llm_error = previous_error
        output.llm_timed_out = previous_timed_out
        output.confidence = _score(output, has_only_generic=output.phone_weight < 0.7 if output.decision_maker_phone else True)
    return output


def build_lead_output(
    *,
    company_name: str,
    website: str,
    decision_output: DecisionEngineOutput,
    general_phone: str,
) -> dict[str, object]:
    return {
        "company_name": company_name or "",
        "website": website or "",
        "decision_maker_name": decision_output.decision_maker_name or "",
        "decision_maker_role": (decision_output.decision_maker_role or "").lower(),
        "decision_maker_email": decision_output.decision_maker_email or "",
        "decision_maker_phone": decision_output.decision_maker_phone or "",
        "general_phone": general_phone or "",
        "confidence_score": float(max(0.0, min(1.0, decision_output.confidence))),
        "source": decision_output.source or "",
    }
