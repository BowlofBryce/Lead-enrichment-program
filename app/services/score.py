from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ScoreResult:
    fit_score: int
    extraction_confidence: float


def score_lead(
    has_email: bool,
    has_phone: bool,
    has_address: bool,
    has_summary: bool,
    classification_confidence: float,
    page_count: int,
) -> ScoreResult:
    fit = 0
    if has_email:
        fit += 20
    if has_phone:
        fit += 15
    if has_address:
        fit += 10
    if has_summary:
        fit += 15
    fit += min(20, page_count * 4)
    fit += int(max(0.0, min(1.0, classification_confidence)) * 20)

    confidence = 0.0
    confidence += 0.2 if has_email else 0.0
    confidence += 0.2 if has_phone else 0.0
    confidence += 0.15 if has_address else 0.0
    confidence += 0.15 if has_summary else 0.0
    confidence += min(0.2, page_count * 0.04)
    confidence += max(0.0, min(1.0, classification_confidence)) * 0.1

    return ScoreResult(
        fit_score=max(0, min(100, fit)),
        extraction_confidence=round(max(0.0, min(1.0, confidence)), 3),
    )
