from types import SimpleNamespace

import enrichment.decision_engine as decision_engine
from enrichment.contact_extractor import ContactExtractionResult, ContactItem


def test_decision_engine_falls_back_to_heuristic_when_llm_invalid(monkeypatch):
    def fake_generate_json(**kwargs):
        return SimpleNamespace(ok=False, data={})

    monkeypatch.setattr(decision_engine, "generate_json", fake_generate_json)

    extracted = ContactExtractionResult(
        items=[
            ContactItem("John Doe", "name", "team", "Owner John Doe"),
            ContactItem("john@company.com", "email", "team", "Owner John Doe john@company.com"),
            ContactItem("555-111-2222", "phone", "team", "direct 555-111-2222"),
        ]
    )

    output = decision_engine.run_decision_engine(extracted, model_name="test-model")

    assert output.decision_maker_name == "John Doe"
    assert output.decision_maker_phone == "555-111-2222"
    assert output.confidence > 0
