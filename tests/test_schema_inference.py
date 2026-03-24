from __future__ import annotations

import unittest

from app.services import schema_inference
from app.services.schema_inference import infer_schema_plan


class SchemaInferenceHeuristicTests(unittest.TestCase):
    def setUp(self) -> None:
        self._original_generate_json = schema_inference.generate_json
        schema_inference.generate_json = lambda **kwargs: type("Result", (), {"ok": False, "data": {}, "error": "mocked"})()

    def tearDown(self) -> None:
        schema_inference.generate_json = self._original_generate_json

    def test_headerless_columns_are_inferred_from_values(self) -> None:
        headers = ["column_1", "column_2", "column_3", "column_4", "column_5"]
        normalized_headers = headers[:]
        sample_rows = [
            {
                "column_1": "Mavie Med Spa",
                "column_2": "4.9",
                "column_3": "(435) 555-0101",
                "column_4": "Medical Spa",
                "column_5": "2825 E Mall Dr",
            },
            {
                "column_1": "Ivory Aesthetics",
                "column_2": "4.8",
                "column_3": "(435) 555-0102",
                "column_4": "Medical Spa",
                "column_5": "123 Main St",
            },
        ]

        result = infer_schema_plan(
            headers=headers,
            normalized_headers=normalized_headers,
            sample_rows=sample_rows,
            custom_instructions="",
            model_name="",
        )

        roles = result.plan_json.get("semantic_column_roles", {})
        self.assertEqual(roles["column_1"]["role"], "primary_entity_name")
        self.assertEqual(roles["column_3"]["role"], "contact_phone")
        self.assertEqual(roles["column_5"]["role"], "street_address")


if __name__ == "__main__":
    unittest.main()
