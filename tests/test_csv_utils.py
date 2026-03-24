from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.services import lead_row
from app.services.csv_utils import inspect_upload_csv


class CSVUtilsInspectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self._original_generate_json = lead_row.generate_json
        self._generate_json_response = {"lead_row_enrichment": {"ok": False, "error": "mocked", "data": {}}}

        def _fake_generate_json(*args, **kwargs):
            stage = kwargs.get("stage", "")
            payload = self._generate_json_response.get(stage) or {"ok": False, "error": "mocked", "data": {}}
            return type("Result", (), payload)()

        lead_row.generate_json = _fake_generate_json

    def tearDown(self) -> None:
        lead_row.generate_json = self._original_generate_json

    def test_headerless_csv_promotes_first_row_to_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "headerless.csv"
            csv_path.write_text(
                "\n".join(
                    [
                        "Mavie Med Spa,4.9,(435) 555-0101,Medical Spa,2825 E Mall Dr",
                        "Ivory Aesthetics,4.8,(435) 555-0102,Medical Spa,123 Main St",
                    ]
                ),
                encoding="utf-8",
            )

            inspection = inspect_upload_csv(csv_path)

            self.assertEqual(inspection.detected_row_count, 2)
            self.assertEqual(inspection.original_headers, ["column_1", "column_2", "column_3", "column_4", "column_5"])
            self.assertEqual(inspection.preview_rows[0]["column_1"], "Mavie Med Spa")
            self.assertTrue(
                any("treated first row as data" in warning.lower() for warning in inspection.warnings),
                "Expected warning when CSV has no recognizable header row.",
            )

    def test_headerless_csv_uses_llm_inferred_column_names(self) -> None:
        self._generate_json_response["csv_header_inference"] = {
            "ok": True,
            "error": "",
            "data": {
                "header_names": {
                    "column_1": "company_name",
                    "column_2": "rating",
                    "column_3": "phone",
                    "column_4": "category",
                    "column_5": "address",
                }
            },
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "headerless_inferred.csv"
            csv_path.write_text(
                "\n".join(
                    [
                        "Mavie Med Spa,4.9,(435) 555-0101,Medical Spa,2825 E Mall Dr",
                        "Ivory Aesthetics,4.8,(435) 555-0102,Medical Spa,123 Main St",
                    ]
                ),
                encoding="utf-8",
            )

            inspection = inspect_upload_csv(csv_path)

            self.assertIn("company_name", inspection.original_headers)
            self.assertIn("phone", inspection.original_headers)
            self.assertEqual(inspection.preview_rows[0]["company_name"], "Mavie Med Spa")
            self.assertTrue(any("inferred column names" in warning.lower() for warning in inspection.warnings))


if __name__ == "__main__":
    unittest.main()
