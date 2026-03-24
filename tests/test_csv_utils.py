from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.services import lead_row
from app.services.csv_utils import inspect_upload_csv


class CSVUtilsInspectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self._original_generate_json = lead_row.generate_json
        lead_row.generate_json = lambda *args, **kwargs: type("Result", (), {"ok": False, "error": "mocked", "data": {}})()

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


if __name__ == "__main__":
    unittest.main()
