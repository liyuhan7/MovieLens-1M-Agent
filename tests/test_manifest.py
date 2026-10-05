"""Inventory preflight only: fixture bytes intentionally are not Parquet content."""
import hashlib
import json
import shutil
import unittest
import uuid
from pathlib import Path

from governance.manifest import preflight_artifacts, required_files


class ManifestTests(unittest.TestCase):
    def setUp(self):
        self.parent = Path(__file__).resolve().parents[1] / "outputs" / "tests"
        self.root = self.parent / uuid.uuid4().hex
        self.root.mkdir(parents=True)
        self.addCleanup(self.cleanup)
        self.expected = {"run_id": "run-test", "attempt_id": "attempt-test", "input_version": "raw-test",
                         "rule_version": "rules-v2.0", "metric_version": "metrics-v2.1",
                         "input_manifest": {"dataset_version": "raw-test", "tables": {}}}
        balance = {table: {"raw": 3, "keep": 1, "isolate": 1, "dedup": 1} for table in ("users", "movies", "ratings")}
        self.report = {**self.expected, "schema_version": "report-v2.0", "input_data_version": "raw-test",
                       "source_balance": balance, "row_change": {table: {"raw": 3, "clean": 1} for table in balance},
                       "jobs": []}
        self.manifest = {**self.expected, "schema_version": "manifest-v1", "source_balance": balance,
                         "report": "report.json", "jobs": [], "files": []}
        for name in sorted(required_files()):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            if name == "report.json":
                path.write_text(json.dumps(self.report), encoding="utf-8")
                item = {"path": name, "format": "json"}
            else:
                path.write_bytes(b"inventory-fixture")
                rows = 30 if name == "quality/summary.parquet" else 3 if name.startswith(("dispositions/", "evidence/", "quality/before/")) else 1
                item = {"path": name, "format": "parquet", "rows": rows, "schema_sha256": "a" * 64}
            item.update(bytes=path.stat().st_size, sha256=hashlib.sha256(path.read_bytes()).hexdigest())
            self.manifest["files"].append(item)
        self.save()

    def cleanup(self):
        if not self.root.resolve().is_relative_to(self.parent.resolve()):
            raise RuntimeError("Test cleanup escaped workspace")
        shutil.rmtree(self.root)

    def save(self):
        (self.root / "manifest.json").write_text(json.dumps(self.manifest), encoding="utf-8")

    def check(self):
        return preflight_artifacts(self.root, expected=self.expected)

    def test_complete_inventory_bound_to_expected_attempt(self):
        verified = self.check()
        self.assertEqual(len(verified["files"]), 21)
        self.assertEqual(verified["manifest_sha256"], hashlib.sha256((self.root / "manifest.json").read_bytes()).hexdigest())
        # This result deliberately carries no published status or authorization.
        self.assertNotIn("published", verified)

    def test_missing_required_evidence_rejected(self):
        self.manifest["files"] = [item for item in self.manifest["files"] if item["path"] != "evidence/users/part-00000.parquet"]
        self.save()
        with self.assertRaisesRegex(ValueError, "required artifacts"):
            self.check()

    def test_modified_bytes_rejected_even_with_same_length(self):
        (self.root / "cleaned/users/part-00000.parquet").write_bytes(b"INVENTORY-FIXTURE")
        with self.assertRaisesRegex(ValueError, "checksum"):
            self.check()

    def test_foreign_attempt_rejected(self):
        self.manifest["attempt_id"] = "attempt-other"
        self.save()
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            self.check()

    def test_repeated_and_escaped_paths_rejected(self):
        original = self.manifest["files"][0]["path"]
        for invalid in ("../outside.parquet", "/absolute.parquet", "cleaned\\users\\part.parquet"):
            with self.subTest(path=invalid):
                self.manifest["files"][0]["path"] = invalid
                self.save()
                with self.assertRaises(ValueError):
                    self.check()
        self.manifest["files"][0]["path"] = original
        self.manifest["files"].append(dict(self.manifest["files"][0]))
        self.save()
        with self.assertRaisesRegex(ValueError, "repeated"):
            self.check()

    def test_inconsistent_count_and_undeclared_file_rejected(self):
        item = next(item for item in self.manifest["files"] if item["path"] == "quality/after/users.parquet")
        item["rows"] = 2
        self.save()
        with self.assertRaisesRegex(ValueError, "row count"):
            self.check()
        item["rows"] = 1
        self.save()
        (self.root / "undeclared.txt").write_text("extra", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "declared inventory"):
            self.check()


if __name__ == "__main__":
    unittest.main()
