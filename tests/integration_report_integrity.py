"""Rehashed report arithmetic must not bypass the content publication gate."""
import json
import shutil
import unittest
import uuid

from governance.validation import validate_content
from metadata.connection import ROOT, load_config
from metadata.store import Store, canonical
from pipeline.run_pipeline import sha256


class DerivedReportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = load_config()
        if config["database"] != "ml_governance_test":
            raise RuntimeError("Report integrity acceptance only uses the test database")
        store = Store(config)
        run = store.get_run("run-965b86fbb9e94bc5ab35f71df5ab91a7")
        if not run or run["status"] != "PUBLISHED":
            raise RuntimeError("Run the real HTTP worker acceptance first")
        publication = store.get_publish(run["run_id"])
        attempt = next(item for item in store.list_attempts(run["run_id"])
                       if item["attempt_id"] == publication["attempt_id"])
        cls.source = ROOT / attempt["work_path"] / "artifacts"
        cls.raw = run["request"]["raw_paths"]
        cls.expected = {"run_id": run["run_id"], "attempt_id": publication["attempt_id"],
                        **{key: run[key] for key in ("input_version", "rule_version", "metric_version")},
                        "input_manifest": store.get_input(run["input_version"])["manifest"]}

    def damage(self, edit):
        root = ROOT / "outputs" / "report-integrity-tests" / uuid.uuid4().hex / "artifacts"
        shutil.copytree(self.source, root)
        path = root / "report.json"
        report = json.loads(path.read_text(encoding="utf-8"))
        edit(report)
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
        item = next(item for item in manifest["files"] if item["path"] == "report.json")
        item.update(sha256=sha256(path), bytes=path.stat().st_size)
        (root / "manifest.json").write_text(canonical(manifest), encoding="utf-8")
        return root

    def test_unchanged_real_report_passes(self):
        result = validate_content(self.source, expected=self.expected, raw_paths=self.raw)
        self.assertEqual(result["quality_rows"], 30)

    def test_dataset_composite_tamper_rejected(self):
        root = self.damage(lambda report: report["dataset_composite"].update(clean=-9000))
        with self.assertRaises(ValueError):
            validate_content(root, expected=self.expected, raw_paths=self.raw)

    def test_table_composite_tamper_rejected(self):
        root = self.damage(lambda report: report["scores"]["ratings"].update(composite_clean=-9000))
        with self.assertRaises(ValueError):
            validate_content(root, expected=self.expected, raw_paths=self.raw)

    def test_delta_tamper_rejected(self):
        root = self.damage(lambda report: report["scores"]["ratings"]["delta"].update(accurate=99))
        with self.assertRaises(ValueError):
            validate_content(root, expected=self.expected, raw_paths=self.raw)

    def test_split_counts_tamper_rejected(self):
        root = self.damage(lambda report: report["split"].update(train=999))
        with self.assertRaises(ValueError):
            validate_content(root, expected=self.expected, raw_paths=self.raw)

    def test_split_boundary_tamper_rejected(self):
        root = self.damage(lambda report: report["split"].update(t1="2001-01-01T00:00:00+00:00"))
        with self.assertRaises(ValueError):
            validate_content(root, expected=self.expected, raw_paths=self.raw)

    def test_removed_count_tamper_rejected(self):
        root = self.damage(lambda report: report["row_change"]["ratings"].update(removed=999))
        with self.assertRaises(ValueError):
            validate_content(root, expected=self.expected, raw_paths=self.raw)


if __name__ == "__main__":
    unittest.main()
