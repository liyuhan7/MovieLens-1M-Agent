"""Content gates on actual artifacts, including mutations with updated file checksums."""
import json
import os
import shutil
import unittest
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from governance.validation import validate_content, _payload, _typed_payload_text
from metadata.connection import ROOT, load_config
from metadata.store import Store, canonical
from pipeline.run_pipeline import RULE_VERSION, METRIC_VERSION, sha256


class ContentValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = load_config()
        if config["database"] != "ml_governance_test":
            raise RuntimeError("Content integration uses only the test database")
        store = Store(config)
        # A fault replay changes error/stage metadata. Bind these content tests
        # to their actual retained artifact fixture rather than an error label.
        run = store.get_run(os.environ.get("ML_CONTENT_RUN", "run-3c3f17ecde754df8b5cbc59fc3c571a9"))
        if not run or (run["rule_version"], run["metric_version"]) != (RULE_VERSION, METRIC_VERSION):
            raise RuntimeError("Run current-version integration_yarn.py and integration_artifacts.py first")
        attempt = store.list_attempts(run["run_id"])[-1]
        cls.root = ROOT / attempt["work_path"] / "artifacts"
        cls.expected = {"run_id": run["run_id"], "attempt_id": attempt["attempt_id"],
                        **{key: run[key] for key in ("input_version", "rule_version", "metric_version")},
                        "input_manifest": store.get_input(run["input_version"])["manifest"]}
        request = json.loads(run["request"]) if isinstance(run["request"], str) else run["request"]
        cls.raw = request["raw_paths"]

    def copy(self):
        path = ROOT / "outputs" / "validation-tests" / uuid.uuid4().hex / "artifacts"
        shutil.copytree(self.root, path)
        return path

    def rewrite(self, root, relative, change):
        path = root / relative
        parquet = pq.ParquetFile(path)
        bodies = parquet.read().to_pylist()
        replacement = change(bodies, parquet.schema_arrow)
        pq.write_table(replacement if replacement is not None else pa.Table.from_pylist(bodies, schema=parquet.schema_arrow), path, compression="zstd")
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
        item = next(item for item in manifest["files"] if item["path"] == relative)
        item.update(sha256=sha256(path), bytes=path.stat().st_size, rows=pq.ParquetFile(path).metadata.num_rows)
        (root / "manifest.json").write_text(canonical(manifest), encoding="utf-8")

    def validate(self, root):
        return validate_content(root, expected=self.expected, raw_paths=self.raw)

    def test_actual_source_quality_and_action_chains(self):
        result = self.validate(self.root)
        self.assertEqual(result["quality_rows"], 30)
        (self.root.parent / "content-acceptance.json").write_text(json.dumps(result, indent=2), encoding="utf-8")

    def test_quality_detail_tamper_rejected_after_checksum_update(self):
        root = self.copy()
        def change(rows, schema):
            rows[0]["present_fields"] = 0
        self.rewrite(root, "quality/before/users.parquet", change)
        with self.assertRaisesRegex(ValueError, "independent source observation"):
            self.validate(root)

    def test_chain_tamper_rejected_after_checksum_update(self):
        root = self.copy()
        def change(rows, schema):
            next(row for row in rows if row["kind"] == "TRANSFORMATION")["before"] = "unrecorded source"
        self.rewrite(root, "evidence/users/part-00000.parquet", change)
        with self.assertRaisesRegex(ValueError, "discontinuous"):
            self.validate(root)

    def test_movie_year_tamper_rejected_after_checksum_update(self):
        root = self.copy()
        def change(rows, schema):
            rows[0]["year"] = 2000
        self.rewrite(root, "cleaned/movies/part-00000.parquet", change)
        with self.assertRaisesRegex(ValueError, "movies value domain"):
            self.validate(root)

    def test_schema_change_rejected_after_checksum_update(self):
        root = self.copy()
        def change(rows, schema):
            table = pa.Table.from_pylist(rows, schema=schema)
            position = schema.get_field_index("zip_code")
            return table.set_column(position, "zip_code", pa.array([1234] * len(rows), type=pa.int64()))
        self.rewrite(root, "cleaned/users/part-00000.parquet", change)
        with self.assertRaisesRegex(ValueError, "schema mismatch"):
            self.validate(root)

    def test_omitted_source_rejected_against_raw_population(self):
        root = self.copy()
        details = pq.ParquetFile(root / "dispositions/users/part-00000.parquet").read().to_pylist()
        removed = next(row["source_record_id"] for row in details if row["disposition"] == "DEDUP")
        def remove(rows, schema):
            rows[:] = [row for row in rows if row["source_record_id"] != removed]
        self.rewrite(root, "dispositions/users/part-00000.parquet", remove)
        self.rewrite(root, "quality/before/users.parquet", remove)
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
        report_path = root / "report.json"
        report = json.loads(report_path.read_text(encoding="utf-8"))
        # Make the inventory/report population look internally consistent. The
        # original immutable raw file still has three records and must reject it.
        for document in (manifest, report):
            document["source_balance"]["users"].update(raw=2, dedup=0)
        report["row_change"]["users"]["raw"] = 2
        report_path.write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")
        item = next(item for item in manifest["files"] if item["path"] == "report.json")
        item.update(sha256=sha256(report_path), bytes=report_path.stat().st_size)
        (root / "manifest.json").write_text(canonical(manifest), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "omits original raw records"):
            self.validate(root)

    def test_numeric_occupation_conversion_preserves_string_identifiers_zip(self):
        row = {"user_id": "001", "gender": "M", "age": 25, "occupation": 3, "zip_code": "01234"}
        self.assertEqual(_payload(row, "users"), _typed_payload_text("001::M::25::03::01234", "users"))
        self.assertTrue(_payload(row, "users").startswith("001::"))
        self.assertTrue(_payload(row, "users").endswith("::01234"))


if __name__ == "__main__":
    unittest.main()
