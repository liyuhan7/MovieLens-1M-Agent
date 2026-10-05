"""Build real Parquet from a verified, unpublished synthetic Hadoop attempt."""
import json
import unittest

import pyarrow.parquet as pq

from governance.artifacts import build_artifacts
from metadata.connection import ROOT, load_config
from metadata.store import Store
from pipeline.run_pipeline import RULE_VERSION, METRIC_VERSION


class ArtifactTests(unittest.TestCase):
    def test_source_complete_parquet_from_actual_hadoop(self):
        config = load_config()
        self.assertEqual(config["database"], "ml_governance_test")
        store = Store(config)
        with store.transaction() as cursor:
            cursor.execute("SELECT * FROM logical_run WHERE rule_version=%s AND metric_version=%s "
                           "AND JSON_UNQUOTE(JSON_EXTRACT(error,'$.reason'))=%s ORDER BY request_seq DESC LIMIT 1",
                           (RULE_VERSION, METRIC_VERSION, "synthetic compute acceptance; not published"))
            run = cursor.fetchone()
        self.assertIsNotNone(run, "Run integration_yarn.py successfully with current versions first")
        work = ROOT / store.list_attempts(run["run_id"])[-1]["work_path"]
        candidate = work / "report.json"
        report = json.loads(candidate.read_text(encoding="utf-8"))
        report["candidate_report"] = str(candidate)
        request = json.loads(run["request"]) if isinstance(run["request"], str) else run["request"]
        root, manifest = build_artifacts(report, request["raw_paths"])
        self.assertEqual(manifest["source_balance"], {
            "users": {"raw": 3, "keep": 2, "isolate": 0, "dedup": 1},
            "movies": {"raw": 3, "keep": 2, "isolate": 0, "dedup": 1},
            "ratings": {"raw": 3, "keep": 1, "isolate": 1, "dedup": 1}})
        movies = pq.ParquetFile(root / "cleaned/movies/part-00000.parquet").read().to_pylist()
        leon = next(row for row in movies if row["movie_id"] == "1")
        self.assertEqual(leon["title"], "Léon (1994)")
        self.assertEqual(leon["genres"], ["Action", "Crime", "Drama"])
        self.assertTrue(leon["related_source_ids"])
        users = pq.ParquetFile(root / "cleaned/users/part-00000.parquet").read().to_pylist()
        self.assertEqual(next(row for row in users if row["user_id"] == "1")["zip_code"], "01234")
        self.assertIsNone(next(row for row in users if row["user_id"] == "2")["gender"])
        for table in ("users", "movies", "ratings"):
            details = pq.ParquetFile(root / ("dispositions/" + table + "/part-00000.parquet")).read().to_pylist()
            self.assertEqual(len({row["source_record_id"] for row in details}), 3)
            evidence = pq.ParquetFile(root / ("evidence/" + table + "/part-00000.parquet")).read().to_pylist()
            self.assertEqual({row["source_record_id"] for row in details}, {row["source_record_id"] for row in evidence})
        self.assertEqual(pq.ParquetFile(root / "quality/summary.parquet").metadata.num_rows, 30)
        (work / "parquet-acceptance.json").write_text(json.dumps({"manifest": str(root / "manifest.json"),
                    "files": len(manifest["files"]), "source_balance": manifest["source_balance"]}), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
