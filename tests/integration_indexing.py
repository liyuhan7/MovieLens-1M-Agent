"""Actual MySQL/Parquet index validation, replaying source-verified synthetic outputs.

This creates a new index fixture, not a new successful Hadoop execution or publish.
"""
import json
import os
import unittest
import uuid
from unittest.mock import patch

import pyarrow.parquet as pq

from governance.artifacts import build_artifacts
from governance.indexing import index_artifacts, verify_evidence_index
from governance.service import submit_run
from metadata.connection import ROOT, load_config, migrate
from metadata.store import Conflict, LostLease, Store
from pipeline.run_pipeline import RULE_VERSION, METRIC_VERSION


class IndexingTests(unittest.TestCase):
    def test_real_index_reentry_positions_conflict_and_fencing(self):
        config = load_config()
        self.assertEqual(config["database"], "ml_governance_test")
        migrate(config)
        store = Store(config)
        # Recovery acceptance deliberately changes the source Run's terminal
        # reason. Bind its retained artifact identity rather than that label.
        original = store.get_run(os.environ.get("ML_INDEX_SOURCE_RUN", "run-3c3f17ecde754df8b5cbc59fc3c571a9"))
        self.assertIsNotNone(original, "Run current-version integration_yarn.py first")
        self.assertEqual((original["rule_version"], original["metric_version"]), (RULE_VERSION, METRIC_VERSION))
        request = json.loads(original["request"]) if isinstance(original["request"], str) else original["request"]
        raw = {table: ROOT / path for table, path in request["raw_paths"].items()}
        run, _ = submit_run("index-" + uuid.uuid4().hex, store=store, raw=raw)
        claim = store.claim("index-acceptance", lease_seconds=3600)
        self.assertEqual(claim.run_id, run["run_id"])
        try:
            original_work = ROOT / store.list_attempts(original["run_id"])[-1]["work_path"]
            report = json.loads((original_work / "report.json").read_text(encoding="utf-8"))
            work = ROOT / claim.work_path
            work.mkdir(parents=True)
            report.update(run_id=claim.run_id, task_id=claim.run_id, attempt_id=claim.attempt_id,
                          output_data_version="clean-" + claim.run_id, candidate_report=str(work / "report.json"))
            (work / "report.json").write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")
            store.advance(claim, "index-fixture", "VALIDATING")
            root, manifest = build_artifacts(report, request["raw_paths"], batch_size=2)
            first = index_artifacts(store, claim, root)
            self.assertEqual(first["quality_rows"], 30)
            self.assertGreater(first["evidence_rows"], 9)
            self.assertEqual(index_artifacts(store, claim, root), first)
            page = store.evidence_positions(claim.attempt_id)
            self.assertEqual(len(page), first["evidence_rows"])
            self.assertTrue(any(row["row_group"] > 0 for row in page))
            self.assertTrue(any(row["row_in_group"] == 1 for row in page))
            for row in page:
                body = pq.ParquetFile(root / row["file_path"]).read_row_group(row["row_group"]).to_pylist()[row["row_in_group"]]
                self.assertEqual(body["evidence_id"], row["evidence_id"])
            inventory = {item["path"]: item for item in manifest["files"]}
            original_parquet = pq.ParquetFile
            reads = []
            class CountingParquet:
                def __init__(self, path):
                    self.path, self.parquet = str(path), original_parquet(path)
                def __getattr__(self, name):
                    return getattr(self.parquet, name)
                def read_row_group(self, group):
                    reads.append((self.path, group))
                    return self.parquet.read_row_group(group)
            expected_groups = sum(original_parquet(root / name).num_row_groups for name in inventory if name.startswith("evidence/"))
            with patch("governance.indexing.pq.ParquetFile", CountingParquet):
                self.assertEqual(verify_evidence_index(store, claim.attempt_id, root, inventory), first["evidence_rows"])
            self.assertEqual(len(reads), expected_groups)
            self.assertEqual(len(reads), len(set(reads)), "A complete verification reads each row group once")
            with store.transaction() as cursor:
                cursor.execute("EXPLAIN SELECT * FROM evidence_index WHERE attempt_id=%s AND file_path=%s AND row_group=%s ORDER BY row_in_group",
                               (claim.attempt_id, page[0]["file_path"], page[0]["row_group"]))
                self.assertEqual(cursor.fetchone()["key"], "evidence_position")
            changed = {**page[0], "row_in_group": 99}
            with self.assertRaises(Conflict):
                store.index_evidence_batch(claim, [changed])
            # A deliberately corrupted test index must be detected against actual bytes.
            with store.transaction() as cursor:
                cursor.execute("UPDATE evidence_index SET row_in_group=99 WHERE evidence_id=%s", (page[0]["evidence_id"],))
            with self.assertRaisesRegex(ValueError, "outside the row group"):
                verify_evidence_index(store, claim.attempt_id, root, {item["path"]: item for item in manifest["files"]})
            with store.transaction() as cursor:
                cursor.execute("UPDATE evidence_index SET row_in_group=%s WHERE evidence_id=%s", (page[0]["row_in_group"], page[0]["evidence_id"]))
                cursor.execute("UPDATE logical_run SET lease_until=DATE_SUB(NOW(6),INTERVAL 1 SECOND) WHERE run_id=%s", (claim.run_id,))
                cursor.execute("UPDATE worker_slot SET lease_until=DATE_SUB(NOW(6),INTERVAL 1 SECOND) WHERE run_id=%s", (claim.run_id,))
            old = claim
            claim = store.claim("index-replacement", lease_seconds=3600)
            self.assertEqual(claim.attempt_id, old.attempt_id)
            with self.assertRaises(LostLease):
                store.index_evidence_batch(old, [page[0]])
            self.assertEqual(index_artifacts(store, claim, root), first)
            (work / "index-acceptance.json").write_text(json.dumps({"run_id": claim.run_id, "attempt_id": claim.attempt_id,
                    **first, "row_group_size": 2, "row_group_reads": len(reads), "publication_verified": False}, indent=2), encoding="utf-8")
        finally:
            store.advance(claim, "index-test-ended", "FAILED", {"reason": "synthetic index acceptance; not published"})


if __name__ == "__main__":
    unittest.main()
