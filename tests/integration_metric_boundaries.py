"""Independently expected metric boundaries through real YARN and publish gates."""
import hashlib
import json
import shutil
import unittest
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from governance.artifacts import build_artifacts
from governance.publication import publish_artifacts
from governance.service import submit_run
from metadata.connection import ROOT, load_config
from metadata.store import Store, canonical
from pipeline.run_pipeline import input_fingerprint, pipeline, sha256
from pipeline.yarn import YarnExecutor
from storage.hdfs import import_input


class MetricBoundaryTests(unittest.TestCase):
    def test_boundaries_and_real_publisher_rejects_incomplete_or_corrupt_candidates(self):
        config = load_config()
        self.assertEqual(config["database"], "ml_governance_test")
        store = Store(config)
        fixture = ROOT / "outputs" / "metric-boundary-integration" / uuid.uuid4().hex
        fixture.mkdir(parents=True)
        raw = {table: fixture / (table + ".dat") for table in ("users", "movies", "ratings")}
        raw["users"].write_text("".join(f"{uid}::M::25::4::01234\n" if uid != 15 else
                                      "15::F::NULL::NULL::NULL\n" for uid in range(1, 22)), encoding="latin-1")
        raw["movies"].write_text("1::Léon (1994)::Action|Crime|Drama\n2::Missing (2000)::NULL\n", encoding="latin-1")
        times = [631151999, 631152000, 1009843200, 1009843201, 1025481600, 1025481601, 1047772799, 1047772800]
        ratings = [f"{uid}::1::4::{stamp}" for uid, stamp in enumerate(times, 1)] + [
            "9::1::0::978000000", "10::1::6::978000000", "11::1::1.5::978000000",
            "12::1::NULL::978000000", "13::1::4::NULL", "14::1::4::1009843201000",
            "15::1::5::978000000", "16::1::4::978000000::extra", "17,1,4,978000000",
            "18 :: 1 :: 4 :: 1009843200", "19::1::4", "999::1::4::978000000",
            "15::1::5::1047772799"]
        raw["ratings"].write_text("\n".join(ratings) + "\n", encoding="latin-1")
        manifest = input_fingerprint(raw)
        paths = {table: path.relative_to(ROOT).as_posix() for table, path in raw.items()}
        store.register_input("movielens-1m", manifest["dataset_version"], manifest)
        import_input(store, manifest["dataset_version"], paths)
        run, _ = submit_run("metric-boundaries-" + uuid.uuid4().hex, store=store, raw=raw)
        claim = store.claim("metric-boundary-acceptance", lease_seconds=3600)
        self.assertEqual(claim.run_id, run["run_id"])
        (fixture / "run.json").write_text(json.dumps({"run_id": claim.run_id, "attempt_id": claim.attempt_id}), encoding="utf-8")
        try:
            report = pipeline(run_id=claim.run_id, attempt_id=claim.attempt_id, raw=raw,
                              executor=YarnExecutor(store, claim, reducers=2), register=False)
            self.assertEqual(report["metrics_raw"]["ratings"], {
                "N": 21, "ACC": 9, "COMP": 78, "CONS": 12, "UD": 14,
                "TRAIN": 10, "VALID": 2, "TEST": 5, "UNIQ_EXCESS": 1})
            self.assertEqual(report["metrics_clean"]["ratings"], {
                "N": 11, "ACC": 11, "COMP": 44, "CONS": 11, "UD": 11,
                "TRAIN": 5, "VALID": 3, "TEST": 3, "UNIQ_EXCESS": 0})
            for phase in ("metrics_raw", "metrics_clean"):
                self.assertEqual(report[phase]["users"], {"N": 21, "ACC": 20, "COMP": 102, "CONS": 20, "UNIQ_EXCESS": 0})
                self.assertEqual(report[phase]["movies"], {"N": 2, "ACC": 1, "COMP": 3, "CONS": 1, "UNIQ_EXCESS": 0})
            store.advance(claim, "metric-boundary-validation", "VALIDATING")
            root, output = build_artifacts(report, paths)
            self.assertEqual(output["source_balance"]["ratings"], {"raw": 21, "keep": 11, "isolate": 9, "dedup": 1})
            cleaned = {row["user_id"]: row for row in pq.read_table(root / "cleaned/ratings/part-00000.parquet").to_pylist()}
            self.assertEqual(cleaned["2"]["timestamp_seconds"], 631152000)
            self.assertEqual(cleaned["3"]["timestamp_seconds"], 1009843200)
            self.assertEqual(cleaned["4"]["timestamp_seconds"], 1009843201)
            self.assertEqual(cleaned["5"]["timestamp_seconds"], 1025481600)
            self.assertEqual(cleaned["6"]["timestamp_seconds"], 1025481601)
            self.assertEqual(cleaned["7"]["timestamp_seconds"], 1047772799)
            self.assertEqual(cleaned["14"]["timestamp_seconds"], 1009843201)
            self.assertEqual(cleaned["15"]["timestamp_seconds"], 1047772799)
            evidence = pq.read_table(root / "evidence/ratings/part-00000.parquet").to_pylist()
            events = {(row["rule_id"], row["action"]) for row in evidence if row["kind"] == "TRANSFORMATION"}
            self.assertTrue({("R2", "isolate"), ("R3", "isolate"), ("R4", "isolate"),
                             ("R5", "repair"), ("R6", "isolate"), ("R7", "dedup"),
                             ("R9", "isolate"), ("R1a", "repair"), ("R1b", "repair"), ("R0", "repair")} <= events)
            summary = pq.read_table(root / "quality/summary.parquet").to_pylist()
            self.assertEqual(len(summary), 30)
            self.assertTrue(all(row["score"] is None and row["denominator"] is None for row in summary
                                if row["source_table"] in {"users", "movies"} and row["metric"] == "up_to_date"))
            rejected = []
            for name in ("missing-report", "missing-evidence", "wrong-hash", "wrong-schema", "wrong-quality",
                         "wrong-report-dataset", "wrong-report-table", "wrong-report-delta",
                         "wrong-report-split", "wrong-report-boundary", "wrong-report-removed"):
                damaged = fixture / name / "artifacts"
                shutil.copytree(root, damaged)
                if name.startswith("missing"):
                    relative = "report.json" if name == "missing-report" else "evidence/ratings/part-00000.parquet"
                    (damaged / relative).rename(damaged.parent / "retained-diagnostic-file")
                elif name == "wrong-hash":
                    with (damaged / "report.json").open("ab") as stream:
                        stream.write(b" ")
                elif name.startswith("wrong-report-"):
                    candidate_report = json.loads((damaged / "report.json").read_text(encoding="utf-8"))
                    if name == "wrong-report-dataset":
                        candidate_report["dataset_composite"]["clean"] = -9000
                    elif name == "wrong-report-table":
                        candidate_report["scores"]["ratings"]["composite_clean"] = -9000
                    elif name == "wrong-report-delta":
                        candidate_report["scores"]["ratings"]["delta"]["accurate"] = 99
                    elif name == "wrong-report-split":
                        candidate_report["split"]["train"] = 999
                    elif name == "wrong-report-boundary":
                        candidate_report["split"]["t1"] = "2001-01-01T00:00:00+00:00"
                    else:
                        candidate_report["row_change"]["ratings"]["removed"] = 999
                    report_path = damaged / "report.json"
                    report_path.write_text(json.dumps(candidate_report, ensure_ascii=False, indent=2), encoding="utf-8")
                    changed = json.loads((damaged / "manifest.json").read_text(encoding="utf-8"))
                    item = next(item for item in changed["files"] if item["path"] == "report.json")
                    item.update(sha256=sha256(report_path), bytes=report_path.stat().st_size)
                    (damaged / "manifest.json").write_text(canonical(changed), encoding="utf-8")
                else:
                    relative = "cleaned/ratings/part-00000.parquet" if name == "wrong-schema" else "quality/before/ratings.parquet"
                    parquet = pq.ParquetFile(damaged / relative)
                    bodies = parquet.read().to_pylist()
                    schema = parquet.schema_arrow
                    if name == "wrong-schema":
                        for row in bodies:
                            row["timestamp_seconds"] = str(row["timestamp_seconds"])
                        schema = pa.schema([pa.field(field.name, pa.string() if field.name == "timestamp_seconds" else field.type)
                                            for field in schema])
                    else:
                        bodies[0]["present_fields"] = 0
                    pq.write_table(pa.Table.from_pylist(bodies, schema=schema), damaged / relative)
                    changed = json.loads((damaged / "manifest.json").read_text(encoding="utf-8"))
                    item = next(item for item in changed["files"] if item["path"] == relative)
                    item.update(sha256=sha256(damaged / relative), bytes=(damaged / relative).stat().st_size,
                                schema_sha256=hashlib.sha256(schema.serialize().to_pybytes()).hexdigest())
                    (damaged / "manifest.json").write_text(canonical(changed), encoding="utf-8")
                with self.assertRaises(ValueError):
                    publish_artifacts(store, claim, damaged)
                self.assertIsNone(store.get_publish(claim.run_id))
                rejected.append(name)
            publication = publish_artifacts(store, claim, root)
            self.assertEqual(publication["status"], "PUBLISHED")
            (fixture / "acceptance.json").write_text(json.dumps({"run_id": claim.run_id, "attempt_id": claim.attempt_id,
                "publish_id": publication["publish_id"], "manifest_sha256": publication["manifest_hash"],
                "raw_metrics": report["metrics_raw"], "clean_metrics": report["metrics_clean"],
                "rejected_before_intent": rejected, "source_balance": output["source_balance"],
                "real_jobs": len(store.list_jobs(claim.attempt_id))}, indent=2), encoding="utf-8")
        finally:
            if store.get_run(claim.run_id)["status"] != "PUBLISHED" and not store.get_publish(claim.run_id):
                store.advance(claim, "metric-boundary-test-ended", "FAILED", {"reason": "boundary acceptance incomplete"})


if __name__ == "__main__":
    unittest.main()
