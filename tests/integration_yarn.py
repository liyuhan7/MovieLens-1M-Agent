"""Real HDFS/YARN small pipeline: source bytes, all jobs, multiple reducers and provenance."""
import json
import unittest
import uuid

from governance.service import submit_run
from metadata.connection import ROOT, load_config, migrate
from metadata.store import Store
from metadata.store import LostLease
from pipeline.run_pipeline import input_fingerprint, pipeline
from pipeline.yarn import YarnExecutor
from storage.hdfs import import_input


class YarnPipelineTests(unittest.TestCase):
    def test_actual_pipeline_with_two_reducers(self):
        config = load_config()
        self.assertEqual(config["database"], "ml_governance_test", "Integration must use the isolated test database")
        migrate(config)
        store = Store(config)
        fixture = ROOT / "outputs" / "integration" / uuid.uuid4().hex
        fixture.mkdir(parents=True)
        raw = {table: fixture / (table + ".dat") for table in ("users", "movies", "ratings")}
        raw["users"].write_bytes(b"1::M::25::4::01234\n1::M::25::4::01234\n2::NULL::18::3::12345\n")
        raw["movies"].write_bytes(
            "1::Léon (1994)::Action|Crime|Drama\n2::Toy (1995)::Animation|Children's|Comedy\n3::Léon (1994)::Action|Crime|Drama\n".encode("latin-1"))
        raw["ratings"].write_bytes(b"1::1::4::978000000\n1::1::5::978000001\n99::2::3::978000002\n")
        manifest = input_fingerprint(raw)
        store.register_input("movielens-1m", manifest["dataset_version"], manifest)
        import_input(store, manifest["dataset_version"], {key: path.relative_to(ROOT).as_posix() for key, path in raw.items()})
        run, _ = submit_run("yarn-" + uuid.uuid4().hex, store=store, raw=raw)
        claim = store.claim("integration-yarn", lease_seconds=3600)
        self.assertEqual(claim.run_id, run["run_id"])
        try:
            report = pipeline(run_id=claim.run_id, attempt_id=claim.attempt_id, raw=raw,
                              executor=YarnExecutor(store, claim, reducers=2), register=False)
            self.assertEqual(len(report["jobs"]), 12)
            self.assertTrue(all(job.get("application_id", "").startswith("application_") for job in report["jobs"]))
            self.assertEqual({table: counts["N"] for table, counts in report["metrics_raw"].items()},
                             {"users": 3, "movies": 3, "ratings": 3})
            self.assertEqual({table: counts["N"] for table, counts in report["metrics_clean"].items()},
                             {"users": 2, "movies": 2, "ratings": 1})
            from pathlib import Path
            movie_text = Path(report["cleaned_paths"]["movies"]).read_text(encoding="utf-8")
            self.assertIn("Léon (1994)::Action|Crime|Drama", movie_text)
            self.assertIn("Animation|Children's|Comedy", movie_text)
            self.assertEqual(report["disposition"]["users"]["counts"]["dedup:U6"], 1)
            self.assertEqual(report["disposition"]["ratings"]["counts"]["dedup:R7"], 1)
            self.assertEqual(len(store.list_jobs(claim.attempt_id)), 12)
            # Inject the submit/record gap after the external job actually completed.
            # Recovery must find the existing YARN application by stable name, without
            # running a second application or creating a replacement Attempt.
            job = next(item for item in store.list_jobs(claim.attempt_id) if item["stage"] == "extractUsers")
            old = claim
            with store.transaction() as cursor:
                cursor.execute("UPDATE hadoop_job SET status='SUBMITTING',job_id=NULL,application_id=NULL "
                               "WHERE submission_id=%s AND attempt_id=%s", (job["submission_id"], claim.attempt_id))
                cursor.execute("UPDATE logical_run SET lease_until=DATE_SUB(NOW(6),INTERVAL 1 SECOND) WHERE run_id=%s", (claim.run_id,))
                cursor.execute("UPDATE worker_slot SET lease_until=DATE_SUB(NOW(6),INTERVAL 1 SECOND) WHERE run_id=%s", (claim.run_id,))
            claim = store.claim("replacement-yarn", lease_seconds=3600)
            self.assertEqual(claim.attempt_id, old.attempt_id)
            with self.assertRaises(LostLease):
                store.advance(old, "old-worker-returned", "VALIDATING")
            recovered = YarnExecutor(store, claim, reducers=2)
            recovered.run("extractUsers", raw["users"])
            self.assertEqual(len(recovered.applications(job["submission_id"])), 1)
            self.assertEqual(store.list_jobs(claim.attempt_id)[0]["status"], "SUCCEEDED")
            (fixture / "acceptance.json").write_text(json.dumps({"run_id": claim.run_id, "attempt_id": claim.attempt_id,
                                                               "candidate_report": report["candidate_report"]}), encoding="utf-8")
        finally:
            store.advance(claim, "integration-ended", "FAILED", {"reason": "synthetic compute acceptance; not published"})


if __name__ == "__main__":
    unittest.main()
