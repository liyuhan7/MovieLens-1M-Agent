"""Real standalone raw scoring without any previous Attempt output."""
import json
import unittest
import uuid

from governance.service import submit_run
from metadata.connection import ROOT, load_config
from metadata.store import Store
from pipeline.run_pipeline import input_fingerprint, pipeline
from pipeline.yarn import YarnExecutor
from storage.hdfs import import_input


class StandaloneScoringTests(unittest.TestCase):
    def test_fresh_attempt_prepares_its_own_references(self):
        config = load_config()
        self.assertEqual(config["database"], "ml_governance_test")
        store = Store(config)
        fixture = ROOT / "outputs" / "raw-scoring-integration" / uuid.uuid4().hex
        fixture.mkdir(parents=True)
        raw = {table: fixture / (table + ".dat") for table in ("users", "movies", "ratings")}
        raw["users"].write_bytes(b"701::M::25::4::01234\n702::F::18::3::12345\n")
        raw["movies"].write_bytes(b"801::First (1994)::Action\n802::Second (1995)::Comedy\n")
        raw["ratings"].write_bytes(b"701::801::4::978000000\n702::802::5::978000001\n999::801::3::978000002\n")
        manifest = input_fingerprint(raw)
        store.register_input("movielens-1m", manifest["dataset_version"], manifest)
        import_input(store, manifest["dataset_version"], {table: path.relative_to(ROOT).as_posix() for table, path in raw.items()})
        run, _ = submit_run("raw-score-" + uuid.uuid4().hex, store=store, raw=raw)
        claim = store.claim("raw-score-acceptance", lease_seconds=3600)
        self.assertEqual(claim.run_id, run["run_id"])
        work = ROOT / claim.work_path
        self.assertFalse(work.exists())
        try:
            report = pipeline(only="score-raw", run_id=claim.run_id, attempt_id=claim.attempt_id,
                              raw=raw, executor=YarnExecutor(store, claim, reducers=2), register=False)
            jobs = store.list_jobs(claim.attempt_id)
            self.assertEqual({job["stage"] for job in jobs},
                             {"extractUsers", "extractMovies", "score-raw-users", "score-raw-movies", "score-raw-ratings"})
            self.assertEqual(len(jobs), 5)
            self.assertTrue(all(job["status"] == "SUCCEEDED" and job["application_id"] and job["job_id"] for job in jobs))
            self.assertCountEqual((work / "ids_users_raw.txt").read_text().splitlines(), ["701", "702"])
            self.assertCountEqual((work / "ids_movies_raw.txt").read_text().splitlines(), ["801", "802"])
            self.assertEqual({table: metrics["N"] for table, metrics in report["raw_metrics"].items()},
                             {"users": 2, "movies": 2, "ratings": 3})
            self.assertEqual(report["raw_metrics"]["ratings"]["ACC"], 2)
            self.assertIsNone(store.get_publish(claim.run_id))
            self.assertFalse((work / "report.json").exists())
            (fixture / "acceptance.json").write_text(json.dumps({
                "run_id": claim.run_id, "attempt_id": claim.attempt_id,
                "fresh_work_directory": True, "raw_metrics": report["raw_metrics"],
                "jobs": [{key: job[key] for key in ("stage", "job_id", "application_id", "status")} for job in jobs],
                "formal_publication": False}, indent=2), encoding="utf-8")
        finally:
            store.advance(claim, "raw-score-acceptance-ended", "FAILED",
                          {"reason": "standalone scoring acceptance; not a complete publication run"})


if __name__ == "__main__":
    unittest.main()
