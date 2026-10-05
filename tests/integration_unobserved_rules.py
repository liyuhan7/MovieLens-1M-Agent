"""Real YARN checks for implemented actions absent from the full input.

The malformed title-stage input checks a defensive stage guard only; it is not
claimed to pass the normal preceding movie stage or become a formal output.
"""
import json
import unittest
import uuid

from governance.service import submit_run
from metadata.connection import ROOT, load_config
from metadata.store import Store
from pipeline.run_pipeline import input_fingerprint, dispositions
from pipeline.yarn import YarnExecutor
from storage.hdfs import import_input


class UnobservedRuleTests(unittest.TestCase):
    def test_whitespace_empty_legal_genres_and_malformed_title_guard(self):
        config = load_config()
        self.assertEqual(config["database"], "ml_governance_test")
        store = Store(config)
        fixture = ROOT / "outputs/unobserved-rule-integration" / uuid.uuid4().hex
        fixture.mkdir(parents=True)
        raw = {table: fixture / (table + ".dat") for table in ("users", "movies", "ratings")}
        raw["users"].write_bytes(b" 1 :: M :: 25 :: 4 :: 01234 \n")
        raw["movies"].write_bytes(b"1::Unknown (1994)::UnlistedGenre\n2::Missing (1995)::NULL\n")
        raw["ratings"].write_bytes(b"1::1::4::978000000\n")
        manifest = input_fingerprint(raw)
        store.register_input("movielens-1m", manifest["dataset_version"], manifest)
        import_input(store, manifest["dataset_version"], {table: path.relative_to(ROOT).as_posix() for table, path in raw.items()})
        run, _ = submit_run("unobserved-rules-" + uuid.uuid4().hex, store=store, raw=raw)
        claim = store.claim("unobserved-rule-acceptance", lease_seconds=3600)
        self.assertEqual(claim.run_id, run["run_id"])
        try:
            executor = YarnExecutor(store, claim, reducers=2)
            users = list(dispositions(executor.run("cleanUsers", raw["users"])))
            self.assertEqual(len(users), 1)
            self.assertEqual(users[0]["payload"], "1::M::25::4::01234")
            self.assertTrue(any(event["rule"] == "U0" and event["action"] == "repair"
                                for event in users[0]["origin"]["events"]))
            movies = list(dispositions(executor.run("cleanMovies", raw["movies"])))
            self.assertEqual({row["payload"] for row in movies},
                             {"1::Unknown (1994)::NULL", "2::Missing (1995)::NULL"})
            events = [event for row in movies for event in row["origin"]["events"]]
            # A preexisting NULL is retained; a wholly illegal vocabulary is logged.
            self.assertEqual(sum(event["rule"] == "M4" and event["action"] == "log" for event in events), 1)
            malformed = fixture / "malformed-title-stage.dat"
            malformed.write_bytes(b"1::OnlyTwoFields\n")
            title = list(dispositions(executor.run("cleanMoviesTitle", malformed)))
            self.assertEqual(len(title), 1)
            self.assertEqual((title[0]["action"], title[0]["rule"], title[0]["payload"]),
                             ("isolate", "M5b-invalid", "1::OnlyTwoFields"))
            self.assertIsNone(store.get_publish(claim.run_id))
            jobs = store.list_jobs(claim.attempt_id)
            self.assertEqual(len(jobs), 3)
            self.assertTrue(all(job["status"] == "SUCCEEDED" for job in jobs))
            record = {"run_id": claim.run_id, "attempt_id": claim.attempt_id,
                      "covered_pairs": ["U0/repair", "M4/log", "M5b-invalid/isolate"],
                      "r0_repair_prior_evidence": "integration_metric_boundaries.py",
                      "title_guard_scope": "defensive malformed intermediate input; not normal end-to-end pipeline",
                      "formal_publication": False,
                      "jobs": [{key: job[key] for key in ("stage", "job_id", "application_id", "status")} for job in jobs]}
            (fixture / "acceptance.json").write_text(json.dumps(record, indent=2), encoding="utf-8")
            print(json.dumps({"acceptance": str(fixture / "acceptance.json"), **record}), flush=True)
        finally:
            store.advance(claim, "unobserved-rule-acceptance-ended", "FAILED",
                          {"reason": "three-stage boundary acceptance; not a complete publication run"})


if __name__ == "__main__":
    unittest.main()
