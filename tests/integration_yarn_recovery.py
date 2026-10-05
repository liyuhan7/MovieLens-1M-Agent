"""Fault replay against a completed synthetic YARN run; never touches published data."""
import unittest

from metadata.connection import ROOT, load_config
from metadata.store import Claim, LostLease, Store
from pipeline.yarn import YarnExecutor


class YarnRecoveryTests(unittest.TestCase):
    def test_saved_identity_gap_recovers_existing_application(self):
        config = load_config()
        self.assertEqual(config["database"], "ml_governance_test")
        store = Store(config)
        with store.transaction() as cursor:
            cursor.execute("SELECT run_id FROM logical_run WHERE status='FAILED' "
                           "AND JSON_UNQUOTE(JSON_EXTRACT(error,'$.reason'))=%s ORDER BY request_seq DESC LIMIT 1",
                           ("synthetic compute acceptance; not published",))
            row = cursor.fetchone()
            self.assertIsNotNone(row, "Run integration_yarn.py successfully first")
            cursor.execute("SELECT publish_id FROM publish_version WHERE run_id=%s", (row["run_id"],))
            self.assertIsNone(cursor.fetchone(), "Never replay a run with a publication intent")
            cursor.execute("SELECT run_id,lease_until>NOW(6) AS alive FROM worker_slot WHERE slot_id=1 FOR UPDATE")
            self.assertFalse(cursor.fetchone()["alive"], "No concurrent test worker allowed")
        run = store.get_run(row["run_id"])
        attempts = store.list_attempts(run["run_id"])
        attempt = attempts[-1]
        jobs = store.list_jobs(attempt["attempt_id"])
        self.assertEqual(len(jobs), 12)
        job = next(item for item in jobs if item["stage"] == "extractUsers")
        original_application = job["application_id"]
        old = Claim(run["run_id"], attempt["attempt_id"], run["fencing_token"], run["lease_owner"],
                    False, attempt["work_path"], "RUNNING")
        # Only the clearly marked synthetic, unpublished test fixture is replayed.
        with store.transaction() as cursor:
            cursor.execute("UPDATE logical_run SET status='RUNNING',lease_until=DATE_SUB(NOW(6),INTERVAL 1 SECOND) WHERE run_id=%s",
                           (run["run_id"],))
            cursor.execute("UPDATE worker_slot SET run_id=%s,lease_owner=%s,lease_until=DATE_SUB(NOW(6),INTERVAL 1 SECOND) WHERE slot_id=1",
                           (run["run_id"], old.owner))
            cursor.execute("UPDATE hadoop_job SET status='SUBMITTING',job_id=NULL,application_id=NULL WHERE submission_id=%s",
                           (job["submission_id"],))
        claim = store.claim("fault-replay-yarn", lease_seconds=3600)
        try:
            self.assertTrue(claim.recovering)
            self.assertEqual(claim.attempt_id, old.attempt_id)
            with self.assertRaises(LostLease):
                store.advance(old, "stale-executor", "VALIDATING")
            executor = YarnExecutor(store, claim, reducers=2)
            output = executor.run("extractUsers", ROOT / run["request"]["raw_paths"]["users"])
            self.assertTrue((output / "_SUCCESS").is_file())
            applications = executor.applications(job["submission_id"])
            self.assertEqual([app["id"] for app in applications], [original_application])
            recovered = next(item for item in store.list_jobs(claim.attempt_id) if item["stage"] == "extractUsers")
            self.assertEqual(recovered["application_id"], original_application)
            self.assertEqual(recovered["job_id"], original_application.replace("application_", "job_"))
            self.assertEqual(recovered["status"], "SUCCEEDED")
        finally:
            store.advance(claim, "fault-replay-ended", "FAILED", {"reason": "synthetic fault replay completed"})


if __name__ == "__main__":
    unittest.main()
