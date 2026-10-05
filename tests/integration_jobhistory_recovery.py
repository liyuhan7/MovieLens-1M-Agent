"""Recover real completed jobs with no live RM record and a lost saved ID.

The RM URL alone is pointed at an absent endpoint to simulate expiry. JobHistory,
MySQL, the original committed HDFS output and its cache are real. No job is run.
"""
import json
import unittest
import uuid
from unittest.mock import patch

from metadata.connection import ROOT, load_config
from metadata.store import Store
from test_yarn_history import yarn


class JobHistoryRecoveryTests(unittest.TestCase):
    def test_history_recovers_real_submission_without_duplicate_compute(self):
        config = load_config()
        self.assertEqual(config["database"], "ml_governance_test")
        store = Store(config)
        run_id = "run-3c3f17ecde754df8b5cbc59fc3c571a9"
        run = store.get_run(run_id)
        self.assertEqual(run["status"], "FAILED")
        self.assertIsNone(store.get_publish(run_id))
        attempt = store.list_attempts(run_id)[-1]
        before = store.list_jobs(attempt["attempt_id"])
        self.assertEqual(len(before), 12)
        jobs = {stage: next(row for row in before if row["stage"] == stage)
                for stage in ("extractUsers", "extractMovies")}
        self.assertTrue(all(row["status"] == "SUCCEEDED" for row in before))
        fixture = ROOT / "outputs" / "jobhistory-recovery-integration" / uuid.uuid4().hex
        fixture.mkdir(parents=True)
        (fixture / "before.json").write_text(json.dumps({"run_id": run_id, "jobs": before},
                                                       default=str, indent=2), encoding="utf-8")
        with store.transaction() as cursor:
            cursor.execute("SELECT lease_until>NOW(6) AS alive FROM worker_slot WHERE slot_id=1 FOR UPDATE")
            self.assertFalse(cursor.fetchone()["alive"], "No concurrent test worker allowed")
            cursor.execute("UPDATE logical_run SET status='RUNNING',lease_until=DATE_SUB(NOW(6),INTERVAL 1 SECOND) WHERE run_id=%s",
                           (run_id,))
            cursor.execute("UPDATE worker_slot SET run_id=%s,lease_owner=%s,lease_until=DATE_SUB(NOW(6),INTERVAL 1 SECOND) WHERE slot_id=1",
                           (run_id, run["lease_owner"]))
            cursor.execute("UPDATE hadoop_job SET status='SUBMITTING',job_id=NULL,application_id=NULL WHERE submission_id=%s",
                           (jobs["extractUsers"]["submission_id"],))
            cursor.execute("UPDATE hadoop_job SET status='RUNNING' WHERE submission_id=%s",
                           (jobs["extractMovies"]["submission_id"],))
        claim = store.claim("history-recovery-acceptance", lease_seconds=3600)
        self.assertEqual((claim.run_id, claim.attempt_id), (run_id, attempt["attempt_id"]))
        try:
            executor = yarn.YarnExecutor(store, claim, reducers=2)
            executor.rm_url += "/acceptance-absent-live-record"
            actual_popen = yarn.subprocess.Popen
            def storage_only(arguments, *args, **kwargs):
                if arguments[:2] != ["hdfs", "dfs"]:
                    raise AssertionError("Recovery must not submit compute")
                return actual_popen(arguments, *args, **kwargs)
            with patch.object(yarn.subprocess, "Popen", side_effect=storage_only):
                for stage, table in (("extractUsers", "users"), ("extractMovies", "movies")):
                    output = executor.run(stage, ROOT / run["request"]["raw_paths"][table])
                    self.assertTrue((output / "_SUCCESS").is_file())
            after = {row["stage"]: row for row in store.list_jobs(claim.attempt_id)}
            self.assertEqual(len(after), len(before))
            for stage, original in jobs.items():
                recovered = after[stage]
                self.assertEqual((recovered["job_id"], recovered["application_id"], recovered["status"]),
                                 (original["job_id"], original["application_id"], "SUCCEEDED"))
                self.assertEqual(recovered["detail"]["history_source"], "JobHistory")
            self.assertIsNone(store.get_publish(run_id))
            (fixture / "acceptance.json").write_text(json.dumps({
                "run_id": run_id, "attempt_id": claim.attempt_id, "token": claim.token,
                "real_jobhistory": True, "real_hdfs_commit_and_cache_checked": True,
                "simulated_rm_expiry": True, "new_job_submission_forbidden": True,
                "lost_id_recovered": after["extractUsers"]["job_id"],
                "saved_id_recovered": after["extractMovies"]["job_id"],
                "durable_jobs_before": len(before), "durable_jobs_after": len(after),
                "history": {stage: after[stage]["detail"] for stage in jobs}}, indent=2), encoding="utf-8")
            print(json.dumps({"acceptance": (fixture / "acceptance.json").relative_to(ROOT).as_posix()}), flush=True)
        finally:
            store.advance(claim, "history-recovery-test-ended", "FAILED",
                          {"reason": "synthetic history recovery acceptance; not published"})
            # Restore only fault-injected fixture job metadata. The recovery
            # event and independently saved acceptance remain as evidence.
            with store.transaction() as cursor:
                for original in jobs.values():
                    cursor.execute("UPDATE hadoop_job SET status=%s,job_id=%s,application_id=%s WHERE submission_id=%s AND attempt_id=%s",
                                   (original["status"], original["job_id"], original["application_id"],
                                    original["submission_id"], claim.attempt_id))


if __name__ == "__main__":
    unittest.main()
