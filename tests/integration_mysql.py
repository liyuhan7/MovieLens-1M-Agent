"""Real MySQL invariants. Run separately from fast tests; never drops test history."""
import concurrent.futures
import unittest
import uuid

from metadata.connection import load_config, migrate
from metadata.store import Conflict, LostLease, Store


class MySQLRunTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = load_config()
        cls.config["database"] = "ml_governance_test"
        migrate(cls.config)
        cls.store = Store(cls.config)

    def setUp(self):
        self.suffix = uuid.uuid4().hex
        self.dataset = "test-" + self.suffix
        self.version = "input-" + self.suffix
        self.store.register_input(self.dataset, self.version, {"tables": {}, "fixture": self.suffix})
        self.store.register_definition("rule", "test-rules-v1", {"rules": []})
        self.store.register_definition("metric", "test-metrics-v1", {"formula": "fixture"})
        self.request = {"dataset_id": self.dataset, "input_version": self.version,
                        "rule_version": "test-rules-v1", "metric_version": "test-metrics-v1",
                        "execution_mode": "local", "parameters": {"fixture": True}}
        self.key = "test-" + self.suffix
        self.claims = []

    def tearDown(self):
        # Complete test-owned work only; do not erase database rows or other runs.
        for claim in reversed(self.claims):
            try:
                self.store.advance(claim, "test-ended", "FAILED", {"reason": "synthetic test completed"})
            except LostLease:
                pass

    def claimed(self, owner="test-worker"):
        claim = self.store.claim(owner)
        self.assertIsNotNone(claim)
        self.claims.append(claim)
        return claim

    def test_concurrent_duplicate_request_is_one_run(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: self.store.submit(self.request, self.key), range(8)))
        self.assertEqual(len({row["run_id"] for row, _ in results}), 1)
        self.assertEqual(sum(created for _, created in results), 1)
        self.claimed()
        changed = {**self.request, "parameters": {"fixture": False}}
        with self.assertRaises(Conflict):
            self.store.submit(changed, self.key)

    def test_new_key_creates_distinct_run_while_same_key_reuses_original(self):
        first, created = self.store.submit(self.request, self.key)
        self.assertTrue(created)
        duplicate, created = self.store.submit(self.request, self.key)
        self.assertFalse(created)
        self.assertEqual(duplicate["run_id"], first["run_id"])
        second, created = self.store.submit(self.request, self.key + "-explicit-new-run")
        self.assertTrue(created)
        self.assertNotEqual(first["run_id"], second["run_id"])
        self.assertEqual(first["request_hash"], second["request_hash"])
        initial = self.claimed()
        self.assertEqual(initial.run_id, first["run_id"])
        self.store.advance(initial, "new-key-first-test-ended", "FAILED",
                           {"reason": "synthetic request identity acceptance; no computation"})
        replacement = self.claimed()
        self.assertEqual(replacement.run_id, second["run_id"])
        self.assertNotEqual(initial.attempt_id, replacement.attempt_id)
        self.assertIsNone(self.store.get_publish(first["run_id"]))
        self.assertIsNone(self.store.get_publish(second["run_id"]))

    def test_new_store_recovers_queue_and_all_jobs(self):
        run, _ = self.store.submit(self.request, self.key)
        fresh_process_store = Store(self.config)
        self.assertEqual(fresh_process_store.get_run(run["run_id"])["status"], "QUEUED")
        claim = self.claimed()
        self.assertIsNone(fresh_process_store.claim("second-worker"))
        for stage in ("score-before", "clean", "score-after"):
            job = self.store.prepare_job(claim, stage, {"stage": stage})
            self.store.update_job(claim, job["submission_id"], "SUCCEEDED", job_id="job-" + stage)
        self.assertEqual(len(fresh_process_store.list_jobs(claim.attempt_id)), 3)
        self.assertEqual(fresh_process_store.list_attempts(run["run_id"])[0]["attempt_id"], claim.attempt_id)

    def test_expired_owner_is_fenced_and_recovery_preserves_attempt(self):
        run, _ = self.store.submit(self.request, self.key)
        old = self.claimed("old-worker")
        with self.store.transaction() as cursor:
            cursor.execute("UPDATE logical_run SET lease_until=DATE_SUB(NOW(6),INTERVAL 1 SECOND) WHERE run_id=%s", (run["run_id"],))
            cursor.execute("UPDATE worker_slot SET lease_until=DATE_SUB(NOW(6),INTERVAL 1 SECOND) WHERE run_id=%s", (run["run_id"],))
        new = self.claimed("replacement-worker")
        self.assertTrue(new.recovering)
        self.assertEqual(old.attempt_id, new.attempt_id)
        self.assertGreater(new.token, old.token)
        with self.assertRaises(LostLease):
            self.store.advance(old, "stale-complete", "VALIDATING")
        with self.assertRaises(LostLease):
            self.store.heartbeat(old)
        self.store.advance(new, "recovered", "RUNNING")

    def test_confirmed_retry_preserves_run_and_creates_new_attempt(self):
        run, _ = self.store.submit(self.request, self.key)
        first = self.claimed()
        self.store.advance(first, "failed-stage", "FAILED", {"stage": "fixture"})
        self.store.retry(run["run_id"])
        second = self.claimed()
        self.assertEqual(first.run_id, second.run_id)
        self.assertNotEqual(first.attempt_id, second.attempt_id)
        self.assertEqual(len(self.store.list_attempts(run["run_id"])), 2)

    def test_version_content_is_immutable(self):
        with self.assertRaises(Conflict):
            self.store.register_input(self.dataset, self.version, {"tables": {"changed": True}})
        with self.assertRaises(Conflict):
            self.store.register_definition("metric", "test-metrics-v1", {"formula": "changed"})

    def test_computation_success_cannot_bypass_publication_gate(self):
        self.store.submit(self.request, self.key)
        claim = self.claimed()
        job = self.store.prepare_job(claim, "fixture-compute", {"job": "fixture"})
        self.store.update_job(claim, job["submission_id"], "SUCCEEDED", job_id="job-fixture")
        self.store.advance(claim, "awaiting-validation", "VALIDATING")
        self.store.advance(claim, "publication-not-complete", "PUBLISHING")
        with self.assertRaises(ValueError):
            self.store.advance(claim, "done", "PUBLISHED")
        self.assertEqual(self.store.get_run(claim.run_id)["status"], "PUBLISHING")


if __name__ == "__main__":
    unittest.main()
