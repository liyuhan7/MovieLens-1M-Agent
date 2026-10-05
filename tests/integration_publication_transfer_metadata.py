"""Real MySQL replacement invariants; synthetic manifests test metadata, not full publication."""
import concurrent.futures
import importlib.util
import os
import sys
import unittest
import uuid

from metadata.connection import ROOT, load_config, migrate

candidate = os.environ.get("ML_STORE_CANDIDATE")
if candidate:
    spec = importlib.util.spec_from_file_location("transfer_store_candidate", ROOT / candidate)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
else:
    import metadata.store as module
Store, Conflict, LostLease, fingerprint = module.Store, module.Conflict, module.LostLease, module.fingerprint


class ReplacementTests(unittest.TestCase):
    def setUp(self):
        config = load_config()
        self.assertIn(config["database"], {"ml_governance_test", "ml_governance_transfer_test"})
        migrate(config)
        self.store = Store(config)
        with self.store.transaction() as cursor:
            cursor.execute("SELECT lease_until>NOW(6) AS alive FROM worker_slot WHERE slot_id=1")
            self.assertFalse(cursor.fetchone()["alive"], "Test database is occupied; use the dedicated transfer test database")
        ident = uuid.uuid4().hex
        self.store.register_input("transfer-" + ident, "transfer-input-" + ident, {"tables": {}})
        self.store.register_definition("rule", "transfer-rules-v1", {})
        self.store.register_definition("metric", "transfer-metrics-v1", {})
        request = {"dataset_id": "transfer-" + ident, "input_version": "transfer-input-" + ident,
                   "rule_version": "transfer-rules-v1", "metric_version": "transfer-metrics-v1"}
        self.run, _ = self.store.submit(request, "transfer-" + ident)
        self.claim = self.store.claim("transfer-test", lease_seconds=3600)
        self.assertEqual(self.claim.run_id, self.run["run_id"])
        self.claims = [self.claim]
        self.store.advance(self.claim, "metadata-fixture", "VALIDATING")
        self.manifest = {key: self.run[key] for key in ("run_id", "input_version", "rule_version", "metric_version")}
        self.manifest.update(attempt_id=self.claim.attempt_id, files=[])
        self.intent = self.store.prepare_publication(self.claim, self.manifest, fingerprint(self.manifest))
        self.intent = self.store.get_publish(self.run["run_id"])

    def tearDown(self):
        for claim in reversed(self.claims):
            try:
                self.store.advance(claim, "metadata-test-ended", "FAILED")
            except LostLease:
                pass
        # Only test-owned queued work; leave intent/events and all history intact.
        with self.store.transaction() as cursor:
            cursor.execute("UPDATE physical_attempt SET status='FAILED',stage='metadata-test-ended',ended_at=NOW(6) "
                           "WHERE run_id=%s AND status!='PUBLISHED'", (self.run["run_id"],))
            cursor.execute("UPDATE logical_run SET status='FAILED',stage='metadata-test-ended',lease_owner=NULL,lease_until=NULL "
                           "WHERE run_id=%s AND status!='PUBLISHED'",
                           (self.run["run_id"],))
            cursor.execute("UPDATE worker_slot SET run_id=NULL,lease_owner=NULL,lease_until=NULL WHERE run_id=%s", (self.run["run_id"],))

    def expire(self):
        with self.store.transaction() as cursor:
            for table in ("logical_run", "worker_slot"):
                cursor.execute(f"UPDATE {table} SET lease_until=DATE_SUB(NOW(6),INTERVAL 1 SECOND) WHERE run_id=%s", (self.run["run_id"],))

    def replace(self, **changes):
        args = dict(expected_publish=self.intent["publish_id"], expected_attempt=self.claim.attempt_id,
                    expected_manifest_hash=self.intent["manifest_hash"], expected_token=self.claim.token,
                    reason="test-owned candidate cannot be completed")
        args.update(changes)
        return self.store.request_publication_replacement(self.run["run_id"], **args)

    def next_claim(self):
        claim = self.store.claim("transfer-next", lease_seconds=3600)
        self.claims.append(claim)
        self.assertEqual(claim.run_id, self.run["run_id"])
        return claim

    def test_live_executor_and_unknown_job_block_replacement(self):
        with self.assertRaises(Conflict):
            self.replace()
        job = self.store.prepare_job(self.claim, "unknown-fixture", {})
        self.expire()
        with self.assertRaises(Conflict):
            self.replace()
        self.assertEqual(self.store.get_publish(self.run["run_id"]), self.intent)

    def test_stale_expectations_and_duplicate_requests_do_not_queue_twice(self):
        self.expire()
        for changes in ({"expected_token": 0}, {"expected_manifest_hash": "0" * 64},
                        {"expected_attempt": "foreign"}, {"expected_publish": "foreign"}):
            with self.assertRaises(Conflict):
                self.replace(**changes)
        self.replace()
        with self.assertRaises(Conflict):
            self.replace()
        new = self.next_claim()
        self.assertNotEqual(new.attempt_id, self.claim.attempt_id)
        self.assertEqual(len(self.store.list_attempts(self.run["run_id"])), 2)
        for action in (lambda: self.store.heartbeat(self.claim), lambda: self.store.prepare_publication(self.claim, self.manifest, fingerprint(self.manifest)),
                       lambda: self.store.confirm_publication(self.claim, self.intent["publish_id"], {})):
            with self.assertRaises(LostLease):
                action()

    def test_same_publish_new_generation_audit_and_concurrent_prepare(self):
        self.expire()
        self.replace()
        self.assertEqual(self.store.get_publish(self.run["run_id"]), self.intent)
        new = self.next_claim()
        manifest = {**self.manifest, "attempt_id": new.attempt_id}
        with self.assertRaises(Conflict):
            self.store.prepare_publication(new, manifest, fingerprint(manifest))
        self.store.advance(new, "new-candidate-validated", "VALIDATING")
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: self.store.prepare_publication(new, manifest, fingerprint(manifest)), range(2)))
        for result in results:
            self.assertEqual(result["publish_id"], self.intent["publish_id"])
            self.assertEqual(result["output_version"], self.intent["output_version"])
            self.assertEqual(result["attempt_id"], new.attempt_id)
            self.assertNotEqual(result["storage_path"], self.intent["storage_path"])
        refs = self.store.storage_references()["publications"]
        self.assertIn(self.intent["storage_path"], {row["storage_path"] for row in refs})
        with self.store.transaction() as cursor:
            cursor.execute("SELECT COUNT(*) AS n FROM publish_version WHERE run_id=%s", (self.run["run_id"],))
            self.assertEqual(cursor.fetchone()["n"], 1)
            cursor.execute("SELECT COUNT(*) AS n FROM run_event WHERE run_id=%s AND event_type='PUBLICATION_QUALIFICATION_TRANSFERRED'", (self.run["run_id"],))
            self.assertEqual(cursor.fetchone()["n"], 1)

    def test_failed_replacement_can_be_explicitly_replaced_again(self):
        self.expire()
        self.replace()
        new = self.next_claim()
        self.store.advance(new, "fixture-failed", "FAILED")
        self.replace(expected_attempt=new.attempt_id, expected_token=new.token)
        third = self.next_claim()
        self.store.advance(third, "third-validated", "VALIDATING")
        manifest = {**self.manifest, "attempt_id": third.attempt_id}
        intent = self.store.prepare_publication(third, manifest, fingerprint(manifest))
        self.assertEqual(intent["publish_id"], self.intent["publish_id"])
        self.assertEqual(intent["attempt_id"], third.attempt_id)

    def test_confirmed_publication_cannot_be_replaced(self):
        # Exercise the real metadata confirm transaction with synthetic summary
        # rows. This proves the terminal guard, not the content/HDFS gate.
        rows = [{"attempt_id": self.claim.attempt_id, "phase": phase, "source_table": table,
                 "metric": str(metric), "metric_version": self.run["metric_version"],
                 "numerator": 0, "denominator": 0, "score": None, "detail": {}}
                for phase in ("before", "after") for table in ("users", "movies", "ratings") for metric in range(5)]
        self.store.record_quality_batch(self.claim, rows)
        verification = {"manifest.json": {"sha256": self.intent["manifest_hash"],
                        "bytes": len(module.canonical(self.manifest).encode("utf-8"))}}
        self.store.confirm_publication(self.claim, self.intent["publish_id"], verification)
        with self.assertRaises(Conflict):
            self.replace()
        self.assertEqual(self.store.get_publish(self.run["run_id"])["status"], "PUBLISHED")

    def test_concurrent_replacement_requests_have_one_winner(self):
        self.expire()
        def request():
            try:
                self.replace()
                return "queued"
            except Conflict:
                return "conflict"
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            self.assertCountEqual(list(pool.map(lambda _: request(), range(2))), ["queued", "conflict"])
        new = self.next_claim()
        self.assertGreater(new.token, self.claim.token)
        with self.store.transaction() as cursor:
            cursor.execute("SELECT COUNT(*) AS n FROM run_event WHERE run_id=%s AND event_type='PUBLICATION_REPLACEMENT_REQUESTED'", (self.run["run_id"],))
            self.assertEqual(cursor.fetchone()["n"], 1)

    def test_live_request_does_not_wait_on_heartbeat_run_lock(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            with self.store.transaction() as cursor:
                cursor.execute("SELECT run_id FROM logical_run WHERE run_id=%s FOR UPDATE", (self.run["run_id"],))
                cursor.fetchone()
                pending = pool.submit(self.replace)
                with self.assertRaises(Conflict):
                    pending.result(timeout=3)


if __name__ == "__main__":
    unittest.main()
