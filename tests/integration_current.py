"""Real MySQL promotion/CAS tests using explicitly synthetic metadata fixtures.

No compute/storage/publication acceptance is claimed for these fixture rows.
"""
import concurrent.futures
import json
import unittest
import uuid

from metadata.connection import ROOT, load_config, migrate
from metadata.store import Conflict, Store, canonical, fingerprint


class CurrentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = load_config()
        if config["database"] != "ml_governance_test":
            raise RuntimeError("Current tests require the dedicated test database")
        migrate(config)
        cls.store = Store(config)

    def setUp(self):
        self.dataset = "current-fixture-" + uuid.uuid4().hex
        self.input = "input-" + uuid.uuid4().hex
        self.store.register_input(self.dataset, self.input, {"fixture_only": True, "tables": {}})
        self.store.register_definition("rule", "current-fixture-rules-v1", {"fixture_only": True})
        self.store.register_definition("metric", "current-fixture-metrics-v1", {"fixture_only": True})
        self.published = []

    def fixture(self):
        request = {"dataset_id": self.dataset, "input_version": self.input, "rule_version": "current-fixture-rules-v1",
                   "metric_version": "current-fixture-metrics-v1", "execution_mode": "local", "parameters": {"fixture_only": True}}
        run, _ = self.store.submit(request, "current-" + uuid.uuid4().hex)
        attempt = "attempt-" + uuid.uuid4().hex
        publish = "publish-" + uuid.uuid4().hex
        output = "current-fixture-output-" + uuid.uuid4().hex
        manifest = {"schema_version": "current-metadata-fixture", "fixture_only": True, "run_id": run["run_id"]}
        with self.store.transaction() as cursor:
            cursor.execute("INSERT INTO physical_attempt (attempt_id,run_id,attempt_no,fencing_token,status,stage,work_path,environment) "
                           "VALUES (%s,%s,1,1,'PUBLISHED','metadata-fixture',%s,%s)",
                           (attempt, run["run_id"], "outputs/current-fixtures/" + run["run_id"], canonical({"fixture_only": True})))
            cursor.execute("INSERT INTO publish_version (publish_id,run_id,attempt_id,output_version,fencing_token,status,manifest,manifest_hash,storage_path,report_path) "
                           "VALUES (%s,%s,%s,%s,1,'PUBLISHED',%s,%s,%s,%s)",
                           (publish, run["run_id"], attempt, output, canonical(manifest), fingerprint(manifest),
                            "metadata-fixture://" + publish, "metadata-fixture://" + publish + "/report"))
            cursor.execute("UPDATE logical_run SET status='PUBLISHED',stage='metadata-fixture',active_attempt=%s,fencing_token=1 WHERE run_id=%s", (attempt, run["run_id"]))
        self.published.append(publish)
        return publish

    def current(self):
        with self.store.transaction() as cursor:
            cursor.execute("SELECT * FROM dataset_current WHERE dataset_id=%s", (self.dataset,))
            return cursor.fetchone()

    def test_old_late_completion_cannot_replace_new_current(self):
        old, new = self.fixture(), self.fixture()
        self.assertTrue(self.store.promote_current(new, expected_publish=None))
        self.assertFalse(self.store.promote_current(old))
        self.assertEqual(self.current()["publish_id"], new)
        self.assertEqual(self.current()["revision"], 1)

    def test_concurrent_promotions_choose_newest_request(self):
        published = [self.fixture() for _ in range(6)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(self.store.promote_current, reversed(published)))
        self.assertEqual(self.current()["publish_id"], published[-1])
        revision = self.current()["revision"]
        self.assertTrue(self.store.promote_current(published[-1]))
        self.assertEqual(self.current()["revision"], revision)

    def test_expected_version_conflict_rejected_without_update(self):
        old, new = self.fixture(), self.fixture()
        self.store.promote_current(old)
        with self.assertRaises(Conflict):
            self.store.promote_current(new, expected_publish=None)
        self.assertEqual(self.current()["publish_id"], old)
        self.store.promote_current(new, expected_publish=old)
        with self.assertRaises(Conflict):
            self.store.promote_current(old, expected_publish=old)
        self.assertEqual(self.current()["publish_id"], new)

    def test_reconciliation_promotes_confirmed_newer_request(self):
        old, new = self.fixture(), self.fixture()
        self.store.promote_current(old)
        self.store.reconcile_current()
        self.assertEqual(self.current()["publish_id"], new)
        with self.store.transaction() as cursor:
            cursor.execute("SELECT event_type FROM run_event WHERE run_id=(SELECT run_id FROM publish_version WHERE publish_id=%s)", (new,))
            self.assertIn("CURRENT_PROMOTED", {row["event_type"] for row in cursor.fetchall()})

    def tearDown(self):
        path = ROOT / "outputs" / "current-fixtures"
        path.mkdir(parents=True, exist_ok=True)
        (path / (self.dataset + ".json")).write_text(json.dumps({"dataset_id": self.dataset, "fixture_only": True,
                "publish_ids": self.published, "current": self.current()}, default=str, indent=2), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
