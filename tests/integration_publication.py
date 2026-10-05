"""Real 12-job computation, HDFS materialization and MySQL publication fault windows."""
import concurrent.futures
import dataclasses
import json
import unittest
import uuid

from governance.artifacts import build_artifacts
from governance.publication import publish_artifacts
from governance.service import submit_run
from metadata.connection import ROOT, load_config
from metadata.store import Conflict, LostLease, Store
from pipeline.run_pipeline import input_fingerprint, pipeline
from pipeline.yarn import YarnExecutor
from storage.hdfs import Hdfs, import_input


class InjectedCrash(RuntimeError):
    pass


class PublicationTests(unittest.TestCase):
    def test_real_publication_recovers_each_commit_window(self):
        config = load_config()
        self.assertEqual(config["database"], "ml_governance_test")
        store = Store(config)
        fixture = ROOT / "outputs" / "publication-integration" / uuid.uuid4().hex
        fixture.mkdir(parents=True)
        raw = {table: fixture / (table + ".dat") for table in ("users", "movies", "ratings")}
        raw["users"].write_bytes(b"1::M::25::4::01234\n1::M::25::4::01234\n2::NULL::18::3::12345\n")
        raw["movies"].write_bytes("1::Léon (1994)::Action|Crime|Drama\n2::Toy Story (1995)::Animation|Children's|Comedy\n3::Léon (1994)::Action\n".encode("latin-1"))
        raw["ratings"].write_bytes(b"1::1::4::978000000\n1::1::5::978000001\n99::2::3::978000002\n")
        manifest = input_fingerprint(raw)
        store.register_input("movielens-1m", manifest["dataset_version"], manifest)
        import_input(store, manifest["dataset_version"], {table: path.relative_to(ROOT).as_posix() for table, path in raw.items()})
        key = "publication-" + uuid.uuid4().hex
        run, _ = submit_run(key, store=store, raw=raw)
        claim = store.claim("publication-first", lease_seconds=3600)
        self.assertEqual(claim.run_id, run["run_id"])
        (fixture / "run.json").write_text(json.dumps({"run_id": claim.run_id, "attempt_id": claim.attempt_id}), encoding="utf-8")
        try:
            report = pipeline(run_id=claim.run_id, attempt_id=claim.attempt_id, raw=raw,
                              executor=YarnExecutor(store, claim, reducers=2), register=False)
            store.advance(claim, "publication-validation", "VALIDATING")
            root, output_manifest = build_artifacts(report, {table: str(path) for table, path in raw.items()})

            def crash_at(target):
                def crash(stage, intent):
                    if stage == target:
                        raise InjectedCrash(target)
                return crash

            with self.assertRaisesRegex(InjectedCrash, "intent"):
                publish_artifacts(store, claim, root, checkpoint=crash_at("intent"))
            intent = store.get_publish(claim.run_id)
            self.assertEqual(intent["status"], "PREPARING")
            self.assertIsNone(store.get_input(intent["output_version"]))
            old = claim
            claim = self.replace_owner(store, claim, "publication-second")
            with self.assertRaises(LostLease):
                store.prepare_publication(old, output_manifest, intent["manifest_hash"])
            foreign = dataclasses.replace(claim, attempt_id="attempt-" + uuid.uuid4().hex)
            with self.assertRaises(LostLease):
                store.prepare_publication(foreign, output_manifest, intent["manifest_hash"])
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                intents = list(pool.map(lambda _: store.prepare_publication(claim, output_manifest, intent["manifest_hash"]), range(2)))
            self.assertEqual({row["publish_id"] for row in intents}, {intent["publish_id"]})
            with self.assertRaisesRegex(InjectedCrash, "materialized"):
                publish_artifacts(store, claim, root, checkpoint=crash_at("materialized"))
            self.assertTrue(Hdfs().exists(intent["storage_path"] + "/manifest.json"))
            self.assertEqual(store.get_publish(claim.run_id)["status"], "PREPARING")
            self.assertIsNone(store.get_input(intent["output_version"]))
            with self.assertRaises(LostLease):
                store.confirm_publication(old, intent["publish_id"], {})
            claim = self.replace_owner(store, claim, "publication-third")
            with self.assertRaisesRegex(InjectedCrash, "confirmed"):
                publish_artifacts(store, claim, root, checkpoint=crash_at("confirmed"))
            self.assertEqual(store.get_run(claim.run_id)["status"], "PUBLISHED")
            self.assertEqual(store.get_input(intent["output_version"])["status"], "PUBLISHED")
            store.reconcile_current()
            again = publish_artifacts(store, claim, root)
            self.assertEqual(again["publish_id"], intent["publish_id"])
            same, created = submit_run(key, store=store, raw=raw)
            self.assertFalse(created)
            self.assertEqual(same["run_id"], claim.run_id)
            with self.assertRaises(Conflict):
                store.retry(claim.run_id)
            with store.transaction() as cursor:
                cursor.execute("SELECT COUNT(*) AS n FROM publish_version WHERE run_id=%s", (claim.run_id,))
                self.assertEqual(cursor.fetchone()["n"], 1)
                cursor.execute("SELECT * FROM dataset_current WHERE dataset_id=%s", (run["dataset_id"],))
                current = cursor.fetchone()
                self.assertEqual(current["publish_id"], intent["publish_id"])
            (fixture / "acceptance.json").write_text(json.dumps({"run_id": claim.run_id, "attempt_id": claim.attempt_id,
                    "publish_id": intent["publish_id"], "storage_path": intent["storage_path"],
                    "manifest_hash": intent["manifest_hash"], "jobs": 12, "publication_verified": True,
                    "fault_windows": ["intent", "materialized", "confirmed"], "current": current}, default=str, indent=2), encoding="utf-8")
        except Exception:
            # Preserve an interrupted intent for reconciliation; never delete formal data.
            state = store.get_run(claim.run_id)
            if state["status"] != "PUBLISHED" and not store.get_publish(claim.run_id):
                store.advance(claim, "publication-test-failed", "FAILED", {"reason": "publication acceptance failed"})
            raise

    def replace_owner(self, store, claim, owner):
        with store.transaction() as cursor:
            cursor.execute("UPDATE logical_run SET lease_until=DATE_SUB(NOW(6),INTERVAL 1 SECOND) WHERE run_id=%s", (claim.run_id,))
            cursor.execute("UPDATE worker_slot SET lease_until=DATE_SUB(NOW(6),INTERVAL 1 SECOND) WHERE run_id=%s", (claim.run_id,))
        replacement = store.claim(owner, lease_seconds=3600)
        self.assertEqual(replacement.attempt_id, claim.attempt_id)
        self.assertGreater(replacement.token, claim.token)
        return replacement


if __name__ == "__main__":
    unittest.main()
