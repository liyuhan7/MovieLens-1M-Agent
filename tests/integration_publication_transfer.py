"""Two real 12-job Attempts replace one interrupted HDFS/SQL publication intent."""
import importlib.util
import json
import os
import sys
import unittest
import uuid

from governance.artifacts import build_artifacts
from governance.publication import publish_artifacts
from governance.service import submit_run
from metadata.connection import ROOT, load_config
from pipeline.run_pipeline import input_fingerprint, pipeline, sha256
from pipeline.yarn import YarnExecutor
from storage.hdfs import Hdfs, import_input

candidate = os.environ.get("ML_STORE_CANDIDATE")
if candidate:
    spec = importlib.util.spec_from_file_location("transfer_store_real_candidate", ROOT / candidate)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
else:
    import metadata.store as module


class IntentInterrupted(RuntimeError):
    pass


class PublicationTransferTests(unittest.TestCase):
    def test_two_real_attempts_one_publication_preserve_old_files(self):
        config = load_config()
        self.assertEqual(config["database"], "ml_governance_test")
        store = module.Store(config)
        fixture = ROOT / "outputs/publication-transfer-integration" / uuid.uuid4().hex
        fixture.mkdir(parents=True)
        raw = {table: fixture / (table + ".dat") for table in ("users", "movies", "ratings")}
        raw["users"].write_bytes(b"1::M::25::4::01234\n1::M::25::4::01234\n2::F::18::3::12345\n")
        raw["movies"].write_bytes("1::Léon (1994)::Action|Crime|Drama\n2::Toy Story (1995)::Animation|Comedy\n".encode("latin-1"))
        raw["ratings"].write_bytes(b"1::1::4::978000000\n1::1::5::978000001\n99::2::3::978000002\n")
        manifest = input_fingerprint(raw)
        store.register_input("movielens-1m", manifest["dataset_version"], manifest)
        import_input(store, manifest["dataset_version"], {table: path.relative_to(ROOT).as_posix() for table, path in raw.items()})
        run, _ = submit_run("transfer-real-" + uuid.uuid4().hex, store=store, raw=raw)
        claim = store.claim("transfer-real-first", lease_seconds=3600)
        self.assertEqual(claim.run_id, run["run_id"])
        (fixture / "run.json").write_text(json.dumps({"run_id": run["run_id"]}), encoding="utf-8")
        print(json.dumps({"fixture": str(fixture), "run_id": run["run_id"], "stage": "first-computation"}), flush=True)

        def compute(claim):
            report = pipeline(run_id=claim.run_id, attempt_id=claim.attempt_id, raw=raw,
                              executor=YarnExecutor(store, claim, reducers=2), register=False)
            store.advance(claim, "transfer-validation", "VALIDATING")
            return build_artifacts(report, {table: path.relative_to(ROOT).as_posix() for table, path in raw.items()})[0]

        def interrupt(stage, intent):
            if stage == "intent":
                raise IntentInterrupted("preserve original intent")

        first_root = compute(claim)
        with self.assertRaises(IntentInterrupted):
            publish_artifacts(store, claim, first_root, checkpoint=interrupt)
        original = store.get_publish(claim.run_id)
        hdfs = Hdfs()
        old_remote = original["storage_path"] + "/report.json"
        old_file = hdfs.put_immutable(first_root / "report.json", old_remote)
        first_files = {path.relative_to(first_root).as_posix(): {"sha256": sha256(path),
                       "bytes": path.stat().st_size} for path in first_root.rglob("*") if path.is_file()}
        with store.transaction() as cursor:
            for table in ("logical_run", "worker_slot"):
                cursor.execute(f"UPDATE {table} SET lease_until=DATE_SUB(NOW(6),INTERVAL 1 SECOND) WHERE run_id=%s", (claim.run_id,))
        store.request_publication_replacement(claim.run_id, expected_publish=original["publish_id"],
            expected_attempt=claim.attempt_id, expected_manifest_hash=original["manifest_hash"], expected_token=claim.token,
            reason="test-owned interrupted candidate is abandoned; preserve its partial HDFS files")
        replacement = store.claim("transfer-real-second", lease_seconds=3600)
        self.assertEqual(replacement.run_id, claim.run_id)
        self.assertNotEqual(replacement.attempt_id, claim.attempt_id)
        for action in (lambda: store.heartbeat(claim), lambda: store.confirm_publication(claim, original["publish_id"], {})):
            with self.assertRaises(module.LostLease):
                action()
        print(json.dumps({"run_id": run["run_id"], "stage": "second-computation", "attempt_id": replacement.attempt_id}), flush=True)
        second_root = compute(replacement)
        self.assertEqual(store.get_publish(claim.run_id)["attempt_id"], claim.attempt_id)
        published = publish_artifacts(store, replacement, second_root)
        self.assertEqual(published["status"], "PUBLISHED")
        self.assertEqual(published["publish_id"], original["publish_id"])
        self.assertEqual(published["output_version"], original["output_version"])
        self.assertEqual(published["attempt_id"], replacement.attempt_id)
        self.assertNotEqual(published["storage_path"], original["storage_path"])
        self.assertEqual(hdfs.digest(old_remote), old_file)
        self.assertEqual(first_files, {path.relative_to(first_root).as_posix(): {"sha256": sha256(path), "bytes": path.stat().st_size}
                                     for path in first_root.rglob("*") if path.is_file()})
        verified = {item["path"]: hdfs.digest(published["storage_path"] + "/" + item["path"])
                    for item in published["manifest"]["files"]}
        for item in published["manifest"]["files"]:
            self.assertEqual(verified[item["path"]], {"sha256": item["sha256"], "bytes": item["bytes"]})
        self.assertEqual(hdfs.digest(published["storage_path"] + "/manifest.json")["sha256"], published["manifest_hash"])
        with store.transaction() as cursor:
            cursor.execute("SELECT COUNT(*) AS n FROM publish_version WHERE run_id=%s", (claim.run_id,))
            self.assertEqual(cursor.fetchone()["n"], 1)
        attempts = store.list_attempts(claim.run_id)
        self.assertEqual(len(attempts), 2)
        for attempt in attempts:
            jobs = store.list_jobs(attempt["attempt_id"])
            self.assertEqual(len(jobs), 12)
            self.assertTrue(all(job["status"] == "SUCCEEDED" for job in jobs))
        record = {"run_id": claim.run_id, "original_attempt": claim.attempt_id, "replacement_attempt": replacement.attempt_id,
                  "publish_id": published["publish_id"], "manifest_hash": published["manifest_hash"], "jobs_per_attempt": 12,
                  "old_hdfs_report_unchanged": True, "old_candidate_unchanged": True, "formal_files_verified": len(verified) + 1,
                  "candidate_store": candidate, "production_store_applied": not bool(candidate)}
        (fixture / "acceptance.json").write_text(json.dumps(record, indent=2), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
