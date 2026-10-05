"""Older real Run retries after a newer real publication.

submit -> external test-db worker --once -> verify using one fixed fixture.
No state is rewritten to pretend computation or publication completed.
"""
import json
import os
import unittest
import uuid
from unittest.mock import patch

from fastapi.testclient import TestClient

from agent import server
from governance.publication import publish_artifacts
from metadata.connection import ROOT, load_config
from metadata.store import Claim, Conflict, LostLease, Store
from pipeline.run_pipeline import sha256


class LateCurrentTests(unittest.TestCase):
    def current(self, store, dataset):
        with store.transaction() as cursor:
            cursor.execute("SELECT * FROM dataset_current WHERE dataset_id=%s", (dataset,))
            return cursor.fetchone()

    def snapshot(self, directory):
        return {path.relative_to(directory).as_posix(): sha256(path)
                for path in directory.rglob("*") if path.is_file()}

    def test_older_run_publishes_late_without_replacing_newer(self):
        with patch.dict(os.environ, {"ML_MYSQL_DATABASE": "ml_governance_test"}):
            self.assertEqual(load_config()["database"], "ml_governance_test")
            store = Store()
            phase = os.environ.get("ML_LATE_PHASE", "submit")
            self.assertIn(phase, {"submit", "verify"})
            old_id = os.environ["ML_LATE_OLD_RUN"]
            new_id = os.environ["ML_LATE_NEW_RUN"]
            fixture = ROOT / os.environ.get("ML_LATE_FIXTURE", "outputs/late-current-integration/" + uuid.uuid4().hex)
            self.assertTrue(fixture.resolve().is_relative_to(ROOT / "outputs" / "late-current-integration"))
            old, new = store.get_run(old_id), store.get_run(new_id)
            self.assertLess(old["request_seq"], new["request_seq"])
            self.assertEqual(old["dataset_id"], new["dataset_id"])
            self.assertEqual(new["status"], "PUBLISHED")
            newer = store.get_publish(new_id)
            self.assertEqual(self.current(store, old["dataset_id"])["publish_id"], newer["publish_id"])
            if phase == "submit":
                self.assertEqual(old["status"], "FAILED")
                self.assertEqual(old["error"]["reason"], "standalone scoring acceptance; not a complete publication run")
                self.assertIsNone(store.get_publish(old_id))
                attempts = store.list_attempts(old_id)
                self.assertEqual(len(attempts), 1)
                previous = attempts[0]
                original_files = self.snapshot(ROOT / previous["work_path"])
                self.assertTrue(original_files)
                fixture.mkdir(parents=True)
                state = {"older_run": old_id, "newer_run": new_id,
                         "newer_publish": newer["publish_id"], "newer_manifest_sha256": newer["manifest_hash"],
                         "previous_attempt": previous["attempt_id"], "previous_path": previous["work_path"],
                         "previous_files": original_files,
                         "current_before": self.current(store, old["dataset_id"])["publish_id"]}
                with store.transaction() as cursor:
                    cursor.execute("SELECT detail FROM run_event WHERE run_id=%s AND attempt_id=%s "
                                   "AND event_type='ATTEMPT_STARTED' ORDER BY event_id LIMIT 1",
                                   (old_id, previous["attempt_id"]))
                    event = cursor.fetchone()["detail"]
                    state["previous_owner"] = (json.loads(event) if isinstance(event, str) else event)["owner"]
                (fixture / "request.json").write_text(json.dumps(state, indent=2), encoding="utf-8")
                with TestClient(server.app) as client:
                    response = client.post("/api/runs/" + old_id + "/retry")
                    self.assertEqual(response.status_code, 202, response.text)
                    self.assertEqual(client.get("/api/runs/" + old_id).json()["status"], "QUEUED")
                print("LATE_FIXTURE=" + fixture.relative_to(ROOT).as_posix(), flush=True)
                return
            state = json.loads((fixture / "request.json").read_text(encoding="utf-8"))
            self.assertEqual(old["status"], "PUBLISHED")
            attempts = store.list_attempts(old_id)
            self.assertEqual(len(attempts), 2)
            previous, winner = attempts
            self.assertEqual(previous["attempt_id"], state["previous_attempt"])
            self.assertEqual(previous["status"], "FAILED")
            self.assertEqual(winner["status"], "PUBLISHED")
            self.assertNotEqual(previous["work_path"], winner["work_path"])
            self.assertEqual(self.snapshot(ROOT / previous["work_path"]), state["previous_files"])
            jobs = store.list_jobs(winner["attempt_id"])
            self.assertEqual(len(jobs), 12)
            self.assertTrue(all(job["status"] == "SUCCEEDED" for job in jobs))
            publication = store.get_publish(old_id)
            self.assertEqual(publication["attempt_id"], winner["attempt_id"])
            storage = json.loads((fixture / "hdfs-acceptance.json").read_text(encoding="utf-8"))
            self.assertEqual(storage["scope"], "ml_governance_test")
            checked = {item["run_id"]: item for item in storage["publications"]}
            self.assertEqual(set(checked), {old_id, new_id})
            for selected in (publication, newer):
                check = checked[selected["run_id"]]
                self.assertEqual(check["publish_id"], selected["publish_id"])
                self.assertEqual(check["manifest_sha256"], selected["manifest_hash"])
                self.assertEqual(len(check["files"]), 22)
                for item in selected["manifest"]["files"]:
                    self.assertEqual(check["files"][item["path"]],
                                     {key: item[key] for key in ("sha256", "bytes")})
            self.assertFalse(store.promote_current(publication["publish_id"]))
            self.assertEqual(self.current(store, old["dataset_id"])["publish_id"], state["newer_publish"])
            self.assertEqual(store.get_publish(new_id)["manifest_hash"], state["newer_manifest_sha256"])
            if "previous_owner" not in state:
                with store.transaction() as cursor:
                    cursor.execute("SELECT detail FROM run_event WHERE run_id=%s AND attempt_id=%s "
                                   "AND event_type='ATTEMPT_STARTED' ORDER BY event_id LIMIT 1",
                                   (old_id, previous["attempt_id"]))
                    event = cursor.fetchone()["detail"]
                    state["previous_owner"] = (json.loads(event) if isinstance(event, str) else event)["owner"]
            stale = Claim(old_id, previous["attempt_id"], previous["fencing_token"],
                          state["previous_owner"], False, previous["work_path"], "RUNNING")
            with self.assertRaises(LostLease):
                store.advance(stale, "stale-attempt-returned", "VALIDATING")
            with self.assertRaisesRegex(Conflict, "Another Attempt"):
                publish_artifacts(store, stale, ROOT / winner["work_path"] / "artifacts")
            with store.transaction() as cursor:
                cursor.execute("SELECT COUNT(*) AS n FROM publish_version WHERE run_id=%s", (old_id,))
                self.assertEqual(cursor.fetchone()["n"], 1)
                cursor.execute("SELECT COUNT(*) AS n FROM run_event WHERE run_id=%s AND event_type='CURRENT_SKIPPED'", (old_id,))
                self.assertGreaterEqual(cursor.fetchone()["n"], 1)
            state.update({"winning_attempt": winner["attempt_id"], "older_publish": publication["publish_id"],
                          "older_manifest_sha256": publication["manifest_hash"],
                          "two_real_published_runs": True, "old_attempt_unchanged": True,
                          "older_retry_finished_after_newer": True, "stale_state_update_rejected": True,
                          "stale_attempt_publication_rejected": True})
            (fixture / "acceptance.json").write_text(json.dumps(state, indent=2), encoding="utf-8")
            print("LATE_ACCEPTANCE=" + str(fixture / "acceptance.json"), flush=True)


if __name__ == "__main__":
    unittest.main()
