"""Host acceptance: real HTTP handlers, MySQL and a separate container worker.

Run prepare, import inputs, submit, external worker, then verify against the same ML_HTTP_FIXTURE.
Only the HTTP input selection is injected to use tiny test-owned datasets.
Run/Attempt/Job/publication operations are real; no LLM call is involved.
"""
import json
import os
import unittest
import uuid
from unittest.mock import patch

from fastapi.testclient import TestClient

from agent import server
from governance.service import submit_run
from metadata.connection import ROOT, load_config
from metadata.store import Store
from pipeline.run_pipeline import input_fingerprint


class HttpWorkerTests(unittest.TestCase):
    def test_http_request_survives_client_restart_and_worker_publishes(self):
        with patch.dict(os.environ, {"ML_MYSQL_DATABASE": "ml_governance_test"}):
            config = load_config()
            self.assertEqual(config["database"], "ml_governance_test")
            store = Store(config)
            phase = os.environ.get("ML_HTTP_PHASE", "prepare")
            self.assertIn(phase, {"prepare", "submit", "verify"})
            fixture = ROOT / os.environ.get("ML_HTTP_FIXTURE", "outputs/http-worker-integration/" + uuid.uuid4().hex)
            self.assertTrue(fixture.resolve().is_relative_to(ROOT / "outputs" / "http-worker-integration"))
            if phase == "verify":
                evidence = json.loads((fixture / "request.json").read_text(encoding="utf-8"))
                run_id = evidence["run_id"]
                with TestClient(server.app) as client:
                    response = client.get("/api/runs/" + run_id)
                    self.assertEqual(response.status_code, 200, response.text)
                    result = response.json()
                    self.assertEqual(result["status"], "PUBLISHED", response.text)
                    self.assertEqual(len(result["attempts"]), 1)
                    attempt = result["attempts"][0]
                    self.assertEqual(len(attempt["jobs"]), 12)
                    self.assertTrue(all(job["status"] == "SUCCEEDED" for job in attempt["jobs"]))
                    self.assertEqual(client.post("/api/runs/" + run_id + "/retry").status_code, 409)
                    formal = client.get("/api/runs/" + run_id + "/report")
                    self.assertEqual(formal.status_code, 200, formal.text)
                    self.assertEqual(formal.json()["run_id"], run_id)
                    self.assertEqual(formal.json()["attempt_id"], attempt["attempt_id"])
                    download = client.get("/api/runs/" + run_id + "/report/download")
                    self.assertEqual(download.status_code, 200, download.text)
                    self.assertEqual(download.content, formal.content)
                publication = store.get_publish(run_id)
                self.assertEqual(publication["status"], "PUBLISHED")
                restart_path = fixture / "api-process-restart-acceptance.json"
                restart = json.loads(restart_path.read_text(encoding="utf-8-sig")) if restart_path.exists() else None
                if restart:
                    self.assertEqual(restart["run_id"], run_id)
                    self.assertEqual(restart["attempt_id"], attempt["attempt_id"])
                    self.assertNotEqual(restart["pid_before"], restart["pid_after"])
                    self.assertTrue(restart["previous_job_ids_preserved"])
                evidence.update({"attempt_id": attempt["attempt_id"], "publish_id": publication["publish_id"],
                                 "storage_path": publication["storage_path"], "manifest_sha256": publication["manifest_hash"],
                                 "jobs": 12, "worker_process": "separate Docker container",
                                 "api_process_kill_tested": bool(restart and restart["api_process_kill_tested"]),
                                 "formal_report_bound": True,
                                 "api_restart_evidence": str(restart_path) if restart else None})
                (fixture / "acceptance.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
                print("HTTP_ACCEPTANCE=" + str(fixture / "acceptance.json"), flush=True)
                return
            prepared = []
            raw_sets = []
            for name, user in (("first", "901"), ("conflict", "902")):
                folder = fixture / name
                folder.mkdir(parents=True, exist_ok=True)
                raw = {table: folder / (table + ".dat") for table in ("users", "movies", "ratings")}
                raw["users"].write_bytes(f"{user}::M::25::4::01234\n".encode())
                raw["movies"].write_bytes(b"951::HTTP Movie (1994)::Action|Comedy\n")
                raw["ratings"].write_bytes(f"{user}::951::4::978000000\n".encode())
                raw_sets.append(raw)
                manifest = input_fingerprint(raw)
                store.register_input("movielens-1m", manifest["dataset_version"], manifest)
                paths = {table: path.relative_to(ROOT).as_posix() for table, path in raw.items()}
                prepared.append({"version": manifest["dataset_version"], "paths": paths})
            (fixture / "inputs.json").write_text(json.dumps(prepared), encoding="utf-8")
            if phase == "prepare":
                print("HTTP_FIXTURE=" + fixture.relative_to(ROOT).as_posix(), flush=True)
                return
            selected = [raw_sets[0]]

            def submit(key, **kwargs):
                return submit_run(key, store=store, raw=selected[0], **kwargs)

            key = "http-worker-" + uuid.uuid4().hex
            headers = {"Idempotency-Key": key}
            with patch.object(server, "submit_run", side_effect=submit):
                with TestClient(server.app) as client:
                    first = client.post("/api/runs", json={}, headers=headers)
                    self.assertEqual(first.status_code, 202, first.text)
                    run_id = first.json()["run_id"]
                    self.assertTrue(first.json()["created"])
                    duplicate = client.post("/api/runs", json={}, headers=headers)
                    self.assertEqual(duplicate.status_code, 202, duplicate.text)
                    self.assertEqual(duplicate.json()["run_id"], run_id)
                    self.assertFalse(duplicate.json()["created"])
                    selected[0] = raw_sets[1]
                    conflict = client.post("/api/runs", json={}, headers=headers)
                    self.assertEqual(conflict.status_code, 409, conflict.text)
                    selected[0] = raw_sets[0]
                # New HTTP application lifecycle reads durable state from MySQL.
                with TestClient(server.app) as restarted:
                    queued = restarted.get("/api/runs/" + run_id)
                    self.assertEqual(queued.status_code, 200, queued.text)
                    self.assertEqual(queued.json()["status"], "QUEUED")
                    self.assertEqual(queued.json()["attempts"], [])
                    self.assertEqual(restarted.get("/api/runs/unknown-http-run").status_code, 404)
                    self.assertEqual(restarted.get("/api/runs/" + run_id + "/report").status_code, 409)
                (fixture / "request.json").write_text(json.dumps({
                    "run_id": run_id, "http_duplicate_reused": True, "http_conflict_rejected": True,
                    "new_client_reads_durable_state": True,
                    "input_selection": "test fixture injected; control and publication are real"}, indent=2), encoding="utf-8")
                print("HTTP_SUBMITTED=" + run_id, flush=True)


if __name__ == "__main__":
    unittest.main()
