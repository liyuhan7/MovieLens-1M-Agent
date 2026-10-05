"""Old task routes and Agent reads use the same authoritative formal publication."""
import json
import os
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from agent import loop, server
from agent.legacy_archive import install_archive_schema
from metadata.connection import ROOT, load_config
from metadata.store import Store


class TaskReportCompatibilityTests(unittest.TestCase):
    def test_published_run_old_routes_and_agent_bind_to_same_manifest(self):
        self.assertEqual(load_config()["database"], "ml_governance_test")
        install_archive_schema(Store())
        run_id = os.environ.get("ML_PUBLISHED_RUN_ID", "run-965b86fbb9e94bc5ab35f71df5ab91a7")
        publication = Store().get_publish(run_id)
        self.assertIsNotNone(publication)
        self.assertEqual(publication["status"], "PUBLISHED")
        client = TestClient(server.app)
        formal = client.get("/api/runs/" + run_id + "/report")
        self.assertEqual(formal.status_code, 200)
        self.assertEqual(formal.json()["attempt_id"], publication["attempt_id"])
        self.assertEqual(formal.headers["X-Publish-ID"], publication["publish_id"])
        report = client.get("/api/tasks/" + run_id + "/report")
        self.assertEqual(report.status_code, 200)
        self.assertEqual(report.json(), formal.json())
        downloaded = client.get("/api/tasks/" + run_id + "/report/download")
        self.assertEqual(downloaded.status_code, 200)
        self.assertEqual(downloaded.content, formal.content)
        self.assertEqual(downloaded.headers["X-Publish-ID"], formal.headers["X-Publish-ID"])
        self.assertEqual(loop._load_report(run_id), formal.json())
        sample = client.get("/api/tasks/" + run_id + "/sample")
        self.assertEqual(sample.json(), formal.json()["disposition"])
        for endpoint in ("report", "report/download", "sample"):
            self.assertEqual(client.get("/api/tasks/unknown-formal-run/" + endpoint).status_code, 404)
        (ROOT / "outputs" / ("task-report-compatibility-" + run_id + ".json")).write_text(json.dumps({
            "run_id": run_id, "attempt_id": publication["attempt_id"], "publish_id": formal.headers["X-Publish-ID"], "old_routes_match_formal": True,
            "download_bytes_match": True, "agent_read_matches": True, "unknown_rejected": True}, indent=2), encoding="utf-8")

    def test_full_run_old_routes_follow_formal_publication_state(self):
        with patch.dict(os.environ, {"ML_MYSQL_DATABASE": "ml_governance"}):
            client = TestClient(server.app)
            run_id = os.environ.get("ML_FULL_RUN_ID", "run-ada73f08647d456d9a3b2c1fd9d85667")
            run = Store().get_run(run_id)
            if run["status"] != "PUBLISHED":
                for endpoint in ("report", "report/download", "sample"):
                    response = client.get("/api/tasks/" + run_id + "/" + endpoint)
                    self.assertEqual(response.status_code, 409)
                return
            publication = Store().get_publish(run_id)
            formal = client.get("/api/runs/" + run_id + "/report")
            self.assertEqual(formal.status_code, 200, formal.text)
            self.assertEqual(formal.headers["X-Publish-ID"], publication["publish_id"])
            self.assertEqual(formal.json()["run_id"], run_id)
            self.assertEqual(formal.json()["attempt_id"], publication["attempt_id"])
            report = client.get("/api/tasks/" + run_id + "/report")
            downloaded = client.get("/api/tasks/" + run_id + "/report/download")
            sample = client.get("/api/tasks/" + run_id + "/sample")
            self.assertEqual((report.status_code, downloaded.status_code, sample.status_code), (200, 200, 200))
            self.assertEqual(report.json(), formal.json())
            self.assertEqual(downloaded.content, formal.content)
            self.assertEqual(downloaded.headers["X-Publish-ID"], publication["publish_id"])
            self.assertEqual(sample.json(), formal.json()["disposition"])
            self.assertEqual(loop._load_report(run_id), formal.json())
            (ROOT / "outputs" / ("full-run-http-acceptance-" + run_id + ".json")).write_text(json.dumps({
                "run_id": run_id, "attempt_id": publication["attempt_id"], "publish_id": publication["publish_id"],
                "manifest_sha256": publication["manifest_hash"], "formal_report_read": True,
                "old_routes_match_formal": True, "download_bytes_match": True, "agent_read_matches": True},
                indent=2), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
