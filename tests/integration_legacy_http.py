"""Real HTTP read-only legacy identities remain separate from formal Run reports."""
import json
import unittest

from fastapi.testclient import TestClient
from agent import server
from metadata.connection import ROOT, load_config


class LegacyHttpTests(unittest.TestCase):
    def test_ten_archives_and_unknown_identity(self):
        database = load_config()["database"]
        self.assertIn(database, {"ml_governance_transfer_test", "ml_governance"})
        inventory = json.loads((ROOT / "docs/历史报告基线清单.json").read_text(encoding="utf-8"))
        client = TestClient(server.app)
        for entry in inventory["legacy_reports"]:
            identity = "legacy-" + entry["import_key"]
            for alias in (identity, entry["source_tag"], entry["reported_task_id"]):
                if not alias:
                    continue
                result = client.get("/api/legacy/reports/" + alias)
                self.assertEqual(result.status_code, 200)
                body = result.json()
                self.assertEqual(body["archive_id"], identity)
                self.assertEqual(body["source_sha256"], entry["sha256"])
                self.assertEqual(body["archive_status"], "LEGACY_INCOMPLETE")
                self.assertFalse(body["formal_publication_verified"])
                compatible = client.get("/api/tasks/" + alias + "/report")
                self.assertEqual(compatible.status_code, 200)
                self.assertEqual(compatible.json(), body["report"])
            self.assertEqual(client.get("/api/runs/" + identity + "/report").status_code, 404)
        self.assertEqual(client.get("/api/legacy/reports/nonexistent-legacy-report").status_code, 404)
        self.assertEqual(client.get("/api/legacy/reports/latest").status_code, 404)
        self.assertEqual(client.get("/api/legacy/reports/").status_code, 404)
        (ROOT / ("outputs/legacy-http-" + database + ".json")).write_text(json.dumps({
            "database": database, "reports": 10, "archive_id_tag_task_id_verified": True,
            "unknown_empty_latest_rejected": True, "formal_run_endpoint_rejected_legacy_ids": True,
            "read_only": True}, indent=2), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
