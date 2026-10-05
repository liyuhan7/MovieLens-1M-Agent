"""Public HTTP regression: an unknown run must never return another report."""
import json
import shutil
import unittest
import uuid
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from agent import server
from agent import loop


class ReportBindingTests(unittest.TestCase):
    def setUp(self):
        self.test_root = Path(__file__).resolve().parents[1] / "outputs" / "tests"
        self.test_root.mkdir(parents=True, exist_ok=True)
        self.root = self.test_root / uuid.uuid4().hex
        (self.root / "other-run").mkdir(parents=True)
        (self.root / "other-run" / "report.json").write_text(json.dumps({"run_id": "other-run"}), encoding="utf-8")
        (self.root / "registry.json").write_text(json.dumps([
            {"task_id": "other-run", "tag": "other-run", "status": "success", "report": "other-run/report.json"}
        ]), encoding="utf-8")

    def tearDown(self):
        if not self.root.resolve().is_relative_to(self.test_root.resolve()):
            raise RuntimeError("test cleanup escaped its workspace")
        shutil.rmtree(self.root)

    def test_unknown_run_does_not_return_latest_success(self):
        with patch.object(server, "OUT_ROOT", str(self.root)), patch.object(loop, "OUT_ROOT", str(self.root)):
            for endpoint in ("report", "report/download", "sample"):
                response = TestClient(server.app).get("/api/tasks/missing-run/" + endpoint)
                self.assertEqual(response.status_code, 404)

    def test_existing_run_remains_addressable(self):
        with patch.object(loop, "OUT_ROOT", str(self.root)):
            response = TestClient(server.app).get("/api/tasks/other-run/report")
        self.assertEqual(response.json()["run_id"], "other-run")

    def test_missing_material_does_not_validate_numeric_claims(self):
        with patch.object(loop, "OUT_ROOT", str(self.root)):
            self.assertEqual(loop.verify_numbers("质量 93.33 分", "missing-run"), ["93.33"])

    def test_followup_validation_uses_requested_report(self):
        (self.root / "other-run" / "report.json").write_text(
            json.dumps({"score": 80.12}), encoding="utf-8")
        with patch.object(loop, "OUT_ROOT", str(self.root)), patch.object(loop, "model_configured", return_value=True), \
             patch.object(loop, "_ask_loop", return_value="质量 93.33 分"):
            result = loop.ask_followup("other-run", "质量是多少")
        self.assertEqual(result["flagged"], ["93.33"])

    def test_bound_tool_cannot_change_historical_material(self):
        context = SimpleNamespace(context=loop.AgentDeps(task_id="other-run", task_tag="other-run", report_tag="other-run"))
        self.assertEqual(loop._bound_report(context, "latest"), "other-run")
        with self.assertRaises(ValueError):
            loop._bound_report(context, "different-run")


if __name__ == "__main__":
    unittest.main()
