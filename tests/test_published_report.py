"""Formal visibility and immutable cache binding, including malicious fallback."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient

from agent import server
from agent.published import ReportUnavailable, read_published_report
from metadata.connection import ROOT
from metadata.store import fingerprint


class PublishedReportTests(unittest.TestCase):
    def setUp(self):
        (ROOT / "outputs/tests").mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=ROOT / "outputs/tests")
        self.root = Path(self.temp.name).resolve()
        self.store = Mock()
        self.run = {"run_id": "run-own", "status": "PUBLISHED", "input_version": "raw-own",
                    "rule_version": "rule-own", "metric_version": "metric-own"}
        self.report = {"run_id": "run-own", "attempt_id": "attempt-own", "input_data_version": "raw-own",
                       "rule_version": "rule-own", "metric_version": "metric-own", "output_data_version": "clean-own"}
        self.work = "outputs/run-own/attempts/attempt-own"
        self.path = self.root / self.work / "artifacts/report.json"
        self.path.parent.mkdir(parents=True)
        self.content = json.dumps(self.report).encode()
        self.path.write_bytes(self.content)
        self.publication = {"run_id": "run-own", "attempt_id": "attempt-own", "publish_id": "publish-own",
                            "output_version": "clean-own", "status": "PUBLISHED", "manifest": {
                                "run_id": "run-own", "attempt_id": "attempt-own", "files": [{
                                    "path": "report.json", "bytes": len(self.content),
                                    "sha256": hashlib.sha256(self.content).hexdigest()}]}}
        self.publication["manifest_hash"] = fingerprint(self.publication["manifest"])
        self.store.get_run.return_value = self.run
        self.store.get_publish.return_value = self.publication
        self.store.list_attempts.return_value = [{"attempt_id": "attempt-own", "work_path": self.work}]

    def tearDown(self):
        self.temp.cleanup()

    def read(self, run_id="run-own"):
        return read_published_report(run_id, store=self.store, root=self.root)

    def test_unknown_run_does_not_select_any_report(self):
        self.store.get_run.return_value = None
        with self.assertRaises(ReportUnavailable) as caught:
            self.read("missing")
        self.assertEqual(caught.exception.status_code, 404)
        self.store.get_publish.assert_not_called()

    def test_candidate_cache_is_not_formally_visible(self):
        for status in ("QUEUED", "RUNNING", "VALIDATING", "PUBLISHING", "FAILED"):
            with self.subTest(status=status):
                self.run["status"] = status
                with self.assertRaises(ReportUnavailable) as caught:
                    self.read()
                self.assertEqual(caught.exception.status_code, 409)

    def test_http_serves_only_verified_bytes_and_publish_identity(self):
        with patch.object(server, "read_published_report", side_effect=lambda run_id: self.read(run_id)):
            client = TestClient(server.app)
            for suffix in ("report", "report/download"):
                response = client.get("/api/runs/run-own/" + suffix)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.content, self.content)
                self.assertEqual(response.headers["x-publish-id"], "publish-own")

    def test_compatibility_routes_use_same_formal_report_and_download(self):
        with patch.object(server.loop,'read_published_report',side_effect=lambda run_id,**kwargs:self.read(run_id)), \
                patch.object(server,'read_published_report',side_effect=lambda run_id:self.read(run_id)), \
                patch('metadata.store.Store',return_value=self.store),TestClient(server.app) as client:
            self.assertEqual(client.get('/api/tasks/run-own/report').json(),self.report)
            downloaded = client.get('/api/tasks/run-own/report/download')
            self.assertEqual(downloaded.content,self.content)
            self.assertEqual(downloaded.headers['x-publish-id'],'publish-own')
            self.assertEqual(client.get('/api/tasks/run-own/sample').json(),{})
            self.run['status'] = 'VALIDATING'
            for suffix in ('report','report/download','sample'):
                self.assertEqual(client.get('/api/tasks/run-own/'+suffix).status_code,409)

    def test_corrupt_cache_never_returns_candidate_or_other_run(self):
        self.path.write_bytes(b'{"run_id":"other"}')
        (self.root / self.work / "report.json").write_bytes(self.content)
        with self.assertRaises(ReportUnavailable) as caught:
            self.read()
        self.assertEqual(caught.exception.status_code, 503)

    def test_rehashed_foreign_report_still_fails_version_binding(self):
        self.report["run_id"] = "other"
        content = json.dumps(self.report).encode()
        self.path.write_bytes(content)
        self.publication["manifest"]["files"][0].update(bytes=len(content), sha256=hashlib.sha256(content).hexdigest())
        self.publication["manifest_hash"] = fingerprint(self.publication["manifest"])
        with self.assertRaisesRegex(ReportUnavailable, "version binding"):
            self.read()

    def test_attempt_cache_path_cannot_escape_selected_attempt(self):
        self.store.list_attempts.return_value[0]["work_path"] = "outputs/other/attempts/attempt-own"
        with self.assertRaisesRegex(ReportUnavailable, "location"):
            self.read()

    def test_modified_manifest_is_not_trusted(self):
        self.publication["manifest"]["files"][0]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ReportUnavailable, "manifest checksum"):
            self.read()

    def test_missing_cache_does_not_select_candidate(self):
        # Point at an unavailable cache while preserving a readable candidate.
        self.path.rename(self.path.parent / "diagnostic-report.json")
        (self.root / self.work / "report.json").write_bytes(self.content)
        with self.assertRaises(ReportUnavailable) as caught:
            self.read()
        self.assertEqual(caught.exception.status_code, 503)


if __name__ == "__main__":
    unittest.main()
