"""Legacy failures keep status and cannot fall back to a different report."""
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from pymysql import OperationalError
from agent import server
from agent.legacy_archive import LegacyUnavailable


class LegacyHttpErrorTests(unittest.TestCase):
    def test_ambiguous_or_missing_material_keeps_explicit_status(self):
        for status in (404, 409, 503):
            with self.subTest(status=status), patch.object(server, "read_legacy_report", side_effect=LegacyUnavailable("fixture", status)):
                response = TestClient(server.app).get("/api/legacy/reports/fixture")
                self.assertEqual(response.status_code, status)

    def test_database_failure_returns_503_without_database_details(self):
        with patch.object(server, "read_legacy_report", side_effect=OperationalError("private connection information")):
            response = TestClient(server.app).get("/api/legacy/reports/fixture")
        self.assertEqual(response.status_code, 503)
        self.assertNotIn("private", response.text)


if __name__ == "__main__":
    unittest.main()
