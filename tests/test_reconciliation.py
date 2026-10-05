"""Namespace recognition retains formal, pending, raw and historical dependencies."""
import unittest

from governance.reconciliation import classify_inventory


class ReconciliationTests(unittest.TestCase):
    def references(self):
        return [{"scope": "test", "raw": [{"storage_uri": "/ml/raw/v/users.dat", "version_id": "v"}],
                 "attempts": [{"run_id": "active", "attempt_id": "a", "status": "RUNNING", "lease_alive": 1},
                              {"run_id": "expired", "attempt_id": "b", "status": "PUBLISHING", "lease_alive": 0},
                              {"run_id": "failed", "attempt_id": "c", "status": "FAILED", "lease_alive": 0}],
                 "publications": [{"publish_id": "published", "run_id": "r1", "storage_path": "/ml/published/v1", "status": "PUBLISHED",
                                  "manifest": {"files": [{"path": "report.json", "bytes": 2}]}},
                                 {"publish_id": "pending", "run_id": "r2", "storage_path": "/ml/published/v2", "status": "PREPARING",
                                  "manifest": {"files": [{"path": "report.json", "bytes": 2}]}}]}]

    def test_all_referenced_states_are_retained(self):
        paths = ["/ml/raw/v/users.dat", "/ml/published/v1/report.json", "/ml/published/v2/report.json",
                 "/ml/staging/run=active/attempt=a/jobs/part", "/ml/staging/run=expired/attempt=b/jobs/part",
                 "/ml/staging/run=failed/attempt=c/jobs/part"]
        result = classify_inventory([{"path": path, "bytes": 2} for path in paths], self.references())
        self.assertEqual([row["classification"] for row in result["files"]], ["PROTECTED_RAW", "PROTECTED_PUBLISHED", "PENDING_PUBLICATION",
                            "ACTIVE_ATTEMPT", "RECOVERY_REQUIRED", "RETAINED_ATTEMPT_HISTORY"])
        self.assertFalse(result["deletion_authorized"])
        self.assertFalse(result["checksums_verified"])

    def test_unregistered_objects_require_scope_review(self):
        paths = ["/ml/staging/run=unknown/attempt=x/file", "/ml/staging/uploads/u/blob", "/ml/published/v1-other/report.json"]
        result = classify_inventory([{"path": path, "bytes": 2} for path in paths], self.references())
        self.assertEqual([row["classification"] for row in result["files"]], ["UNREGISTERED_ATTEMPT_REVIEW", "UNREFERENCED_UPLOAD_REVIEW", "UNREGISTERED_PATH_REVIEW"])
        self.assertFalse(result["deletion_authorized"])

    def test_missing_and_unexpected_formal_files_are_reported(self):
        result = classify_inventory([{"path": "/ml/published/v1/report.json", "bytes": 1},
                                     {"path": "/ml/published/v1/extra.txt", "bytes": 3}], self.references())
        self.assertEqual({row["kind"] for row in result["publication_mismatches"]}, {"byte_length", "undeclared_file"})
        self.assertIn({"publish_id": "published", "status": "PUBLISHED", "missing": ["manifest.json"]}, result["incomplete_publications"])
        self.assertTrue(all(row["classification"] == "PROTECTED_PUBLISHED" for row in result["files"]))

    def test_escaped_or_duplicate_inventory_is_rejected(self):
        for paths in (["/ml/../outside"], ["/ml/raw/same", "/ml/raw/same"]):
            with self.subTest(paths=paths), self.assertRaises(ValueError):
                classify_inventory([{"path": path, "bytes": 1} for path in paths], self.references())


if __name__ == "__main__":
    unittest.main()
