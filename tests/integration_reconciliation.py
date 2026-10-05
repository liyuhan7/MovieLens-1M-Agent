"""Inspect real shared HDFS using both project metadata scopes; preserve every file."""
import json
import unittest
import uuid

from governance.reconciliation import scan_storage
from metadata.connection import ROOT, load_config
from metadata.store import Store
from storage.hdfs import Hdfs


class StorageReconciliationTests(unittest.TestCase):
    def test_actual_published_dependencies_and_unregistered_fixture(self):
        config = load_config()
        self.assertEqual(config["database"], "ml_governance_test")
        stores = [Store(config), Store({**config, "database": "ml_governance"})]
        identity = uuid.uuid4().hex
        root = ROOT / "outputs/reconciliation-fixtures" / identity
        root.mkdir(parents=True)
        source = root / "fixture.txt"
        source.write_text("synthetic unregistered object", encoding="utf-8")
        remote = f"/ml/staging/run=scanner-{identity}/attempt=scanner-{identity}/fixture.txt"
        hdfs = Hdfs()
        hdfs.put_immutable(source, remote)
        result = scan_storage(stores, hdfs=hdfs)
        indexed = {row["path"]: row for row in result["files"]}
        self.assertEqual(indexed[remote]["classification"], "UNREGISTERED_ATTEMPT_REVIEW")
        self.assertFalse(result["deletion_authorized"])
        self.assertFalse(result["publication_mismatches"])
        published = [row for row in result["files"] if row["classification"] == "PROTECTED_PUBLISHED"]
        self.assertGreaterEqual(len(published), 22, "Run actual publication integration first")
        self.assertFalse([row for row in result["incomplete_publications"] if row["status"] == "PUBLISHED"])
        self.assertTrue(any(row["classification"] == "PROTECTED_RAW" for row in result["files"]))
        self.assertTrue(hdfs.exists(remote))
        (root / "acceptance.json").write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
