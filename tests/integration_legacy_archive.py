"""Real MySQL legacy import, complete baseline hashes and explicit alias conflicts."""
import hashlib
import json
import unittest
import uuid

from agent.legacy_archive import LegacyUnavailable, import_legacy_reports, install_archive_schema, read_legacy_report
from metadata.connection import ROOT, load_config
from metadata.store import Conflict, Store


class LegacyArchiveTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = load_config()
        assert config["database"] == "ml_governance_transfer_test"
        cls.store = Store(config)
        install_archive_schema(cls.store)

    def fixture(self, alias=None):
        ident = uuid.uuid4().hex
        directory = ROOT / "outputs/legacy-archive-tests" / ident
        directory.mkdir(parents=True)
        path = directory / "report.json"
        path.write_text(json.dumps({"task_id": "legacy-task-" + ident, "score": 12.34}), encoding="utf-8")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        relative = path.relative_to(ROOT).as_posix()
        key = hashlib.sha256((relative + "\0" + digest).encode()).hexdigest()
        entry = {"path": relative, "sha256": digest, "bytes": path.stat().st_size,
                 "import_key": key, "source_tag": alias, "archive_status": "LEGACY_INCOMPLETE"}
        return path, entry

    def test_actual_ten_baseline_reports_import_twice_without_publication(self):
        inventory = json.loads((ROOT / "docs/历史报告基线清单.json").read_text(encoding="utf-8"))
        registry = json.loads((ROOT / "outputs/registry.json").read_text(encoding="utf-8"))
        before = self.counts()
        result = import_legacy_reports(inventory, registry, store=self.store)
        second = import_legacy_reports(inventory, registry, store=self.store)
        self.assertEqual(result, second)
        self.assertEqual(result["reports_verified"], 10)
        after = self.counts()
        self.assertEqual(before, after)
        for entry in inventory["legacy_reports"]:
            for identity in ("legacy-" + entry["import_key"], entry["source_tag"], entry["reported_task_id"]):
                if not identity:
                    continue
                report = read_legacy_report(identity, store=self.store)
                self.assertEqual(report["source_sha256"], entry["sha256"])
                self.assertEqual(report["archive_status"], "LEGACY_INCOMPLETE")
                self.assertFalse(report["formal_publication_verified"])
        with self.store.transaction() as cursor:
            keys = tuple(entry["import_key"] for entry in inventory["legacy_reports"])
            cursor.execute("SELECT COUNT(*) AS n FROM legacy_report_archive WHERE import_key IN (" + ",".join(["%s"] * len(keys)) + ")", keys)
            self.assertEqual(cursor.fetchone()["n"], 10)
        target = ROOT / "outputs/legacy-baseline-acceptance.json"
        target.write_text(json.dumps({**result, "database": self.store.config["database"], "reimport_identical": True,
                                     "all_ten_bytes_and_aliases_checked": True, "run_attempt_publish_counts_unchanged": before}, indent=2), encoding="utf-8")

    def counts(self):
        with self.store.transaction() as cursor:
            result = {}
            for table in ("logical_run", "physical_attempt", "publish_version", "evidence_index"):
                cursor.execute("SELECT COUNT(*) AS n FROM " + table)
                result[table] = cursor.fetchone()["n"]
            return result

    def test_alias_conflict_requires_archive_id_and_empty_tag_has_no_default(self):
        alias = "conflict-" + uuid.uuid4().hex
        _, first = self.fixture(alias)
        _, second = self.fixture(alias)
        import_legacy_reports({"legacy_reports": [first, second]}, [], store=self.store)
        with self.assertRaises(LegacyUnavailable) as rejected:
            read_legacy_report(alias, store=self.store)
        self.assertEqual(rejected.exception.status_code, 409)
        for entry in (first, second):
            self.assertEqual(read_legacy_report("legacy-" + entry["import_key"], store=self.store)["source_sha256"], entry["sha256"])
        _, empty = self.fixture("")
        import_legacy_reports({"legacy_reports": [empty]}, [], store=self.store)
        for identity in ("", "latest", "missing-" + uuid.uuid4().hex):
            with self.assertRaises(LegacyUnavailable) as missing:
                read_legacy_report(identity, store=self.store)
            self.assertEqual(missing.exception.status_code, 404)

    def test_changed_missing_or_redirected_material_does_not_fall_back(self):
        path, entry = self.fixture()
        import_legacy_reports({"legacy_reports": [entry]}, [], store=self.store)
        path.write_text('{"changed":true}', encoding="utf-8")
        with self.assertRaises(LegacyUnavailable) as changed:
            read_legacy_report("legacy-" + entry["import_key"], store=self.store)
        self.assertEqual(changed.exception.status_code, 503)
        with self.assertRaises(Conflict):
            import_legacy_reports({"legacy_reports": [entry]}, [], store=self.store)
        path.rename(path.with_name("retained-changed-report.json"))
        with self.assertRaises(LegacyUnavailable) as missing:
            read_legacy_report("legacy-" + entry["import_key"], store=self.store)
        self.assertEqual(missing.exception.status_code, 503)
        target, target_entry = self.fixture()
        # Preserve the legitimate file, then redirect its original pathname.
        import_legacy_reports({"legacy_reports": [target_entry]}, [], store=self.store)
        retained = target.with_name("retained-original-report.json")
        target.rename(retained)
        target.symlink_to(retained)
        with self.assertRaises(LegacyUnavailable) as redirected:
            read_legacy_report("legacy-" + target_entry["import_key"], store=self.store)
        self.assertEqual(redirected.exception.status_code, 503)

    def test_batch_hash_failure_does_not_partially_register(self):
        _, first = self.fixture()
        _, second = self.fixture()
        second["sha256"] = "0" * 64
        with self.assertRaises(Conflict):
            import_legacy_reports({"legacy_reports": [first, second]}, [], store=self.store)
        with self.assertRaises(LegacyUnavailable):
            read_legacy_report("legacy-" + first["import_key"], store=self.store)

    def test_changed_import_key_cannot_duplicate_same_source(self):
        _, entry = self.fixture()
        import_legacy_reports({"legacy_reports": [entry]}, [], store=self.store)
        changed = {**entry, "import_key": "0" * 64}
        with self.assertRaises(Conflict):
            import_legacy_reports({"legacy_reports": [changed]}, [], store=self.store)
        with self.store.transaction() as cursor:
            cursor.execute("SELECT COUNT(*) AS n FROM legacy_report_archive WHERE source_path=%s AND source_sha256=%s", (entry["path"], entry["sha256"]))
            self.assertEqual(cursor.fetchone()["n"], 1)


if __name__ == "__main__":
    unittest.main()
