"""Source gates tested independently of MySQL, Hadoop and Arrow runtimes."""
import copy
import shutil
import uuid
import unittest
from pathlib import Path

from governance.provenance import source_id, validate_origin


class ProvenanceTests(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).resolve().parents[1] / "outputs" / "tests"
        root.mkdir(parents=True, exist_ok=True)
        self.scratch = root / uuid.uuid4().hex
        self.scratch.mkdir()
        self.addCleanup(self.cleanup)
        self.raw = self.scratch / "users.dat"
        self.raw.write_bytes(b"1::M::25::4::01234\r\n2::X::25::4::12345\n")
        self.version = "raw-test"
        self.text = "2::X::25::4::12345"
        self.fixed = "2::NULL::25::4::12345"
        self.offset = len(b"1::M::25::4::01234\r\n")
        self.origin = {"input_version": self.version, "table": "users", "file": self.raw.name,
                       "offset": self.offset, "source_record_id": source_id(self.version, "users", self.raw.name, self.offset),
                       "raw_record": self.text, "related_source_ids": [],
                       "events": [{"rule": "U2", "action": "log", "before": self.text, "after": self.fixed}]}

    def cleanup(self):
        root = Path(__file__).resolve().parents[1] / "outputs" / "tests"
        if not self.scratch.resolve().is_relative_to(root.resolve()):
            raise RuntimeError("Test cleanup escaped its workspace")
        shutil.rmtree(self.scratch)

    def check(self, origin=None, *, payload=None, disposition="KEEP", files=None):
        return validate_origin(self.origin if origin is None else origin, version=self.version, table="users",
                               files=files or {self.raw.name: self.raw}, payload=self.fixed if payload is None else payload,
                               disposition=disposition)

    def test_crlf_original_and_logged_change_preserve_byte_identity(self):
        self.assertEqual(self.check(), self.origin["source_record_id"])

    def test_same_offset_in_different_files_has_distinct_identity(self):
        other = copy.deepcopy(self.origin)
        other["file"] = "nested/users.dat"
        other["source_record_id"] = source_id(self.version, "users", other["file"], self.offset)
        self.assertNotEqual(self.check(), self.check(other, files={other["file"]: self.raw}))

    def test_interior_offset_rejected_even_if_suffix_matches(self):
        origin = copy.deepcopy(self.origin)
        origin["offset"] += 3
        origin["source_record_id"] = source_id(self.version, "users", self.raw.name, origin["offset"])
        origin["raw_record"] = self.text[3:]
        origin["events"] = [{"rule": "U0", "action": "keep", "before": self.text[3:], "after": self.text[3:]}]
        with self.assertRaisesRegex(ValueError, "record boundary"):
            self.check(origin, payload=self.text[3:])

    def test_unknown_version_file_or_identity_rejected(self):
        for key, value in (("input_version", "raw-other"), ("file", "../users.dat"),
                           ("source_record_id", "0" * 64), ("offset", True), ("offset", 9999)):
            with self.subTest(key=key, value=value):
                origin = copy.deepcopy(self.origin)
                origin[key] = value
                with self.assertRaises(ValueError):
                    self.check(origin)

    def test_chain_gap_and_false_keep_rejected(self):
        for action, before in (("log", "unrecorded change"), ("keep", self.text)):
            with self.subTest(action=action):
                origin = copy.deepcopy(self.origin)
                origin["events"][0].update(action=action, before=before)
                with self.assertRaises(ValueError):
                    self.check(origin)

    def test_dedup_chain_requires_other_source_and_final_disposition(self):
        winner = source_id(self.version, "users", self.raw.name, 0)
        origin = copy.deepcopy(self.origin)
        origin["events"].append({"rule": "U6", "action": "dedup", "before": self.fixed,
                                 "after": self.fixed, "target_source_id": winner})
        self.assertEqual(self.check(origin, disposition="DEDUP"), origin["source_record_id"])
        with self.assertRaises(ValueError):
            self.check(origin)
        origin["events"][-1]["target_source_id"] = origin["source_record_id"]
        with self.assertRaises(ValueError):
            self.check(origin, disposition="DEDUP")

    def test_duplicate_or_self_merge_relationship_rejected(self):
        winner = source_id(self.version, "users", self.raw.name, 0)
        for related in ([winner, winner], [self.origin["source_record_id"]]):
            with self.subTest(related=related):
                origin = copy.deepcopy(self.origin)
                origin["related_source_ids"] = related
                with self.assertRaises(ValueError):
                    self.check(origin)


if __name__ == "__main__":
    unittest.main()
