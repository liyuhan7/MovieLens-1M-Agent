"""Multi-file version identities preserve single-file history and ignore Hadoop hidden files."""
import hashlib
import json
import shutil
import unittest
import uuid
from pathlib import Path

from governance.raw import input_files
from pipeline.run_pipeline import input_fingerprint, sha256


class RawInventoryTests(unittest.TestCase):
    def setUp(self):
        self.parent = Path(__file__).resolve().parents[1] / "outputs/tests"
        self.root = self.parent / uuid.uuid4().hex
        self.root.mkdir(parents=True)
        self.addCleanup(self.cleanup)

    def cleanup(self):
        if not self.root.resolve().is_relative_to(self.parent.resolve()):
            raise RuntimeError("Test cleanup escaped its workspace")
        shutil.rmtree(self.root)

    def test_single_file_version_is_backward_compatible(self):
        file = self.root / "users.dat"
        file.write_bytes(b"1::M::25::4::01234\n")
        tables = {"users": {"sha256": sha256(file), "bytes": file.stat().st_size, "encoding": "ISO-8859-1"}}
        legacy = "raw-" + hashlib.sha256(json.dumps(tables, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
        self.assertEqual(input_fingerprint({"users": file}), {"dataset_version": legacy, "tables": tables})

    def test_nested_inventory_ignores_hidden_files_and_root_location(self):
        (self.root / "nested").mkdir()
        (self.root / "a.dat").write_bytes(b"one\n")
        (self.root / "nested/b.dat").write_bytes(b"two\n")
        before = input_fingerprint({"users": self.root})
        (self.root / "_SUCCESS").write_bytes(b"ignored")
        (self.root / ".hidden").mkdir()
        (self.root / ".hidden/secret.dat").write_bytes(b"ignored")
        self.assertEqual(input_fingerprint({"users": self.root}), before)
        moved = self.parent / uuid.uuid4().hex
        shutil.copytree(self.root, moved)
        try:
            self.assertEqual(input_fingerprint({"users": moved}), before)
        finally:
            if not moved.resolve().is_relative_to(self.parent.resolve()):
                raise RuntimeError("Test cleanup escaped workspace")
            shutil.rmtree(moved)
        self.assertEqual(list(input_files(self.root)), ["a.dat", "nested/b.dat"])

    def test_member_content_and_relative_name_change_version(self):
        file = self.root / "a.dat"
        file.write_bytes(b"one\n")
        original = input_fingerprint({"users": self.root})["dataset_version"]
        file.write_bytes(b"two\n")
        changed = input_fingerprint({"users": self.root})["dataset_version"]
        self.assertNotEqual(original, changed)
        file.rename(self.root / "b.dat")
        self.assertNotEqual(changed, input_fingerprint({"users": self.root})["dataset_version"])


if __name__ == "__main__":
    unittest.main()
