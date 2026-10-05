"""Regression checks for Hadoop output fidelity and aggregation."""
import shutil
import os
import unittest
import uuid
from pathlib import Path

from pipeline import run_pipeline as pipeline


class HadoopOutputTests(unittest.TestCase):
    def setUp(self):
        self.test_root = Path(__file__).resolve().parents[1] / "outputs" / "tests"
        self.test_root.mkdir(parents=True, exist_ok=True)
        self.root = self.test_root / uuid.uuid4().hex
        self.root.mkdir()

    def tearDown(self):
        # Temporary cleanup is confined to this repository's test-output directory.
        if not self.root.resolve().is_relative_to(self.test_root.resolve()):
            raise RuntimeError("test cleanup escaped its workspace")
        shutil.rmtree(self.root)

    def test_metrics_include_every_reducer_partition(self):
        (self.root / "part-r-00000").write_text("N\t2\nACC\t1\nUNIQ_EXCESS\t1\n", encoding="utf-8")
        (self.root / "part-r-00001").write_text("N\t3\nACC\t3\nUNIQ_EXCESS\t2\n", encoding="utf-8")
        self.assertEqual(pipeline.read_part(self.root), {"N": 5, "ACC": 4, "UNIQ_EXCESS": 3})

    def test_dedup_records_are_counted_even_with_rule_prefix(self):
        part = self.root / "part-r-00000"
        part.write_text(
            "log\tU6|dedup|12|log|U2-U5c|1::M::25::4::01234\n"
            "log\tR7|dedup|30|repair|R1|1::2::4::978000000\n",
            encoding="utf-8",
        )
        self.assertEqual(pipeline.counts_by_rule(part), {("dedup", "U6"): 1, ("dedup", "R7"): 1})

    def test_clean_export_preserves_genres_title_and_origin(self):
        part = self.root / "part-r-00000"
        payload = "1::Léon (1994)::Action|Crime|Drama"
        part.write_text(f"clean\t42|repair|M8|{payload}\n", encoding="utf-8")
        exported = self.root / "movies.dat"
        self.assertEqual(pipeline.write_clean_input(part, exported), 1)
        self.assertEqual(exported.read_text(encoding="utf-8"), payload + "\n")
        pipeline.write_clean_input(part, exported, keep_envelope=True)
        self.assertEqual(exported.read_text(encoding="utf-8"), f"42|repair|M8|{payload}\n")

    def test_input_identity_depends_on_all_tables_not_import_date(self):
        raw = {}
        for table in ("users", "movies", "ratings"):
            path = self.root / (table + ".dat")
            path.write_text(table, encoding="latin-1")
            raw[table] = path
        first = pipeline.input_fingerprint(raw)
        self.assertEqual(first, pipeline.input_fingerprint(raw))
        # Changing only filesystem import timestamps must not create a new input.
        for path in raw.values():
            old = path.stat()
            os.utime(path, ns=(old.st_atime_ns, old.st_mtime_ns + 86400 * 10**9))
        self.assertEqual(first, pipeline.input_fingerprint(raw))
        # Independently mutate every table, including same-size replacements.
        for table, path in raw.items():
            with self.subTest(table=table):
                original = path.read_bytes()
                path.write_bytes(original.upper())
                second = pipeline.input_fingerprint(raw)
                self.assertNotEqual(first["dataset_version"], second["dataset_version"])
                self.assertNotEqual(first["tables"][table]["sha256"], second["tables"][table]["sha256"])
                for other in raw.keys() - {table}:
                    self.assertEqual(first["tables"][other], second["tables"][other])
                path.write_bytes(original)


if __name__ == "__main__":
    unittest.main()
