"""Real recursive HDFS/YARN computation and provenance for equal offsets in two files."""
import json
import unittest
import uuid

import pyarrow.parquet as pq

from governance.artifacts import build_artifacts
from governance.indexing import expected_identity, index_artifacts
from governance.service import submit_run
from governance.validation import validate_content
from metadata.connection import ROOT, load_config
from metadata.store import Store
from pipeline.run_pipeline import input_fingerprint, pipeline
from pipeline.yarn import YarnExecutor
from storage.hdfs import import_input


class MultiFileTests(unittest.TestCase):
    def test_nested_files_equal_offsets_and_deterministic_ties(self):
        config = load_config()
        self.assertEqual(config["database"], "ml_governance_test")
        store = Store(config)
        fixture = ROOT / "outputs/multifile-integration" / uuid.uuid4().hex
        raw = {table: fixture / table for table in ("users", "movies", "ratings")}
        lines = {"users": ("1::M::25::4::01234\n", "1::M::25::4::01234\n2::NULL::18::3::12345\n"),
                 "movies": ("1::Léon (1994)::Action|Crime|Drama\n", "1::Léon (1994)::Action|Crime|Drama\n2::Toy Story (1995)::Animation|Children's|Comedy\n"),
                 "ratings": ("1::1::5::978000001\n", "1::1::5::978000001\n2::2::3::978000002\n")}
        for table, directory in raw.items():
            (directory / "nested").mkdir(parents=True)
            (directory / "a.dat").write_bytes(lines[table][0].encode("latin-1"))
            (directory / "nested/b.dat").write_bytes(lines[table][1].encode("latin-1"))
            (directory / "_ignored.dat").write_bytes(b"this must not be read by Hadoop\n")
        manifest = input_fingerprint(raw)
        store.register_input("movielens-1m", manifest["dataset_version"], manifest)
        paths = {table: path.relative_to(ROOT).as_posix() for table, path in raw.items()}
        import_input(store, manifest["dataset_version"], paths)
        run, _ = submit_run("multifile-" + uuid.uuid4().hex, store=store, raw=raw)
        claim = store.claim("multifile-acceptance", lease_seconds=3600)
        self.assertEqual(claim.run_id, run["run_id"])
        try:
            report = pipeline(run_id=claim.run_id, attempt_id=claim.attempt_id, raw=raw,
                              executor=YarnExecutor(store, claim, reducers=2), register=False)
            store.advance(claim, "multifile-validation", "VALIDATING")
            root, output = build_artifacts(report, paths)
            result = validate_content(root, expected=expected_identity(store, claim), raw_paths=paths)
            index = index_artifacts(store, claim, root)
            for table in raw:
                self.assertEqual(output["source_balance"][table], {"raw": 3, "keep": 2, "isolate": 0, "dedup": 1})
                sources = pq.ParquetFile(root / f"dispositions/{table}/part-00000.parquet").read().to_pylist()
                zero = [row for row in sources if row["source_offset"] == 0]
                self.assertEqual({row["source_file"] for row in zero}, {"a.dat", "nested/b.dat"})
                self.assertEqual(len({row["source_record_id"] for row in zero}), 2)
                self.assertEqual(next(row for row in zero if row["disposition"] == "KEEP")["source_file"], "a.dat")
                kept = pq.ParquetFile(root / f"cleaned/{table}/part-00000.parquet").read().to_pylist()
                self.assertTrue(next(row for row in kept if row["source_file"] == "a.dat")["related_source_ids"])
            (fixture / "acceptance.json").write_text(json.dumps({"run_id": claim.run_id, "attempt_id": claim.attempt_id,
                    "source_balance": output["source_balance"], "jobs": len(report["jobs"]),
                    "content_validation": result, "index": index, "publication_verified": False}, indent=2), encoding="utf-8")
        finally:
            store.advance(claim, "multifile-test-ended", "FAILED", {"reason": "synthetic multifile acceptance; not published"})


if __name__ == "__main__":
    unittest.main()
