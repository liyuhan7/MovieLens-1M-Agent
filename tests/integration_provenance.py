"""Inspect current-version real YARN outputs locally, without starting external services."""
import json
import unittest
from pathlib import Path

from governance.provenance import validate_origin
from pipeline.run_pipeline import METRIC_VERSION, RULE_VERSION, dispositions, sha256


ROOT = Path(__file__).resolve().parents[1]


def local(path):
    return ROOT / path[6:] if path.startswith("/work/") else Path(path)


class ActualProvenanceTests(unittest.TestCase):
    def test_complete_original_identity_through_movie_second_stage(self):
        candidates = []
        for path in (ROOT / "outputs").glob("run-*/attempts/attempt-*/report.json"):
            report = json.loads(path.read_text(encoding="utf-8"))
            if (report.get("rule_version"), report.get("metric_version"), report.get("execution_mode")) == (RULE_VERSION, METRIC_VERSION, "yarn"):
                candidates.append((path, report))
        self.assertTrue(candidates, "Run current-version integration_yarn.py first")
        candidate, report = max(candidates, key=lambda item: item[0].stat().st_mtime_ns)
        raw = {}
        for table, item in report["input_manifest"]["tables"].items():
            for path in (ROOT / "outputs" / "integration").rglob(table + ".dat"):
                if path.stat().st_size == item["bytes"] and sha256(path) == item["sha256"]:
                    raw[table] = path
                    break
        self.assertEqual(set(raw), {"users", "movies", "ratings"}, "Original synthetic input bytes must still exist")
        source_sets = {}
        balances = {}
        for table in ("users", "movies", "ratings"):
            final = {}
            stages = ("movies_first", "movies") if table == "movies" else (table,)
            for stage in stages:
                for record in dispositions(local(report["disposition_paths"][stage])):
                    state = ("INTERMEDIATE" if stage == "movies_first" and record["stream"] == "clean" else
                             "KEEP" if record["stream"] == "clean" else "DEDUP" if record["action"] == "dedup" else "ISOLATE")
                    identity = validate_origin(record["origin"], version=report["input_data_version"], table=table,
                                               files={raw[table].name: raw[table]}, payload=record["payload"], disposition=state)
                    if stage == "movies":
                        self.assertEqual(final.get(identity), "INTERMEDIATE")
                    else:
                        self.assertNotIn(identity, final)
                    final[identity] = state
            self.assertNotIn("INTERMEDIATE", final.values())
            self.assertEqual(len(final), report["row_change"][table]["raw"])
            self.assertEqual(sum(state == "KEEP" for state in final.values()), report["row_change"][table]["clean"])
            source_sets[table] = set(final)
            balances[table] = {state: sum(value == state for value in final.values()) for state in ("KEEP", "ISOLATE", "DEDUP")}
        self.assertEqual(len(set.union(*source_sets.values())), sum(len(value) for value in source_sets.values()))
        (candidate.parent / "provenance-acceptance.json").write_text(json.dumps({"run_id": report["run_id"],
                "attempt_id": report["attempt_id"], "balances": balances, "verified": "original byte boundaries and full event chains",
                "publication_verified": False}, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
