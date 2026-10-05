"""Final source lineage must conserve duplicates across multiple cleaning stages."""
import importlib.util
import json
import os
import sqlite3
import unittest

from metadata.connection import ROOT


path = os.environ.get("ML_VALIDATION_CANDIDATE")
if path:
    spec = importlib.util.spec_from_file_location("candidate_validation_graph", ROOT / path)
    validation = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(validation)
else:
    from governance import validation


class DedupGraphTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        self.db.executescript("CREATE TABLE sources(id TEXT PRIMARY KEY,t TEXT,state TEXT,related TEXT);"
                              "CREATE TABLE events(id TEXT,n INTEGER,body TEXT);")

    def source(self, identity, state="KEEP", related=(), table="movies"):
        self.db.execute("INSERT INTO sources VALUES (?,?,?,?)", (identity, table, state, json.dumps(list(related))))

    def edge(self, source, target, order=0):
        self.db.execute("INSERT INTO events VALUES (?,?,?)", (source, order, json.dumps({
            "action": "dedup", "target_source_id": target})))

    def verify(self):
        return validation._validate_dedup_graph(self.db)

    def test_movie_id_then_title_dedup_resolves_final_winner(self):
        self.source("first-loser", "DEDUP")
        self.source("second-loser", "DEDUP", ["first-loser"])
        self.source("winner", related=["first-loser", "second-loser"])
        self.edge("first-loser", "second-loser")
        self.edge("second-loser", "winner")
        self.assertEqual(self.verify(), {"dedup_sources": 2, "retained_winners": 1})

    def test_cycle_rejected(self):
        self.source("a", "DEDUP")
        self.source("b", "DEDUP")
        self.edge("a", "b")
        self.edge("b", "a")
        with self.assertRaisesRegex(ValueError, "cycle"):
            self.verify()

    def test_isolated_final_target_is_not_a_retained_winner(self):
        self.source("a", "DEDUP")
        self.source("b", "ISOLATE")
        self.edge("a", "b")
        with self.assertRaisesRegex(ValueError, "retained final winner"):
            self.verify()

    def test_unknown_and_foreign_table_target_rejected(self):
        self.source("a", "DEDUP")
        self.edge("a", "b")
        with self.assertRaisesRegex(ValueError, "graph edge"):
            self.verify()
        self.source("b", table="users")
        with self.assertRaisesRegex(ValueError, "graph edge"):
            self.verify()

    def test_dedup_without_edge_rejected(self):
        self.source("a", "DEDUP")
        with self.assertRaisesRegex(ValueError, "every removed source"):
            self.verify()

    def test_winner_must_preserve_all_duplicate_sources(self):
        self.source("a", "DEDUP")
        self.source("b")
        self.edge("a", "b")
        with self.assertRaisesRegex(ValueError, "omits a merged"):
            self.verify()

    def test_merged_reference_cannot_point_to_other_retained_record(self):
        self.source("a", related=["b"])
        self.source("b")
        with self.assertRaisesRegex(ValueError, "different final winner"):
            self.verify()

    def test_duplicate_edges_or_edge_from_retained_source_rejected(self):
        self.source("a", "DEDUP")
        self.source("b", related=["a"])
        self.edge("a", "b")
        self.edge("a", "b", 1)
        with self.assertRaisesRegex(ValueError, "graph edge"):
            self.verify()

    def test_no_dedup_is_valid(self):
        self.source("a")
        self.assertEqual(self.verify(), {"dedup_sources": 0, "retained_winners": 0})


if __name__ == "__main__":
    unittest.main()
