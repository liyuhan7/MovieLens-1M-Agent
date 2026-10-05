"""Run the actual watchdog probe against large process lists and missing daemons."""
import hashlib
import json
import os
import uuid
from pathlib import Path
import subprocess
import unittest
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def probe(path, present):
    line = next(line.strip() for line in path.read_text(encoding="utf-8").splitlines()
                if line.strip().startswith("if ! ps -eo args"))
    command = line[len("if ! "):-len("; then")]
    producer = "awk 'BEGIN {" + ('print "namenode";' if present else '') + \
               'for(i=0;i<20000;i++) print "synthetic unrelated process"}' + "'"
    return subprocess.run(["bash", "-c", "set -o pipefail; service=namenode; " +
                           command.replace("ps -eo args", producer)], capture_output=True).returncode


class RuntimeHealthProbeTests(unittest.TestCase):
    @unittest.skipIf(os.name == "nt", "Watchdog requires the deployed Linux bash/grep runtime")
    def test_original_false_positive_and_candidate_drains_pipeline(self):
        candidate = ROOT / "runtime/start-hadoop.sh"
        # The former grep -q pipeline is a reproducible fixture, not a dependency
        # on an ignored/deleted historical execution-material directory.
        with tempfile.TemporaryDirectory() as scratch:
            original = Path(scratch) / 'old-probe.sh'
            original.write_text('if ! ps -eo args | grep -v grep | grep -qi "${service}"; then\n',encoding='utf-8')
            old = probe(original, True)
        fixed = probe(candidate, True)
        missing = probe(candidate, False)
        self.assertEqual(old, 141)
        self.assertEqual(fixed, 0)
        self.assertEqual(missing, 1)
        target = ROOT / "outputs/runtime-health-probe" / uuid.uuid4().hex
        target.mkdir(parents=True)
        (target / "acceptance.json").write_text(json.dumps({
            "original_present_service_status": old, "candidate_present_service_status": fixed,
            "candidate_missing_service_status": missing, "synthetic_process_lines": 20001,
            "candidate_sha256": hashlib.sha256(candidate.read_bytes()).hexdigest(),
            "production_script_applied": True}, indent=2), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
