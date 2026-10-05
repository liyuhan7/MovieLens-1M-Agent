"""Restore only a SQL/Manifest-bound report cache from authoritative HDFS bytes."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from metadata.connection import ROOT
from metadata.store import Store, fingerprint
from storage.hdfs import Hdfs

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--run", required=True)
args = parser.parse_args()
store = Store()
run = store.get_run(args.run)
publication = store.get_publish(args.run)
if not run or run["status"] != "PUBLISHED" or not publication or publication["status"] != "PUBLISHED":
    raise ValueError("Only a formal published Run may populate the report cache")
manifest = publication["manifest"]
if (fingerprint(manifest) != publication["manifest_hash"] or manifest["run_id"] != args.run or
        manifest["attempt_id"] != publication["attempt_id"]):
    raise ValueError("Publication manifest binding differs")
reports = [item for item in manifest["files"] if item["path"] == "report.json"]
if len(reports) != 1:
    raise ValueError("Manifest must declare exactly one report")
attempt = next(row for row in store.list_attempts(args.run) if row["attempt_id"] == publication["attempt_id"])
expected_work = ROOT / "outputs" / args.run / "attempts" / publication["attempt_id"]
if (ROOT / attempt["work_path"]).resolve() != expected_work.resolve():
    raise ValueError("Unexpected published Attempt cache path")
target = expected_work / "artifacts/report.json"
expected = {key: reports[0][key] for key in ("sha256", "bytes")}

def verify(path):
    if path.is_symlink() or path.resolve() != path:
        raise ValueError("Redirected report cache path")
    body = path.read_bytes()
    if {"sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)} != expected:
        raise ValueError("Report cache differs from immutable manifest")

if target.exists():
    verify(target)
else:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name("report-cache-" + uuid.uuid4().hex + ".json")
    Hdfs().get(publication["report_path"], temporary)
    verify(temporary)
    # Only one cache producer is authorized; never overwrite a concurrent writer's result.
    if target.exists():
        raise FileExistsError("Cache was concurrently created; retained verified scratch")
    os.rename(temporary, target)
verify(target)
print(json.dumps({"run_id": args.run, "publish_id": publication["publish_id"],
                  "manifest_sha256": publication["manifest_hash"], "cache": target.relative_to(ROOT).as_posix(),
                  "verified": expected, "source": "formal HDFS report"}), flush=True)
