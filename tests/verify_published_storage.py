"""Read-only full-byte audit of explicitly selected published Runs."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from metadata.connection import ROOT
from metadata.store import Store
from storage.hdfs import Hdfs


def verify(store, hdfs, run_id):
    run = store.get_run(run_id)
    publication = store.get_publish(run_id)
    if not run or run["status"] != "PUBLISHED" or not publication or publication["status"] != "PUBLISHED":
        raise ValueError("Run has not been formally published: " + run_id)
    manifest = publication["manifest"]
    entries = {item["path"]: item for item in manifest["files"]}
    root = publication["storage_path"]
    expected_names = set(entries) | {"manifest.json"}
    inventory = hdfs.inventory(roots=(root,))
    actual_names = {item["path"][len(root) + 1:] for item in inventory}
    if actual_names != expected_names:
        raise ValueError("Published inventory omits or adds files")
    checked = {}
    for name in sorted(expected_names):
        observed = hdfs.digest(root + "/" + name)
        if name == "manifest.json":
            if observed["sha256"] != publication["manifest_hash"]:
                raise ValueError("Published manifest differs from SQL")
        elif observed != {key: entries[name][key] for key in ("sha256", "bytes")}:
            raise ValueError("Published bytes differ from immutable manifest: " + name)
        checked[name] = observed
        print("VERIFIED " + run_id + " " + name, flush=True)
    return {"run_id": run_id, "attempt_id": publication["attempt_id"],
            "publish_id": publication["publish_id"], "storage_path": root,
            "manifest_sha256": publication["manifest_hash"], "files": checked}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", action="append", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    target = (ROOT / args.output).resolve()
    if not target.is_relative_to(ROOT / "outputs"):
        raise ValueError("Audit output must stay in the workspace outputs directory")
    store, hdfs = Store(), Hdfs()
    checked = [verify(store, hdfs, run_id) for run_id in args.run]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps({"scope": store.config["database"], "publications": checked}, indent=2), encoding="utf-8")
    print("PUBLISHED_STORAGE_ACCEPTANCE=" + str(target), flush=True)


if __name__ == "__main__":
    main()
