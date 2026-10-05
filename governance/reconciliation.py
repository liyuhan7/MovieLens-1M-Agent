"""Read-only identification of protected, recovering and unreferenced storage."""
import collections
import re
from pathlib import PurePosixPath


def classify_inventory(inventory, references):
    raw = []
    attempts = {}
    publications = []
    for reference in references:
        scope = reference["scope"]
        raw.extend({**row, "scope": scope} for row in reference["raw"])
        for row in reference["attempts"]:
            attempts.setdefault((row["run_id"], row["attempt_id"]), []).append({**row, "scope": scope})
        publications.extend({**row, "scope": scope} for row in reference["publications"])
    # Transfer audit snapshots preserve abandoned partial generations. Their
    # intent is historical, not a second live publication requiring completion.
    latest_tokens = {}
    for row in publications:
        key = (row["scope"], row["publish_id"])
        latest_tokens[key] = max(latest_tokens.get(key, -1), row.get("fencing_token", 0))
    for row in publications:
        row["superseded"] = row.get("fencing_token", 0) < latest_tokens[(row["scope"], row["publish_id"])]
    publications.sort(key=lambda row: (row["superseded"], row["status"] != "PUBLISHED", -len(row["storage_path"])))
    entries = []
    by_path = {}
    for item in inventory:
        path = item["path"]
        parsed = PurePosixPath(path)
        if not parsed.is_absolute() or ".." in parsed.parts or not path.startswith("/ml/"):
            raise ValueError("Inventory path escaped governance storage")
        if path in by_path:
            raise ValueError("Repeated namespace inventory path")
        by_path[path] = item
        matched = next((row for row in publications if path.startswith(row["storage_path"] + "/")), None)
        if matched:
            state = ("RETAINED_PUBLICATION_HISTORY" if matched["superseded"] else
                     "PROTECTED_PUBLISHED" if matched["status"] == "PUBLISHED" else "PENDING_PUBLICATION")
            detail = {"publish_id": matched["publish_id"], "run_id": matched["run_id"], "scope": matched["scope"]}
        elif matched := next((row for row in raw if path == row["storage_uri"] or path.startswith(row["storage_uri"] + "/")), None):
            state, detail = "PROTECTED_RAW", {"version_id": matched["version_id"], "scope": matched["scope"]}
        elif match := re.match(r"^/ml/staging/run=([^/]+)/attempt=([^/]+)/", path):
            owners = attempts.get(match.groups(), [])
            if owners:
                active = next((row for row in owners if row["status"] not in {"FAILED", "PUBLISHED"}), None)
                if active:
                    state = "ACTIVE_ATTEMPT" if active["lease_alive"] else "RECOVERY_REQUIRED"
                else:
                    state = "RETAINED_ATTEMPT_HISTORY"
                detail = {"run_id": match[1], "attempt_id": match[2], "scopes": [row["scope"] for row in owners]}
            else:
                state, detail = "UNREGISTERED_ATTEMPT_REVIEW", {"run_id": match[1], "attempt_id": match[2]}
        elif path.startswith("/ml/staging/uploads/"):
            state, detail = "UNREFERENCED_UPLOAD_REVIEW", {"reason": "upload object has no durable ownership marker"}
        else:
            state, detail = "UNREGISTERED_PATH_REVIEW", {"reason": "no reference in selected metadata scopes"}
        entries.append({**item, "classification": state, "reference": detail})
    incomplete, mismatches = [], []
    for publish in publications:
        if publish["superseded"]:
            continue
        expected = {row["path"]: row for row in publish["manifest"]["files"]}
        expected["manifest.json"] = None
        missing = []
        for name, row in expected.items():
            path = publish["storage_path"] + "/" + name
            item = by_path.get(path)
            if not item:
                missing.append(name)
            elif row and item["bytes"] != row["bytes"]:
                mismatches.append({"publish_id": publish["publish_id"], "path": path, "kind": "byte_length"})
        if missing:
            incomplete.append({"publish_id": publish["publish_id"], "status": publish["status"], "missing": missing})
        for path in by_path:
            if path.startswith(publish["storage_path"] + "/") and path[len(publish["storage_path"]) + 1:] not in expected:
                mismatches.append({"publish_id": publish["publish_id"], "path": path, "kind": "undeclared_file"})
    return {"schema_version": "storage-reconciliation-v1", "metadata_scopes": [row["scope"] for row in references],
            "files": entries, "counts": dict(collections.Counter(row["classification"] for row in entries)),
            "incomplete_publications": incomplete, "publication_mismatches": mismatches,
            "deletion_authorized": False, "checksums_verified": False}


def scan_storage(stores, *, hdfs=None):
    from storage.hdfs import Hdfs
    hdfs = hdfs or Hdfs()
    return classify_inventory(hdfs.inventory(), [store.storage_references() for store in stores])
