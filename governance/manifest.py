"""Filesystem and identity preflight; Parquet content and execution lease gates follow separately."""
import hashlib
import json
import re
from pathlib import PurePosixPath


TABLES = ("users", "movies", "ratings")


def required_files():
    required = {"report.json", "quality/groups.parquet", "quality/summary.parquet"}
    for table in TABLES:
        for kind in ("cleaned", "quarantine", "dispositions", "evidence"):
            required.add(f"{kind}/{table}/part-00000.parquet")
        for phase in ("before", "after"):
            required.add(f"quality/{phase}/{table}.parquet")
    return required


def _integer(value, name):
    if type(value) is not int or value < 0:
        raise ValueError(f"Invalid nonnegative count: {name}")
    return value


def _digest(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def preflight_artifacts(root, *, expected):
    """Reject missing, altered or foreign candidates against durable Run identities.

    This returns a verified inventory, NOT publication authorization. It does not
    replace Parquet value/evidence validation or the MySQL fencing transaction.
    """
    root = root.resolve()
    manifest_path = root / "manifest.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ValueError("Missing regular artifact manifest")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != "manifest-v1":
        raise ValueError("Unsupported manifest schema")
    for key in ("run_id", "attempt_id", "input_version", "rule_version", "metric_version"):
        if not expected.get(key) or manifest.get(key) != expected[key]:
            raise ValueError(f"Candidate identity mismatch: {key}")
    if manifest.get("input_manifest") != expected.get("input_manifest"):
        raise ValueError("Candidate input manifest differs from registered bytes")
    if manifest["input_manifest"].get("dataset_version") != expected["input_version"]:
        raise ValueError("Input manifest version mismatch")
    inventory = {}
    for item in manifest.get("files", []):
        name = item.get("path")
        if not isinstance(name, str) or not name or "\\" in name:
            raise ValueError("Invalid artifact-relative path")
        relative = PurePosixPath(name)
        if relative.is_absolute() or ".." in relative.parts or relative.as_posix() != name or name in inventory:
            raise ValueError("Escaped or repeated artifact path")
        path = root / name
        if not path.resolve().is_relative_to(root) or any(parent.is_symlink() for parent in (path, *path.parents) if parent != root):
            raise ValueError("Artifact path traverses a link or escapes its root")
        if not path.is_file() or path.stat().st_size != _integer(item.get("bytes"), name + "/bytes"):
            raise ValueError("Missing artifact or changed byte length: " + name)
        digest = item.get("sha256")
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest) or _digest(path) != digest:
            raise ValueError("Artifact checksum mismatch: " + name)
        if name.endswith(".parquet"):
            if item.get("format") != "parquet" or not re.fullmatch(r"[0-9a-f]{64}", str(item.get("schema_sha256", ""))):
                raise ValueError("Missing Parquet format/schema identity")
            _integer(item.get("rows"), name + "/rows")
        elif name != "report.json" or item.get("format") != "json":
            raise ValueError("Unexpected formal artifact format")
        inventory[name] = item
    if set(inventory) != required_files():
        raise ValueError("Missing required artifacts or unsupported inventory entries")
    actual = {path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file()}
    if actual != set(inventory) | {"manifest.json"}:
        raise ValueError("Artifact directory differs from declared inventory")
    if manifest.get("report") != "report.json":
        raise ValueError("Missing structured report binding")
    report = json.loads((root / "report.json").read_text(encoding="utf-8"))
    for manifest_key, report_key in (("run_id", "run_id"), ("attempt_id", "attempt_id"),
            ("input_version", "input_data_version"), ("rule_version", "rule_version"), ("metric_version", "metric_version")):
        if report.get(report_key) != manifest[manifest_key]:
            raise ValueError("Structured report identity mismatch: " + report_key)
    if report.get("input_manifest") != manifest["input_manifest"] or report.get("schema_version") != "report-v2.0":
        raise ValueError("Structured report input/schema mismatch")
    balance = manifest.get("source_balance", {})
    if set(balance) != set(TABLES) or balance != report.get("source_balance"):
        raise ValueError("Missing or mismatched source balance")
    for table in TABLES:
        counts = balance[table]
        if set(counts) != {"raw", "keep", "isolate", "dedup"}:
            raise ValueError("Invalid source disposition balance")
        for key, value in counts.items():
            _integer(value, table + "/" + key)
        if counts["raw"] != sum(counts[key] for key in ("keep", "isolate", "dedup")):
            raise ValueError("Source conservation failed: " + table)
        rows = report.get("row_change", {}).get(table, {})
        if rows.get("raw") != counts["raw"] or rows.get("clean") != counts["keep"]:
            raise ValueError("Report row counts disagree with source balance")
        for name, count in ((f"cleaned/{table}/part-00000.parquet", counts["keep"]),
                (f"quarantine/{table}/part-00000.parquet", counts["isolate"]),
                (f"dispositions/{table}/part-00000.parquet", counts["raw"]),
                (f"quality/before/{table}.parquet", counts["raw"]),
                (f"quality/after/{table}.parquet", counts["keep"])):
            if inventory[name]["rows"] != count:
                raise ValueError("Artifact row count disagrees with source balance: " + name)
        if inventory[f"evidence/{table}/part-00000.parquet"]["rows"] < counts["raw"]:
            raise ValueError("Evidence cannot cover every original source")
    if inventory["quality/summary.parquet"]["rows"] != 30:
        raise ValueError("Missing quality dimensions")
    if manifest.get("jobs") != report.get("jobs"):
        raise ValueError("Report job lineage mismatch")
    if manifest.get("execution") != report.get("execution"):
        raise ValueError("Report execution build/configuration mismatch")
    return {"manifest": manifest, "report": report, "files": inventory,
            "manifest_sha256": _digest(manifest_path)}
