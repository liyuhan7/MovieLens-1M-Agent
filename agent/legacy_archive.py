"""Explicit legacy report metadata; old success never implies formal publication."""
import hashlib
import json
from pathlib import Path
import re

from metadata.connection import ROOT
from metadata.store import Conflict, Store, canonical, decoded


class LegacyUnavailable(LookupError):
    def __init__(self, message, status_code):
        super().__init__(message)
        self.status_code = status_code


def checked_path(root, relative):
    root = Path(root).resolve()
    if not isinstance(relative, str):
        raise ValueError("Invalid legacy report path")
    path = root / relative
    if (not relative.startswith("outputs/")
            or ".." in Path(relative).parts or path.resolve() != path
            or not path.is_relative_to(root / "outputs") or not path.is_file()):
        raise ValueError("Legacy report must be an existing unredirected outputs file")
    return path


def install_archive_schema(store):
    sql = (ROOT / "metadata/003_legacy_archive.sql").read_text(encoding="utf-8")
    with store.transaction() as cursor:
        cursor.execute("SELECT GET_LOCK('ml_governance_legacy_archive_v1',30) AS acquired")
        if cursor.fetchone()["acquired"] != 1:
            raise RuntimeError("Legacy archive schema is busy")
        try:
            for statement in sql.split(";"):
                if statement.strip():
                    cursor.execute(statement)
        finally:
            cursor.execute("SELECT RELEASE_LOCK('ml_governance_legacy_archive_v1')")


def import_legacy_reports(inventory, registry, *, store=None, root=ROOT):
    """Verify every baseline report before any batch metadata write. No file rewriting."""
    store = store or Store()
    records = []
    for entry in inventory["legacy_reports"]:
        path = checked_path(root, entry["path"])
        content = path.read_bytes()
        if len(content) != entry["bytes"] or hashlib.sha256(content).hexdigest() != entry["sha256"]:
            raise Conflict("Legacy report differs from the recorded baseline")
        report = json.loads(content.decode("utf-8-sig"))
        if not isinstance(report, dict):
            raise ValueError("Legacy report must be an object")
        key = entry["import_key"]
        if not re.fullmatch(r"[0-9a-f]{64}", key):
            raise ValueError("Invalid archive import key")
        expected_key = hashlib.sha256((entry["path"] + "\0" + entry["sha256"]).encode("utf-8")).hexdigest()
        if key != expected_key:
            raise Conflict("Archive import key must be derived from its source path and checksum")
        matches = [row for row in registry if "outputs/" + row.get("report", "") == entry["path"]]
        aliases = [(entry.get("source_tag"), "source_tag"), (report.get("task_id"), "report_task_id"),
                   (report.get("run_id"), "report_run_id")]
        for row in matches:
            aliases.extend(((row.get("tag"), "registry_tag"), (row.get("task_id"), "registry_task_id")))
        aliases = [(alias, kind) for alias, kind in aliases if alias not in (None, "")]
        if any(not isinstance(alias, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,120}", alias) for alias, _ in aliases):
            raise ValueError("Invalid legacy alias")
        aliases = set(aliases)
        provenance = {"baseline": entry, "registry_entries": matches,
                      "reported_task_id": report.get("task_id"), "reported_run_id": report.get("run_id"),
                      "formal_publication_verified": False}
        records.append((key, entry["path"], entry["sha256"], entry["bytes"], provenance, aliases))
    with store.transaction() as cursor:
        for key, path, digest, size, provenance, aliases in records:
            cursor.execute("INSERT INTO legacy_report_archive (import_key,source_path,source_sha256,source_bytes,archive_status,provenance) "
                           "VALUES (%s,%s,%s,%s,'LEGACY_INCOMPLETE',%s) ON DUPLICATE KEY UPDATE import_key=import_key",
                           (key, path, digest, size, canonical(provenance)))
            cursor.execute("SELECT * FROM legacy_report_archive WHERE import_key=%s FOR UPDATE", (key,))
            old = decoded(cursor.fetchone())
            if isinstance(old["provenance"], str):
                old["provenance"] = json.loads(old["provenance"])
            expected = {"source_path": path, "source_sha256": digest, "source_bytes": size,
                        "archive_status": "LEGACY_INCOMPLETE", "provenance": provenance}
            if any(old[field] != value for field, value in expected.items()):
                raise Conflict("An archive identity cannot change content or provenance")
            for alias, kind in sorted(aliases):
                cursor.execute("INSERT IGNORE INTO legacy_report_alias (alias,import_key,alias_kind) VALUES (%s,%s,%s)", (alias, key, kind))
    return {"reports_verified": len(records), "archive_ids": ["legacy-" + row[0] for row in records],
            "formal_publications_created": 0, "files_modified": False}


def resolve_legacy_report(identity, *, store=None):
    store = store or Store()
    if not isinstance(identity, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,120}", identity):
        raise LegacyUnavailable("Invalid legacy report identity", 404)
    with store.transaction() as cursor:
        if re.fullmatch(r"legacy-[0-9a-f]{64}", identity):
            cursor.execute("SELECT * FROM legacy_report_archive WHERE import_key=%s", (identity[7:],))
        else:
            cursor.execute("SELECT DISTINCT a.* FROM legacy_report_archive a JOIN legacy_report_alias x "
                           "ON x.import_key=a.import_key WHERE x.alias=%s", (identity,))
        rows = [decoded(row) for row in cursor.fetchall()]
    if not rows:
        raise LegacyUnavailable("Legacy report not found", 404)
    if len(rows) != 1:
        raise LegacyUnavailable("Legacy alias is ambiguous; use an explicit archive ID", 409)
    return rows[0]


def read_legacy_report(identity, *, store=None, root=ROOT):
    archive = resolve_legacy_report(identity, store=store)
    try:
        content = checked_path(root, archive["source_path"]).read_bytes()
        if len(content) != archive["source_bytes"] or hashlib.sha256(content).hexdigest() != archive["source_sha256"]:
            raise ValueError("Legacy report checksum changed")
        report = json.loads(content.decode("utf-8-sig"))
    except (ValueError, OSError, UnicodeError) as error:
        raise LegacyUnavailable("Legacy report material is unavailable or changed", 503) from error
    return {"archive_id": "legacy-" + archive["import_key"], "archive_status": archive["archive_status"],
            "formal_publication_verified": False, "source_sha256": archive["source_sha256"], "report": report}
