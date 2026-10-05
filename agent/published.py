"""Read a manifest-verified report cache selected by the authoritative Publish.

HDFS owns published bytes. The worker's identical local artifact is a derived
cache; its absence or corruption must never select another Run or candidate.
"""
import hashlib
import json
from pathlib import Path

from metadata.connection import ROOT
from metadata.store import Store, fingerprint


class ReportUnavailable(ValueError):
    def __init__(self, message, status_code):
        super().__init__(message)
        self.status_code = status_code


def read_published_report(run_id, *, store=None, root=ROOT):
    store = store or Store()
    run = store.get_run(run_id)
    if run is None:
        raise ReportUnavailable("run not found", 404)
    publication = store.get_publish(run_id)
    if run["status"] != "PUBLISHED" or not publication or publication["status"] != "PUBLISHED":
        raise ReportUnavailable("report is not formally published", 409)
    if publication["run_id"] != run_id:
        raise ReportUnavailable("publication identity mismatch", 503)
    manifest = publication["manifest"]
    if fingerprint(manifest) != publication["manifest_hash"]:
        raise ReportUnavailable("publication manifest checksum mismatch", 503)
    if manifest.get("run_id") != run_id or manifest.get("attempt_id") != publication["attempt_id"]:
        raise ReportUnavailable("publication manifest identity mismatch", 503)
    attempts = [attempt for attempt in store.list_attempts(run_id)
                if attempt["attempt_id"] == publication["attempt_id"]]
    if len(attempts) != 1:
        raise ReportUnavailable("published Attempt is unavailable", 503)
    root = Path(root).resolve()
    expected_work = root / "outputs" / run_id / "attempts" / publication["attempt_id"]
    work = (root / attempts[0]["work_path"]).resolve()
    if work != expected_work or not work.is_relative_to(root / "outputs"):
        raise ReportUnavailable("published cache location is invalid", 503)
    path = work / "artifacts" / "report.json"
    reports = [item for item in manifest["files"] if item["path"] == "report.json"]
    if len(reports) == 1 and not path.exists() and publication.get("storage_path"):
        from agent.results import artifact_file
        artifact_file(publication, work / "artifacts", "report.json")
    if len(reports) != 1 or not path.is_file() or path.is_symlink():
        raise ReportUnavailable("published report cache is unavailable", 503)
    if path.resolve() != path:
        raise ReportUnavailable("published cache contains a redirected path", 503)
    content = path.read_bytes()
    if len(content) != reports[0]["bytes"] or hashlib.sha256(content).hexdigest() != reports[0]["sha256"]:
        raise ReportUnavailable("published report cache differs from immutable manifest", 503)
    try:
        report = json.loads(content)
    except (ValueError, UnicodeError) as error:
        raise ReportUnavailable("published report is unreadable", 503) from error
    identity = {"run_id": run_id, "attempt_id": publication["attempt_id"],
                "input_data_version": run["input_version"], "rule_version": run["rule_version"],
                "metric_version": run["metric_version"], "output_data_version": publication["output_version"]}
    if any(report.get(key) != value for key, value in identity.items()):
        raise ReportUnavailable("published report version binding is invalid", 503)
    return content, report, publication
