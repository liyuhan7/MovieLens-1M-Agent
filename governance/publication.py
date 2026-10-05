"""Recover one fixed publication intent across HDFS and MySQL; visibility is SQL-owned."""
import re

from governance.build_identity import capture_execution
from governance.indexing import expected_identity, index_artifacts, verify_evidence_index
from governance.manifest import preflight_artifacts
from governance.validation import validate_content
from metadata.store import Conflict
from storage.hdfs import Hdfs


STAGES = {"extractUsers", "extractMovies", "score-raw-users", "score-raw-movies", "score-raw-ratings",
          "cleanUsers", "cleanMovies", "cleanMoviesTitle", "cleanRatings",
          "score-clean-users", "score-clean-movies", "score-clean-ratings"}


def validate_execution(store, claim, report):
    run = store.get_run(claim.run_id)
    execution = run["request"].get("execution")
    if not execution or report.get("execution") != execution or capture_execution() != execution:
        raise Conflict("Execution build/configuration is missing or differs from fixed request")
    if execution["files"].get("hadoop/build/iter1.jar") != run["request"]["jar_sha256"]:
        raise Conflict("Executable identity does not match request")
    if run["request"]["execution_mode"] != "yarn":
        raise Conflict("Formal publication requires verified HDFS/YARN execution")
    if not all(re.fullmatch(r"sha256:[0-9a-f]{64}", execution["images"].get(key, "")) for key in ("ML_HADOOP_IMAGE", "ML_WORKER_IMAGE")):
        raise Conflict("Formal publication requires fixed Hadoop and worker images")
    jobs = store.list_jobs(claim.attempt_id)
    declared = report.get("jobs", [])
    if len(jobs) != 12 or len(declared) != 12 or {job["stage"] for job in jobs} != STAGES or {job["stage"] for job in declared} != STAGES:
        raise Conflict("Report does not cover the full durable job set")
    by_stage = {job["stage"]: job for job in jobs}
    for job in declared:
        stored = by_stage[job["stage"]]
        if stored["status"] != "SUCCEEDED" or stored["attempt_id"] != claim.attempt_id:
            raise Conflict("A publication job is not confirmed successful for this Attempt")
        for key in ("submission_id", "application_id", "job_id"):
            if not stored[key] or job.get(key) != stored[key]:
                raise Conflict("Report external job identity differs from durable execution")
    with store.transaction() as cursor:
        store.require_lease(cursor, claim)


def publish_artifacts(store, claim, root, *, hdfs=None, progress=None, checkpoint=None):
    """Checkpoint hooks inject crashes in integration tests; production uses none."""
    existing = store.get_publish(claim.run_id)
    if existing and existing["status"] == "PUBLISHED":
        if existing["attempt_id"] != claim.attempt_id:
            raise Conflict("Another Attempt already owns this Run publication")
        store.promote_current(existing["publish_id"])
        return existing
    expected = expected_identity(store, claim)
    verified = preflight_artifacts(root, expected=expected)
    validate_execution(store, claim, verified["report"])
    run = store.get_run(claim.run_id)
    validate_content(root, expected=expected, raw_paths=run["request"]["raw_paths"], progress=progress)
    index_artifacts(store, claim, root, progress=progress)
    verify_evidence_index(store, claim.attempt_id, root, verified["files"])
    intent = store.prepare_publication(claim, verified["manifest"], verified["manifest_sha256"])
    if checkpoint:
        checkpoint("intent", intent)
    hdfs = hdfs or Hdfs()
    verification = {}
    # Manifest is copied last, but no file becomes formally visible until SQL confirms.
    for name in [*sorted(verified["files"]), "manifest.json"]:
        if progress:
            progress("publish-" + name)
        with store.transaction() as cursor:
            store.require_lease(cursor, claim)
        verification[name] = hdfs.put_immutable(root / name, intent["storage_path"] + "/" + name)
        if checkpoint:
            checkpoint("file:" + name, intent)
    if checkpoint:
        checkpoint("materialized", intent)
    # Revalidate local/index contents after materialization. The HDFS bytes were fully
    # hashed against the same immutable manifest; no cross-resource atomicity is assumed.
    if preflight_artifacts(root, expected=expected)["manifest_sha256"] != intent["manifest_hash"]:
        raise Conflict("Candidate changed while publishing")
    verify_evidence_index(store, claim.attempt_id, root, verified["files"])
    store.confirm_publication(claim, intent["publish_id"], verification)
    if checkpoint:
        checkpoint("confirmed", intent)
    store.promote_current(intent["publish_id"])
    return store.get_publish(claim.run_id)
