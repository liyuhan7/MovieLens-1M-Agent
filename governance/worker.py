"""Single durable worker. Uncertain computation is reconciled, never blindly retried."""
import argparse
import json
import threading
import time
import uuid

from metadata.connection import ROOT
from metadata.store import LostLease, Store
from pipeline.execution import LocalExecutor
from pipeline.run_pipeline import input_fingerprint, pipeline, sha256
from pipeline.yarn import YarnExecutor, RecoveryPending
from governance.artifacts import build_artifacts
from governance.build_identity import capture_execution
from governance.publication import publish_artifacts


class RecordedLocalExecutor(LocalExecutor):
    def __init__(self, store, claim, *, reducers=1):
        super().__init__(ROOT, ROOT / claim.work_path / "jobs", reducers=reducers)
        self.store, self.claim = store, claim

    def run(self, stage, input_path, *, refs=(), encoding="ISO-8859-1", name=None):
        logical_stage = name or stage
        specification = {"job": stage, "input": str(input_path), "encoding": encoding,
                         "references": [str(path) for path in refs], "mode": "local"}
        job = self.store.prepare_job(self.claim, logical_stage, specification)
        if job["status"] != "SUBMITTING":
            raise RuntimeError("Existing local job requires explicit reconciliation; use YARN for restart recovery")
        try:
            output = super().run(stage, input_path, refs=refs, encoding=encoding, name=name)
        except (OSError, RuntimeError) as error:
            # A child may have started before connectivity or observation was lost.
            self.store.update_job(self.claim, job["submission_id"], "UNKNOWN", detail={"error": str(error)})
            raise
        result = self.jobs[-1]
        self.store.update_job(self.claim, job["submission_id"], "SUCCEEDED", job_id=result.get("job_id"), detail=result)
        return output


def execute_one(store, owner):
    store.reconcile_current()
    claim = store.claim(owner, environment={"worker": "governance.worker", "host_mode": "yarn"})
    if claim is None:
        return False
    stop = threading.Event()
    lost = []

    def heartbeat():
        while not stop.wait(8):
            try:
                store.heartbeat(claim)
            except Exception as error:
                lost.append(error)
                stop.set()

    thread = threading.Thread(target=heartbeat, daemon=True)
    thread.start()

    def progress(stage):
        if lost:
            raise LostLease(str(lost[0]))
        store.advance(claim, stage)

    try:
        run = store.get_run(claim.run_id)
        request = run["request"]
        publication_recovery = claim.recovering and claim.status in {"VALIDATING", "PUBLISHING"}
        if claim.recovering and not publication_recovery:
            store.advance(claim, "reconciling-existing-jobs", "RUNNING")
        raw = {key: ROOT / path for key, path in request["raw_paths"].items()}
        if input_fingerprint(raw)["dataset_version"] != request["input_version"]:
            raise ValueError("Input files changed after submission")
        if sha256(ROOT / "hadoop" / "build" / "iter1.jar") != request["jar_sha256"]:
            raise ValueError("Executable changed after submission")
        if capture_execution() != request.get("execution"):
            raise ValueError("Execution materials differ from the fixed submission")
        if publication_recovery:
            candidate = ROOT / claim.work_path / "report.json"
            report = json.loads(candidate.read_text(encoding="utf-8"))
            report["candidate_report"] = str(candidate)
        else:
            executor = YarnExecutor(store, claim) if request["execution_mode"] == "yarn" else RecordedLocalExecutor(store, claim)
            report = pipeline(run_id=claim.run_id, attempt_id=claim.attempt_id, raw=raw,
                              executor=executor, register=False, progress=progress, resume=claim.recovering)
            store.advance(claim, "validating-artifacts", "VALIDATING")
        root, _ = build_artifacts(report, request["raw_paths"], progress=progress)
        published = publish_artifacts(store, claim, root, progress=progress)
        print(json.dumps({"run_id": claim.run_id, "status": published["status"], "publish_id": published["publish_id"]}), flush=True)
    except RecoveryPending as error:
        if store.get_run(claim.run_id)["status"] in {"RUNNING", "RECOVERING"}:
            store.advance(claim, "execution-needs-reconciliation", "RECOVERING", {"message": str(error)})
        print(f"RECOVERY_REQUIRED run={claim.run_id}: {error}", flush=True)
    except LostLease as error:
        print(f"LOST_LEASE run={claim.run_id}: {error}", flush=True)
    except Exception as error:
        jobs = store.list_jobs(claim.attempt_id)
        current = store.get_run(claim.run_id)
        intent = store.get_publish(claim.run_id)
        if current["status"] == "PUBLISHED":
            print(f"CURRENT_RECONCILIATION_REQUIRED run={claim.run_id}: {error}", flush=True)
        elif intent:
            store.advance(claim, "publication-needs-reconciliation", "PUBLISHING", {"message": str(error)})
        elif any(job["status"] in {"SUBMITTING", "UNKNOWN", "SUBMITTED", "RUNNING"} for job in jobs):
            store.advance(claim, "execution-needs-reconciliation", "RECOVERING", {"message": str(error)})
        else:
            store.advance(claim, "failed", "FAILED", {"message": str(error)})
        print(f"EXECUTION_INCOMPLETE run={claim.run_id}: {error}", flush=True)
    finally:
        stop.set()
        thread.join(timeout=10)
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    store = Store()
    owner = "worker-" + uuid.uuid4().hex
    while True:
        execute_one(store, owner)
        if args.once:
            return
        time.sleep(2)


if __name__ == "__main__":
    main()
