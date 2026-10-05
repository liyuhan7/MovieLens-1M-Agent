"""Tracked YARN submission and resumption using durable submission identities."""
import json
import os
import re
import subprocess
import time
from pathlib import Path

import httpx

from metadata.connection import ROOT
from storage.hdfs import Hdfs


class RecoveryPending(RuntimeError):
    """Execution is still running or its submission cannot be proven absent."""


class YarnExecutor:
    def __init__(self, store, claim, *, reducers=2):
        self.store, self.claim = store, claim
        self.work = ROOT / claim.work_path / "jobs"
        self.remote = f"/ml/staging/run={claim.run_id}/attempt={claim.attempt_id}"
        self.hdfs = Hdfs()
        self.jobs = []
        self.reducers = reducers
        self.rm_url = os.environ.get("ML_YARN_URL", "http://ml-governance-hadoop:8088").rstrip("/")
        self.history_url = os.environ.get("ML_JOBHISTORY_URL", "http://ml-governance-hadoop:19888").rstrip("/")

    @staticmethod
    def history_application(job):
        if not re.fullmatch(r"job_[0-9]+_[0-9]+", job.get("id", "")):
            raise RecoveryPending("JobHistory returned an invalid external identity")
        status = job.get("state")
        if status not in {"SUCCEEDED", "FAILED", "KILLED", "ERROR"}:
            raise RecoveryPending("JobHistory has no confirmed terminal execution")
        status = "FAILED" if status == "ERROR" else status
        return {"id": job["id"].replace("job_", "application_", 1), "name": job["name"],
                "state": "FINISHED" if status == "SUCCEEDED" else status, "finalStatus": status,
                "diagnostics": job.get("diagnostics", ""), "history_source": "JobHistory",
                "history_job": job}

    def applications(self, submission_id):
        # Exact job name is independent of the API/worker process and survives the
        # window between external submission and saving the returned application ID.
        matches = {}
        with httpx.Client(trust_env=False, timeout=20) as client:
            try:
                response = client.get(self.rm_url + "/ws/v1/cluster/apps", params={"applicationTypes": "MAPREDUCE"})
                response.raise_for_status()
                for app in (response.json().get("apps") or {}).get("app", []):
                    if app["name"] == submission_id:
                        matches[app["id"]] = app
            except httpx.HTTPError:
                # A history match can recover a completed submission even while
                # RM is unavailable. Neither service returning a match proves
                # that a submission never happened.
                pass
            try:
                response = client.get(self.history_url + "/ws/v1/history/mapreduce/jobs")
                response.raise_for_status()
                for job in (response.json().get("jobs") or {}).get("job", []):
                    # History's filename index may truncate a name. A prefix
                    # only selects candidates; the full Job API must match.
                    if job.get("name") and submission_id.startswith(job["name"]):
                        try:
                            detail = client.get(self.history_url + "/ws/v1/history/mapreduce/jobs/" + job["id"])
                            detail.raise_for_status()
                        except httpx.HTTPError as error:
                            raise RecoveryPending("A candidate history identity could not be confirmed") from error
                        full_job = detail.json()["job"]
                        if full_job.get("id") != job["id"] or full_job.get("name") != submission_id:
                            continue
                        app = self.history_application(full_job)
                        previous = matches.get(app["id"])
                        if previous and previous.get("finalStatus") not in {None, "UNDEFINED", app["finalStatus"]}:
                            raise RecoveryPending("RM and JobHistory disagree on terminal execution")
                        matches[app["id"]] = app
            except httpx.HTTPError:
                pass
        return list(matches.values())

    def application(self, app_id, submission_id=None):
        if not re.fullmatch(r"application_[0-9]+_[0-9]+", app_id):
            raise RecoveryPending("Invalid durable application identity")
        with httpx.Client(trust_env=False, timeout=20) as client:
            try:
                response = client.get(self.rm_url + "/ws/v1/cluster/apps/" + app_id)
                response.raise_for_status()
                app = response.json()["app"]
            except httpx.HTTPError:
                job_id = app_id.replace("application_", "job_", 1)
                try:
                    response = client.get(self.history_url + "/ws/v1/history/mapreduce/jobs/" + job_id)
                    response.raise_for_status()
                except httpx.HTTPError as error:
                    raise RecoveryPending("Neither RM nor JobHistory confirms the recorded execution") from error
                app = self.history_application(response.json()["job"])
            if app.get("id") != app_id or submission_id is not None and app.get("name") != submission_id:
                raise RecoveryPending("External execution differs from its durable submission identity")
            return app

    def collect(self, remote, local):
        if not self.hdfs.exists(remote + "/_SUCCESS"):
            raise RuntimeError("YARN success is missing its committed output marker")
        if local.exists():
            # Recheck output against HDFS instead of trusting an old local cache.
            partitions = list(local.glob("part-*-*"))
            remote_parts = {Path(path).name for path in self.hdfs.files(remote) if Path(path).name.startswith("part-")}
            if not (local / "_SUCCESS").is_file() or not partitions:
                raise RuntimeError("Incomplete local output cache; manual reconciliation required")
            if {path.name for path in partitions} != remote_parts:
                raise RuntimeError("Local output cache omits or adds a committed partition")
            from pipeline.run_pipeline import sha256
            for part in partitions:
                if self.hdfs.digest(remote + "/" + part.name)["sha256"] != sha256(part):
                    raise RuntimeError("Local output cache differs from committed HDFS output")
        else:
            self.hdfs.get(remote, local)
        return local

    def run(self, stage, input_path, *, refs=(), encoding="ISO-8859-1", name=None):
        self.work.mkdir(parents=True, exist_ok=True)
        name = name or stage
        local = self.work / name
        remote = self.remote + "/jobs/" + name
        input_remote = self.remote + "/inputs/" + Path(input_path).name
        # Raw can be read directly from its immutable registered HDFS location.
        run = self.store.get_run(self.claim.run_id)
        locations = self.store.input_locations(run["input_version"])
        table = next((key for key, path in run["request"]["raw_paths"].items()
                      if (ROOT / path).resolve() == Path(input_path).resolve()), None)
        if table:
            input_remote = locations[table]["storage_uri"]
        else:
            self.hdfs.put_immutable(input_path, input_remote)
        ref_uris = []
        for ref in refs:
            uri = self.remote + "/references/" + Path(ref).name
            self.hdfs.put_immutable(ref, uri)
            ref_uris.append("hdfs://ml-governance-hadoop:9000" + uri)
        specification = {"job": stage, "input": input_remote, "output": remote,
                         "encoding": encoding, "references": ref_uris, "reducers": self.reducers}
        job = self.store.prepare_job(self.claim, name, specification)
        if job["status"] == "SUCCEEDED":
            output = self.collect(remote, local)
            self.jobs.append({"stage": name, "mode": "yarn", "job_id": job["job_id"],
                              "application_id": job["application_id"], "submission_id": job["submission_id"],
                              "hdfs_output": remote})
            return output
        if job["status"] in {"FAILED", "KILLED"}:
            raise RuntimeError(f"Existing job is terminal: {job['status']}")
        app_id = job["application_id"]
        if not app_id and not job["created"]:
            candidates = self.applications(job["submission_id"])
            if len(candidates) != 1:
                raise RecoveryPending("Submission identity cannot be resolved uniquely; refusing duplicate submission")
            app_id = candidates[0]["id"]
        if not app_id:
            arguments = ["hadoop", "jar", str(ROOT / "hadoop" / "build" / "iter1.jar"), "mliter1.IterationOne",
                         "-Dml.submission.id=" + job["submission_id"],
                         "-Dmapreduce.job.tags=" + job["submission_id"],
                         "-Dml.input.encoding=" + encoding,
                         "-Dml.input.version=" + run["input_version"],
                         "-Dml.source.root=" + str(input_remote if Path(input_path).is_dir() else Path(input_remote).parent),
                         "-Dmapreduce.input.fileinputformat.input.dir.recursive=true",
                         "-Dmapreduce.job.reduces=" + str(self.reducers)]
            if ref_uris:
                arguments += ["-files", ",".join(ref_uris)]
            arguments += [stage, input_remote, remote]
            logs = self.work / "logs"
            logs.mkdir(exist_ok=True)
            log_file = logs / (name + ".log")
            # Store IDs as soon as the client emits them, before waiting for completion.
            with log_file.open("w", encoding="utf-8") as stream:
                process = subprocess.Popen(arguments, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                           text=True, encoding="utf-8", errors="replace")
                for line in process.stdout:
                    stream.write(line)
                    stream.flush()
                    if line.startswith("ML_JOB "):
                        submitted = json.loads(line[7:])
                        app_id = submitted["application_id"]
                        self.store.update_job(self.claim, job["submission_id"], "SUBMITTED",
                                              job_id=submitted["job_id"], application_id=app_id,
                                              detail={"log": str(log_file.relative_to(ROOT)), **submitted})
                code = process.wait(timeout=30)
                process.stdout.close()
            if not app_id:
                self.store.update_job(self.claim, job["submission_id"], "UNKNOWN", detail={"client_exit_code": code})
                raise RecoveryPending("Client ended without a proven application identity")
        self.store.update_job(self.claim, job["submission_id"], "RUNNING", application_id=app_id,
                              job_id=app_id.replace("application_", "job_"))
        deadline = time.monotonic() + 7200
        while True:
            app = self.application(app_id, job["submission_id"])
            if app["state"] in {"FINISHED", "FAILED", "KILLED"}:
                status = app["finalStatus"]
                if status not in {"SUCCEEDED", "FAILED", "KILLED"}:
                    raise RecoveryPending("External terminal execution has no confirmed outcome")
                self.store.update_job(self.claim, job["submission_id"], status, application_id=app_id, detail=app)
                if status != "SUCCEEDED":
                    raise RuntimeError(f"YARN {app_id} ended {status}: {app.get('diagnostics', '')[-1200:]}")
                self.jobs.append({"stage": name, "mode": "yarn", "job_id": app_id.replace("application_", "job_"),
                                  "application_id": app_id, "submission_id": job["submission_id"], "hdfs_output": remote})
                return self.collect(remote, local)
            if time.monotonic() >= deadline:
                raise RecoveryPending("YARN still active after observation deadline")
            time.sleep(2)
