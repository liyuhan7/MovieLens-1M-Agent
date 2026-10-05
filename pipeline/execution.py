"""Hadoop local execution for regression checks; YARN execution is added separately."""
import json
import subprocess
from pathlib import Path


class LocalExecutor:
    def __init__(self, root, work, image="movielens-hadoop:3.3.6", reducers=1):
        self.root = Path(root).resolve()
        self.work = Path(work).resolve()
        if not self.work.is_relative_to(self.root / "outputs"):
            raise ValueError("Execution output must be inside outputs/")
        self.image = image
        self.reducers = reducers
        self.jobs = []

    def container_path(self, path):
        return "/work/" + Path(path).resolve().relative_to(self.root).as_posix()

    def run(self, stage, input_path, *, refs=(), encoding="ISO-8859-1", name=None):
        self.work.mkdir(parents=True, exist_ok=True)
        name = name or stage
        output = self.work / name
        if output.exists():
            raise FileExistsError(f"Attempt stage output already exists: {output}")
        arguments = ["docker", "run", "--rm", "-v", f"{self.root.as_posix()}:/work",
                     "-w", "/work", "--entrypoint", "hadoop", self.image,
                     "jar", "/work/hadoop/build/iter1.jar", "mliter1.IterationOne",
                     "-Dml.input.encoding=" + encoding,
                     "-Dml.input.version=" + getattr(self, "input_version", "unregistered"),
                     "-Dml.source.root=" + self.container_path(input_path if Path(input_path).is_dir() else Path(input_path).parent),
                     "-Dmapreduce.input.fileinputformat.input.dir.recursive=true",
                     "-Dmapreduce.job.reduces=" + str(self.reducers)]
        if refs:
            arguments.extend(["-files", ",".join(self.container_path(path) for path in refs)])
        arguments.extend([stage, self.container_path(input_path), self.container_path(output)])
        result = subprocess.run(arguments, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, encoding="utf-8", errors="replace", timeout=7200)
        logs = self.work / "logs"
        logs.mkdir(exist_ok=True)
        (logs / (name + ".log")).write_text(result.stdout, encoding="utf-8")
        job = {"stage": name, "job_name": stage, "mode": "local", "exit_code": result.returncode,
               "log": str(logs / (name + ".log"))}
        for line in result.stdout.splitlines():
            if line.startswith("ML_JOB "):
                job.update(json.loads(line[7:]))
        self.jobs.append(job)
        if result.returncode:
            raise RuntimeError(f"Hadoop stage {stage} failed; see {job['log']}\n{result.stdout[-1600:]}")
        if not (output / "_SUCCESS").is_file():
            raise RuntimeError(f"Hadoop stage {stage} has no committed _SUCCESS marker")
        return output
