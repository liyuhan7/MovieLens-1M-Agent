"""Published evidence and comparisons, recovered from immutable artifact bytes."""
import hashlib
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import uuid

import pyarrow.parquet as pq

from agent.published import ReportUnavailable, read_published_report
from metadata.connection import ROOT
from metadata.store import Store, fingerprint
from storage.hdfs import Hdfs


def publication_context(run_id, store, root=ROOT):
    run = store.get_run(run_id)
    if not run:
        raise ReportUnavailable("run not found", 404)
    pub = store.get_publish(run_id)
    if run["status"] != "PUBLISHED" or not pub or pub["status"] != "PUBLISHED":
        raise ReportUnavailable("result is not formally published", 409)
    manifest = pub["manifest"]
    if (pub["run_id"] != run_id or fingerprint(manifest) != pub["manifest_hash"] or
            manifest.get("run_id") != run_id or manifest.get("attempt_id") != pub["attempt_id"]):
        raise ReportUnavailable("publication manifest binding differs", 503)
    attempts = [a for a in store.list_attempts(run_id) if a["attempt_id"] == pub["attempt_id"]]
    root = Path(root).resolve()
    work = root / "outputs" / run_id / "attempts" / pub["attempt_id"]
    if len(attempts) != 1 or (root / attempts[0]["work_path"]).resolve() != work or work.resolve() != work:
        raise ReportUnavailable("published cache location is invalid", 503)
    return run, pub, work / "artifacts"


def artifact_file(pub, cache, relative, *, hdfs=None):
    name = PurePosixPath(relative)
    if name.is_absolute() or ".." in name.parts or str(name) != relative or "\\" in relative:
        raise ReportUnavailable("invalid artifact path", 503)
    declared = [f for f in pub["manifest"]["files"] if f["path"] == relative]
    if len(declared) != 1:
        raise ReportUnavailable("artifact is not declared by publication", 503)
    target = cache / relative
    if target.resolve() != target or target.is_symlink():
        raise ReportUnavailable("redirected artifact cache", 503)

    def verify(path):
        digest, size = hashlib.sha256(), 0
        try:
            with path.open("rb") as stream:
                while chunk := stream.read(1 << 20):
                    digest.update(chunk)
                    size += len(chunk)
        except OSError as error:
            raise ReportUnavailable("published artifact cache is unreadable", 503) from error
        if size != declared[0]["bytes"] or digest.hexdigest() != declared[0]["sha256"]:
            raise ReportUnavailable("artifact cache differs from immutable manifest", 503)

    if not target.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.resolve() != target:
            raise ReportUnavailable("redirected artifact cache", 503)
        temporary = target.with_name("cache-" + uuid.uuid4().hex)
        try:
            (hdfs or Hdfs()).get(pub["storage_path"] + "/" + relative, temporary)
            verify(temporary)
            # Atomic exclusive link exposes only complete bytes, including on Windows.
            try:
                os.link(temporary, target)
            except FileExistsError:
                pass
        except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
            raise ReportUnavailable("published artifact storage is unavailable", 503) from error
        finally:
            if temporary.exists():
                temporary.unlink()
    verify(target)
    return target


def read_evidence(run_id, *, store=None, root=ROOT, hdfs=None, **filters):
    store = store or Store()
    run, pub, cache = publication_context(run_id, store, root)
    for key in ("after", "source_record_id", "evidence_id"):
        if filters.get(key) and not re.fullmatch(r"[0-9a-f]{64}", filters[key]):
            raise ReportUnavailable("invalid evidence identity or cursor", 400)
    if filters.get("source_table") not in (None, "users", "movies", "ratings"):
        raise ReportUnavailable("invalid source table", 400)
    page = store.query_evidence(pub["attempt_id"], **filters)
    groups, files, bodies = {}, {}, []
    for position in page["items"]:
        relative = position["file_path"]
        if not relative.startswith("evidence/") or not relative.endswith(".parquet"):
            raise ReportUnavailable("index does not select an evidence artifact", 503)
        key = (relative, position["row_group"])
        if key not in groups:
            if relative not in files:
                files[relative] = artifact_file(pub, cache, relative, hdfs=hdfs)
            path = files[relative]
            try:
                parquet = pq.ParquetFile(path)
                if not 0 <= key[1] < parquet.num_row_groups or parquet.metadata.row_group(key[1]).num_rows > 5000:
                    raise ValueError("invalid row group")
                groups[key] = parquet.read_row_group(key[1]).to_pylist()
            except (ValueError, OSError) as error:
                raise ReportUnavailable("evidence Parquet is unreadable", 503) from error
        index = position["row_in_group"]
        if not 0 <= index < len(groups[key]):
            raise ReportUnavailable("evidence position is outside its row group", 503)
        body = groups[key][index]
        if (any(body[k] != position[k] for k in ("evidence_id", "attempt_id", "source_record_id", "source_table", "rule_id", "metric"))
                or body["run_id"] != run_id or body["attempt_id"] != pub["attempt_id"]
                or any(body[k] != run[k] for k in ("input_version", "rule_version", "metric_version"))
                or fingerprint(body) != position["detail"]["body_sha256"]):
            raise ReportUnavailable("evidence body/index binding differs", 503)
        bodies.append(body)
    return {"run_id": run_id, "publish_id": pub["publish_id"], "attempt_id": pub["attempt_id"],
            "items": bodies, "next_after": page["next_after"]}


def compare_runs(left, right, *, store=None, root=ROOT):
    store = store or Store()
    _, a, pa = read_published_report(left, store=store, root=root)
    _, b, pb = read_published_report(right, store=store, root=root)
    ra, rb = store.get_run(left), store.get_run(right)
    if ra["dataset_id"] != rb["dataset_id"]:
        raise ReportUnavailable("runs belong to different datasets", 400)
    versions = {key: {"left": a.get(key), "right": b.get(key), "changed": a.get(key) != b.get(key)}
                for key in ("input_data_version", "rule_version", "metric_version", "output_data_version")}
    compatible = a["metric_version"] == b["metric_version"]
    rows = []
    for table in ("users", "movies", "ratings"):
        for phase in ("raw", "clean"):
            for metric in sorted(set(a.get("scores", {}).get(table, {}).get(phase, {})) |
                                 set(b.get("scores", {}).get(table, {}).get(phase, {}))):
                av = a.get("scores", {}).get(table, {}).get(phase, {}).get(metric)
                bv = b.get("scores", {}).get(table, {}).get(phase, {}).get(metric)
                rows.append({"table": table, "phase": phase, "metric": metric, "left": av, "right": bv,
                             "delta": round(bv-av, 6) if compatible and av is not None and bv is not None else None})
    return {"left": {"run_id": left, "publish_id": pa["publish_id"]},
            "right": {"run_id": right, "publish_id": pb["publish_id"]}, "versions": versions,
            "metric_compatible": compatible, "scores": rows,
            "row_change": {"left": a.get("row_change"), "right": b.get("row_change")},
            "limitations": "差值为右侧减左侧；指标版本不同时不计算差值。输入或规则变化不能直接归因于单一规则。"}
