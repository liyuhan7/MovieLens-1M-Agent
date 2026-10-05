# -*- coding: utf-8 -*-
"""编排层:调用 Hadoop 容器执行清洗与评分,登记版本,生成报告。

用法:
    python run_pipeline.py --tag <task_tag>          # 全管线
    python run_pipeline.py --only score-raw          # 只跑原始评分

所有结果写入 outputs/<tag>/ 与 outputs/registry.json。
"""
import argparse
import base64
import datetime as dt
import hashlib
import json
import os
import re
import sys
import uuid
from pathlib import Path

BASE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.dirname(BASE)              # 仓库根(ml-1m)
sys.path.insert(0, PROJECT)
OUT_ROOT = os.path.join(PROJECT, "outputs")
RAW = {
    "users": os.path.join(PROJECT, "data", "users.dat"),
    "movies": os.path.join(PROJECT, "data", "movies.dat"),
    "ratings": os.path.join(PROJECT, "data", "ratings.dat"),
}
IMAGE = "movielens-hadoop:3.3.6"
RULE_VERSION = "rules-v2.0"
METRIC_VERSION = "metrics-v2.1"
T1 = 1009843200  # 2002-01-01 00:00:00 UTC
T2 = 1025481600  # 2002-07-01 00:00:00 UTC

def log(msg):
    try:
        print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)
    except (OSError, ValueError):
        # stdout 句柄可能因宿主环境(服务化/管道关闭)失效;丢弃控制台输出,不影响任务
        with open(os.path.join(PROJECT, "outputs", "pipeline.log"), "a", encoding="utf-8") as f:
            f.write(f"[{dt.datetime.now().isoformat(timespec='seconds')}] {msg}\n")

def part_files(path):
    """Locate all committed Hadoop data partitions, excluding markers and CRCs."""
    path = Path(path)
    files = sorted(path.glob("part-*-*")) if path.is_dir() else [path]
    if not files or any(not file.is_file() for file in files):
        raise FileNotFoundError(f"Hadoop output partitions missing: {path}")
    return files


def read_part(out_dir):
    """Aggregate metric counts across every reducer or mapper output partition."""
    metrics = {"UNIQ_EXCESS": 0, "N": 0}
    allowed = {"N", "ACC", "COMP", "CONS", "UD", "TRAIN", "VALID", "TEST", "UNIQ_EXCESS"}
    for part in part_files(out_dir):
        with part.open(encoding="utf-8") as stream:
            for line in stream:
                key, value = line.rstrip("\n").split("\t", 1)
                if key in allowed:
                    count = int(value)
                    if count < 0:
                        raise ValueError(f"Negative metric count in {part}: {key}")
                    metrics[key] = metrics.get(key, 0) + count
    return metrics

def five_dim(metrics, table):
    """由计数计算五维分数(公式与设计文档 §4.3 一致)。"""
    n = metrics.get("N", 0)
    if n == 0:
        return {"accurate": 0, "complete": 0, "unique": 0, "up_to_date": 0 if table == "ratings" else None, "consistent": 0}
    acc = 100 * metrics.get("ACC", 0) / n
    denom = 4 if table == "ratings" else (5 if table == "users" else 2)
    comp = 100 * metrics.get("COMP", 0) / (n * denom)
    uniq = 100 * (1 - metrics.get("UNIQ_EXCESS", 0) / n)
    cons = 100 * metrics.get("CONS", 0) / n
    ud = None if table in ("users", "movies") else 100 * metrics.get("UD", 0) / n
    return {"accurate": round(acc, 2), "complete": round(comp, 2),
            "unique": round(uniq, 2), "up_to_date": ud, "consistent": round(cons, 2)}

def table_composite(dim):
    """表内综合分:ratings 五维均值;users/movies 无时效性,四维均值。"""
    vals = [v for v in dim.values() if v is not None]
    return round(sum(vals) / len(vals), 2) if vals else 0.0

def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def input_fingerprint(raw):
    from governance.raw import input_files, inventory_hash
    tables = {}
    for name, path in sorted(raw.items()):
        path = Path(path)
        if path.is_file():
            tables[name] = {"sha256": sha256(path), "bytes": path.stat().st_size, "encoding": "ISO-8859-1"}
        else:
            members = [{"name": relative, "sha256": sha256(file), "bytes": file.stat().st_size}
                       for relative, file in input_files(path).items()]
            tables[name] = {"sha256": inventory_hash(members), "bytes": sum(file["bytes"] for file in members),
                            "encoding": "ISO-8859-1", "files": members}
    canonical = json.dumps(tables, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {"dataset_version": "raw-" + hashlib.sha256(canonical).hexdigest(), "tables": tables}

def counts_by_rule(part_file, *, include_clean=True):
    """统计各规则处置量。"""
    out = {}
    for record in dispositions(part_file):
        if not include_clean and record["stream"] == "clean":
            continue
        if record["origin"]:
            for event in record["origin"]["events"]:
                if event["action"] == "keep" and record["stream"] != "clean":
                    continue
                key = (event["action"], event["rule"])
                out[key] = out.get(key, 0) + 1
            continue
        action = "dedup" if record["action"] == "dedup" else record["stream"]
        key = (action, record["rule"])
        out[key] = out.get(key, 0) + 1
    return out


def dispositions(path):
    """Read an envelope without splitting pipes or tabs within its data payload."""
    for part in part_files(path):
        with part.open(encoding="utf-8") as stream:
            for line in stream:
                kind, envelope = line.rstrip("\n").split("\t", 1)
                if kind not in {"clean", "isolate", "log"}:
                    raise ValueError(f"Unknown disposition stream: {kind}")
                dedup_rule = None
                if kind == "log":
                    prefix = envelope.split("|", 2)
                    if len(prefix) == 3 and prefix[1] == "dedup":
                        dedup_rule, envelope = prefix[0], prefix[2]
                seq, action, rule, payload = envelope.split("|", 3)
                position, separator, token = seq.partition("@")
                origin = json.loads(base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))) if separator else None
                yield {"stream": kind, "seq": int(position), "origin": origin, "action": "dedup" if dedup_rule else action,
                       "rule": dedup_rule or rule, "payload": payload, "envelope": envelope,
                       "original": line.rstrip("\n").split("\t", 1)[1]}


def write_clean_input(source, destination, *, keep_envelope=False):
    """Export complete clean records, optionally retaining origin for the next job."""
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with destination.open("w", encoding="utf-8", newline="\n") as stream:
        for record in dispositions(source):
            if record["stream"] == "clean":
                stream.write((record["envelope"] if keep_envelope else record["payload"]) + "\n")
                count += 1
    return count


def write_reference_ids(source, destination, *, disposition=False):
    """Export reference IDs from all partitions or from a structured clean stream."""
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="\n") as stream:
        if disposition:
            for record in dispositions(source):
                if record["stream"] == "clean":
                    stream.write(record["payload"].split("::", 1)[0] + "\n")
        else:
            for part in part_files(source):
                with part.open(encoding="utf-8") as data:
                    for line in data:
                        stream.write(line.split("\t", 1)[0].strip() + "\n")

def samples(part_file, n=3):
    res = {"clean": [], "isolate": [], "log": []}
    for record in dispositions(part_file):
        kind = record["stream"]
        if len(res[kind]) < n:
            res[kind].append(record["original"])
    return res

def pipeline(tag=None, only=None, rule_pack="default", progress=None, *,
             run_id=None, attempt_id=None, raw=None, executor=None, register=False, resume=False):
    """Compute one isolated attempt; publication is handled by Run Control."""
    from governance.build_identity import capture_execution
    execution_identity = capture_execution()
    if rule_pack != "default":
        raise ValueError(f"未登记的规则档位: {rule_pack}")
    task_id = run_id or tag or f"task-{uuid.uuid4().hex}"
    attempt_id = attempt_id or f"attempt-{uuid.uuid4().hex}"
    for identity in (task_id, attempt_id):
        if not re.fullmatch(r"[A-Za-z0-9_-]+", identity):
            raise ValueError("Invalid run or attempt identifier")
    raw = {name: Path(path) for name, path in (raw or RAW).items()}
    outdir = Path(OUT_ROOT) / task_id
    work = outdir / "attempts" / attempt_id
    if work.exists() and not resume:
        raise FileExistsError(f"Attempt already exists: {work}")
    work.mkdir(parents=True, exist_ok=resume)
    if executor is None:
        from pipeline.execution import LocalExecutor
        executor = LocalExecutor(PROJECT, work / "jobs", IMAGE)
    stages = []

    def stage(name):
        stages.append({"stage": name, "time": dt.datetime.now(dt.timezone.utc).isoformat()})
        log(f"STAGE {name}")
        if progress is not None:
            progress(name)

    stage("fingerprint")
    fingerprint = input_fingerprint(raw)
    data_version = fingerprint["dataset_version"]
    executor.input_version = data_version
    raw_hash = {name: info["sha256"][:8] for name, info in fingerprint["tables"].items()}
    (work / "input-manifest.json").write_text(json.dumps(fingerprint, indent=2), encoding="utf-8")

    stage("extract-ids-raw")
    users_ids = work / "ids_users_raw.txt"
    movies_ids = work / "ids_movies_raw.txt"
    write_reference_ids(executor.run("extractUsers", raw["users"]), users_ids)
    write_reference_ids(executor.run("extractMovies", raw["movies"]), movies_ids)

    stage("score-raw")
    raw_m = {}
    for table in ("ratings", "users", "movies"):
        refs = (users_ids, movies_ids) if table == "ratings" else ()
        output = executor.run("score" + table.capitalize(), raw[table], refs=refs,
                              name="score-raw-" + table)
        raw_m[table] = read_part(output)
    if only == "score-raw":
        return {"task_id": task_id, "attempt_id": attempt_id,
                "input_data_version": data_version, "raw_metrics": raw_m, "jobs": executor.jobs}

    stage("clean-users")
    cu = executor.run("cleanUsers", raw["users"])
    stage("clean-movies")
    cm = executor.run("cleanMovies", raw["movies"])
    stage("dedup-movie-titles")
    movie_intermediate = work / "movies-pre-title.dat"
    write_clean_input(cm, movie_intermediate, keep_envelope=True)
    cmt = executor.run("cleanMoviesTitle", movie_intermediate, encoding="UTF-8")
    stage("refs-clean")
    users_clean_ids = work / "clean_ids_users.txt"
    movies_clean_ids = work / "clean_ids_movies.txt"
    write_reference_ids(cu, users_clean_ids, disposition=True)
    write_reference_ids(cmt, movies_clean_ids, disposition=True)
    stage("clean-ratings")
    cr = executor.run("cleanRatings", raw["ratings"], refs=(users_clean_ids, movies_clean_ids))

    stage("score-clean")
    cleaned_paths, clean_m = {}, {}
    for table, output in (("users", cu), ("movies", cmt), ("ratings", cr)):
        path = work / "cleaned" / (table + ".dat")
        write_clean_input(output, path)
        cleaned_paths[table] = str(path)
        refs = (users_clean_ids, movies_clean_ids) if table == "ratings" else ()
        score = executor.run("score" + table.capitalize(), path, encoding="UTF-8", refs=refs,
                             name="score-clean-" + table)
        clean_m[table] = read_part(score)

    stage("report")
    raw_dims = {table: five_dim(raw_m[table], table) for table in raw}
    clean_dims = {table: five_dim(clean_m[table], table) for table in raw}
    disposition = {}
    for table, output in (("users", cu), ("movies", cmt), ("ratings", cr)):
        counts = counts_by_rule(output)
        preview = samples(output)
        if table == "movies":
            for key, value in counts_by_rule(cm, include_clean=False).items():
                counts[key] = counts.get(key, 0) + value
            original_samples = samples(cm)
            for kind in ("isolate", "log"):
                preview[kind] = (original_samples[kind] + preview[kind])[:3]
        disposition[table] = {"counts": {f"{action}:{rule}": value for (action, rule), value in counts.items()},
                              "samples": preview}
    row_change = {table: {"raw": raw_m[table]["N"], "clean": clean_m[table]["N"],
                          "removed": raw_m[table]["N"] - clean_m[table]["N"]} for table in raw}
    split = {"t1": dt.datetime.fromtimestamp(T1, dt.timezone.utc).isoformat(),
             "t2": dt.datetime.fromtimestamp(T2, dt.timezone.utc).isoformat(),
             **{name.lower(): clean_m["ratings"].get(name, 0) for name in ("TRAIN", "VALID", "TEST")}}
    if capture_execution() != execution_identity:
        raise ValueError("Execution materials changed during computation; refusing candidate report")
    report = {
        "schema_version": "report-v2.0", "task_id": task_id, "run_id": task_id,
        "attempt_id": attempt_id, "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "rule_version": RULE_VERSION, "metric_version": METRIC_VERSION,
        "input_data_version": data_version, "output_data_version": "clean-" + task_id,
        "input_manifest": fingerprint, "raw_sha8": raw_hash, "stages": stages,
        "jobs": executor.jobs, "execution_mode": executor.jobs[0]["mode"],
        "execution": execution_identity,
        "scores": {table: {"raw": raw_dims[table], "clean": clean_dims[table],
                   "delta": {key: round(clean_dims[table][key] - value, 2) if value is not None else None
                             for key, value in raw_dims[table].items()},
                   "composite_raw": table_composite(raw_dims[table]),
                   "composite_clean": table_composite(clean_dims[table])} for table in raw},
        "dataset_composite": {phase: round(sum(table_composite(dims[table]) * weight
                              for table, weight in (("ratings", .6), ("users", .2), ("movies", .2))), 2)
                              for phase, dims in (("raw", raw_dims), ("clean", clean_dims))},
        "metrics_raw": raw_m, "metrics_clean": clean_m, "row_change": row_change,
        "disposition": disposition, "split": split,
        "cleaned_paths": cleaned_paths,
        "disposition_paths": {"users": str(cu), "movies": str(cmt),
                              "movies_first": str(cm), "ratings": str(cr)},
        "limitations": [
            "执行方式见 execution_mode；local 不代表 HDFS/YARN 集群。",
            "Accurate 仅评价值域合规与引用可解析，不能证明真实世界准确性。",
            "users/movies 的 Up-to-date 不适用，不参与综合分。",
            "置空或登记问题仍在数据中，登记不等于修复。",
            "隔离和去重会缩小分母，分数提升应结合数据量变化解释。",
            "T1/T2 仅用于切分，固定为 2002-01-01 和 2002-07-01 UTC。",
            "同名电影归并后，引用被移出 ID 的评分按 R9 隔离。",
            "同名电影按合法字段数和来源顺序选择，尚未按有评分记录的 ID 优先。",
            "当前版本未完整实现 M6 电影 ID 归并及 M2 缺失标题补全。",
        ],
    }
    rep_path = work / "report.json"
    rep_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    report["candidate_report"] = str(rep_path)
    if register:
        # Compatibility export only. The MySQL worker disables this path.
        compat_report = outdir / "report.json"
        if compat_report.exists():
            raise FileExistsError(f"Existing report is immutable: {compat_report}")
        compat_report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        reg_path = Path(OUT_ROOT) / "registry.json"
        registry = json.loads(reg_path.read_text(encoding="utf-8")) if reg_path.exists() else []
        registry.append({"task_id": task_id, "tag": tag or task_id,
                         "created_at": report["created_at"], "input_data_version": data_version,
                         "output_data_version": report["output_data_version"], "rule_version": RULE_VERSION,
                         "metric_version": report["metric_version"], "t1": split["t1"], "t2": split["t2"],
                         "split_counts": {key: split[key] for key in ("train", "valid", "test")},
                         "status": "success", "report": compat_report.relative_to(OUT_ROOT).as_posix()})
        reg_path.write_text(json.dumps(registry, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"COMPUTED run_id={task_id} attempt_id={attempt_id} report={rep_path}")
    return report

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default=None)
    ap.add_argument("--only", default=None, choices=[None, "score-raw"])
    a = ap.parse_args()
    r = pipeline(a.tag, a.only)
    if a.only is None:
        print(json.dumps({k: r[k] for k in ("scores", "dataset_composite", "row_change", "split")},
                         ensure_ascii=False, indent=1))
