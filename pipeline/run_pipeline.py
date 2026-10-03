# -*- coding: utf-8 -*-
"""迭代一 编排层:调用 Hadoop 容器执行清洗与评分,登记版本,生成报告。

用法:
    python run_pipeline.py --tag <task_tag>          # 全管线
    python run_pipeline.py --only score-raw          # 只跑原始评分

所有结果写入 outputs/<tag>/ 与 outputs/registry.json。
"""
import argparse
import datetime as dt
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import uuid

BASE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.dirname(BASE)              # 仓库根(ml-1m)
OUT_ROOT = os.path.join(PROJECT, "outputs")
RAW = {
    "users": os.path.join(PROJECT, "data", "users.dat"),
    "movies": os.path.join(PROJECT, "data", "movies.dat"),
    "ratings": os.path.join(PROJECT, "data", "ratings.dat"),
}
JAR = os.path.join(PROJECT, "hadoop", "build", "iter1.jar")
IMAGE = "movielens-hadoop:3.3.6"
RULE_VERSION = "rules-v1.0"
T1 = 1009843200  # 2002-01-01 00:00:00 UTC
T2 = 1025481600  # 2002-07-01 00:00:00 UTC

def log(msg):
    try:
        print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)
    except (OSError, ValueError):
        # stdout 句柄可能因宿主环境(服务化/管道关闭)失效;丢弃控制台输出,不影响任务
        with open(os.path.join(PROJECT, "outputs", "pipeline.log"), "a", encoding="utf-8") as f:
            f.write(f"[{dt.datetime.now().isoformat(timespec='seconds')}] {msg}\n")

# Docker Desktop 引擎空闲约 10 分钟后休眠,唤醒要十几秒;期间 docker 命令连不上 named pipe
ENGINE_DOWN_PAT = re.compile(
    r"permission denied while trying to connect"
    r"|error during connect"
    r"|Cannot connect to the Docker daemon"
    r"|is not running"
    r"|The system cannot find the file specified",
    re.I,
)

def wait_engine(timeout=180):
    """轮询 docker info 直到引擎响应(触发并等待休眠中的引擎唤醒)。"""
    deadline = time.time() + timeout
    while True:
        r = subprocess.run(["docker", "info"], capture_output=True, text=True,
                           encoding="utf-8", errors="replace", timeout=120)
        if r.returncode == 0:
            return
        if time.time() > deadline:
            raise RuntimeError(f"docker 引擎不可用:\n{r.stderr[-800:]}")
        log("docker 引擎未就绪,等待唤醒…")
        time.sleep(3)

def sh(cmd, timeout=7200):
    """在 Hadoop 容器内执行 shell(把仓库根挂到 /work)。"""
    mount = PROJECT.replace("\\", "/")
    full = ["docker", "run", "--rm",
            "-v", f"{mount}:/work", "-w", "/work",
            "--entrypoint", "sh", IMAGE, "-c", cmd]
    log(f"$ docker ... {cmd[:120]}")
    for attempt in (1, 2):
        r = subprocess.run(full, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout)
        if r.returncode == 0:
            return r.stdout
        if attempt == 1 and ENGINE_DOWN_PAT.search(r.stderr or ""):
            log("docker 连接失败,等待引擎唤醒后重试")
            wait_engine()
            continue
        raise RuntimeError(f"command failed ({r.returncode}):\n{r.stderr[-3000:]}")

def run_job(job, in_path, out_dir, extra="", desc=""):
    out = out_dir.replace("\\", "/")
    sh(f"rm -rf {out}")
    cmd = f"hadoop jar /work/hadoop/build/iter1.jar mliter1.IterationOne {job} {in_path} {out} {extra} >/dev/null 2>&1"
    sh(cmd)
    log(f"OK {desc or job} -> {out}")

def read_part(out_dir):
    """读取 MR 输出的 part 文件并聚合指标。"""
    out = os.path.join(out_dir, "part-r-00000")
    metrics = {}
    uniq_excess = 0
    if not os.path.exists(out):
        raise FileNotFoundError(out)
    with open(out, encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            k, v = parts[0], parts[1]
            if k == "UNIQ_EXCESS":
                uniq_excess += int(v)
            elif k in ("N", "ACC", "COMP", "CONS", "UD", "TRAIN", "VALID", "TEST"):
                metrics[k] = int(v)
    metrics["UNIQ_EXCESS"] = uniq_excess
    return metrics

def five_dim(metrics, table):
    """由计数计算五维分数(公式与设计文档 §4.3 一致)。"""
    n = metrics.get("N", 0)
    if n == 0:
        return {"accurate": 0, "complete": 0, "unique": 0, "up_to_date": None, "consistent": 0}
    acc = 100 * metrics.get("ACC", 0) / n
    denom = 4 if table == "ratings" else (5 if table == "users" else 2)
    comp = 100 * metrics.get("COMP", 0) / (n * denom)
    uniq = 100 * (1 - metrics.get("UNIQ_EXCESS", 0) / n)
    cons = 100 * metrics.get("CONS", 0) / n
    ud = None if table in ("users", "movies") else 100 * metrics.get("UD", 0) / n
    return {"accurate": round(acc, 2), "complete": round(comp, 2),
            "unique": round(uniq, 2), "up_to_date": ud, "consistent": round(cons, 2)}

def table_composite(dim):
    """表内综合分:ratings 四维均值;users/movies 无时效性,四维均值。"""
    vals = [v for v in dim.values() if v is not None]
    return round(sum(vals) / len(vals), 2) if vals else 0.0

def sha8(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()[:8]

def counts_by_rule(part_file):
    """统计各规则处置量。"""
    out = {}
    if not os.path.exists(part_file):
        return out
    with open(part_file, encoding="utf-8") as f:
        for line in f:
            cols = line.rstrip("\n").split("\t")
            if len(cols) < 2:
                continue
            key = cols[0]
            if key not in ("isolate", "log", "clean"):
                continue
            payload = cols[1]
            m = re.match(r"(\d+)\|([a-z]+)\|([^|]+)\|", payload)
            if not m:
                continue
            rule = m.group(3)
            if key == "isolate":
                out[("isolate", rule)] = out.get(("isolate", rule), 0) + 1
            elif key == "log":
                # log 行格式 "R7|dedup|seq|action|rule|payload" 或 "U6|dedup|..."
                m2 = re.match(r"([A-Z]\d+[a-z]?)\|dedup\|", payload)
                if m2:
                    out[("dedup", m2.group(1))] = out.get(("dedup", m2.group(1)), 0) + 1
            else:
                out[("clean", rule)] = out.get(("clean", rule), 0) + 1
    return out

def samples(part_file, n=3):
    res = {"clean": [], "isolate": [], "log": []}
    if not os.path.exists(part_file):
        return res
    with open(part_file, encoding="utf-8") as f:
        for line in f:
            cols = line.rstrip("\n").split("\t")
            if len(cols) < 2:
                continue
            k = cols[0]
            if k in res and len(res[k]) < n:
                res[k].append(cols[1])
    return res

def pipeline(tag, only=None, rule_pack="default", progress=None):
    # 预留规则档位接口:当前仅登记 default(rules-v1.0);未知档位显式报错,
    # 由 Agent 如实转述"该规则档位未登记",绝不静默降级。
    if rule_pack != "default":
        raise ValueError(f"未登记的规则档位: {rule_pack}(当前仅 default=rules-v1.0)")
    task_id = f"task-{uuid.uuid4().hex[:12]}"
    outdir = os.path.join(OUT_ROOT, tag or task_id)
    os.makedirs(outdir, exist_ok=True)
    stages = []

    def stage(name):
        stages.append({"stage": name, "time": dt.datetime.now().isoformat(timespec="seconds")})
        log(f"STAGE {name}")
        if progress is not None:
            try:
                progress(name)
            except Exception:
                pass  # 进度回调失败不影响管线

    # ---------- 0. 输入指纹 ----------
    stage("fingerprint")
    raw_hash = {k: sha8(v) for k, v in RAW.items()}
    data_version = f"raw-{dt.date.today():%Y%m%d}-{raw_hash['ratings']}"

    # ---------- 1. 参照 ID(原始表) ----------
    if only != "score-raw":
        stage("extract-ids-raw")
        run_job("extractUsers", "/work/data/users.dat", "/work/outputs/_tmp_idsu")
        run_job("extractMovies", "/work/data/movies.dat", "/work/outputs/_tmp_idm")
        sh("cat /work/outputs/_tmp_idsu/part-r-00000 > /work/outputs/ids_users_raw.txt && "
           "cat /work/outputs/_tmp_idm/part-r-00000 > /work/outputs/ids_movies_raw.txt")

    # ---------- 2. 原始数据评分 ----------
    stage("score-raw")
    run_job("scoreRatings", "/work/data/ratings.dat", "/work/outputs/_tmp_sr",
            extra="-files /work/outputs/ids_users_raw.txt,/work/outputs/ids_movies_raw.txt")
    raw_m = {"ratings": read_part(os.path.join(PROJECT, "outputs", "_tmp_sr"))}
    run_job("scoreUsers", "/work/data/users.dat", "/work/outputs/_tmp_su")
    raw_m["users"] = read_part(os.path.join(PROJECT, "outputs", "_tmp_su"))
    run_job("scoreMovies", "/work/data/movies.dat", "/work/outputs/_tmp_sm")
    raw_m["movies"] = read_part(os.path.join(PROJECT, "outputs", "_tmp_sm"))

    if only == "score-raw":
        return {"raw_metrics": raw_m}

    # ---------- 3. 清洗三表 ----------
    stage("clean-users")
    run_job("cleanUsers", "/work/data/users.dat", "/work/outputs/_tmp_cu")
    stage("clean-movies")
    run_job("cleanMovies", "/work/data/movies.dat", "/work/outputs/_tmp_cm")
    stage("dedup-movie-titles")
    sh("grep \"^clean\" /work/outputs/_tmp_cm/part-r-00000 | cut -f2 | cut -d'|' -f4 > /work/clean_movies_pre_title_dedup.dat")
    run_job("cleanMoviesTitle", "/work/clean_movies_pre_title_dedup.dat", "/work/outputs/_tmp_cm_title")
    stage("refs-clean")
    sh("grep \"^clean\" /work/outputs/_tmp_cu/part-r-00000 | cut -f2 | cut -d'|' -f4 | cut -d: -f1 > /work/outputs/clean_ids_users.txt && "
       "grep \"^clean\" /work/outputs/_tmp_cm_title/part-r-00000 | cut -f2 | cut -d'|' -f4 | cut -d: -f1 > /work/outputs/clean_ids_movies.txt")
    stage("clean-ratings")
    run_job("cleanRatings", "/work/data/ratings.dat", "/work/outputs/_tmp_cr",
            extra="-files /work/outputs/clean_ids_users.txt,/work/outputs/clean_ids_movies.txt")

    # ---------- 4. 清洗后评分 ----------
    stage("score-clean")
    # 生成清洗后数据行文件(仅 clean 行的 payload)
    sh("grep \"^clean\" /work/outputs/_tmp_cu/part-r-00000 | cut -f2 | cut -d'|' -f4 > /work/clean_users_input.dat")
    sh("grep \"^clean\" /work/outputs/_tmp_cm_title/part-r-00000 | cut -f2 | cut -d'|' -f4 > /work/clean_movies_input.dat")
    sh("grep \"^clean\" /work/outputs/_tmp_cr/part-r-00000 | cut -f2 | cut -d'|' -f4 > /work/clean_ratings_input.dat")
    run_job("scoreRatings", "/work/clean_ratings_input.dat", "/work/outputs/_tmp_src2",
            extra="-files /work/outputs/clean_ids_users.txt,/work/outputs/clean_ids_movies.txt")
    run_job("scoreUsers", "/work/clean_users_input.dat", "/work/outputs/_tmp_suc")
    run_job("scoreMovies", "/work/clean_movies_input.dat", "/work/outputs/_tmp_smc")

    clean_m = {
        "ratings": read_part(os.path.join(PROJECT, "outputs", "_tmp_src2")),
        "users": read_part(os.path.join(PROJECT, "outputs", "_tmp_suc")),
        "movies": read_part(os.path.join(PROJECT, "outputs", "_tmp_smc")),
    }

    # ---------- 5. 汇总报告 ----------
    stage("report")
    raw_dims = {t: five_dim(raw_m[t], t) for t in ("ratings", "users", "movies")}
    clean_dims = {t: five_dim(clean_m[t], t) for t in ("ratings", "users", "movies")}
    disposition = {}
    for tbl, tmp in (("users", "_tmp_cu"), ("movies", "_tmp_cm_title"), ("ratings", "_tmp_cr")):
        part = os.path.join(PROJECT, "outputs", tmp, "part-r-00000")
        counts = counts_by_rule(part)
        if tbl == "movies":
            first = counts_by_rule(os.path.join(PROJECT, "outputs", "_tmp_cm", "part-r-00000"))
            for key, value in first.items():
                counts[key] = counts.get(key, 0) + value
        disposition[tbl] = {
            "counts": {f"{a}:{b}": c for (a, b), c in counts.items()},
            "samples": samples(part),
        }

    row_change = {}
    for tbl, tmp in (("users", "_tmp_cu"), ("movies", "_tmp_cm_title"), ("ratings", "_tmp_cr")):
        # 行数统计只看最终阶段的输出;movies 的中间阶段(_tmp_cm)计数仅供处置审计,不得计入行数
        part = os.path.join(PROJECT, "outputs", tmp, "part-r-00000")
        fc = counts_by_rule(part)
        clean_rows = sum(v for (a, b), v in fc.items() if a == "clean")
        raw_rows = {"users": 6946, "movies": 4465, "ratings": 1150241}[tbl]
        row_change[tbl] = {"raw": raw_rows, "clean": clean_rows,
                           "removed": raw_rows - clean_rows}

    split = {
        "t1": dt.datetime.fromtimestamp(T1, dt.timezone.utc).isoformat(),
        "t2": dt.datetime.fromtimestamp(T2, dt.timezone.utc).isoformat(),
        "train": clean_m["ratings"].get("TRAIN", 0),
        "valid": clean_m["ratings"].get("VALID", 0),
        "test": clean_m["ratings"].get("TEST", 0),
    }

    report = {
        "task_id": task_id,
        "created_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "rule_version": RULE_VERSION,
        "input_data_version": data_version,
        "output_data_version": f"clean-v1.0-{task_id[5:13]}",
        "raw_sha8": raw_hash,
        "stages": stages,
        "scores": {
            t: {
                "raw": raw_dims[t],
                "clean": clean_dims[t],
                "delta": {k: (round(clean_dims[t][k] - raw_dims[t][k], 2)
                              if raw_dims[t][k] is not None and clean_dims[t][k] is not None else None)
                          for k in raw_dims[t]},
                "composite_raw": table_composite(raw_dims[t]),
                "composite_clean": table_composite(clean_dims[t]),
            } for t in ("ratings", "users", "movies")
        },
        "dataset_composite": {
            "raw": round(sum(table_composite(raw_dims[t]) * w for t, w in (("ratings", .6), ("users", .2), ("movies", .2))), 2),
            "clean": round(sum(table_composite(clean_dims[t]) * w for t, w in (("ratings", .6), ("users", .2), ("movies", .2))), 2),
        },
        "metrics_raw": raw_m,
        "metrics_clean": clean_m,
        "row_change": row_change,
        "disposition": disposition,
        "split": split,
        "limitations": [
            "执行模式为容器内 Hadoop local MapReduce(真实 MR 管线),非 HDFS/YARN 多节点集群。",
            "Accurate 评的是值域合规+引用可解析,无法验证真实世界准确性(用户自填属性未核验)。",
            "users/movies 的 Up-to-date 不适用(静态快照无时间字段),标 None 且不参与综合分。",
            "U2-U5c/M3 等置 NULL/登记字段仍留在数据中,登记≠修复。",
            "隔离使分母变小,分数上升部分来自删除劣质记录而非修复。",
            "时间参照只覆盖 1990–2003;T1/T2 只用于切分,不参与打分。",
            "movies Unique 的 (title,year) 键在标题含年份时才成立;年份缺失的行按行唯一处理。",
            "M5b 标题归并后,指向被归并副本 ID 的评分按 R9 悬空引用隔离(ratings 有效行少于不去重版本)。",
        ],
    }
    rep_path = os.path.join(outdir, "report.json")
    with open(rep_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    # registry 追加
    reg_path = os.path.join(OUT_ROOT, "registry.json")
    entry = {
        "task_id": task_id,
        "created_at": report["created_at"],
        "tag": tag,
        "input_data_version": data_version,
        "output_data_version": report["output_data_version"],
        "rule_version": RULE_VERSION,
        "t1": split["t1"], "t2": split["t2"],
        "split_counts": {"train": split["train"], "valid": split["valid"], "test": split["test"]},
        "status": "success",
        "report": os.path.relpath(rep_path, OUT_ROOT).replace("\\", "/"),
    }
    reg = []
    if os.path.exists(reg_path):
        with open(reg_path, encoding="utf-8") as f:
            reg = json.load(f)
    reg.append(entry)
    with open(reg_path, "w", encoding="utf-8") as f:
        json.dump(reg, f, ensure_ascii=False, indent=2)

    log(f"DONE task_id={task_id} report={rep_path}")
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
