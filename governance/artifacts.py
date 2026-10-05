"""Stream source-complete Parquet artifacts; reconcile independent counts with Hadoop."""
import collections
import hashlib
import json
import re
import sqlite3
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from metadata.connection import ROOT
from metadata.store import canonical, fingerprint
from pipeline.run_pipeline import RULE_VERSION, METRIC_VERSION, dispositions, sha256, input_fingerprint
from governance.provenance import validate_origin
from governance.manifest import preflight_artifacts
from governance.raw import input_files

AGES = {"1", "18", "25", "35", "45", "50", "56"}
GENRES = {"Action", "Adventure", "Animation", "Children's", "Comedy", "Crime", "Documentary", "Drama",
          "Fantasy", "Film-Noir", "Horror", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"}
TS_MIN, TS_MAX = 631152000, 1047772799
T1, T2 = 1009843200, 1025481600
COMMON = [("run_id", pa.string()), ("attempt_id", pa.string()), ("input_version", pa.string()),
          ("rule_version", pa.string()), ("metric_version", pa.string())]
SOURCE = [("source_record_id", pa.string()), ("source_table", pa.string()),
          ("source_file", pa.string()), ("source_offset", pa.int64())]
SCHEMAS = {
    "users": pa.schema(COMMON + SOURCE + [("user_id", pa.string()), ("gender", pa.string()),
             ("age", pa.int32()), ("occupation", pa.int32()), ("zip_code", pa.string()),
             ("related_source_ids", pa.list_(pa.string()))]),
    "movies": pa.schema(COMMON + SOURCE + [("movie_id", pa.string()), ("title", pa.string()),
              ("year", pa.int32()), ("genres", pa.list_(pa.string())), ("related_source_ids", pa.list_(pa.string()))]),
    "ratings": pa.schema(COMMON + SOURCE + [("user_id", pa.string()), ("movie_id", pa.string()),
               ("rating", pa.int8()), ("timestamp_seconds", pa.int64()), ("related_source_ids", pa.list_(pa.string()))]),
    "quarantine": pa.schema(COMMON + SOURCE + [("raw_record", pa.string()), ("processed_record", pa.string()),
                   ("reason_rule", pa.string()), ("disposition", pa.string())]),
    "disposition": pa.schema(COMMON + SOURCE + [("raw_record", pa.string()), ("processed_record", pa.string()),
                    ("disposition", pa.string()), ("related_source_ids", pa.list_(pa.string()))]),
    "evidence": pa.schema(COMMON + SOURCE + [("evidence_id", pa.string()), ("event_order", pa.int32()),
                 ("kind", pa.string()), ("phase", pa.string()), ("metric", pa.string()), ("rule_id", pa.string()),
                 ("action", pa.string()), ("before", pa.string()), ("after", pa.string()),
                 ("target_source_id", pa.string()), ("numerator", pa.int64()), ("denominator", pa.int64()),
                 ("final_disposition", pa.string())]),
    "quality_detail": pa.schema(COMMON + SOURCE + [("phase", pa.string()), ("business_key", pa.string()),
                      ("accurate", pa.int8()), ("present_fields", pa.int8()), ("required_fields", pa.int8()),
                      ("consistent", pa.int8()), ("up_to_date", pa.int8()), ("unique_primary", pa.int8())]),
    "quality_groups": pa.schema(COMMON + [("phase", pa.string()), ("source_table", pa.string()),
                      ("business_key", pa.string()), ("records", pa.int64()), ("duplicate_excess", pa.int64())]),
    "quality_summary": pa.schema(COMMON + [("phase", pa.string()), ("source_table", pa.string()),
                       ("metric", pa.string()), ("numerator", pa.int64()), ("denominator", pa.int64()), ("score", pa.float64())]),
}


def missing(value):
    return not value.strip() or value.strip() == "NULL"


def number(value):
    if not re.fullmatch(r"[0-9]+", value):
        return None
    try:
        value = int(value)
        return value if value <= 9223372036854775807 else None
    except ValueError:
        return None


def observations(line, table, identity, refs):
    fields = line.split("::")
    n = 4 if table == "ratings" else 5 if table == "users" else 2
    present = sum(not missing(value) for value in (fields[:n] if table != "movies" else fields[1:3]))
    acc = cons = ud = 0
    key = "ROW:" + identity
    split = None
    if table == "users":
        if len(fields) == 5 and re.fullmatch(r"[0-9]+", fields[0]):
            key = "U:" + fields[0]
        if len(fields) == 5:
            uid, gender, age, occ, zip_code = fields
            valid = bool(re.fullmatch(r"[0-9]+", uid)) and gender in {"F", "M"} and age in AGES
            valid = valid and number(occ) is not None and number(occ) <= 20
            acc = int(bool(valid and re.fullmatch(r"[0-9]{5}", zip_code)))
            cons = int(bool(valid and re.fullmatch(r"[0-9]{5}(-[0-9]{4})?", zip_code)))
    elif table == "movies":
        title = fields[1].strip() if len(fields) >= 2 else ""
        match = re.search(r"\(([0-9]{4})\)\s*$", title)
        normalized = re.sub(r"\s*\([0-9]{4}\)\s*$", "", title).strip().lower()
        if normalized and match:
            key = "M:" + canonical([normalized, match.group(1)])
        genres = fields[2].split("|") if len(fields) == 3 else []
        while genres and genres[-1] == "":
            genres.pop()  # Java String.split drops trailing empty tokens.
        genres_ok = len(fields) == 3 and not missing(fields[2]) and all(g.strip() in GENRES for g in genres)
        acc = int(bool(len(fields) == 3 and title and match and int(match.group(1)) <= 2003 and genres_ok))
        cons = int(bool(len(fields) == 3 and re.fullmatch(r"[0-9]+", fields[0]) and title and match and genres_ok))
    else:
        if len(fields) >= 2 and all(re.fullmatch(r"[0-9]+", f.strip()) for f in fields[:2]):
            key = "R:" + canonical([f.strip() for f in fields[:2]])
        if len(fields) == 4:
            uid, mid, rating, timestamp = (f.strip() for f in fields)
            valid_ids = bool(re.fullmatch(r"[0-9]+", uid) and re.fullmatch(r"[0-9]+", mid))
            rating_ok = bool(re.fullmatch(r"[1-5]", rating))
            ts = number(timestamp)
            ud = int(ts is not None and TS_MIN <= ts <= TS_MAX)
            acc = int(bool(valid_ids and rating_ok and ud and uid in refs["users"] and mid in refs["movies"]))
            cons = int(bool(valid_ids and rating_ok and re.fullmatch(r"[0-9]{9,10}", timestamp)))
            if ts is not None:
                split = "TRAIN" if ts <= T1 else "VALID" if ts <= T2 else "TEST"
    return {"business_key": key, "accurate": acc, "present_fields": present, "required_fields": n,
            "consistent": cons, "up_to_date": ud if table == "ratings" else None, "split": split}


class BatchFile:
    def __init__(self, root, relative, schema, batch_size=5000):
        self.root, self.relative, self.schema = root, relative, schema
        self.path = root / relative
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.writer = pq.ParquetWriter(self.path, schema, compression="zstd")
        self.closed = False
        self.batch_size = batch_size
        self.buffer, self.rows, self.group = [], 0, 0

    def append(self, row):
        location = (self.group, len(self.buffer))
        self.buffer.append(row)
        self.rows += 1
        if len(self.buffer) >= self.batch_size:
            self.flush()
        return location

    def flush(self):
        if self.buffer:
            self.writer.write_table(pa.Table.from_pylist(self.buffer, schema=self.schema), row_group_size=self.batch_size)
            self.buffer.clear()
            self.group += 1

    def close(self):
        self.flush()
        self.writer.close()
        self.closed = True
        return {"path": self.relative, "format": "parquet", "rows": self.rows,
                "schema_sha256": hashlib.sha256(self.schema.serialize().to_pybytes()).hexdigest(),
                "sha256": sha256(self.path), "bytes": self.path.stat().st_size}


def resolve_workspace(path):
    path = Path(path)
    # A report written by the Linux worker can be inspected on the host too.
    if path.as_posix().startswith("/work/"):
        path = ROOT / path.as_posix()[6:]
    path = path.resolve()
    if not path.is_relative_to(ROOT):
        raise ValueError("Artifact input escaped the workspace")
    return path


def build_artifacts(report, raw_paths, *, progress=None, batch_size=5000):
    if type(batch_size) is not int or not 1 <= batch_size <= 5000:
        raise ValueError("Artifact batch size must be between 1 and 5000")
    if report["rule_version"] != RULE_VERSION or report["metric_version"] != METRIC_VERSION:
        raise ValueError("Legacy/candidate versions cannot become source-complete formal artifacts")
    work = resolve_workspace(report["candidate_report"]).parent
    final = work / "artifacts"
    expected_identity = {"run_id": report["run_id"], "attempt_id": report["attempt_id"],
                "input_version": report["input_data_version"], "rule_version": RULE_VERSION,
                "metric_version": METRIC_VERSION, "input_manifest": report["input_manifest"]}
    if final.exists():
        return final, preflight_artifacts(final, expected=expected_identity)["manifest"]
    raw = {table: resolve_workspace(path) for table, path in raw_paths.items()}
    if input_fingerprint(raw) != report["input_manifest"]:
        raise ValueError("Registered raw bytes changed before artifact generation")
    raw_files = {table: input_files(path) for table, path in raw.items()}
    root = work / ("artifacts-building-" + uuid.uuid4().hex)
    root.mkdir()
    database = sqlite3.connect(root / "source-spool.sqlite")
    database.execute("CREATE TABLE sources (id TEXT PRIMARY KEY, source_table TEXT, source_file TEXT, source_offset INTEGER, "
                     "origin TEXT, payload TEXT, disposition TEXT)")
    database.execute("CREATE TABLE groups (phase TEXT, source_table TEXT, business_key TEXT, n INTEGER, "
                     "PRIMARY KEY (phase,source_table,business_key))")
    files = []
    common = {"run_id": report["run_id"], "attempt_id": report["attempt_id"],
              "input_version": report["input_data_version"], "rule_version": RULE_VERSION, "metric_version": METRIC_VERSION}
    handles = {}
    writers = []

    def writer(relative, schema):
        result = BatchFile(root, relative, schema, batch_size=batch_size)
        writers.append(result)
        return result

    def ingest(table, path, intermediate=False, update=False):
        for record in dispositions(resolve_workspace(path)):
            origin = record["origin"]
            disposition = "INTERMEDIATE" if intermediate and record["stream"] == "clean" else (
                "KEEP" if record["stream"] == "clean" else "DEDUP" if record["action"] == "dedup" else "ISOLATE")
            file = origin.get("file") if isinstance(origin, dict) else None
            if file not in raw_files[table]:
                raise ValueError("Source file is not in the registered raw inventory")
            handle_key = (table, file)
            if handle_key not in handles:
                handles[handle_key] = raw_files[table][file].open("rb")
            identity = validate_origin(origin, version=common["input_version"], table=table,
                                       files=raw_files[table], payload=record["payload"],
                                       disposition=disposition, handle=handles[handle_key])
            values = (canonical(origin), record["payload"], disposition, identity)
            if update:
                changed = database.execute("UPDATE sources SET origin=?,payload=?,disposition=? WHERE id=? AND disposition='INTERMEDIATE'", values).rowcount
                if changed != 1:
                    raise ValueError("Second movie stage duplicated or lost a first-stage source")
            else:
                database.execute("INSERT INTO sources VALUES (?,?,?,?,?,?,?)", (identity, table, origin["file"],
                                 origin["offset"], values[0], values[1], values[2]))

    try:
        for table in ("users", "ratings"):
            ingest(table, report["disposition_paths"][table])
        ingest("movies", report["disposition_paths"]["movies_first"], intermediate=True)
        ingest("movies", report["disposition_paths"]["movies"], update=True)
        database.commit()
        balance = {}
        for table in raw:
            counts = dict(database.execute("SELECT disposition,COUNT(*) FROM sources WHERE source_table=? GROUP BY disposition", (table,)))
            if counts.get("INTERMEDIATE", 0) or sum(counts.values()) != report["row_change"][table]["raw"]:
                raise ValueError("Source conservation failed")
            if counts.get("KEEP", 0) != report["row_change"][table]["clean"]:
                raise ValueError("Cleaned source count differs from Hadoop")
            balance[table] = {"raw": sum(counts.values()), "keep": counts.get("KEEP", 0),
                              "isolate": counts.get("ISOLATE", 0), "dedup": counts.get("DEDUP", 0)}
        refs = {phase: {table: set() for table in ("users", "movies")} for phase in ("before", "after")}
        for table in ("users", "movies"):
            for origin_text, payload, disposition in database.execute("SELECT origin,payload,disposition FROM sources WHERE source_table=?", (table,)):
                first = json.loads(origin_text)["raw_record"].split("::", 1)[0].strip()
                if re.fullmatch(r"[0-9]+", first):
                    refs["before"][table].add(first)
                if disposition == "KEEP":
                    refs["after"][table].add(payload.split("::", 1)[0])
        summary = writer("quality/summary.parquet", SCHEMAS["quality_summary"])
        groups = writer("quality/groups.parquet", SCHEMAS["quality_groups"])
        action_counts = {}
        for table in ("users", "movies", "ratings"):
            if progress:
                progress("artifacts-" + table)
            clean = writer("cleaned/" + table + "/part-00000.parquet", SCHEMAS[table])
            quarantine = writer("quarantine/" + table + "/part-00000.parquet", SCHEMAS["quarantine"])
            disposition_file = writer("dispositions/" + table + "/part-00000.parquet", SCHEMAS["disposition"])
            evidence = writer("evidence/" + table + "/part-00000.parquet", SCHEMAS["evidence"])
            quality = {phase: writer("quality/" + phase + "/" + table + ".parquet", SCHEMAS["quality_detail"])
                       for phase in ("before", "after")}
            totals = {phase: collections.Counter() for phase in ("before", "after")}
            actions = collections.Counter()
            for identity, file, offset, origin_text, payload, disposition in database.execute(
                    "SELECT id,source_file,source_offset,origin,payload,disposition FROM sources WHERE source_table=? ORDER BY source_file,source_offset", (table,)):
                origin = json.loads(origin_text)
                base = {**common, "source_record_id": identity, "source_table": table, "source_file": file, "source_offset": offset}
                record = {**base, "raw_record": origin["raw_record"], "processed_record": payload,
                          "disposition": disposition, "related_source_ids": origin["related_source_ids"]}
                disposition_file.append(record)
                for index, event in enumerate(origin["events"]):
                    if event.get("target_source_id") and not database.execute("SELECT 1 FROM sources WHERE id=?", (event["target_source_id"],)).fetchone():
                        raise ValueError("Deduplication refers to an unknown source")
                    if event["action"] != "keep" or disposition == "KEEP":
                        actions[event["action"] + ":" + event["rule"]] += 1
                    evidence.append({**base, "evidence_id": fingerprint([report["run_id"], report["attempt_id"], identity, "event", index]),
                                     "event_order": index, "kind": "TRANSFORMATION", "phase": "cleaning", "metric": None,
                                     "rule_id": event["rule"], "action": event["action"], "before": event["before"], "after": event["after"],
                                     "target_source_id": event.get("target_source_id"), "numerator": None, "denominator": None,
                                     "final_disposition": disposition})
                if disposition == "ISOLATE":
                    quarantine.append({**record, "reason_rule": origin["events"][-1]["rule"]})
                if disposition == "KEEP":
                    fields = payload.split("::")
                    lineage = {**base, "related_source_ids": origin["related_source_ids"]}
                    if table == "users":
                        if len(fields) != 5:
                            raise ValueError("Invalid cleaned users schema")
                        uid, gender, age, occ, zip_code = fields
                        clean.append({**lineage, "user_id": uid, "gender": None if missing(gender) else gender,
                                      "age": None if missing(age) else int(age), "occupation": None if missing(occ) else int(occ),
                                      "zip_code": None if missing(zip_code) else zip_code})
                    elif table == "movies":
                        if len(fields) != 3:
                            raise ValueError("Invalid cleaned movies schema")
                        mid, title, genres = fields
                        match = re.search(r"\(([0-9]{4})\)\s*$", title)
                        year = int(match.group(1)) if match and int(match.group(1)) <= 2003 else None
                        clean.append({**lineage, "movie_id": mid, "title": title, "year": year,
                                      "genres": None if missing(genres) else genres.split("|")})
                    else:
                        if len(fields) != 4:
                            raise ValueError("Invalid cleaned ratings schema")
                        uid, mid, rating, timestamp = fields
                        if uid not in refs["after"]["users"] or mid not in refs["after"]["movies"]:
                            raise ValueError("Cleaned ratings contains a dangling reference")
                        clean.append({**lineage, "user_id": uid, "movie_id": mid, "rating": int(rating), "timestamp_seconds": int(timestamp)})
                for phase in ("before", "after"):
                    if phase == "after" and disposition != "KEEP":
                        continue
                    obs = observations(origin["raw_record"] if phase == "before" else payload, table, identity, refs[phase])
                    total = totals[phase]
                    total.update({"N": 1, "ACC": obs["accurate"], "COMP": obs["present_fields"], "CONS": obs["consistent"]})
                    if table == "ratings":
                        total["UD"] += obs["up_to_date"]
                        if obs["split"]:
                            total[obs["split"]] += 1
                    count = database.execute("INSERT INTO groups VALUES (?,?,?,1) ON CONFLICT(phase,source_table,business_key) "
                                             "DO UPDATE SET n=n+1 RETURNING n", (phase, table, obs["business_key"])).fetchone()[0]
                    obs["unique_primary"] = int(count == 1)
                    quality[phase].append({**base, "phase": phase, **{k: v for k, v in obs.items() if k != "split"}})
                    for metric, value, denominator in (("accurate", obs["accurate"], 1),
                            ("complete", obs["present_fields"], obs["required_fields"]), ("consistent", obs["consistent"], 1),
                            ("unique", obs["unique_primary"], 1), ("up_to_date", obs["up_to_date"], 1)):
                        if value is None:
                            continue
                        evidence.append({**base, "evidence_id": fingerprint([report["run_id"], report["attempt_id"], identity, phase, metric]),
                                         "event_order": -1, "kind": "QUALITY", "phase": phase, "metric": metric,
                                         "rule_id": "metric:" + metric, "action": "observation", "before": None, "after": None,
                                         "target_source_id": None, "numerator": value, "denominator": denominator,
                                         "final_disposition": disposition})
            database.commit()
            for phase in ("before", "after"):
                excess = 0
                for key, n in database.execute("SELECT business_key,n FROM groups WHERE phase=? AND source_table=?", (phase, table)):
                    excess += n - 1
                    groups.append({**common, "phase": phase, "source_table": table, "business_key": key,
                                   "records": n, "duplicate_excess": n - 1})
                totals[phase]["UNIQ_EXCESS"] = excess
                expected = report["metrics_raw" if phase == "before" else "metrics_clean"][table]
                if any(totals[phase][key] != expected.get(key, 0) for key in set(totals[phase]) | set(expected)):
                    raise ValueError(f"Independent quality detail disagrees with Hadoop: {table}/{phase}: {dict(totals[phase])} != {expected}")
                n = totals[phase]["N"]
                field_count = 4 if table == "ratings" else 5 if table == "users" else 2
                for metric, numerator, denominator in (
                    ("accurate", totals[phase]["ACC"], n), ("complete", totals[phase]["COMP"], n * field_count),
                    ("unique", n - excess, n), ("consistent", totals[phase]["CONS"], n),
                    ("up_to_date", totals[phase]["UD"] if table == "ratings" else None, n if table == "ratings" else None)):
                    score = None if numerator is None else round(100 * numerator / denominator, 2) if denominator else 0.0
                    if score != report["scores"][table]["raw" if phase == "before" else "clean"][metric]:
                        if score is None or abs(score - report["scores"][table]["raw" if phase == "before" else "clean"][metric]) > 0.011:
                            raise ValueError("Report scores disagree with independent integer counts")
                    summary.append({**common, "phase": phase, "source_table": table, "metric": metric,
                                    "numerator": numerator, "denominator": denominator, "score": score})
            action_counts[table] = dict(actions)
            for output in (clean, quarantine, disposition_file, evidence, *quality.values()):
                files.append(output.close())
        files.extend((groups.close(), summary.close()))
        database.close()
        # Keep scratch outside the formal artifact tree without deleting diagnostic input.
        (root / "source-spool.sqlite").rename(work / (root.name + "-spool.sqlite"))
        report_copy = {key: value for key, value in report.items() if key not in {"candidate_report", "cleaned_paths", "disposition_paths"}}
        report_copy.update(source_balance=balance, action_counts=action_counts)
        (root / "report.json").write_text(json.dumps(report_copy, ensure_ascii=False, indent=2), encoding="utf-8")
        files.append({"path": "report.json", "format": "json", "sha256": sha256(root / "report.json"),
                      "bytes": (root / "report.json").stat().st_size})
        manifest = {"schema_version": "manifest-v1", **common, "input_manifest": report["input_manifest"],
                    "files": files, "source_balance": balance, "action_counts": action_counts,
                    "report": "report.json", "jobs": report["jobs"], "execution": report.get("execution")}
        (root / "manifest.json").write_text(canonical(manifest), encoding="utf-8")
        preflight_artifacts(root, expected=expected_identity)
        root.rename(final)
        return final, manifest
    finally:
        # Failed builds retain scratch files for diagnosis, but release native handles.
        # Do not flush pending records or promote an incomplete build on this path.
        for output in writers:
            if not output.closed:
                try:
                    output.writer.close()
                except Exception:
                    pass  # Preserve the primary validation/write error.
        for handle in handles.values():
            handle.close()
        try:
            database.close()
        except sqlite3.ProgrammingError:
            pass
