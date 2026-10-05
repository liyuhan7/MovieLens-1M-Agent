"""Independent content gate for source-complete Parquet candidates."""
import collections
import datetime as dt
import hashlib
import json
import re
import sqlite3
import uuid

import pyarrow.parquet as pq

from governance.artifacts import SCHEMAS, AGES, GENRES, TS_MIN, TS_MAX, observations, resolve_workspace
from governance.manifest import TABLES, preflight_artifacts
from governance.provenance import validate_origin
from pipeline.run_pipeline import input_fingerprint, five_dim, table_composite, T1, T2
from metadata.store import fingerprint
from governance.raw import input_files


def validate_content(root, *, expected, raw_paths, progress=None):
    verified = preflight_artifacts(root, expected=expected)
    manifest, report = verified["manifest"], verified["report"]
    raw = {table: resolve_workspace(path) for table, path in raw_paths.items()}
    if input_fingerprint(raw) != expected["input_manifest"]:
        raise ValueError("Input bytes changed before content validation")
    raw_files = {table: input_files(path) for table, path in raw.items()}
    # Disk-backed joins keep full source/evidence coverage independent of sample size.
    spool = root.parent / ("validation-" + uuid.uuid4().hex + ".sqlite")
    db = sqlite3.connect(spool)
    db.executescript("CREATE TABLE sources (id TEXT PRIMARY KEY,t TEXT,f TEXT,o INTEGER,raw TEXT,payload TEXT,state TEXT,related TEXT,UNIQUE(t,f,o));"
                     "CREATE TABLE events (id TEXT,n INTEGER,body TEXT,PRIMARY KEY(id,n));"
                     "CREATE TABLE quality (id TEXT,phase TEXT,body TEXT,PRIMARY KEY(id,phase));"
                     "CREATE TABLE observations (id TEXT,phase TEXT,metric TEXT,body TEXT,PRIMARY KEY(id,phase,metric));"
                     "CREATE TABLE cleaned (id TEXT PRIMARY KEY,body TEXT);"
                     "CREATE TABLE quarantine (id TEXT PRIMARY KEY,body TEXT);"
                     "CREATE TABLE groups (phase TEXT,t TEXT,k TEXT,n INTEGER,PRIMARY KEY(phase,t,k));"
                     "CREATE TABLE declared_groups (phase TEXT,t TEXT,k TEXT,n INTEGER,excess INTEGER,PRIMARY KEY(phase,t,k));")
    refs = {phase: {table: set() for table in ("users", "movies")} for phase in ("before", "after")}
    handles = {}

    def rows(path, schema_name):
        parquet = pq.ParquetFile(root / path)
        schema = SCHEMAS[schema_name]
        item = verified["files"][path]
        if not parquet.schema_arrow.equals(schema, check_metadata=False):
            raise ValueError("Parquet schema mismatch: " + path)
        if item["schema_sha256"] != hashlib.sha256(schema.serialize().to_pybytes()).hexdigest() or parquet.metadata.num_rows != item["rows"]:
            raise ValueError("Parquet schema/row metadata differs from manifest")
        for batch in parquet.iter_batches(batch_size=5000):
            for row in batch.to_pylist():
                if any(row[key] != expected[key] for key in ("run_id", "attempt_id", "input_version", "rule_version", "metric_version")):
                    raise ValueError("Parquet row belongs to a foreign execution")
                yield row

    try:
        for table in TABLES:
            # Every source is a unique valid byte boundary (checked below). Matching
            # the independently counted raw population therefore proves no source
            # was omitted; report counters alone cannot provide that proof.
            raw_count = 0
            for file in raw_files[table].values():
                with file.open("rb") as raw_handle:
                    raw_count += sum(1 for _ in raw_handle)
            for row in rows(f"dispositions/{table}/part-00000.parquet", "disposition"):
                if row["source_table"] != table:
                    raise ValueError("Source table disagrees with artifact path")
                db.execute("INSERT INTO sources VALUES (?,?,?,?,?,?,?,?)", (row["source_record_id"], table,
                           row["source_file"], row["source_offset"], row["raw_record"], row["processed_record"],
                           row["disposition"], json.dumps(row["related_source_ids"])))
                if table in ("users", "movies"):
                    key = row["raw_record"].split("::", 1)[0].strip()
                    if key.isascii() and key.isdigit():
                        refs["before"][table].add(key)
                    if row["disposition"] == "KEEP":
                        refs["after"][table].add(row["processed_record"].split("::", 1)[0])
            if db.execute("SELECT COUNT(*) FROM sources WHERE t=?", (table,)).fetchone()[0] != raw_count:
                raise ValueError("Disposition population omits original raw records")
            for row in rows(f"cleaned/{table}/part-00000.parquet", table):
                db.execute("INSERT INTO cleaned VALUES (?,?)", (row["source_record_id"], json.dumps(row)))
            for row in rows(f"quarantine/{table}/part-00000.parquet", "quarantine"):
                db.execute("INSERT INTO quarantine VALUES (?,?)", (row["source_record_id"], json.dumps(row)))
            for phase in ("before", "after"):
                for row in rows(f"quality/{phase}/{table}.parquet", "quality_detail"):
                    if row["phase"] != phase or row["source_table"] != table:
                        raise ValueError("Quality phase/table mismatch")
                    db.execute("INSERT INTO quality VALUES (?,?,?)", (row["source_record_id"], phase, json.dumps(row)))
            for row in rows(f"evidence/{table}/part-00000.parquet", "evidence"):
                if row["source_table"] != table:
                    raise ValueError("Evidence table mismatch")
                if row["kind"] == "TRANSFORMATION":
                    if row["phase"] != "cleaning" or row["metric"] is not None or row["event_order"] < 0:
                        raise ValueError("Invalid transformation evidence shape")
                    db.execute("INSERT INTO events VALUES (?,?,?)", (row["source_record_id"], row["event_order"], json.dumps(row)))
                elif row["kind"] == "QUALITY":
                    if row["event_order"] != -1 or row["phase"] not in {"before", "after"} or row["action"] != "observation":
                        raise ValueError("Invalid metric evidence shape")
                    db.execute("INSERT INTO observations VALUES (?,?,?,?)", (row["source_record_id"], row["phase"], row["metric"], json.dumps(row)))
                else:
                    raise ValueError("Unknown evidence kind")
        for row in rows("quality/groups.parquet", "quality_groups"):
            db.execute("INSERT INTO declared_groups VALUES (?,?,?,?,?)", (row["phase"], row["source_table"], row["business_key"], row["records"], row["duplicate_excess"]))
        summary = {}
        for row in rows("quality/summary.parquet", "quality_summary"):
            key = (row["source_table"], row["phase"], row["metric"])
            if key in summary:
                raise ValueError("Repeated quality summary")
            summary[key] = row
        db.commit()
        totals = {(table, phase): collections.Counter() for table in TABLES for phase in ("before", "after")}
        actions = {table: collections.Counter() for table in TABLES}
        balances = {table: collections.Counter() for table in TABLES}
        for table in TABLES:
            if progress:
                progress("validate-" + table)
            for identity, file, offset, original, payload, state, related_text in db.execute(
                    "SELECT id,f,o,raw,payload,state,related FROM sources WHERE t=? ORDER BY f,o", (table,)):
                related = json.loads(related_text)
                events = [json.loads(row[0]) for row in db.execute("SELECT body FROM events WHERE id=? ORDER BY n", (identity,))]
                if [event["event_order"] for event in events] != list(range(len(events))):
                    raise ValueError("Evidence action ordinals are incomplete")
                origin = {"input_version": expected["input_version"], "table": table, "file": file, "offset": offset,
                          "source_record_id": identity, "raw_record": original, "related_source_ids": related,
                          "events": [{"rule": event["rule_id"], **{key: event[key] for key in ("action", "before", "after", "target_source_id")}} for event in events]}
                if file not in raw_files[table]:
                    raise ValueError("Source file is not in the immutable input inventory")
                handle_key = (table, file)
                if handle_key not in handles:
                    handles[handle_key] = raw_files[table][file].open("rb")
                validate_origin(origin, version=expected["input_version"], table=table, files=raw_files[table],
                                payload=payload, disposition=state, handle=handles[handle_key])
                balances[table][state] += 1
                for row in events:
                    _source_matches(row, identity, table, file, offset, state)
                    if row["evidence_id"] != fingerprint([expected["run_id"], expected["attempt_id"], identity, "event", row["event_order"]]):
                        raise ValueError("Transformation evidence identity mismatch")
                    target = row["target_source_id"]
                    if target and not db.execute("SELECT 1 FROM sources WHERE id=? AND t=?", (target, table)).fetchone():
                        raise ValueError("Dedup target does not exist in its source table")
                    if row["action"] != "keep" or state == "KEEP":
                        actions[table][row["action"] + ":" + row["rule_id"]] += 1
                for target in related:
                    if not db.execute("SELECT 1 FROM sources WHERE id=? AND t=?", (target, table)).fetchone():
                        raise ValueError("Merged source relationship cannot be resolved")
                clean = db.execute("SELECT body FROM cleaned WHERE id=?", (identity,)).fetchone()
                quarantine = db.execute("SELECT body FROM quarantine WHERE id=?", (identity,)).fetchone()
                if bool(clean) != (state == "KEEP") or bool(quarantine) != (state == "ISOLATE"):
                    raise ValueError("Final source disposition differs from output membership")
                if clean:
                    row = json.loads(clean[0])
                    _source_matches(row, identity, table, file, offset)
                    if row["related_source_ids"] != related or _payload(row, table) != _typed_payload_text(payload, table):
                        raise ValueError("Cleaned values/lineage differ from source disposition")
                    if table == "ratings" and (row["user_id"] not in refs["after"]["users"] or row["movie_id"] not in refs["after"]["movies"]):
                        raise ValueError("Cleaned ratings has an unresolved reference")
                if quarantine:
                    row = json.loads(quarantine[0])
                    _source_matches(row, identity, table, file, offset, state)
                    if row["raw_record"] != original or row["processed_record"] != payload or row["reason_rule"] != events[-1]["rule_id"]:
                        raise ValueError("Quarantine cannot be reconciled with its source")
                for phase in ("before", "after"):
                    detail = db.execute("SELECT body FROM quality WHERE id=? AND phase=?", (identity, phase)).fetchone()
                    applicable = phase == "before" or state == "KEEP"
                    if bool(detail) != applicable:
                        raise ValueError("Quality detail does not cover exactly its source population")
                    if not applicable:
                        if db.execute("SELECT 1 FROM observations WHERE id=? AND phase=?", (identity, phase)).fetchone():
                            raise ValueError("Removed source has after-quality evidence")
                        continue
                    obs = observations(original if phase == "before" else payload, table, identity, refs[phase])
                    group_n = db.execute("INSERT INTO groups VALUES (?,?,?,1) ON CONFLICT(phase,t,k) DO UPDATE SET n=n+1 RETURNING n",
                                         (phase, table, obs["business_key"])).fetchone()[0]
                    obs["unique_primary"] = int(group_n == 1)
                    row = json.loads(detail[0])
                    _source_matches(row, identity, table, file, offset)
                    if any(row[key] != value for key, value in obs.items() if key != "split"):
                        raise ValueError("Quality detail disagrees with independent source observation")
                    total = totals[table, phase]
                    total.update({"N": 1, "ACC": obs["accurate"], "COMP": obs["present_fields"], "CONS": obs["consistent"], "UNIQ_EXCESS": int(group_n > 1)})
                    metrics = {"accurate": (obs["accurate"], 1), "complete": (obs["present_fields"], obs["required_fields"]),
                               "consistent": (obs["consistent"], 1), "unique": (obs["unique_primary"], 1)}
                    if table == "ratings":
                        total["UD"] += obs["up_to_date"]
                        if obs["split"]:
                            total[obs["split"]] += 1
                        metrics["up_to_date"] = (obs["up_to_date"], 1)
                    observed = {metric: json.loads(body) for metric, body in db.execute("SELECT metric,body FROM observations WHERE id=? AND phase=?", (identity, phase))}
                    if set(observed) != set(metrics):
                        raise ValueError("Metric evidence does not cover applicable dimensions")
                    for metric, (numerator, denominator) in metrics.items():
                        row = observed[metric]
                        _source_matches(row, identity, table, file, offset, state)
                        if row["evidence_id"] != fingerprint([expected["run_id"], expected["attempt_id"], identity, phase, metric]):
                            raise ValueError("Metric evidence identity mismatch")
                        if (row["numerator"], row["denominator"], row["rule_id"]) != (numerator, denominator, "metric:" + metric):
                            raise ValueError("Metric evidence disagrees with quality detail")
        if db.execute("SELECT 1 FROM events WHERE id NOT IN (SELECT id FROM sources) UNION ALL SELECT 1 FROM quality WHERE id NOT IN (SELECT id FROM sources) "
                      "UNION ALL SELECT 1 FROM observations WHERE id NOT IN (SELECT id FROM sources) UNION ALL SELECT 1 FROM cleaned WHERE id NOT IN (SELECT id FROM sources) "
                      "UNION ALL SELECT 1 FROM quarantine WHERE id NOT IN (SELECT id FROM sources) LIMIT 1").fetchone():
            raise ValueError("Artifact contains an unknown source")
        _validate_dedup_graph(db)
        if db.execute("SELECT 1 FROM groups g LEFT JOIN declared_groups d ON g.phase=d.phase AND g.t=d.t AND g.k=d.k WHERE d.k IS NULL OR d.n<>g.n OR d.excess<>g.n-1 "
                      "UNION ALL SELECT 1 FROM declared_groups d LEFT JOIN groups g ON g.phase=d.phase AND g.t=d.t AND g.k=d.k WHERE g.k IS NULL LIMIT 1").fetchone():
            raise ValueError("Declared uniqueness groups differ from detail")
        expected_summary = set()
        for table in TABLES:
            balance = {"raw": sum(balances[table].values()), **{key: balances[table][state] for key, state in (("keep", "KEEP"), ("isolate", "ISOLATE"), ("dedup", "DEDUP"))}}
            if balance != manifest["source_balance"][table] or dict(actions[table]) != manifest["action_counts"][table]:
                raise ValueError("Manifest dispositions/actions disagree with actual evidence")
            for phase in ("before", "after"):
                total = totals[table, phase]
                recorded = report["metrics_raw" if phase == "before" else "metrics_clean"][table]
                if any(total[key] != recorded.get(key, 0) for key in set(total) | set(recorded)):
                    raise ValueError("Report integer counters disagree with content")
                n = total["N"]
                dimensions = five_dim(total, table)
                for metric, numerator, denominator in (("accurate", total["ACC"], n), ("complete", total["COMP"], n * (4 if table == "ratings" else 5 if table == "users" else 2)),
                        ("unique", n - total["UNIQ_EXCESS"], n), ("consistent", total["CONS"], n),
                        ("up_to_date", total["UD"] if table == "ratings" else None, n if table == "ratings" else None)):
                    key = (table, phase, metric)
                    expected_summary.add(key)
                    row = summary.get(key)
                    if not row or (row["numerator"], row["denominator"]) != (numerator, denominator):
                        raise ValueError("Quality summary disagrees with detail numerators/denominators")
                    score = None if numerator is None else round(100 * numerator / denominator, 2) if denominator else 0.0
                    if row["score"] != score or report["scores"][table]["raw" if phase == "before" else "clean"] != dimensions:
                        raise ValueError("Quality scores disagree with detail")
        if set(summary) != expected_summary:
            raise ValueError("Missing or extra summary dimensions")
        ratings = totals["ratings", "after"]
        if sum(ratings[key] for key in ("TRAIN", "VALID", "TEST")) != ratings["N"]:
            raise ValueError("Cleaned rating split does not conserve rows")
        _validate_derived_report(report, totals)
        db.commit()
        return {"manifest_sha256": verified["manifest_sha256"], "source_balance": manifest["source_balance"],
                "quality_rows": len(summary), "validation_spool": str(spool)}
    finally:
        for handle in handles.values():
            handle.close()
        db.close()


def _source_matches(row, identity, table, file, offset, state=None):
    if (row["source_record_id"], row["source_table"], row["source_file"], row["source_offset"]) != (identity, table, file, offset):
        raise ValueError("Evidence/output original source coordinates mismatch")
    if state is not None and row.get("final_disposition", row.get("disposition")) != state:
        raise ValueError("Evidence/output final disposition mismatch")


def _payload(row, table):
    def text(value):
        return "NULL" if value is None else str(value)
    if table == "users":
        if (not re.fullmatch(r"[0-9]+", row["user_id"]) or row["gender"] not in {"F", "M", None}
                or row["age"] is not None and str(row["age"]) not in AGES
                or row["occupation"] is not None and not 0 <= row["occupation"] <= 20
                or row["zip_code"] is not None and not re.fullmatch(r"[0-9]{5}", row["zip_code"])):
            raise ValueError("Cleaned users value domain violation")
        return "::".join(text(row[key]) for key in ("user_id", "gender", "age", "occupation", "zip_code"))
    if table == "movies":
        year = re.search(r"\(([0-9]{4})\)\s*$", row["title"])
        year = int(year.group(1)) if year and int(year.group(1)) <= 2003 else None
        if (not re.fullmatch(r"[0-9]+", row["movie_id"]) or not row["title"].strip() or row["year"] != year
                or row["genres"] is not None and (not row["genres"] or len(row["genres"]) != len(set(row["genres"])) or any(value not in GENRES for value in row["genres"]))):
            raise ValueError("Cleaned movies value domain violation")
        return "::".join((row["movie_id"], row["title"], "|".join(row["genres"]) if row["genres"] is not None else "NULL"))
    if (not re.fullmatch(r"[0-9]+", row["user_id"]) or not re.fullmatch(r"[0-9]+", row["movie_id"])
            or not 1 <= row["rating"] <= 5 or not TS_MIN <= row["timestamp_seconds"] <= TS_MAX):
        raise ValueError("Cleaned ratings value domain violation")
    return "::".join(text(row[key]) for key in ("user_id", "movie_id", "rating", "timestamp_seconds"))


def _typed_payload_text(payload, table):
    # Occupation is a numeric field whose valid textual input can have leading
    # zeros. Parquet stores its value as int32; identifiers and ZIP remain strings.
    if table == "users":
        fields = payload.split("::")
        if len(fields) != 5:
            raise ValueError("Cleaned users must have exactly five fields")
        if fields[3] != "NULL":
            fields[3] = str(int(fields[3]))
        return "::".join(fields)
    return payload


def _validate_derived_report(report, totals):
    """All user-visible aggregates must derive from content-verified counters."""
    def number(actual, expected, label):
        if expected is None:
            valid = actual is None
        else:
            valid = type(actual) in (int, float) and actual == expected
        if not valid:
            raise ValueError("Report derived arithmetic mismatch: " + label)

    composites = {phase: {} for phase in ("before", "after")}
    for table in TABLES:
        scores = report.get("scores", {}).get(table, {})
        dimensions = {phase: five_dim(totals[table, phase], table) for phase in ("before", "after")}
        for phase, report_phase in (("before", "raw"), ("after", "clean")):
            composite = table_composite(dimensions[phase])
            composites[phase][table] = composite
            number(scores.get("composite_" + report_phase), composite, table + "/composite_" + report_phase)
        expected_delta = {metric: None if before is None else round(dimensions["after"][metric] - before, 2)
                          for metric, before in dimensions["before"].items()}
        delta = scores.get("delta", {})
        if set(delta) != set(expected_delta):
            raise ValueError("Report delta dimensions are incomplete")
        for metric, expected in expected_delta.items():
            number(delta[metric], expected, table + "/delta/" + metric)
        raw_n, clean_n = totals[table, "before"]["N"], totals[table, "after"]["N"]
        counts = report.get("row_change", {}).get(table, {})
        for key, expected in (("raw", raw_n), ("clean", clean_n), ("removed", raw_n - clean_n)):
            if type(counts.get(key)) is not int or counts[key] != expected:
                raise ValueError("Report row-change count mismatch: " + table + "/" + key)
    dataset = report.get("dataset_composite", {})
    if set(dataset) != {"raw", "clean"}:
        raise ValueError("Report dataset composite phases are incomplete")
    for phase, label in (("before", "raw"), ("after", "clean")):
        expected = round(sum(composites[phase][table] * weight for table, weight in
                             (("ratings", .6), ("users", .2), ("movies", .2))), 2)
        number(dataset[label], expected, "dataset_composite/" + label)
    split = report.get("split", {})
    for label, key in (("train", "TRAIN"), ("valid", "VALID"), ("test", "TEST")):
        if type(split.get(label)) is not int or split[label] != totals["ratings", "after"][key]:
            raise ValueError("Report split count mismatch: " + label)
    for label, timestamp in (("t1", T1), ("t2", T2)):
        if split.get(label) != dt.datetime.fromtimestamp(timestamp, dt.timezone.utc).isoformat():
            raise ValueError("Report split boundary mismatch: " + label)


def _validate_dedup_graph(db):
    """Every removed duplicate must resolve to one retained original source.

    Only duplicate components are held in memory. Movie ID and title stages
    can form a chain; its final winner must carry the full merged source set.
    """
    nodes, edges, resolved = {}, {}, {}
    for identity, target, table, state, related, target_table, target_state, target_related in db.execute(
            "SELECT e.id,json_extract(e.body,'$.target_source_id'),s.t,s.state,s.related,d.t,d.state,d.related "
            "FROM events e JOIN sources s ON s.id=e.id "
            "LEFT JOIN sources d ON d.id=json_extract(e.body,'$.target_source_id') "
            "WHERE json_extract(e.body,'$.action')='dedup'"):
        if identity in edges or state != "DEDUP" or not target or target_table != table:
            raise ValueError("Invalid deduplication source graph edge")
        edges[identity] = target
        nodes[identity] = (table, state, related)
        nodes[target] = (target_table, target_state, target_related)
    if set(edges) != {row[0] for row in db.execute("SELECT id FROM sources WHERE state='DEDUP'")}:
        raise ValueError("Deduplication graph does not cover every removed source")

    def node(identity):
        if identity not in nodes:
            value = db.execute("SELECT t,state,related FROM sources WHERE id=?", (identity,)).fetchone()
            if value is None:
                raise ValueError("Merged source graph contains an unknown original source")
            nodes[identity] = value
        return nodes[identity]

    def winner(identity):
        path, seen = [], set()
        current = identity
        while current not in resolved:
            if current in seen:
                raise ValueError("Deduplication source graph contains a cycle")
            seen.add(current)
            path.append(current)
            table, state, related = node(current)
            if state == "KEEP":
                resolved[current] = current
                break
            if state != "DEDUP" or current not in edges:
                raise ValueError("Deduplication chain has no retained final winner")
            current = edges[current]
        final = resolved[current]
        for source in path:
            resolved[source] = final
        return final

    merged_sets = {}
    for identity in edges:
        final = winner(identity)
        if final not in merged_sets:
            merged_sets[final] = set(json.loads(node(final)[2]))
        if identity not in merged_sets[final]:
            raise ValueError("Retained winner omits a merged duplicate source")
    for identity, table, state, related in db.execute("SELECT id,t,state,related FROM sources WHERE related<>'[]'"):
        final = winner(identity)
        for target in json.loads(related):
            if node(target)[0] != table or winner(target) != final:
                raise ValueError("Merged source points to a different final winner")
    return {"dedup_sources": len(edges), "retained_winners": len(merged_sets)}
