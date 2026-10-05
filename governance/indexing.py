"""Persist and verify actual Parquet row-group positions, without one file per evidence."""
import pyarrow.parquet as pq

from governance.manifest import preflight_artifacts
from metadata.store import Conflict, fingerprint


def expected_identity(store, claim):
    run = store.get_run(claim.run_id)
    return {"run_id": claim.run_id, "attempt_id": claim.attempt_id,
            **{key: run[key] for key in ("input_version", "rule_version", "metric_version")},
            "input_manifest": store.get_input(run["input_version"])["manifest"]}


def index_artifacts(store, claim, root, *, progress=None):
    expected = expected_identity(store, claim)
    verified = preflight_artifacts(root, expected=expected)
    total = 0
    for path in sorted(name for name in verified["files"] if name.startswith("evidence/")):
        if progress:
            progress("index-" + path.split("/")[1])
        parquet = pq.ParquetFile(root / path)
        if parquet.metadata.num_rows != verified["files"][path]["rows"]:
            raise ValueError("Parquet evidence rows differ from manifest")
        for group in range(parquet.num_row_groups):
            rows = []
            for position, body in enumerate(parquet.read_row_group(group).to_pylist()):
                if any(body[key] != expected[key] for key in ("run_id", "attempt_id", "input_version", "rule_version", "metric_version")):
                    raise Conflict("Evidence body differs from its fixed execution")
                rows.append({"evidence_id": body["evidence_id"], "attempt_id": claim.attempt_id,
                             "source_record_id": body["source_record_id"], "source_table": body["source_table"],
                             "rule_id": body["rule_id"], "metric": body["metric"], "file_path": path,
                             "row_group": group, "row_in_group": position,
                             "detail": {key: body[key] for key in ("run_id", "input_version", "rule_version", "metric_version")}
                                       | {"body_sha256": fingerprint(body)}})
                if len(rows) == 500:
                    store.index_evidence_batch(claim, rows)
                    total += len(rows)
                    rows = []
            if rows:
                store.index_evidence_batch(claim, rows)
                total += len(rows)
    rows = pq.ParquetFile(root / "quality/summary.parquet").read().to_pylist()
    quality = []
    for body in rows:
        if any(body[key] != expected[key] for key in ("run_id", "attempt_id", "input_version", "rule_version", "metric_version")):
            raise Conflict("Quality body differs from its fixed execution")
        quality.append({key: body[key] for key in ("attempt_id", "phase", "source_table", "metric", "metric_version", "numerator", "denominator", "score")}
                       | {"detail": {"file_path": "quality/summary.parquet", "body_sha256": fingerprint(body)}})
    store.record_quality_batch(claim, quality)
    verified_count = verify_evidence_index(store, claim.attempt_id, root, verified["files"])
    if verified_count != total:
        raise ValueError("Indexed evidence count differs from artifact rows")
    return {"evidence_rows": total, "quality_rows": len(quality)}


def verify_evidence_index(store, attempt_id, root, inventory):
    """Read each declared row group once, using its covering database position index."""
    total = 0
    for path, item in sorted(inventory.items()):
        if not path.startswith("evidence/"):
            continue
        parquet = pq.ParquetFile(root / path)
        count = 0
        for group in range(parquet.num_row_groups):
            if parquet.metadata.row_group(group).num_rows > 5000:
                raise ValueError("Evidence row groups exceed the bounded validation batch")
            positions = store.evidence_group_positions(attempt_id, path, group)
            bodies = parquet.read_row_group(group).to_pylist()
            seen = set()
            for row in positions:
                position = row["row_in_group"]
                if not 0 <= position < len(bodies):
                    raise ValueError("Evidence index position is outside the row group")
                if position in seen:
                    raise ValueError("Two evidence indexes select the same body")
                seen.add(position)
                body = bodies[position]
                for key in ("evidence_id", "attempt_id", "source_record_id", "source_table", "rule_id", "metric"):
                    if row[key] != body[key]:
                        raise ValueError("Evidence index does not recover its declared body")
                if row["detail"]["body_sha256"] != fingerprint(body):
                    raise ValueError("Evidence body changed after indexing")
                count += 1
            if len(positions) != len(bodies):
                raise ValueError("Evidence index does not cover its entire row group")
        if count != item["rows"]:
            raise ValueError("Evidence index does not cover the declared file rows")
        total += count
    if store.evidence_count(attempt_id) != total:
        raise ValueError("Evidence index does not cover the complete evidence inventory")
    return total
