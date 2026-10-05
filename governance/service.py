"""Deterministic submission service; requests never depend on an LLM being available."""
import json
from pathlib import Path

from metadata.connection import ROOT
from metadata.store import Store
from pipeline.run_pipeline import RAW, RULE_VERSION, METRIC_VERSION, input_fingerprint, sha256
from governance.build_identity import capture_execution


def register_contracts(store):
    # Record the rule design document and the executable contract separately. The contract
    # captures known rule/design differences; registering a definition fixes its identity,
    # not a correctness verdict.
    rule_source = ROOT / "docs" / "清洗规则与五维评分设计.md"
    store.register_definition("rule", RULE_VERSION, {
        "source_document": rule_source.relative_to(ROOT).as_posix(),
        "source_sha256": sha256(rule_source),
        "rule_pack": "default",
        "implementation_contract": "docs/规则与指标契约.md",
        "contract_sha256": sha256(ROOT / "docs" / "规则与指标契约.md"),
    })
    metric = json.loads((ROOT / "schemas" / (METRIC_VERSION + ".json")).read_text(encoding="utf-8"))
    store.register_definition("metric", metric["version"], metric)


def submit_run(idempotency_key, *, store=None, raw=None, parameters=None, run_id=None, execution_mode="yarn"):
    if not isinstance(idempotency_key,str) or not idempotency_key or len(idempotency_key)>120:
        raise ValueError('Invalid idempotency identity')
    parameters = {'rule_pack':'default'} if parameters is None else parameters
    if parameters != {'rule_pack':'default'}:
        raise ValueError('Only the registered default rule pack is supported')
    store = store or Store()
    register_contracts(store)
    raw = {name: Path(path).resolve() for name, path in (raw or RAW).items()}
    if set(raw) != {"users", "movies", "ratings"}:
        raise ValueError("Exactly users, movies and ratings inputs are required")
    for table, path in raw.items():
        if not path.is_relative_to(ROOT) or not (path.is_file() or path.is_dir()):
            raise ValueError("Input must be an existing file or directory inside this workspace")
        if path.is_file() and path.name != table + ".dat":
            raise ValueError("Single raw files must use the canonical <table>.dat name; use a directory for named members")
    manifest = input_fingerprint(raw)
    store.register_input("movielens-1m", manifest["dataset_version"], manifest)
    jar = ROOT / "hadoop" / "build" / "iter1.jar"
    request = {"dataset_id": "movielens-1m", "input_version": manifest["dataset_version"],
               "rule_version": RULE_VERSION, "metric_version": METRIC_VERSION,
               "execution_mode": execution_mode, "raw_paths": {key: value.relative_to(ROOT).as_posix() for key, value in raw.items()},
               "jar_sha256": sha256(jar), "execution": capture_execution(),
               "parameters": parameters}
    return store.submit(request, idempotency_key, run_id=run_id)
