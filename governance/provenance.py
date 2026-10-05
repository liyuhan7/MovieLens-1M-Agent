"""Validate original byte positions and transformation chains without storage dependencies."""
import hashlib
import re
from contextlib import nullcontext
from pathlib import PurePosixPath


ACTIONS = {"keep", "repair", "log", "isolate", "dedup"}


def source_id(version, table, file, offset):
    value = "\0".join((version, table, file, str(offset)))
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _identity(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def validate_origin(origin, *, version, table, files, payload, disposition, handle=None):
    """Resolve a source against registered files and check its complete event chain.

    ``files`` maps source-relative POSIX names to raw local paths. Referential
    checks between sources remain the responsibility of the artifact validator.
    """
    if not isinstance(origin, dict) or origin.get("input_version") != version or origin.get("table") != table:
        raise ValueError("Missing or mismatched original provenance")
    file = origin.get("file")
    if not isinstance(file, str) or not file or "\\" in file:
        raise ValueError("Invalid source-relative file name")
    relative = PurePosixPath(file)
    if relative.is_absolute() or ".." in relative.parts or relative.as_posix() != file or file not in files:
        raise ValueError("Source file is not in the registered input")
    offset = origin.get("offset")
    if type(offset) is not int or offset < 0:
        raise ValueError("Source offset must be a nonnegative byte position")
    identity = source_id(version, table, file, offset)
    if identity != origin.get("source_record_id"):
        raise ValueError("Source identity does not resolve to the registered raw file")
    with (nullcontext(handle) if handle is not None else files[file].open("rb")) as handle:
        if offset:
            handle.seek(offset - 1)
            if handle.read(1) != b"\n":
                raise ValueError("Source offset is not a record boundary")
        handle.seek(offset)
        original = handle.readline()
    if not original:
        raise ValueError("Source offset is beyond the last record")
    if original.endswith(b"\n"):
        original = original[:-1]
    if original.endswith(b"\r"):
        original = original[:-1]
    raw_record = original.decode("latin-1")
    if origin.get("raw_record") != raw_record:
        raise ValueError("Source offset does not recover the recorded original bytes")
    events = origin.get("events")
    if not isinstance(events, list) or not events:
        raise ValueError("Missing transformation event chain")
    before = raw_record
    for event in events:
        if not isinstance(event, dict) or not isinstance(event.get("rule"), str) or not event["rule"]:
            raise ValueError("Missing transformation rule")
        action = event.get("action")
        if action not in ACTIONS or event.get("before") != before or not isinstance(event.get("after"), str):
            raise ValueError("Invalid or discontinuous transformation event chain")
        if action == "keep" and event["after"] != before:
            raise ValueError("Normal retention cannot rewrite the source")
        target = event.get("target_source_id")
        if action == "dedup":
            if not _identity(target) or target == identity:
                raise ValueError("Deduplication requires another original source")
        elif target is not None:
            raise ValueError("Only deduplication can select a winner source")
        before = event["after"]
    if before != payload:
        raise ValueError("Transformation chain does not reach the emitted record")
    if disposition not in {"KEEP", "INTERMEDIATE", "ISOLATE", "DEDUP"}:
        raise ValueError("Unknown final source disposition")
    terminal = events[-1]["action"]
    if (disposition == "DEDUP") != (terminal == "dedup") or (disposition == "ISOLATE") != (terminal == "isolate"):
        raise ValueError("Final disposition disagrees with the terminal action")
    if disposition in {"KEEP", "INTERMEDIATE"} and any(event["action"] in {"dedup", "isolate"} for event in events):
        raise ValueError("Removed source cannot become a retained record")
    related = origin.get("related_source_ids")
    if not isinstance(related, list) or any(not _identity(value) or value == identity for value in related):
        raise ValueError("Invalid merged source identity")
    if len(related) != len(set(related)):
        raise ValueError("Merged source identities must not repeat")
    return identity
