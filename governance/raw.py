"""Deterministic raw input inventory; Hadoop hidden-file filtering is explicit."""
import hashlib
import json
from pathlib import Path


def input_files(path):
    path = Path(path).resolve()
    if path.is_file():
        return {path.name: path}
    if not path.is_dir():
        raise ValueError("Raw input must be a regular file or directory")
    files = {}
    for candidate in sorted(path.rglob("*")):
        relative = candidate.relative_to(path)
        if any(part.startswith((".", "_")) for part in relative.parts):
            continue
        if candidate.is_symlink():
            raise ValueError("Raw directory cannot contain symbolic links")
        if candidate.is_file():
            if not candidate.resolve().is_relative_to(path):
                raise ValueError("Raw directory member escaped its root")
            files[relative.as_posix()] = candidate
    if not files:
        raise ValueError("A raw directory must contain at least one visible file")
    return files


def inventory_hash(files):
    return hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
