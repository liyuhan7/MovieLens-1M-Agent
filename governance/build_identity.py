"""Capture immutable execution material without including credentials."""
import hashlib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def capture_execution():
    paths = [ROOT / "hadoop/build/iter1.jar", ROOT / "requirements.txt", ROOT / "runtime/requirements-worker.txt",
             ROOT / "runtime/compose.governance.yaml", ROOT / "runtime/start-hadoop.sh"]
    for directory, pattern in (("hadoop/src/main/java", "*.java"), ("pipeline", "*.py"),
                               ("governance", "*.py"), ("metadata", "*.py"), ("storage", "*.py"),
                               ("runtime/hadoop-conf", "*.xml")):
        paths.extend((ROOT / directory).rglob(pattern))
    hashes = {}
    for path in sorted(set(paths)):
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
        hashes[path.relative_to(ROOT).as_posix()] = digest.hexdigest()
    images = {}
    env = ROOT / "runtime/.env"
    if env.is_file():
        for line in env.read_text(encoding="utf-8-sig").splitlines():
            key, separator, value = line.partition("=")
            if separator and key in {"ML_HADOOP_IMAGE", "ML_WORKER_IMAGE"}:
                images[key] = value
    import os
    for key in ('ML_HADOOP_IMAGE','ML_WORKER_IMAGE'):
        if os.environ.get(key):
            images[key] = os.environ[key]
    return {"files": hashes, "images": images}
