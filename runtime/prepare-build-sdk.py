"""Verify and unpack the fixed build-only JDK; never alter runtime images."""
import hashlib
import json
from pathlib import Path
import tarfile

ROOT = Path(__file__).resolve().parents[1]
ARCHIVE = ROOT / "outputs/build-sdk/OpenJDK11U-jdk_x64_linux_hotspot_11.0.28_6.tar.gz"
SHA256 = "7dfd551795a8884b26cbb02e0301da95db40160bb194f48271dc2ef9367f50c2"
URL = "https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.28%2B6/OpenJDK11U-jdk_x64_linux_hotspot_11.0.28_6.tar.gz"


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            value.update(block)
    return value.hexdigest()


if __name__ == "__main__":
    if digest(ARCHIVE) != SHA256:
        raise ValueError("Build SDK archive checksum mismatch")
    target = ARCHIVE.parent / "temurin-11.0.28+6"
    target.mkdir(exist_ok=False)
    with tarfile.open(ARCHIVE) as archive:
        archive.extractall(target, filter="data")
    homes = list(target.glob("*/bin/javac"))
    if len(homes) != 1:
        raise ValueError("Archive must contain exactly one JDK")
    home = homes[0].parent.parent
    record = {"version": "Temurin 11.0.28+6", "source": URL,
              "archive_sha256": SHA256, "archive_bytes": ARCHIVE.stat().st_size,
              "java_home": home.relative_to(ROOT).as_posix(),
              "files": [{"path": path.relative_to(home).as_posix(), "bytes": path.stat().st_size,
                         "sha256": digest(path)} for path in sorted(home.rglob("*")) if path.is_file()]}
    (target / "manifest.json").write_text(json.dumps(record, indent=2), encoding="utf-8")
    print(json.dumps({key: record[key] for key in ("version", "archive_sha256", "java_home")}), flush=True)
