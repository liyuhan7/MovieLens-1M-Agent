"""Build in the pinned Linux worker using a verified build-only JDK/Hadoop SDK.

Requires an explicit new outputs path; never overwrites a live execution's iter1.jar.
"""
import argparse
import glob
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            value.update(block)
    return value.hexdigest()


def build(output):
    output = Path(output).resolve()
    if not output.is_relative_to(ROOT / "outputs") or output.suffix != ".jar" or output.exists():
        raise ValueError("Build output must be a new .jar under outputs")
    source = ROOT / "hadoop/src/main/java/mliter1/IterationOne.java"
    sdk_root = ROOT / "outputs/build-sdk/temurin-11.0.28+6"
    sdk = json.loads((sdk_root / "manifest.json").read_text(encoding="utf-8"))
    java_home = ROOT / sdk["java_home"]
    for member in sdk["files"]:
        path = java_home / member["path"]
        if path.stat().st_size != member["bytes"] or digest(path) != member["sha256"]:
            raise ValueError("Build SDK file checksum mismatch: " + member["path"])
    compiler = java_home / "bin/javac"
    classpath = subprocess.check_output(["hadoop", "classpath", "--glob"], text=True).strip()
    dependencies = sorted({Path(value).resolve() for member in classpath.split(os.pathsep)
                           for value in glob.glob(member) if Path(value).is_file() and Path(value).suffix == ".jar"})
    if not dependencies:
        raise ValueError("Hadoop SDK classpath is empty")
    classes = output.parent / (output.stem + "-classes")
    classes.mkdir(parents=True, exist_ok=False)
    subprocess.run([str(compiler), "-encoding", "UTF-8", "--release", "8", "-cp", classpath,
                    "-d", str(classes), str(source)], check=True, timeout=180)
    files = sorted(classes.rglob("*.class"))
    if not files:
        raise ValueError("Compiler produced no application classes")
    for path in files:
        header = path.read_bytes()[:8]
        if header[:4] != b"\xca\xfe\xba\xbe" or struct.unpack(">H", header[6:8])[0] != 52:
            raise ValueError("Application classes must target Java 8 bytecode")
    with zipfile.ZipFile(output, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        entries = [("META-INF/MANIFEST.MF", b"Manifest-Version: 1.0\r\nMain-Class: mliter1.IterationOne\r\n\r\n")]
        entries.extend((path.relative_to(classes).as_posix(), path.read_bytes()) for path in files)
        for name, content in entries:
            info = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, content, compresslevel=9)
    record = {"source": source.relative_to(ROOT).as_posix(), "source_sha256": digest(source),
              "sdk_manifest_sha256": digest(sdk_root / "manifest.json"),
              "sdk_archive_sha256": sdk["archive_sha256"],
              "compiler_version": subprocess.check_output([str(compiler), "-version"], text=True, stderr=subprocess.STDOUT).strip(),
              "compiler_sha256": digest(compiler), "target_class_major": 52,
              "dependencies": [{"path": str(path), "bytes": path.stat().st_size, "sha256": digest(path)} for path in dependencies],
              "classes": [{"path": path.relative_to(classes).as_posix(), "sha256": digest(path)} for path in files],
              "jar_sha256": digest(output), "jar_bytes": output.stat().st_size,
              "deployment_jar_unchanged": True}
    output.with_suffix(".build.json").write_text(json.dumps(record, indent=2), encoding="utf-8")
    return record


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    result = build(parser.parse_args().output)
    print(json.dumps({key: result[key] for key in ("jar_sha256", "jar_bytes", "compiler_version", "target_class_major")}), flush=True)
