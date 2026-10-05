"""Two clean builds inside the pinned runtime; no Windows JDK or local deps jars."""
import importlib.util
import json
import os
from pathlib import Path
import sys
import subprocess
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from governance.build_identity import capture_execution

spec = importlib.util.spec_from_file_location("portable_hadoop_build", ROOT / "hadoop/build-reproducible.py")
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)


class ReproducibleBuildTests(unittest.TestCase):
    def test_clean_builds_are_byte_identical_and_keep_deployed_jar(self):
        execution = capture_execution()
        deployed = builder.digest(ROOT / "hadoop/build/iter1.jar")
        directory = ROOT / "outputs/reproducible-build" / uuid.uuid4().hex
        first = builder.build(directory / "first.jar")
        second = builder.build(directory / "second.jar")
        self.assertEqual(first["jar_sha256"], second["jar_sha256"])
        self.assertEqual(first["classes"], second["classes"])
        self.assertEqual(first["dependencies"], second["dependencies"])
        self.assertEqual(first["target_class_major"], 52)
        self.assertEqual(builder.digest(ROOT / "hadoop/build/iter1.jar"), deployed)
        self.assertEqual(capture_execution(), execution)
        self.assertTrue(all(path["path"].startswith("/opt/hadoop-3.3.6/") for path in first["dependencies"]))
        raw = directory / "users.dat"
        raw.write_bytes(b"701::M::25::4::01234\n702::F::18::3::12345\n")
        extracted = directory / "local-extract"
        conf = directory / "local-conf"
        conf.mkdir()
        (conf / "core-site.xml").write_text("<configuration><property><name>fs.defaultFS</name><value>file:///</value></property></configuration>")
        (conf / "mapred-site.xml").write_text("<configuration><property><name>mapreduce.framework.name</name><value>local</value></property></configuration>")
        smoke = subprocess.run(["hadoop", "jar", str(directory / "first.jar"),
                                "extractUsers", str(raw), str(extracted)],
                               env={**os.environ, "HADOOP_CONF_DIR": str(conf)},
                               text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
        (directory / "local-extract.log").write_text(smoke.stdout, encoding="utf-8")
        self.assertEqual(smoke.returncode, 0, smoke.stdout[-1800:])
        self.assertTrue((extracted / "_SUCCESS").is_file())
        ids = [line.split("\t", 1)[0] for path in extracted.glob("part-*") for line in path.read_text().splitlines()]
        self.assertCountEqual(ids, ["701", "702"])
        record = {"jar_sha256": first["jar_sha256"], "compiler_version": first["compiler_version"],
                  "sdk_archive_sha256": first["sdk_archive_sha256"],
                  "sdk_manifest_sha256": first["sdk_manifest_sha256"],
                  "dependency_count": len(first["dependencies"]), "class_count": len(first["classes"]),
                  "target_class_major": 52, "two_clean_builds_byte_identical": True,
                  "deployed_execution_unchanged": True, "fixed_images": execution["images"],
                  "physical_second_machine_tested": False,
                  "local_hadoop_extraction_passed": True,
                  "scope": "two clean compilation directories in pinned Linux worker; Java 8 target; host build/deps unused"}
        (directory / "acceptance.json").write_text(json.dumps(record, indent=2), encoding="utf-8")
        print(json.dumps({"acceptance": str(directory / "acceptance.json"), **record}), flush=True)


if __name__ == "__main__":
    unittest.main()
