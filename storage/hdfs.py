"""Native Hadoop client inside the worker; no shell interpolation or host Docker API."""
import hashlib
import subprocess
import uuid
from pathlib import Path, PurePosixPath

from metadata.connection import ROOT


class Hdfs:
    @staticmethod
    def path(value):
        path = PurePosixPath(value)
        if not path.is_absolute() or ".." in path.parts or not str(path).startswith("/ml/"):
            raise ValueError("Governance storage is confined to /ml/")
        return str(path)

    def command(self, *arguments, check=True):
        result = subprocess.run(["hdfs", "dfs", *arguments], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, encoding="utf-8", errors="replace", timeout=180)
        if check and result.returncode:
            raise RuntimeError(f"HDFS {arguments[0]} failed: {result.stderr[-1200:]}")
        return result

    def exists(self, path):
        result = self.command("-test", "-e", self.path(path), check=False)
        if result.returncode not in (0, 1):
            raise RuntimeError(result.stderr)
        return result.returncode == 0

    def mkdir(self, path):
        self.command("-mkdir", "-p", self.path(path))

    def files(self, path):
        result = self.command("-ls", self.path(path))
        return [line.split(maxsplit=7)[-1] for line in result.stdout.splitlines()
                if line.startswith("-")]

    def inventory(self, roots=("/ml/raw", "/ml/staging", "/ml/published")):
        entries = []
        for root in roots:
            if not self.exists(root):
                continue
            result = self.command("-ls", "-R", self.path(root))
            for line in result.stdout.splitlines():
                if line.startswith("-"):
                    fields = line.split(maxsplit=7)
                    if len(fields) != 8:
                        raise ValueError("Cannot parse HDFS namespace inventory")
                    entries.append({"path": self.path(fields[7]), "bytes": int(fields[4]),
                                    "modified": fields[5] + " " + fields[6]})
        return entries

    def digest(self, path):
        process = subprocess.Popen(["hdfs", "dfs", "-cat", self.path(path)], stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        digest = hashlib.sha256()
        length = 0
        while chunk := process.stdout.read(1 << 20):
            digest.update(chunk)
            length += len(chunk)
        errors = process.stderr.read()
        code = process.wait(timeout=180)
        process.stdout.close()
        process.stderr.close()
        if code:
            raise RuntimeError(errors.decode("utf-8", errors="replace")[-1200:])
        return {"sha256": digest.hexdigest(), "bytes": length}

    def put_immutable(self, local, remote):
        local, remote = Path(local).resolve(), self.path(remote)
        if not local.is_relative_to(ROOT) or not local.is_file():
            raise ValueError("Upload source must be a workspace file")
        from pipeline.run_pipeline import sha256
        expected = {"sha256": sha256(local), "bytes": local.stat().st_size}
        if not self.exists(remote):
            self.mkdir(str(PurePosixPath(remote).parent))
            temporary = "/ml/staging/uploads/" + uuid.uuid4().hex + "/blob"
            self.mkdir(str(PurePosixPath(temporary).parent))
            self.command("-put", str(local), temporary)
            if self.digest(temporary) != expected:
                raise ValueError("Incomplete immutable upload; preserving scratch for diagnosis")
            # A completed blob is atomically renamed. Interruption while uploading
            # leaves only an unreferenced staging object, never a partial final file.
            result = self.command("-mv", temporary, remote, check=False)
            if result.returncode and not self.exists(remote):
                raise RuntimeError(result.stderr[-1200:])
        if self.digest(remote) != expected:
            raise ValueError(f"Existing HDFS bytes differ; refusing overwrite: {remote}")
        return expected

    def get(self, remote, local):
        local = Path(local).resolve()
        if not local.is_relative_to(ROOT / "outputs"):
            raise ValueError("Downloaded artifacts must stay under outputs/")
        if local.exists():
            raise FileExistsError(local)
        local.parent.mkdir(parents=True, exist_ok=True)
        self.command("-get", self.path(remote), str(local))
        return local


def import_input(store, version, local_paths):
    row = store.get_input(version)
    if not row:
        raise ValueError("Input version must be registered before HDFS import")
    hdfs = Hdfs()
    locations = {}
    for table, relative in local_paths.items():
        from governance.raw import input_files, inventory_hash
        from pipeline.run_pipeline import input_fingerprint
        source = ROOT / relative
        registered = row["manifest"]["tables"][table]
        if input_fingerprint({table: source})["tables"][table] != registered:
            raise ValueError("Local raw input changed before immutable import")
        prefix = f"/ml/raw/dataset={row['dataset_id']}/version={version}/"
        if "files" in registered:
            remote = prefix + table
            members = []
            for name, local in input_files(source).items():
                verified = hdfs.put_immutable(local, remote + "/" + name)
                members.append({"name": name, **verified})
            actual = {item["path"][len(remote) + 1:] for item in hdfs.inventory((remote,))}
            if actual != {file["name"] for file in members}:
                raise ValueError("Raw HDFS directory differs from its registered file inventory")
            verification = {"sha256": inventory_hash(members), "bytes": sum(file["bytes"] for file in members)}
        else:
            remote = prefix + table + ".dat"
            verification = hdfs.put_immutable(source, remote)
        locations[table] = {"uri": remote, **verification}
    store.mark_input_available(version, locations)
    return locations
