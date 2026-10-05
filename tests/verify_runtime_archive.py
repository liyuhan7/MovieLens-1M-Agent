"""Verify Docker save config identities and every uncompressed layer without extraction."""
import argparse
import hashlib
import gzip
import json
from pathlib import Path
import tarfile


def digest(stream):
    value = hashlib.sha256()
    for chunk in iter(lambda: stream.read(2 << 20), b""):
        value.update(chunk)
    return "sha256:" + value.hexdigest()


def verify(metadata_path):
    metadata_path = Path(metadata_path).resolve()
    metadata = json.loads(metadata_path.read_text(encoding="utf-8-sig"))
    if metadata["schema_version"] != "runtime-image-archive-v1" or metadata["archive"] != "images.tar":
        raise ValueError("Unsupported image archive")
    archive = metadata_path.parent / "images.tar"
    if archive.stat().st_size != metadata["archive_bytes"]:
        raise ValueError("Archive size mismatch")
    with archive.open("rb") as stream:
        if digest(stream) != "sha256:" + metadata["archive_sha256"]:
            raise ValueError("Archive checksum mismatch")
    observed = set()
    verified_layers = {}
    with tarfile.open(archive, "r:") as contents:
        manifests = json.load(contents.extractfile("manifest.json"))
        configs = {image["Config"]: image for image in manifests}
        index = json.load(contents.extractfile("index.json"))
        def descriptor_body(descriptor):
            name = "blobs/sha256/" + descriptor["digest"].removeprefix("sha256:")
            body = contents.extractfile(name).read()
            if len(body) != descriptor["size"] or "sha256:" + hashlib.sha256(body).hexdigest() != descriptor["digest"]:
                raise ValueError("OCI descriptor checksum mismatch")
            return json.loads(body)
        image_configs = {}
        # Docker Desktop records OCI index IDs. Those differ from the config
        # IDs used by the legacy Docker manifest. Verify the exact index and
        # its available linux/amd64 child, without claiming other platforms.
        for descriptor in index["manifests"]:
            identity = descriptor["digest"]
            if identity not in metadata["image_ids"]:
                raise ValueError("Archive includes an unexpected image")
            body = descriptor_body(descriptor)
            if "manifests" in body:
                selected = [entry for entry in body["manifests"] if
                            entry.get("platform", {}).get("os") == "linux" and
                            entry.get("platform", {}).get("architecture") == "amd64"]
                if len(selected) != 1:
                    raise ValueError("Fixed image has no unique linux/amd64 manifest")
                body = descriptor_body(selected[0])
            config_descriptor = body["config"]
            descriptor_body(config_descriptor)
            config_path = "blobs/sha256/" + config_descriptor["digest"].removeprefix("sha256:")
            if config_path not in configs or configs[config_path]["Layers"] != [
                    "blobs/sha256/" + layer["digest"].removeprefix("sha256:") for layer in body["layers"]]:
                raise ValueError("Docker manifest differs from its fixed OCI image")
            image_configs[config_path] = identity
            observed.add(identity)
        for image in manifests:
            config_bytes = contents.extractfile(image["Config"]).read()
            config_id = "sha256:" + hashlib.sha256(config_bytes).hexdigest()
            if image["Config"] not in image_configs or image["Config"] != "blobs/sha256/" + config_id.removeprefix("sha256:"):
                raise ValueError("Archive includes an unbound image config")
            config = json.loads(config_bytes)
            if config["os"] != "linux" or config["architecture"] != "amd64":
                raise ValueError("Runtime archive platform differs from the validated environment")
            if len(config["rootfs"]["diff_ids"]) != len(image["Layers"]):
                raise ValueError("Archive layer set differs from its image config")
            for layer, expected in zip(image["Layers"], config["rootfs"]["diff_ids"]):
                if layer not in verified_layers:
                    layer_hash = digest(contents.extractfile(layer))
                    if layer != "blobs/sha256/" + layer_hash.removeprefix("sha256:"):
                        raise ValueError("Compressed OCI layer checksum mismatch")
                    stream = contents.extractfile(layer)
                    magic = stream.read(2)
                    stream.seek(0)
                    verified_layers[layer] = digest(gzip.GzipFile(fileobj=stream)) if magic == b"\x1f\x8b" else layer_hash
                if verified_layers[layer] != expected:
                    raise ValueError("Archive layer checksum mismatch")
            print("VERIFIED_IMAGE " + image_configs[image["Config"]], flush=True)
    if observed != set(metadata["image_ids"]):
        raise ValueError("Archive omits a fixed runtime image")
    result = {"execution_sha256": metadata["execution_sha256"], "archive_sha256": metadata["archive_sha256"],
              "image_ids": sorted(observed), "verified_unique_layers": len(verified_layers),
              "full_archive_and_all_layers_hashed": True, "cross_machine_execution_tested": False,
              "verified_platform": "linux/amd64",
              "data_volumes_exported": False}
    target = metadata_path.parent / "verification.json"
    target.write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest")
    print(json.dumps(verify(parser.parse_args().manifest)), flush=True)
