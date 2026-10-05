"""Explicit operator CAS; use only after reconciliation proves an abandoned candidate."""
import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from metadata.store import Store


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True)
    parser.add_argument("--publish", required=True)
    parser.add_argument("--attempt", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--token", type=int, required=True)
    parser.add_argument("--reason", required=True)
    args = parser.parse_args()
    store = Store()
    if not hasattr(store, "request_publication_replacement"):
        parser.error("Publication replacement is not installed in the execution module yet")
    store.request_publication_replacement(args.run, expected_publish=args.publish, expected_attempt=args.attempt,
        expected_manifest_hash=args.manifest_sha256, expected_token=args.token, reason=args.reason)
    print("Replacement queued; original intent and files remain preserved until the new candidate passes validation.")


if __name__ == "__main__":
    main()
