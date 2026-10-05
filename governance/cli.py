"""Administrative preparation and inspection without model calls."""
import argparse
import json

from governance.service import register_contracts, submit_run
from metadata.connection import ROOT, migrate
from metadata.store import Store
from pipeline.run_pipeline import RAW, input_fingerprint
from storage.hdfs import import_input


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("init", "import-data", "submit", "status", "reconcile-storage", "retention-preview", "hive-projection", "hive-register", "history-analysis", "history-series", "performance-capture", "performance-compare", "compare-semantics", "compare-optimization", "maintenance-status", "maintenance-begin", "maintenance-end", "maintenance-switch-begin", "maintenance-switch-end", "backup", "verify-backup", "restore-backup"))
    parser.add_argument("--key")
    parser.add_argument("--run")
    parser.add_argument("--runs", nargs="+", help="Ordered 2 to 10 formal Run identities for history-series")
    parser.add_argument("--left", help="First baseline identity, or formal Run for compare-semantics")
    parser.add_argument("--right", help="Second baseline identity, or formal Run for compare-semantics")
    parser.add_argument("--include-test-metadata", action="store_true")
    parser.add_argument("--retain-days", type=int, default=30)
    parser.add_argument("--task-details", action="store_true", help="Capture all Task Attempts and partition counters, within bounded limits")
    parser.add_argument("--owner", help="Maintenance owner identity")
    parser.add_argument("--reason", help="Explicit reason for maintenance mode change")
    parser.add_argument("--expected-revision", type=int, help="Inspected runtime admission revision")
    parser.add_argument("--switch-id", help="Reserved service switch identity")
    parser.add_argument("--path", help="Backup package under outputs/")
    parser.add_argument("--target-database", help="Existing empty ml_governance_restore_<name> database")
    args = parser.parse_args()
    required = {
        'submit':('key',), 'status':('run',),
        'performance-capture':('run',), 'history-series':('runs',),
        'hive-projection':('run',), 'hive-register':('run',), 'history-analysis':('run',),
        'performance-compare':('left','right'), 'compare-semantics':('left','right'),
        'compare-optimization':('left','right'), 'backup':('path',), 'verify-backup':('path',),
        'restore-backup':('path','target_database'),
        'maintenance-begin':('owner','reason','expected_revision'),
        'maintenance-end':('owner','reason','expected_revision'),
        'maintenance-switch-begin':('owner','switch_id','expected_revision'),
        'maintenance-switch-end':('owner','switch_id','expected_revision'),
    }
    missing = [name for name in required.get(args.command,())
               if getattr(args,name) is None or getattr(args,name) == '']
    if missing:
        parser.error(args.command + ' requires ' + ', '.join('--'+name.replace('_','-') for name in missing))
    if args.command == 'history-series' and (not 2 <= len(args.runs) <= 10 or len(set(args.runs)) != len(args.runs)
            or any(not r or len(r)>128 for r in args.runs)):
        parser.error('history-series requires 2 to 10 distinct Run identities of at most 128 characters')
    if args.command == 'retention-preview' and not 1 <= args.retain_days <= 3650:
        parser.error('retention-preview requires 1 <= --retain-days <= 3650')
    if args.command.startswith('maintenance-') and args.expected_revision is not None and args.expected_revision < 0:
        parser.error('--expected-revision must be nonnegative')
    if args.command == "performance-compare":
        if not args.left or not args.right:
            parser.error("performance-compare requires --left and --right")
        from governance.performance import compare_baselines
        print(json.dumps(compare_baselines(args.left,args.right),ensure_ascii=False))
        return
    if args.command == "verify-backup":
        if not args.path:
            parser.error("verify-backup requires --path")
        from governance.backup import verify_backup
        manifest = verify_backup(ROOT / args.path)
        print(json.dumps({"verified":True,"files":len(manifest["files"]),"checkpoint":manifest["checkpoint"]}))
        return
    store = Store()
    if args.command == "maintenance-status":
        print(json.dumps(store.maintenance_status(),default=str,ensure_ascii=False))
    elif args.command in {"maintenance-switch-begin","maintenance-switch-end"}:
        if args.owner is None or args.switch_id is None or args.expected_revision is None:
            parser.error(args.command+' requires --owner, --switch-id and --expected-revision')
        print(json.dumps(store.reserve_switch(owner=args.owner,switch_id=args.switch_id,
                    expected_revision=args.expected_revision,finish=args.command=='maintenance-switch-end')))
    elif args.command in {"maintenance-begin","maintenance-end"}:
        if args.owner is None or args.reason is None or args.expected_revision is None:
            parser.error(args.command + ' requires --owner, --reason and --expected-revision')
        print(json.dumps(store.control_admission(open_admission=args.command=='maintenance-end',owner=args.owner,
                         reason=args.reason,expected_revision=args.expected_revision),default=str,ensure_ascii=False))
    elif args.command == "init":
        migrate(store.config)
        from agent.legacy_archive import install_archive_schema
        install_archive_schema(store)
        register_contracts(store)
        print("Governance schema and version definitions registered")
    elif args.command == "import-data":
        manifest = input_fingerprint(RAW)
        store.register_input("movielens-1m", manifest["dataset_version"], manifest)
        locations = import_input(store, manifest["dataset_version"], {
            table: str(ROOT.joinpath(path).relative_to(ROOT)) for table, path in RAW.items()})
        print(json.dumps({"input_version": manifest["dataset_version"], "locations": locations}))
    elif args.command == "submit":
        if not args.key:
            parser.error("submit requires --key")
        run, created = submit_run(args.key, store=store)
        print(json.dumps({"run_id": run["run_id"], "status": run["status"], "created": created}))
    elif args.command in {"reconcile-storage", "retention-preview"}:
        from governance.reconciliation import scan_storage
        stores = [store]
        if args.include_test_metadata and store.config["database"] != "ml_governance_test":
            stores.append(Store({**store.config, "database": "ml_governance_test"}))
        if args.command == "retention-preview":
            from governance.maintenance import preview_storage
            result = preview_storage(stores, retain_days=args.retain_days)
        else:
            result = scan_storage(stores)
        path = ROOT / "outputs" / (args.command + ".json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(json.dumps({"report": str(path), "counts": result.get("counts"),
                          "candidate_bytes": result.get("candidate_bytes"),
                          "incomplete_publications": len(result["incomplete_publications"]),
                          "publication_mismatches": len(result["publication_mismatches"]), "deletion_authorized": False}))
    elif args.command in {"backup", "restore-backup"}:
        if not args.path:
            parser.error(args.command + " requires --path")
        from governance.backup import create_backup, restore_backup
        if args.command == "backup":
            manifest = create_backup(ROOT / args.path,store.config)
            print(json.dumps({"checkpoint":manifest["checkpoint"],"files":len(manifest["files"]),"path":args.path}))
        else:
            if not args.target_database:
                parser.error("restore-backup requires --target-database")
            result = restore_backup(ROOT / args.path,{**store.config,"database":args.target_database})
            print(json.dumps(result))
    elif args.command in {"compare-semantics", "compare-optimization"}:
        if not args.left or not args.right:
            parser.error(args.command + " requires --left and --right")
        from governance.equivalence import compare_semantics, compare_optimization
        compare = compare_semantics if args.command == "compare-semantics" else compare_optimization
        print(json.dumps(compare(args.left,args.right,store=store),ensure_ascii=False))
    elif args.command == "performance-capture":
        if not args.run:
            parser.error("performance-capture requires --run")
        from governance.performance import capture_baseline
        print(json.dumps(capture_baseline(args.run,store=store,task_details=args.task_details),ensure_ascii=False))
    elif args.command == "history-series":
        if not args.runs:
            parser.error("history-series requires --runs")
        from governance.history import history_series
        print(json.dumps(history_series(args.runs,store=store),ensure_ascii=False))
    elif args.command in {"hive-projection", "hive-register", "history-analysis"}:
        if not args.run:
            parser.error(args.command + " requires --run")
        from governance.history import history_analysis, projection, register_projection
        from agent.results import publication_context
        if args.command == "history-analysis":
            print(json.dumps(history_analysis(args.run, store=store), ensure_ascii=False))
        elif args.command == "hive-register":
            print(json.dumps(register_projection(args.run, store=store), ensure_ascii=False))
        else:
            _, publication, _ = publication_context(args.run, store)
            print(projection(publication)["sql"])
    else:
        if not args.run:
            parser.error("status requires --run")
        run = store.get_run(args.run)
        if run is None:
            parser.exit(1,'Run not found: '+args.run+'\n')
        print(json.dumps(run, default=str, ensure_ascii=False))


if __name__ == "__main__":
    main()
