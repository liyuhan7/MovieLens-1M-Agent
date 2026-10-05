"""Required command parameters fail before metadata construction or side effects."""
import contextlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from governance.cli import main
from governance.service import submit_run


class CliContractTests(unittest.TestCase):
    def test_all_commands_dispatch_to_their_business_service(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cases = [
                ('init',[], 'governance.cli.migrate',None),
                ('import-data',[], 'governance.cli.import_input',{}),
                ('submit',['--key','key'], 'governance.cli.submit_run',({'run_id':'own','status':'QUEUED'},True)),
                ('status',['--run','own'],None,None),
                ('maintenance-status',[],None,None),
                ('maintenance-begin',['--owner','ops','--reason','upgrade','--expected-revision','3'],None,None),
                ('maintenance-end',['--owner','ops','--reason','upgrade','--expected-revision','3'],None,None),
                ('maintenance-switch-begin',['--owner','ops','--switch-id','a'*32,'--expected-revision','3'],None,None),
                ('maintenance-switch-end',['--owner','ops','--switch-id','a'*32,'--expected-revision','3'],None,None),
                ('reconcile-storage',[], 'governance.reconciliation.scan_storage',{'incomplete_publications':[],'publication_mismatches':[]}),
                ('retention-preview',['--retain-days','7'], 'governance.maintenance.preview_storage',{'incomplete_publications':[],'publication_mismatches':[]}),
                ('hive-projection',['--run','own'], 'governance.history.projection',{'sql':'SELECT formal;'}),
                ('hive-register',['--run','own'], 'governance.history.register_projection',{}),
                ('history-analysis',['--run','own'], 'governance.history.history_analysis',{}),
                ('history-series',['--runs','two','one'], 'governance.history.history_series',{}),
                ('performance-capture',['--run','own','--task-details'], 'governance.performance.capture_baseline',{}),
                ('performance-compare',['--left','left','--right','right'], 'governance.performance.compare_baselines',{}),
                ('compare-semantics',['--left','left','--right','right'], 'governance.equivalence.compare_semantics',{}),
                ('compare-optimization',['--left','left','--right','right'], 'governance.equivalence.compare_optimization',{}),
                ('backup',['--path','outputs/package'], 'governance.backup.create_backup',{'checkpoint':'c','files':[]}),
                ('verify-backup',['--path','outputs/package'], 'governance.backup.verify_backup',{'checkpoint':'c','files':[]}),
                ('restore-backup',['--path','outputs/package','--target-database','ml_governance_restore_test'], 'governance.backup.restore_backup',{}),
            ]
            for command,args,target,result in cases:
                with self.subTest(command=command),contextlib.ExitStack() as stack:
                    stack.enter_context(patch('sys.argv',['cli',command]+args))
                    stack.enter_context(patch('governance.cli.ROOT',root))
                    output = stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
                    store_type = stack.enter_context(patch('governance.cli.Store'))
                    store = store_type.return_value
                    store.config = {'database':'main'}
                    for name in ('maintenance_status','control_admission','reserve_switch'):
                        getattr(store,name).return_value = {}
                    store.get_run.return_value = {'run_id':'own'}
                    register = stack.enter_context(patch('governance.cli.register_contracts'))
                    archive = stack.enter_context(patch('agent.legacy_archive.install_archive_schema'))
                    stack.enter_context(patch('governance.cli.input_fingerprint',return_value={'dataset_version':'raw'}))
                    stack.enter_context(patch('governance.cli.RAW',{t:t+'.dat' for t in ('users','movies','ratings')}))
                    context = stack.enter_context(patch('agent.results.publication_context',return_value=({}, {'publish_id':'pub'},None)))
                    service = stack.enter_context(patch(target,return_value=result)) if target else None
                    main()
                    if command in {'performance-compare','verify-backup'}:
                        store_type.assert_not_called()
                    else:
                        store_type.assert_called_once_with()
                    if command=='init':
                        service.assert_called_once_with(store.config)
                        archive.assert_called_once_with(store)
                        register.assert_called_once_with(store)
                    elif command=='import-data':
                        store.register_input.assert_called_once_with('movielens-1m','raw',{'dataset_version':'raw'})
                        service.assert_called_once_with(store,'raw',{t:t+'.dat' for t in ('users','movies','ratings')})
                    elif command=='submit':
                        service.assert_called_once_with('key',store=store)
                    elif command=='status':
                        store.get_run.assert_called_once_with('own')
                    elif command=='maintenance-status':
                        store.maintenance_status.assert_called_once_with()
                    elif command in {'maintenance-begin','maintenance-end'}:
                        store.control_admission.assert_called_once_with(open_admission=command=='maintenance-end',owner='ops',reason='upgrade',expected_revision=3)
                    elif command.startswith('maintenance-switch'):
                        store.reserve_switch.assert_called_once_with(owner='ops',switch_id='a'*32,expected_revision=3,finish=command.endswith('end'))
                    elif command=='reconcile-storage':
                        service.assert_called_once_with([store])
                    elif command=='retention-preview':
                        service.assert_called_once_with([store],retain_days=7)
                    elif command=='hive-projection':
                        context.assert_called_once_with('own',store)
                        service.assert_called_once_with({'publish_id':'pub'})
                    elif command in {'hive-register','history-analysis'}:
                        service.assert_called_once_with('own',store=store)
                    elif command=='history-series':
                        service.assert_called_once_with(['two','one'],store=store)
                    elif command=='performance-capture':
                        service.assert_called_once_with('own',store=store,task_details=True)
                    elif command=='performance-compare':
                        service.assert_called_once_with('left','right')
                    elif command in {'compare-semantics','compare-optimization'}:
                        service.assert_called_once_with('left','right',store=store)
                    elif command=='verify-backup':
                        service.assert_called_once_with(root/'outputs/package')
                    elif command in {'backup','restore-backup'}:
                        config = store.config if command=='backup' else {'database':'ml_governance_restore_test'}
                        service.assert_called_once_with(root/'outputs/package',config)
                    self.assertTrue(output.getvalue())

    def test_unknown_status_exits_unsuccessfully(self):
        with patch('sys.argv',['cli','status','--run','unknown']),patch('governance.cli.Store') as store, \
                contextlib.redirect_stderr(io.StringIO()) as errors:
            store.return_value.get_run.return_value = None
            with self.assertRaises(SystemExit) as raised:
                main()
            self.assertEqual(raised.exception.code,1)
            self.assertIn('Run not found',errors.getvalue())

    def test_invalid_submission_is_rejected_before_metadata_or_file_access(self):
        with patch('governance.service.Store') as store,patch('governance.service.register_contracts') as register:
            for key,parameters in (('',None),('x'*121,None),('valid',{}),('valid',{'rule_pack':'unknown'})):
                with self.assertRaises(ValueError):
                    submit_run(key,parameters=parameters)
            store.assert_not_called()
            register.assert_not_called()

    def test_required_parameters_are_checked_without_metadata(self):
        commands = ('submit','status','hive-projection','hive-register','history-analysis','history-series',
                    'performance-capture','performance-compare','compare-semantics','compare-optimization',
                    'backup','verify-backup','restore-backup','maintenance-begin','maintenance-end',
                    'maintenance-switch-begin','maintenance-switch-end')
        for command in commands:
            with self.subTest(command=command),patch('sys.argv',['cli',command]), \
                    patch('governance.cli.Store') as store,contextlib.redirect_stderr(io.StringIO()) as errors:
                with self.assertRaises(SystemExit) as raised:
                    main()
                self.assertEqual(raised.exception.code,2)
                self.assertIn('requires',errors.getvalue())
                store.assert_not_called()

    def test_bounded_parameters_are_checked_without_metadata(self):
        arguments = [['history-series','--runs','same','same'],['history-series','--runs','one'],
                     ['retention-preview','--retain-days','0'],['retention-preview','--retain-days','3651'],
                     ['maintenance-begin','--owner','owner','--reason','upgrade','--expected-revision','-1']]
        for args in arguments:
            with self.subTest(args=args),patch('sys.argv',['cli']+args),patch('governance.cli.Store') as store, \
                    contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    main()
                store.assert_not_called()

    def test_history_order_and_revision_zero_are_forwarded(self):
        with patch('sys.argv',['cli','history-series','--runs','two','one']),patch('governance.cli.Store') as store, \
                patch('governance.history.history_series',return_value={}) as series,contextlib.redirect_stdout(io.StringIO()):
            main()
            series.assert_called_once_with(['two','one'],store=store.return_value)
        with patch('sys.argv',['cli','maintenance-begin','--owner','owner','--reason','upgrade','--expected-revision','0']), \
                patch('governance.cli.Store') as store,contextlib.redirect_stdout(io.StringIO()):
            store.return_value.control_admission.return_value = {}
            main()
            store.return_value.control_admission.assert_called_once_with(open_admission=False,owner='owner',reason='upgrade',expected_revision=0)
