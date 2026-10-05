"""Recovery package bytes, reference completeness and transactional restore order."""
import datetime as dt
from decimal import Decimal
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from governance.backup import (TABLES, create_backup, verify_backup, restore_backup,
                               encode, decode, write_metadata, canonical, metadata_rows)
from metadata.connection import ROOT
from metadata.store import fingerprint


class Cursor:
    def __init__(self,events,columns,nonempty=False):
        self.events,self.columns,self.nonempty = events,columns,nonempty
        self.sql = ''
    def __enter__(self): return self
    def __exit__(self,*args): pass
    def execute(self,sql,args=None):
        self.sql = sql
        self.events.append(('sql',sql))
    def fetchone(self):
        if 'GET_LOCK' in self.sql:return {'acquired':1}
        return {'n':int(self.nonempty)}
    def fetchall(self):
        if 'SHOW COLUMNS' in self.sql:
            name = self.sql.split('`')[1]
            return [{'Field':c} for c in self.columns[name]]
        return []
    def executemany(self,sql,rows):
        self.events.append(('insert',sql,list(rows)))


class BackupTests(unittest.TestCase):
    def setUp(self):
        base = ROOT / 'outputs/tests'
        base.mkdir(parents=True,exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=base)
        self.target = Path(self.temp.name).resolve() / 'checkpoint'
        self.raw,self.report = b'raw bytes',b'{"formal":true}'
        self.sha = lambda b:hashlib.sha256(b).hexdigest()
        self.manifest = {'run_id':'run-own','attempt_id':'attempt-own','files':[
            {'path':'report.json','sha256':self.sha(self.report),'bytes':len(self.report)}]}
        source = {'sha256':self.sha(self.raw),'bytes':len(self.raw)}
        self.rows = {t:[] for t in TABLES}
        self.rows['dataset_version'] = [{'version_id':'raw-own','parent_version_id':None,
                                         'manifest':canonical({'tables':{'users':source}})}]
        self.rows['dataset_location'] = [{'version_id':'raw-own','source_table':'users',
                                         'storage_uri':'/ml/raw/users.dat',**source}]
        self.rows['logical_run'] = [{'run_id':'run-own','status':'PUBLISHED'}]
        self.rows['physical_attempt'] = [{'run_id':'run-own','attempt_id':'attempt-own','status':'PUBLISHED'}]
        self.rows['publish_version'] = [{'publish_id':'pub-own','run_id':'run-own','attempt_id':'attempt-own',
                                        'status':'PUBLISHED','manifest':canonical(self.manifest),
                                        'manifest_hash':fingerprint(self.manifest),'storage_path':'/ml/published/own'}]
        self.columns = {}
        self.config = {'database':'ml_governance_restore_fixture'}
        self.remote = {'/ml/raw/users.dat':self.raw,'/ml/published/own/report.json':self.report,
                       '/ml/published/own/manifest.json':canonical(self.manifest).encode()}
        self.hdfs = Mock()
        def download(remote,local):
            Path(local).parent.mkdir(parents=True,exist_ok=True)
            Path(local).write_bytes(self.remote[remote])
        self.hdfs.get.side_effect = download

    def tearDown(self):
        self.temp.cleanup()

    def export(self,root,config):
        files,captured = [],{}
        for table in TABLES:
            columns = list(self.rows[table][0]) if self.rows[table] else ['fixture']
            self.columns[table] = columns
            item,records = write_metadata(root,table,columns,iter(self.rows[table]))
            files.append(item)
            captured[table] = records
        return files,captured

    def create(self):
        return create_backup(self.target,self.config,hdfs=self.hdfs,exporter=self.export)

    def rewrite_manifest(self,manifest):
        content = canonical(manifest).encode()
        (self.target/'manifest.json').write_bytes(content)
        (self.target/'manifest.sha256').write_text(self.sha(content))

    def connection(self,events,nonempty=False):
        connection = Mock()
        connection.cursor.side_effect = lambda:Cursor(events,self.columns,nonempty)
        connection.commit.side_effect = lambda:events.append(('commit',))
        connection.rollback.side_effect = lambda:events.append(('rollback',))
        return connection

    def restore(self,connection):
        with patch('governance.backup.migrate'),patch('agent.legacy_archive.install_archive_schema'),patch('governance.backup.connect',return_value=connection):
            return restore_backup(self.target,self.config,hdfs=self.hdfs)

    def test_typed_cells_round_trip_without_json_string_confusion(self):
        values = [None,12,'{"type":"datetime"}',Decimal('93.330000'),dt.datetime(2026,10,5,12,3,4),b'\x00\xff']
        self.assertEqual([decode(encode(v)) for v in values],values)

    def test_snapshot_and_immutable_files_are_verified(self):
        manifest = self.create()
        self.assertEqual(len(manifest['storage']),3)
        self.assertEqual(verify_backup(self.target)['checkpoint'],'QUIESCENT')
        item = next(f for f in manifest['files'] if f.get('table') == 'logical_run')
        self.assertEqual(list(metadata_rows(self.target,item)),self.rows['logical_run'])

    def test_corrupt_object_never_verifies(self):
        manifest = self.create()
        (self.target/manifest['storage'][0]['path']).write_bytes(b'corrupt')
        with self.assertRaisesRegex(ValueError,'checksum'):
            verify_backup(self.target)

    def test_rehashed_manifest_cannot_omit_formal_storage_reference(self):
        manifest = self.create()
        manifest['storage'].pop()
        self.rewrite_manifest(manifest)
        with self.assertRaisesRegex(ValueError,'cover metadata'):
            verify_backup(self.target)

    def test_active_runs_do_not_form_recovery_checkpoint(self):
        self.rows['logical_run'][0]['status'] = 'RUNNING'
        with self.assertRaisesRegex(ValueError,'executing Run'):
            self.create()
        self.hdfs.get.assert_not_called()
        self.assertFalse((self.target/'manifest.json').exists())

    def test_published_metadata_without_its_winning_attempt_is_rejected(self):
        self.rows['physical_attempt'] = []
        with self.assertRaisesRegex(ValueError,'matching published Attempt'):
            self.create()
        self.hdfs.get.assert_not_called()

    def test_restore_materializes_all_files_before_metadata_commit(self):
        self.create()
        events = []
        self.hdfs.put_immutable.side_effect = lambda local,remote:events.append(('file',remote))
        result = self.restore(self.connection(events))
        first_insert = next(i for i,e in enumerate(events) if e[0] == 'insert')
        last_file = max(i for i,e in enumerate(events) if e[0] == 'file')
        self.assertLess(last_file,first_insert)
        self.assertEqual(sum(e[0]=='commit' for e in events),1)
        self.assertTrue(result['metadata_restored'])
        self.assertFalse(result['real_environment_verified'])

    def test_file_failure_rolls_back_before_any_run_becomes_visible(self):
        self.create()
        events = []
        self.hdfs.put_immutable.side_effect = RuntimeError('storage failed')
        with self.assertRaises(RuntimeError):
            self.restore(self.connection(events))
        self.assertTrue(any(e[0]=='rollback' for e in events))
        self.assertFalse(any(e[0] in {'insert','commit'} for e in events))

    def test_nonempty_or_production_target_cannot_be_overwritten(self):
        self.create()
        with self.assertRaisesRegex(ValueError,'empty'):
            self.restore(self.connection([],nonempty=True))
        self.hdfs.put_immutable.assert_not_called()
        with self.assertRaisesRegex(ValueError,'separate'):
            restore_backup(self.target,{'database':'ml_governance'},hdfs=self.hdfs)

    def test_parent_versions_restore_in_dependency_order(self):
        parent = self.rows['dataset_version'][0]
        child = {**parent,'version_id':'child','parent_version_id':'raw-own'}
        self.rows['dataset_version'] = [child,parent]
        self.create()
        events = []
        self.restore(self.connection(events))
        inserted = next(e[2] for e in events if e[0]=='insert' and '`dataset_version`' in e[1])
        self.assertEqual([r[0] for r in inserted],['raw-own','child'])

    def test_maintenance_reservation_is_preserved_in_recovery_metadata(self):
        self.rows['runtime_control'] = [{'control_id':1,'admission_open':0,'revision':7,'owner':'operator',
                                        'reason':'switch','switch_id':'a'*32,'updated_at':dt.datetime(2026,10,5)}]
        self.create()
        events = []
        self.restore(self.connection(events))
        record = next(e for e in events if e[0]=='insert' and '`runtime_control`' in e[1])
        self.assertTrue(record[1].startswith('REPLACE'))
        self.assertEqual(record[2][0][1],0)
        self.assertEqual(record[2][0][5],'a'*32)


if __name__ == '__main__':
    unittest.main()
