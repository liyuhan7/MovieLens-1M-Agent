"""Run-bound evidence bytes, index positions, history cursors and comparison semantics."""
import contextlib
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient
from pymysql import OperationalError

from agent import server
from agent.results import read_evidence, compare_runs, artifact_file
from agent.published import ReportUnavailable
from metadata.store import Store, fingerprint


class ResultTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name).resolve()
        self.cache = self.root / 'outputs/run-own/attempts/attempt-own/artifacts'
        self.path = self.cache / 'evidence/users.parquet'
        self.path.parent.mkdir(parents=True)
        self.body = {'run_id': 'run-own', 'attempt_id': 'attempt-own', 'input_version': 'raw',
                     'rule_version': 'rules', 'metric_version': 'metrics', 'source_record_id': 'a'*64,
                     'source_table': 'users', 'rule_id': 'U6', 'metric': 'unique', 'evidence_id': 'b'*64,
                     'action': 'dedup', 'before': '1::M', 'after': '1::M'}
        pq.write_table(pa.Table.from_pylist([self.body]), self.path)
        self.item = {'path': 'evidence/users.parquet', 'bytes': self.path.stat().st_size,
                     'sha256': hashlib.sha256(self.path.read_bytes()).hexdigest()}
        self.pub = {'run_id': 'run-own', 'attempt_id': 'attempt-own', 'publish_id': 'pub-own',
                    'status': 'PUBLISHED', 'storage_path': '/ml/published/pub-own', 'manifest': {
                        'run_id': 'run-own', 'attempt_id': 'attempt-own', 'files': [self.item]}}
        self.pub['manifest_hash'] = fingerprint(self.pub['manifest'])
        self.run = {'run_id': 'run-own', 'status': 'PUBLISHED', 'input_version': 'raw',
                    'rule_version': 'rules', 'metric_version': 'metrics', 'dataset_id': 'ml'}
        self.index = {k: self.body[k] for k in ('evidence_id','attempt_id','source_record_id','source_table','rule_id','metric')}
        self.index.update(file_path=self.item['path'], row_group=0, row_in_group=0,
                          detail={'body_sha256': fingerprint(self.body)})
        self.store = Mock()
        self.store.get_run.return_value = self.run
        self.store.get_publish.return_value = self.pub
        self.store.list_attempts.return_value = [{'attempt_id': 'attempt-own', 'work_path': 'outputs/run-own/attempts/attempt-own'}]
        self.store.query_evidence.return_value = {'items': [self.index], 'next_after': None}

    def tearDown(self):
        self.tmp.cleanup()

    def read(self, **filters):
        return read_evidence('run-own', store=self.store, root=self.root, **filters)

    def test_body_is_returned_from_the_selected_publication(self):
        page = self.read(rule_id='U6', limit=1)
        self.assertEqual(page['items'], [self.body])
        self.assertEqual(page['publish_id'], 'pub-own')
        self.store.query_evidence.assert_called_once_with('attempt-own', rule_id='U6', limit=1)

    def test_candidate_or_missing_run_cannot_query_evidence(self):
        self.store.get_run.return_value = None
        with self.assertRaises(ReportUnavailable) as e:
            self.read()
        self.assertEqual(e.exception.status_code, 404)
        self.store.query_evidence.assert_not_called()
        self.store.get_run.return_value = self.run
        self.run['status'] = 'VALIDATING'
        with self.assertRaises(ReportUnavailable) as e:
            self.read()
        self.assertEqual(e.exception.status_code, 409)

    def test_index_body_and_positions_must_match(self):
        for field, bad in [('source_record_id','c'*64),('row_in_group',1),('row_group',1),('file_path','../report.json')]:
            with self.subTest(field=field):
                old = self.index[field]
                self.index[field] = bad
                with self.assertRaises(ReportUnavailable):
                    self.read()
                self.index[field] = old
        self.index['detail']['body_sha256'] = '0'*64
        with self.assertRaisesRegex(ReportUnavailable, 'binding'):
            self.read()

    def test_modified_cache_and_manifest_are_rejected(self):
        self.path.write_bytes(b'corrupt')
        with self.assertRaisesRegex(ReportUnavailable, 'immutable manifest'):
            self.read()
        self.pub['manifest_hash'] = '0'*64
        with self.assertRaisesRegex(ReportUnavailable, 'manifest binding'):
            self.read()

    def test_missing_cache_can_be_rebuilt_only_from_verified_hdfs_bytes(self):
        content = self.path.read_bytes()
        self.path.unlink()
        hdfs = Mock()
        hdfs.get.side_effect = lambda remote, local: Path(local).write_bytes(content)
        self.assertEqual(read_evidence('run-own', store=self.store, root=self.root, hdfs=hdfs)['items'], [self.body])
        hdfs.get.assert_called_once()
        self.assertEqual(self.path.read_bytes(), content)

    def test_bad_download_is_never_promoted(self):
        self.path.unlink()
        hdfs = Mock()
        hdfs.get.side_effect = lambda remote, local: Path(local).write_bytes(b'bad')
        with self.assertRaises(ReportUnavailable):
            artifact_file(self.pub, self.cache, self.item['path'], hdfs=hdfs)
        self.assertFalse(self.path.exists())

    def test_invalid_cursor_is_rejected_before_query(self):
        for filters in ({'after':'bogus'}, {'source_record_id':'short'}, {'source_table':'other'}):
            with self.subTest(filters=filters), self.assertRaises(ReportUnavailable):
                self.read(**filters)
        self.store.query_evidence.assert_not_called()

    def test_metric_versions_control_comparison_deltas(self):
        a = {'metric_version': 'v1', 'scores': {'users': {'raw': {'unique': 80}, 'clean': {'unique': 90}}}}
        b = {'metric_version': 'v1', 'scores': {'users': {'raw': {'unique': 85}, 'clean': {'unique': None}}}}
        with patch('agent.results.read_published_report', side_effect=[(b'',a,self.pub),(b'',b,self.pub)]):
            compared = compare_runs('left','right',store=self.store)
        self.assertEqual(compared['scores'][0]['delta'],5)
        self.assertIsNone(compared['scores'][1]['delta'])
        b['metric_version'] = 'v2'
        with patch('agent.results.read_published_report', side_effect=[(b'',a,self.pub),(b'',b,self.pub)]):
            compared = compare_runs('left','right',store=self.store)
        self.assertFalse(compared['metric_compatible'])
        self.assertTrue(all(row['delta'] is None for row in compared['scores']))

    def test_history_and_evidence_sql_are_parameterized_and_paginated(self):
        cursor = Mock()
        cursor.fetchall.return_value = [{'evidence_id':'a'}, {'evidence_id':'b'}]
        store = Store(config={'fixture': True})
        @contextlib.contextmanager
        def transaction():
            yield cursor
        store.transaction = transaction
        page = store.query_evidence('attempt',rule_id="U6' OR 1=1",after='start',limit=1)
        sql, args = cursor.execute.call_args.args
        self.assertNotIn("OR 1=1",sql)
        self.assertIn("U6' OR 1=1",args)
        self.assertEqual(page['next_after'],'a')
        cursor.fetchall.return_value = [{'request_seq':10}, {'request_seq':9}]
        self.assertEqual(store.list_runs(before=11,limit=1)['next_before'],10)

    def test_http_query_validation_and_storage_unavailability(self):
        client = TestClient(server.app)
        self.assertEqual(client.get('/api/runs/run-own/evidence?limit=101').status_code,422)
        with patch.object(server,'read_evidence',side_effect=ReportUnavailable('missing',404)):
            self.assertEqual(client.get('/api/runs/missing/evidence').status_code,404)
        with patch.object(server,'Store',side_effect=OperationalError('secret')):
            r = client.get('/api/runs')
        self.assertEqual(r.status_code,503)
        self.assertNotIn('secret',r.text)
        self.assertIn('governance.js',client.get('/').text)


if __name__ == '__main__':
    unittest.main()
