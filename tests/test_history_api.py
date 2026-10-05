"""History version filters and explicit current identities remain query-bound."""
import contextlib
import unittest
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient

from agent import server
from agent.published import ReportUnavailable
from governance.history import HistoryUnavailable
from metadata.connection import MetadataUnavailable
from metadata.store import Store


class HistoryApiTests(unittest.TestCase):
    def test_series_preserves_formal_error_status_and_query_order(self):
        with TestClient(server.app) as client,patch('agent.server.history_series') as series:
            series.return_value = {'points':[]}
            self.assertEqual(client.get('/api/history-series?runs=two&runs=one').status_code,200)
            series.assert_called_once_with(['two','one'])
            for error,status in ((ReportUnavailable('missing',404),404),
                                 (ReportUnavailable('candidate',409),409),
                                 (ReportUnavailable('cache corrupt',503),503),
                                 (ValueError('duplicate runs'),400),
                                 (HistoryUnavailable('Hive unavailable'),503),
                                 (MetadataUnavailable('offline'),503)):
                series.side_effect = error
                self.assertEqual(client.get('/api/history-series?runs=one&runs=two').status_code,status)
            series.reset_mock()
            self.assertEqual(client.get('/api/history-series?runs=one').status_code,422)
            series.assert_not_called()

    def test_evidence_filters_and_formal_errors_are_forwarded(self):
        with TestClient(server.app) as client,patch('agent.server.read_evidence') as evidence:
            evidence.return_value = {'run_id':'own','items':[]}
            params = {'limit':10,'after':'a'*64,'source_table':'movies','rule_id':'M1',
                      'source_record_id':'b'*64,'metric':'accurate','evidence_id':'c'*64}
            self.assertEqual(client.get('/api/runs/own/evidence',params=params).status_code,200)
            evidence.assert_called_once_with('own',**params)
            for status in (400,404,409,503):
                evidence.side_effect = ReportUnavailable('unavailable',status)
                self.assertEqual(client.get('/api/runs/own/evidence').status_code,status)
            evidence.reset_mock()
            self.assertEqual(client.get('/api/runs/own/evidence?limit=101').status_code,422)
            evidence.assert_not_called()

    def test_version_filters_are_parameterized_and_keep_keyset_pagination(self):
        cursor = Mock()
        cursor.fetchall.return_value = [{'run_id':'new','request_seq':42},{'run_id':'older','request_seq':41}]
        store = Store({'database':'fixture'})
        @contextlib.contextmanager
        def transaction():
            yield cursor
        store.transaction = transaction
        value = "x' OR TRUE --"
        result = store.list_runs(before=50,limit=1,status='PUBLISHED',dataset_id=value,input_version='raw',
                                 rule_version='rules',metric_version='metrics')
        sql,args = cursor.execute.call_args.args
        self.assertNotIn(value,sql)
        self.assertIn('r.input_version = %s',sql)
        self.assertEqual(args,(50,'PUBLISHED',value,'raw','rules','metrics',2))
        self.assertEqual(result['next_before'],42)
        self.assertEqual(result['items'][0]['run_id'],'new')

    def test_http_history_forwards_all_explicit_filters(self):
        store = Mock()
        store.list_runs.return_value = {'items':[],'next_before':None}
        with patch('agent.server.Store',return_value=store),TestClient(server.app) as client:
            response = client.get('/api/runs',params={'dataset_id':'ml','input_version':'raw','rule_version':'rules',
                'metric_version':'metrics','status':'PUBLISHED','before':50,'limit':10})
            self.assertEqual(response.status_code,200)
            self.assertEqual(client.get('/api/runs',params={'input_version':''}).status_code,422)
        store.list_runs.assert_called_once_with(before=50,limit=10,status='PUBLISHED',dataset_id='ml',
                                               input_version='raw',rule_version='rules',metric_version='metrics')

    def test_current_returns_fixed_publish_identity_and_missing_never_falls_back(self):
        store = Mock()
        first = {'dataset_id':'ml','run_id':'one','publish_id':'pub-one','attempt_id':'attempt-one',
                 'output_version':'output-one','input_version':'raw','rule_version':'rules','metric_version':'metrics',
                 'revision':1,'manifest_hash':'a'*64,'request_seq':1}
        store.current_result.side_effect = [first,{**first,'run_id':'two','publish_id':'pub-two','revision':2},None]
        with patch('agent.server.Store',return_value=store),TestClient(server.app) as client:
            held = client.get('/api/datasets/ml/current').json()
            self.assertEqual(client.get('/api/datasets/ml/current').json()['publish_id'],'pub-two')
            self.assertEqual(held['publish_id'],'pub-one')
            self.assertEqual(client.get('/api/datasets/unknown/current').status_code,404)
        store.current_run.assert_not_called()

    def test_current_sql_requires_formal_winning_attempt_and_dataset(self):
        cursor = Mock()
        cursor.fetchone.return_value = None
        store = Store({'database':'fixture'})
        @contextlib.contextmanager
        def transaction():
            yield cursor
        store.transaction = transaction
        self.assertIsNone(store.current_result('ml'))
        sql,args = cursor.execute.call_args.args
        self.assertIn("p.status='PUBLISHED'",sql)
        self.assertIn('p.attempt_id=r.active_attempt',sql)
        self.assertEqual(args,('ml',))
        with self.assertRaises(ValueError):
            store.current_result('invalid;sql')


if __name__ == '__main__':
    unittest.main()
