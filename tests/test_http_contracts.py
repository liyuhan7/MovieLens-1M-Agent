"""Business HTTP adapters preserve selected identities and domain failures."""
import unittest
from unittest.mock import Mock,MagicMock,patch

from fastapi.testclient import TestClient
from agent import server
from agent.published import ReportUnavailable
from agent.legacy_archive import LegacyUnavailable
from governance.history import HistoryUnavailable
from metadata.store import Conflict,AdmissionPaused


class HttpContractTests(unittest.TestCase):
    def test_agent_adapter_entrypoints_and_readiness_failures(self):
        with TestClient(server.app) as client,patch.object(server.loop,'submit_task') as submit, \
                patch.object(server.loop,'get_status') as status,patch.object(server.loop,'read_transcript',return_value=[]) as transcript:
            submit.return_value = {'status':'rejected','reason':'model missing'}
            self.assertEqual(client.post('/api/tasks',json={'message':'query'}).status_code,503)
            submit.return_value = {'status':'accepted','task_id':'conversation'}
            self.assertEqual(client.post('/api/tasks',json={'message':'query'}).json()['task_id'],'conversation')
            status.return_value = None
            self.assertEqual(client.get('/api/tasks/unknown').status_code,404)
            status.return_value = {'run_status':'VALIDATING','status':'running'}
            self.assertEqual(client.get('/api/tasks/own').json()['run_status'],'VALIDATING')
            self.assertEqual(client.get('/api/tasks/own/transcript').json(),[])
            transcript.assert_called_once_with('own')
        store = MagicMock()
        cursor = store.transaction.return_value.__enter__.return_value
        with TestClient(server.app) as client,patch.object(server,'Store',return_value=store):
            cursor.fetchone.side_effect = [None]
            self.assertEqual(client.get('/api/readiness').status_code,503)
            cursor.fetchone.side_effect = [{'version':5},None]
            self.assertEqual(client.get('/api/readiness').status_code,503)

    def test_formal_query_adapters_preserve_identity_and_errors(self):
        cases = [('compare_runs','/api/comparisons?left=one&right=two',('one','two')),
                 ('history_analysis','/api/runs/own/history-analysis',('own',)),
                 ('performance_baseline','/api/runs/own/performance',('own',)),
                 ('read_legacy_report','/api/legacy/reports/old',('old',))]
        with TestClient(server.app) as client:
            for name,url,arguments in cases:
                with self.subTest(name=name),patch.object(server,name,return_value={'selected':'own'}) as service:
                    self.assertEqual(client.get(url).json(),{'selected':'own'})
                    service.assert_called_once_with(*arguments)
                    for status in (404,409,503):
                        error_type = LegacyUnavailable if name=='read_legacy_report' else ReportUnavailable
                        service.side_effect = error_type('unavailable',status)
                        self.assertEqual(client.get(url).status_code,status)
                    if name=='history_analysis':
                        service.side_effect = HistoryUnavailable('Hive unavailable')
                        self.assertEqual(client.get(url).status_code,503)

    def test_submission_and_retry_status_and_conflicts(self):
        store = Mock()
        with TestClient(server.app) as client,patch.object(server,'Store',return_value=store), \
                patch.object(server,'submit_run',return_value=({'run_id':'own','status':'QUEUED','request':{'execution_mode':'yarn'}},False)) as submit:
            result = client.post('/api/runs',json={},headers={'Idempotency-Key':'same'})
            self.assertEqual(result.status_code,202)
            self.assertFalse(result.json()['created'])
            submit.assert_called_once_with('same',parameters={'rule_pack':'default'})
            submit.reset_mock()
            self.assertEqual(client.post('/api/runs',json={}).status_code,422)
            submit.assert_not_called()
            for error,status in ((Conflict('conflict'),409),(AdmissionPaused('paused'),503),(ValueError('rule'),400)):
                submit.side_effect = error
                self.assertEqual(client.post('/api/runs',json={},headers={'Idempotency-Key':'key'}).status_code,status)
            self.assertEqual(client.post('/api/runs/own/retry').status_code,202)
            store.retry.assert_called_once_with('own')
            for error,status in ((Conflict('not failed'),409),(AdmissionPaused('paused'),503)):
                store.retry.side_effect = error
                self.assertEqual(client.post('/api/runs/own/retry').status_code,status)

    def test_run_detail_contains_only_its_attempts_and_jobs(self):
        store = Mock()
        store.get_run.return_value = {'run_id':'own'}
        store.list_attempts.return_value = [{'attempt_id':'attempt-own'}]
        store.list_jobs.return_value = [{'job_id':'job-own'}]
        with patch.object(server,'Store',return_value=store),TestClient(server.app) as client:
            result = client.get('/api/runs/own')
            self.assertEqual(result.json()['attempts'][0]['jobs'],[{'job_id':'job-own'}])
            store.get_run.assert_called_once_with('own')
            store.list_attempts.assert_called_once_with('own')
            store.list_jobs.assert_called_once_with('attempt-own')
            store.get_run.return_value = None
            store.list_attempts.reset_mock()
            self.assertEqual(client.get('/api/runs/unknown').status_code,404)
            store.list_attempts.assert_not_called()
