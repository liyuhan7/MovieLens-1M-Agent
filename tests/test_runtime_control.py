"""Durable admission and service-switch fencing without external services."""
import contextlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from agent import server
from metadata.store import Store, AdmissionPaused, Conflict, fingerprint


class Cursor:
    def __init__(self):
        self.control = {'control_id':1,'admission_open':False,'revision':1,'owner':'operator','reason':'upgrade','switch_id':None}
        self.slot = {'run_id':None,'lease_owner':None,'alive':0}
        self.queries,self.events = [],[]
        self.active,self.incomplete = 0,0
        self.runs = {'QUEUED':2,'PUBLISHED':1}
        self.existing = None
    def execute(self,sql,args=None):
        self.sql = sql
        self.queries.append((sql,args))
        if sql.startswith('UPDATE runtime_control SET admission_open'):
            self.control.update(admission_open=args[0],revision=args[1],owner=args[2],reason=args[3])
        if sql.startswith('UPDATE runtime_control SET switch_id'):
            self.control.update(switch_id=args[0],revision=args[1])
        if sql.startswith('INSERT INTO runtime_control_event'):
            self.events.append(args)
    def fetchone(self):
        sql = self.sql
        if 'FROM runtime_control' in sql: return dict(self.control)
        if 'FROM worker_slot' in sql: return dict(self.slot)
        if 'FROM schema_migration' in sql: return {'version':5}
        if 'work_path FROM physical_attempt' in sql: return {'work_path':'outputs/run-own/attempts/attempt-own'}
        if 'idempotency_scope' in sql: return self.existing
        if 'SELECT * FROM logical_run' in sql:
            return {'run_id':'run-own','status':'RUNNING','fencing_token':3,'active_attempt':'attempt-own'}
        if 'COUNT(*)' in sql:
            return {'n':self.incomplete if 'publish_version' in sql else self.active}
        raise AssertionError(sql)
    def fetchall(self):
        if 'GROUP BY status' in self.sql:
            return [{'status':status,'n':n} for status,n in self.runs.items()]
        raise AssertionError(self.sql)


class RuntimeControlTests(unittest.TestCase):
    def setUp(self):
        self.cursor = Cursor()
        self.store = Store({'database':'fixture'})
        @contextlib.contextmanager
        def transaction():
            yield self.cursor
        self.store.transaction = transaction

    def test_new_submissions_and_retries_are_blocked(self):
        with self.assertRaises(AdmissionPaused):
            self.store.submit({},'new')
        with self.assertRaises(AdmissionPaused):
            self.store.retry('failed')
        self.assertFalse(any(q.startswith('INSERT INTO logical_run') for q,_ in self.cursor.queries))

    def test_existing_identical_request_is_reusable_but_conflict_is_rejected(self):
        self.cursor.existing = {'run_id':'existing','request_hash':fingerprint({})}
        self.assertFalse(self.store.submit({},'same')[1])
        with self.assertRaises(Conflict):
            self.store.submit({'different':True},'same')

    def test_first_claim_is_blocked_but_existing_attempt_can_recover(self):
        self.assertIsNone(self.store.claim('worker'))
        self.assertFalse(any("status='QUEUED'" in q for q,_ in self.cursor.queries))
        self.cursor.slot.update(run_id='run-own')
        claim = self.store.claim('worker')
        self.assertTrue(claim.recovering)
        self.assertEqual(claim.attempt_id,'attempt-own')
        self.assertEqual(claim.token,4)

    def test_owner_revision_and_switch_reservation_fence_resume(self):
        for owner,revision in [('foreign',1),('operator',0)]:
            with self.assertRaises(Conflict):
                self.store.control_admission(open_admission=True,owner=owner,reason='resume',expected_revision=revision)
        identity = 'a'*32
        result = self.store.reserve_switch(owner='operator',expected_revision=1,switch_id=identity)
        self.assertEqual(result['revision'],2)
        with self.assertRaises(Conflict):
            self.store.control_admission(open_admission=True,owner='operator',reason='resume',expected_revision=2)
        with self.assertRaises(Conflict):
            self.store.reserve_switch(owner='operator',expected_revision=2,switch_id='b'*32,finish=True)
        self.store.reserve_switch(owner='operator',expected_revision=2,switch_id=identity,finish=True)
        self.assertTrue(self.store.control_admission(open_admission=True,owner='operator',reason='resume',expected_revision=3)['admission_open'])
        self.assertEqual(len(self.cursor.events),3)

    def test_inflight_attempt_or_unfinished_publication_blocks_switch(self):
        self.cursor.active = 1
        with self.assertRaises(Conflict):
            self.store.reserve_switch(owner='operator',expected_revision=1,switch_id='a'*32)
        self.cursor.active,self.cursor.incomplete = 0,1
        with self.assertRaises(Conflict):
            self.store.reserve_switch(owner='operator',expected_revision=1,switch_id='a'*32)
        self.assertIsNone(self.cursor.control['switch_id'])

    def test_drained_status_retains_queue_and_readiness_does_not_resume(self):
        self.assertTrue(self.store.maintenance_status()['ready_for_switch'])
        self.assertEqual(self.store.maintenance_status()['queued_runs'],2)
        self.cursor.slot['run_id'] = 'run-own'
        self.assertFalse(self.store.maintenance_status()['ready_for_switch'])
        with patch('agent.server.Store',return_value=self.store), TestClient(server.app) as client:
            self.assertEqual(client.get('/api/health').status_code,200)
            result = client.get('/api/readiness').json()
        self.assertFalse(result['admission_open'])
        self.assertEqual(self.cursor.events,[])

    def test_http_pause_is_service_unavailable(self):
        with patch('agent.server.submit_run',side_effect=AdmissionPaused('maintenance')), \
                patch('agent.server.Store',return_value=self.store), TestClient(server.app) as client:
            self.assertEqual(client.post('/api/runs',json={},headers={'Idempotency-Key':'new'}).status_code,503)
            self.assertEqual(client.post('/api/runs/failed/retry').status_code,503)

    def test_switch_reservation_blocks_claim_and_new_replacement_authority(self):
        self.cursor.control['switch_id'] = 'a'*32
        self.cursor.slot['run_id'] = 'run-own'
        self.assertIsNone(self.store.claim('worker'))
        with self.assertRaises(Conflict):
            self.store.request_publication_replacement('run-own',expected_publish='pub',expected_attempt='attempt',
                expected_manifest_hash='a'*64,expected_token=1,reason='replace')
        self.assertFalse(any('FROM worker_slot' in q for q,_ in self.cursor.queries))


if __name__=='__main__':
    unittest.main()
