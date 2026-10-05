"""History projection gates, retention dependencies and measured performance."""
import datetime as dt
import unittest
from unittest.mock import Mock, patch

import httpx

from agent.published import ReportUnavailable
from governance.history import projection, history_analysis, HistoryUnavailable, HiveClient
from governance.maintenance import retention_preview, preview_storage
from governance.performance import measure_job
from hive_fixtures import projection_files


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.pub = {'publish_id':'publish-own','output_version':'clean-own','storage_path':'/ml/published/version-own',
                    'attempt_id':'attempt-own','manifest':{'files':projection_files()}}
        self.run = {'input_version':'raw-own','rule_version':'rules-own','metric_version':'metric-own'}

    def test_ddl_is_external_partitioned_and_never_purges_or_reads_staging(self):
        result = projection(self.pub)
        self.assertEqual(result['sql'].count('CREATE EXTERNAL TABLE'),8)
        self.assertEqual(result['sql'].count("'external.table.purge'='false'"),8)
        self.assertIn('ARRAY<STRING>',result['sql'])
        self.assertNotIn('/ml/staging',result['sql'])
        self.assertEqual(result['expected_rows']['quality_before'],6)
        self.assertEqual(result['expected_rows']['evidence_users'],2)
        self.assertIn('PARTITIONED BY (`output_version` STRING)',result['sql'])

    def test_projection_rejects_injection_and_wrong_schema(self):
        with self.assertRaises(ValueError):
            projection(self.pub,database='history;drop')
        self.pub['manifest']['files'][0]['schema_sha256'] = 'bad'
        with self.assertRaises(ReportUnavailable):
            projection(self.pub)

    def test_history_counts_and_rating_distribution_are_reconciled(self):
        client = Mock()
        counts = [{'source_table':t,'actual_rows':str(n),'bound_rows':str(n)} for t,n in projection(self.pub)['expected_rows'].items()]
        client.execute.side_effect = [counts,[{'rating':'3','records':'1'},{'rating':'5','records':'1'}]]
        with patch('governance.history.publication_context',return_value=(self.run,self.pub,None)), \
                patch('governance.history.verify_projection'):
            result = history_analysis('run-own',store=Mock(),client=client)
        self.assertTrue(result['manifest_reconciled'])
        self.assertEqual(result['rating_distribution'][1],{'rating':5,'records':1})
        self.assertIn("`run_id`='run-own'",client.execute.call_args_list[0].args[0])

    def test_wrong_binding_or_distribution_is_rejected(self):
        counts = [{'source_table':t,'actual_rows':str(n),'bound_rows':str(n)} for t,n in projection(self.pub)['expected_rows'].items()]
        client = Mock()
        counts[0]['bound_rows'] = '1'
        client.execute.return_value = counts
        with patch('governance.history.publication_context',return_value=(self.run,self.pub,None)), \
                patch('governance.history.verify_projection'), self.assertRaises(HistoryUnavailable):
            history_analysis('run-own',store=Mock(),client=client)
        self.assertEqual(client.execute.call_count,1)
        counts[0]['bound_rows'] = '2'
        client.execute.side_effect = [counts,[{'rating':'5','records':'3'}]]
        with patch('governance.history.publication_context',return_value=(self.run,self.pub,None)), \
                patch('governance.history.verify_projection'), self.assertRaises(HistoryUnavailable):
            history_analysis('run-own',store=Mock(),client=client)

    def test_unconfigured_hive_is_an_explicit_unavailable_state(self):
        with patch.dict('os.environ',{'ML_HIVE_JDBC_URL':''}), self.assertRaises(HistoryUnavailable):
            HiveClient().execute('SELECT 1;')


class MaintenanceTests(unittest.TestCase):
    def setUp(self):
        self.now = dt.datetime(2026,10,5,tzinfo=dt.timezone.utc)
        self.old = dt.datetime(2026,1,1)
        self.attempt = {'run_id':'run-own','attempt_id':'attempt-own','status':'FAILED','ended_at':self.old,
                        'lease_alive':0,'active_attempt':'attempt-new'}
        self.ref = {'scope':'fixture','raw':[],'attempts':[self.attempt],'publications':[]}
        self.item = {'path':'/ml/staging/run=run-own/attempt=attempt-own/jobs/file','bytes':10,'modified':'2026-01-01 12:00'}

    def preview(self):
        return retention_preview([self.item],[self.ref],now=self.now)

    def test_only_expired_failed_unreferenced_files_are_candidates(self):
        result = self.preview()
        self.assertEqual(result['candidate_bytes'],10)
        self.assertFalse(result['deletion_authorized'])
        for field,bad in [('status','RUNNING'),('lease_alive',1),('ended_at',None),('active_attempt','attempt-own')]:
            old = self.attempt[field]
            self.attempt[field] = bad
            with self.subTest(field=field):
                self.assertEqual(self.preview()['candidates'],[])
            self.attempt[field] = old
        self.item['modified'] = '2026-10-04 12:00'
        self.assertEqual(self.preview()['candidates'],[])

    def test_publication_dependency_protects_even_failed_old_attempt(self):
        self.ref['publications'] = [{'run_id':'run-own','attempt_id':'attempt-own','publish_id':'pub',
                                    'status':'PREPARING','storage_path':'/ml/published/own','manifest':{'files':[]}}]
        self.assertEqual(self.preview()['candidates'],[])

    def test_concurrent_metadata_change_cancels_preview(self):
        store,hdfs = Mock(),Mock()
        store.storage_references.side_effect = [self.ref,{**self.ref,'attempts':[]}]
        hdfs.inventory.return_value = [self.item]
        with self.assertRaisesRegex(ValueError,'changed'):
            preview_storage([store],hdfs=hdfs)


class PerformanceTests(unittest.TestCase):
    def test_measured_job_counters_and_missing_history(self):
        job = {'stage':'cleanUsers','job_id':'job_1_0001','submission_id':'submission-own','status':'SUCCEEDED'}
        payload = {'job':{'id':'job_1_0001','name':'submission-own','state':'SUCCEEDED','startTime':10,'finishTime':110}}
        counters = {'jobCounters':{'id':'job_1_0001','counterGroup':[{'counterGroupName':'group',
                                 'counter':[{'name':'HDFS_BYTES_READ','totalCounterValue':120}]}]}}
        def respond(request):
            return httpx.Response(200,json=counters if request.url.path.endswith('/counters') else payload)
        with httpx.Client(transport=httpx.MockTransport(respond)) as client:
            result = measure_job(job,'http://history',client=client)
        self.assertEqual(result['duration_ms'],100)
        self.assertEqual(result['counters']['group/HDFS_BYTES_READ'],120)
        payload['job']['name'] = 'foreign'
        with httpx.Client(transport=httpx.MockTransport(respond)) as client:
            result = measure_job(job,'http://history',client=client)
        self.assertFalse(result['available'])
        self.assertIsNone(result['duration_ms'])


if __name__ == '__main__':
    unittest.main()
