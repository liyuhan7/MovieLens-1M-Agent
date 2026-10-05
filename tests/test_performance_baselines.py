"""Task timing and persisted-baseline completeness are business gates."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import httpx

from governance.performance import measure_job, capture_baseline, load_baseline, compare_baselines, EXPECTED_STAGES
from metadata.store import fingerprint


class PerformanceBaselineTests(unittest.TestCase):
    def test_task_distribution_and_job_identity_are_measured(self):
        job = {'stage':'cleanUsers','job_id':'job_1_0001','submission_id':'own','status':'SUCCEEDED'}
        detail = {'id':job['job_id'],'name':'own','state':'SUCCEEDED','startTime':0,'finishTime':100,
                  'mapsTotal':2,'reducesTotal':0}
        tasks = [{'id':f'task_1_0001_m_{i}','type':'MAP','state':'SUCCEEDED',
                  'startTime':0,'finishTime':end,'elapsedTime':end} for i,end in enumerate((10,90))]
        def respond(request):
            if request.url.path.endswith('/counters'):
                data = {'jobCounters':{'id':job['job_id'],'counterGroup':[]}}
            elif request.url.path.endswith('/tasks'):
                data = {'tasks':{'task':tasks}}
            else:
                data = {'job':detail}
            return httpx.Response(200,json=data)
        with httpx.Client(transport=httpx.MockTransport(respond)) as client:
            result = measure_job(job,'http://history',client=client)
            self.assertEqual(result['tasks']['distributions']['MAP'],
                {'count':2,'min_ms':10,'median_ms':50,'p95_ms':90,'max_ms':90})
            self.assertIsNone(result['tasks']['distributions']['REDUCE']['p95_ms'])
            tasks[1]['id'] = 'task_2_0001_m_1'
            self.assertFalse(measure_job(job,'http://history',client=client)['tasks']['available'])
            tasks[1]['id'] = 'task_1_0001_m_1'
            tasks[1]['state'] = 'FAILED'
            self.assertFalse(measure_job(job,'http://history',client=client)['tasks']['available'])

    def test_saved_baselines_reject_corruption_incomplete_values_and_different_versions(self):
        with tempfile.TemporaryDirectory() as tmp, patch('governance.performance.ROOT',Path(tmp)):
            base = {'schema_version':'performance-baseline-v2','dataset_id':'ml','run_id':'one','attempt_id':'attempt',
                'execution':None,'execution_hash':fingerprint(None),'versions':{'input_version':'raw'},'complete':True,
                'jobs':[{'stage':stage,'job_id':f'job_1_{i:04}','available':True,'status':'SUCCEEDED',
                         'duration_ms':100,'tasks':{'available':True}} for i,stage in enumerate(sorted(EXPECTED_STAGES))]}
            with patch('governance.performance.performance_baseline',return_value=base):
                first = capture_baseline('one')['baseline_id']
                second = capture_baseline('one')['baseline_id']
            self.assertEqual(len(compare_baselines(first,second)['stages']),12)
            path = Path(tmp)/'outputs/performance-baselines'/(second+'.json')
            envelope = json.loads(path.read_text(encoding='utf-8'))
            original = copy.deepcopy(envelope)
            envelope['payload']['jobs'][0]['duration_ms'] = 50
            path.write_text(json.dumps(envelope),encoding='utf-8')
            with self.assertRaises(ValueError):
                load_baseline(second)
            envelope['sha256'] = fingerprint(envelope['payload'])
            envelope['payload']['jobs'][0]['tasks']['available'] = False
            envelope['sha256'] = fingerprint(envelope['payload'])
            path.write_text(json.dumps(envelope),encoding='utf-8')
            with self.assertRaises(ValueError):
                compare_baselines(first,second)
            envelope = original
            envelope['payload']['versions']['input_version'] = 'other'
            envelope['sha256'] = fingerprint(envelope['payload'])
            path.write_text(json.dumps(envelope),encoding='utf-8')
            with self.assertRaises(ValueError):
                compare_baselines(first,second)


if __name__ == '__main__':
    unittest.main()
