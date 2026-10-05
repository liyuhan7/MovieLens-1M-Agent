"""Task Attempt retries, reduce phases and partition counts are identity-bound."""
import copy
import unittest
from unittest.mock import patch

import httpx

from governance.task_metrics import capture_task_details, TASK_GROUP, _fetch


class TaskMetricTests(unittest.TestCase):
    def setUp(self):
        self.tasks = [{'id':'task_1_0001_'+kind+'_0','type':label,'state':'SUCCEEDED','elapsedTime':100,
            'successfulAttempt':'attempt_1_0001_'+kind+'_0_1'} for kind,label in [('m','MAP'),('r','REDUCE')]]
        self.attempts = {}
        for task in self.tasks:
            prefix = task['successfulAttempt'][:-1]
            successful = {'id':prefix+'1','type':task['type'],'state':'SUCCEEDED','startTime':20,
                'finishTime':100,'elapsedTime':80}
            if task['type']=='REDUCE':
                successful.update(elapsedShuffleTime=30,elapsedMergeTime=10,elapsedReduceTime=40)
            failed = {**successful,'id':prefix+'0','state':'FAILED','startTime':0,'finishTime':10,'elapsedTime':10}
            self.attempts[task['id']] = [failed,successful] if task['type']=='MAP' else [successful]
        self.foreign_counter,self.omit_partition_counter = False,False

    def respond(self,request):
        tail = request.url.path.split('/tasks/')[1].split('/')
        task = next(t for t in self.tasks if t['id']==tail[0])
        if tail[-1]=='attempts':
            payload = {'taskAttempts':{'taskAttempt':self.attempts[task['id']]}}
        else:
            attempt = 'attempts' in tail
            identity = tail[-2] if attempt else tail[0]
            counter = [] if self.omit_partition_counter else [{'name':'REDUCE_INPUT_RECORDS','value':30}]
            payload = {('jobTaskAttemptCounters' if attempt else 'jobTaskCounters'):{
                'id':'foreign' if self.foreign_counter else identity,
                ('taskAttemptCounterGroup' if attempt else 'taskCounterGroup'):[
                    {'counterGroupName':TASK_GROUP,'counter':counter}]}}
        return httpx.Response(200,json=payload)

    def capture(self):
        with httpx.Client(transport=httpx.MockTransport(self.respond)) as client:
            return capture_task_details(client,'http://history/jobs/job_1_0001',self.tasks)

    def test_retry_cost_phases_and_partition_records(self):
        result = self.capture()
        self.assertTrue(result['available'])
        self.assertEqual(result['attempt_counts'],{'SUCCEEDED':2,'FAILED':1,'KILLED':0})
        self.assertEqual(result['nonwinning_attempt_duration_ms'],10)
        self.assertEqual(result['reduce_partitions'][0]['input_records'],30)
        self.assertEqual(result['partition_input_records']['max_over_positive_median'],1)
        self.assertEqual(result['tasks'][1]['attempts'][0]['shuffle_ms'],30)

    def test_foreign_counter_and_foreign_attempt_are_rejected(self):
        self.foreign_counter = True
        self.assertFalse(self.capture()['available'])
        self.foreign_counter = False
        self.attempts[self.tasks[0]['id']][0]['id'] = 'attempt_2_0001_m_0_0'
        self.assertFalse(self.capture()['available'])

    def test_winner_and_phase_inconsistency_are_rejected(self):
        self.tasks[0]['successfulAttempt'] = 'attempt_1_0001_m_0_2'
        self.assertFalse(self.capture()['available'])
        self.tasks[0]['successfulAttempt'] = 'attempt_1_0001_m_0_1'
        self.attempts[self.tasks[1]['id']][0]['elapsedShuffleTime'] = 100
        self.assertFalse(self.capture()['available'])

    def test_missing_partition_counter_is_unknown_not_zero(self):
        self.omit_partition_counter = True
        result = self.capture()
        self.assertTrue(result['available'])
        self.assertFalse(result['partition_counts_complete'])
        self.assertIsNone(result['partition_input_records']['total'])
        self.assertIsNone(result['reduce_partitions'][0]['input_records'])

    def test_oversized_task_collection_is_not_sampled(self):
        self.tasks = [copy.deepcopy(self.tasks[0]) for _ in range(257)]
        self.assertFalse(self.capture()['available'])

    def test_response_size_and_deadline_limits(self):
        with httpx.Client(transport=httpx.MockTransport(lambda r:httpx.Response(200,content=b' '*1048577))) as client:
            with self.assertRaises(ValueError):
                _fetch(client,'http://history',float('inf'))
        with patch('governance.task_metrics.time.monotonic',return_value=100):
            with self.assertRaises(ValueError):
                _fetch(None,'http://history',99)


if __name__ == '__main__':
    unittest.main()
