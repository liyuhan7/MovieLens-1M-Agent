"""Full real-Parquet comparisons, including unchanged scores with changed evidence."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.parquet as pq

from agent.published import ReportUnavailable
from governance.artifacts import SCHEMAS
from governance.equivalence import compare_semantics, _schema_name, compare_optimization
from governance.manifest import required_files
from metadata.store import fingerprint


class EquivalenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name).resolve()
        self.store = Mock()
        self.runs,self.publications,self.rows = {},{},{}
        for identity in ('left','right'):
            self.runs[identity] = {'run_id':identity,'status':'PUBLISHED','dataset_id':'ml',
                'input_version':'raw','rule_version':'rules','metric_version':'metrics'}
            self.rows[identity] = {}
            for path in required_files() - {'report.json'}:
                self.rows[identity][path] = []
            common = {'run_id':identity,'attempt_id':'attempt-'+identity,'input_version':'raw',
                      'rule_version':'rules','metric_version':'metrics'}
            self.rows[identity]['cleaned/movies/part-00000.parquet'] = [{**common,
                'source_record_id':'s','source_table':'movies','source_file':'movies.dat','source_offset':0,
                'movie_id':'1','title':'Title','year':2000,'genres':['Drama','Comedy'],'related_source_ids':['s']}]
            self.rows[identity]['evidence/movies/part-00000.parquet'] = [{**common,
                'source_record_id':'s','source_table':'movies','source_file':'movies.dat','source_offset':0,
                'evidence_id':fingerprint([identity,'attempt-'+identity,'s','event',0]),
                'event_order':0,'kind':'TRANSFORMATION','phase':'cleaning','metric':None,'rule_id':'M1',
                'action':'repair','before':'wrong','after':'Title','target_source_id':None,
                'numerator':None,'denominator':None,'final_disposition':'KEEP'}]
            self.write(identity)
        self.store.get_run.side_effect = lambda r:self.runs.get(r)
        self.store.get_publish.side_effect = lambda r:self.publications.get(r)
        self.store.list_attempts.side_effect = lambda r:[{'attempt_id':'attempt-'+r,
             'work_path':'outputs/'+r+'/attempts/attempt-'+r}]

    def tearDown(self):
        self.tmp.cleanup()

    def write(self,identity,report_extra=None):
        cache = self.root/'outputs'/identity/'attempts'/('attempt-'+identity)/'artifacts'
        cache.mkdir(parents=True,exist_ok=True)
        items = []
        for name,rows in self.rows[identity].items():
            schema = SCHEMAS[_schema_name(name)]
            path = cache/name
            path.parent.mkdir(parents=True,exist_ok=True)
            pq.write_table(pa.Table.from_pylist(rows,schema=schema),path)
            items.append({'path':name,'format':'parquet','rows':len(rows),
                'schema_sha256':hashlib.sha256(schema.serialize().to_pybytes()).hexdigest(),
                'bytes':path.stat().st_size,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
        report = {'run_id':identity,'task_id':identity,'attempt_id':'attempt-'+identity,
            'input_data_version':'raw','rule_version':'rules','metric_version':'metrics',
            'output_data_version':'output-'+identity,'scores':{'movies':{'accurate':100}},
            'limitations':['cannot prove real-world accuracy'],**(report_extra or {})}
        path = cache/'report.json'
        path.write_text(json.dumps(report),encoding='utf-8')
        items.append({'path':'report.json','format':'json','bytes':path.stat().st_size,
                      'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
        pub = {'run_id':identity,'attempt_id':'attempt-'+identity,'publish_id':'pub-'+identity,
            'output_version':'output-'+identity,'status':'PUBLISHED','storage_path':'/ml/published/'+identity,
            'manifest':{'run_id':identity,'attempt_id':'attempt-'+identity,'files':items}}
        pub['manifest_hash'] = fingerprint(pub['manifest'])
        self.publications[identity] = pub

    def compare(self):
        return compare_semantics('left','right',store=self.store,root=self.root)

    def test_identity_differences_and_parquet_bytes_do_not_change_business_semantics(self):
        result = self.compare()
        self.assertTrue(result['equivalent'])
        self.assertEqual(len(result['artifacts']),20)
        self.assertTrue(Path(result['record']).is_file())

    def test_same_score_with_changed_rule_evidence_is_not_equivalent(self):
        self.rows['right']['evidence/movies/part-00000.parquet'][0]['action'] = 'isolate'
        self.write('right')
        result = self.compare()
        self.assertFalse(result['equivalent'])
        self.assertEqual(result['different_report_fields'],[])
        self.assertEqual(next(r for r in result['artifacts'] if r['artifact'].startswith('evidence/movies'))['different_row_values'],2)

    def test_duplicate_count_and_genre_order_are_preserved(self):
        rows = self.rows['right']['cleaned/movies/part-00000.parquet']
        rows.append(dict(rows[0]))
        self.write('right')
        self.assertFalse(self.compare()['equivalent'])
        rows.pop()
        rows[0]['genres'].reverse()
        self.write('right')
        self.assertFalse(self.compare()['equivalent'])

    def test_foreign_rows_invalid_evidence_ids_and_corrupt_bytes_are_rejected(self):
        row = self.rows['right']['evidence/movies/part-00000.parquet'][0]
        original = dict(row)
        for key,value in [('attempt_id','foreign'),('evidence_id','invalid')]:
            row.update(original)
            row[key] = value
            self.write('right')
            with self.assertRaises(ReportUnavailable):
                self.compare()
        row.update(original)
        self.write('right')
        (self.root/'outputs/right/attempts/attempt-right/artifacts/quality/summary.parquet').write_bytes(b'bad')
        with self.assertRaises(ReportUnavailable):
            self.compare()

    def test_report_business_fields_and_input_versions_are_gates(self):
        self.write('right',{'split':{'train':1}})
        self.assertEqual(self.compare()['different_report_fields'],['split'])
        self.runs['right']['input_version'] = 'different'
        with self.assertRaises(ReportUnavailable):
            self.compare()

    def test_optimization_cannot_bind_performance_to_a_losing_attempt(self):
        left = {'run_id':'left','attempt_id':'loser','versions':{k:self.runs['left'][k]
                     for k in ('input_version','rule_version','metric_version')}}
        right = {**left,'run_id':'right','attempt_id':'attempt-right'}
        with patch('governance.performance.compare_baselines',return_value={}), \
                patch('governance.performance.load_baseline',side_effect=[left,right]), \
                self.assertRaises(ReportUnavailable):
            compare_optimization('baseline-left','baseline-right',store=self.store,root=self.root)


if __name__ == '__main__':
    unittest.main()
