"""Hive registration and reads reject incompatible existing metadata."""
import unittest
from unittest.mock import Mock, patch

from governance.artifacts import SCHEMAS
from governance.history import register_projection, history_analysis, hive_type, HistoryUnavailable, history_series, projection
from hive_fixtures import projection_files
from test_history_series import summary_report


class HiveRegistrationTests(unittest.TestCase):
    def setUp(self):
        self.pub = {'publish_id':'pub-own','output_version':'clean-own','attempt_id':'attempt-own','storage_path':'/ml/published/pub-own',
            'manifest':{'files':projection_files()}}
        self.run = {'dataset_id':'ml','input_version':'raw','rule_version':'rules','metric_version':'metrics'}
        self.client,self.hdfs = Mock(),Mock()
        self.purge,self.kind,self.location = 'false','EXTERNAL_TABLE',None
        self.bad_schema,self.distribution_bad,self.quality_bound_bad = False,False,False
        self.client.execute.side_effect = self.execute
        self.hdfs.inventory.side_effect = self.inventory
        self.hdfs.digest.return_value = {'bytes':20,'sha256':'a'*64}
        self.context = patch('governance.history.publication_context',return_value=(self.run,self.pub,None))
        self.context.start()
        self.addCleanup(self.context.stop)

    @staticmethod
    def rows(cells):
        return [dict(zip(('col_name','data_type','comment'),cell)) for cell in cells]

    def inventory(self,roots):
        return [{'path':self.pub['storage_path']+'/'+f['path'],'bytes':f['bytes']}
                for f in self.pub['manifest']['files'] if self.pub['storage_path']+'/'+f['path'].rsplit('/',1)[0]==roots[0]]

    def execute(self,sql):
        if sql.startswith('DESCRIBE'):
            name = sql.split('`')[3]
            table = name.removeprefix('ml_cleaned_') if name.startswith('ml_cleaned_') else name.removeprefix('ml_')
            item = projection(self.pub)['tables'][table]
            cells = [(f.name,hive_type(f.type),'') for f in SCHEMAS[item['schema']]]
            if self.bad_schema:
                cells[0] = ('wrong','STRING','')
            # Configurable repeated partition columns are valid in Hive.
            cells += [('output_version','STRING',''),('# Partition Information','',''),
                      ('# col_name','data_type','comment'),('output_version','STRING',''),
                      ('# Detailed Table Information','',''),('Table Type:',self.kind,''),
                      ('Location:',self.location or 'hdfs://ml-governance-hadoop:9000'+item['location'],''),
                      ('Table Parameters:','',''),('', 'external.table.purge',self.purge),
                      ('','parquet.column.index.access','false'),('# Storage Information','',''),
                      ('SerDe Library:','org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',''),
                      ('InputFormat:','org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat','')]
            return self.rows(cells)
        if sql.startswith('SELECT rating'):
            return [{'rating':'5','records':'3' if self.distribution_bad else '2'}]
        if sql.startswith('SELECT'):
            return [{'source_table':t,'actual_rows':str(n),'bound_rows':str(n-1 if self.quality_bound_bad and t=='quality_before' else n)}
                    for t,n in projection(self.pub)['expected_rows'].items()]
        return []

    def register(self):
        return register_projection('run-own',store=Mock(),client=self.client,hdfs=self.hdfs)

    def test_register_reentry_checks_all_bytes_and_metadata_then_reconciles(self):
        for _ in range(2):
            self.assertTrue(self.register()['registration_verified'])
        self.assertEqual(self.hdfs.digest.call_count,24)
        self.assertEqual(sum(c.args[0].startswith('ALTER') for c in self.client.execute.call_args_list),16)

    def test_managed_table_purge_or_schema_conflict_prevent_partition_registration(self):
        for field,value in [('purge','true'),('kind','MANAGED_TABLE'),('bad_schema',True)]:
            setattr(self,field,value)
            self.client.reset_mock()
            with self.assertRaises(HistoryUnavailable):
                self.register()
            self.assertFalse(any(c.args[0].startswith('ALTER') for c in self.client.execute.call_args_list))
            setattr(self,field,{'purge':'false','kind':'EXTERNAL_TABLE','bad_schema':False}[field])

    def test_extra_hdfs_file_or_bad_bytes_prevent_any_ddl(self):
        self.hdfs.inventory.side_effect = lambda roots:[{'path':roots[0]+'/extra.parquet','bytes':20}]
        with self.assertRaises(HistoryUnavailable):
            self.register()
        self.client.execute.assert_not_called()
        self.hdfs.inventory.side_effect = self.inventory
        self.hdfs.digest.return_value = {'bytes':20,'sha256':'b'*64}
        with self.assertRaises(HistoryUnavailable):
            self.register()
        self.client.execute.assert_not_called()

    def test_foreign_namenode_or_wrong_partition_cannot_be_read(self):
        for location in ('hdfs://foreign:9000/ml/published/pub-own/cleaned/users',
                         '/ml/published/other/cleaned/users'):
            self.location = location
            self.client.reset_mock()
            with self.assertRaises(HistoryUnavailable):
                history_analysis('run-own',store=Mock(),client=self.client)
            self.assertTrue(all(c.args[0].startswith('DESCRIBE') for c in self.client.execute.call_args_list))

    def test_reads_never_execute_ddl_and_bad_distribution_is_rejected(self):
        self.assertTrue(history_analysis('run-own',store=Mock(),client=self.client)['manifest_reconciled'])
        self.assertFalse(any(c.args[0].startswith(('CREATE','ALTER')) for c in self.client.execute.call_args_list))
        self.distribution_bad = True
        with self.assertRaises(HistoryUnavailable):
            self.register()

    def test_history_series_requires_explicit_distinct_bounded_versions(self):
        for runs in (['one'],['same','same'],list(map(str,range(11)))):
            with self.assertRaises(ValueError):
                history_series(runs,store=Mock(),client=self.client)
        report = summary_report()
        self.pub['manifest']['action_counts'] = report['action_counts']
        with patch('governance.history.read_published_report',return_value=(b'',report,self.pub)):
            self.assertEqual(history_series(['one','two'],store=Mock(),client=self.client)['transitions'][0]['row_delta'],
                             dict.fromkeys(('users','movies','ratings'),0))

    def test_quality_and_evidence_materials_are_required_and_bound(self):
        self.quality_bound_bad = True
        with self.assertRaises(HistoryUnavailable):
            history_analysis('run-own',store=Mock(),client=self.client)
        self.quality_bound_bad = False
        self.pub['manifest']['files'] = [f for f in self.pub['manifest']['files'] if f['path']!='quality/before/users.parquet']
        with self.assertRaises(ValueError):
            projection(self.pub)


if __name__ == '__main__':
    unittest.main()
