"""History summaries are full formal facts with explicit comparison limits."""
import copy
import unittest
from unittest.mock import Mock, patch

from governance.history import history_series, HistoryUnavailable
from agent.published import ReportUnavailable


def summary_report():
    dimensions = dict(accurate=90,complete=80,unique=100,up_to_date=None,consistent=100)
    return {'scores':{t:{'raw':dict(dimensions),'clean':dict(dimensions),
                        'composite_raw':92.5,'composite_clean':92.5} for t in ('users','movies','ratings')},
            'dataset_composite':{'raw':92.5,'clean':92.5},
            'action_counts':{'users':{'repair:U1':2},'movies':{},'ratings':{}}}


class HistorySeriesTests(unittest.TestCase):
    def setUp(self):
        self.reports = {r:summary_report() for r in ('one','two')}
        self.runs = {r:{'dataset_id':'ml','input_version':'raw','rule_version':'rules','metric_version':'metrics'}
                     for r in self.reports}
        self.pubs = {r:{'publish_id':'pub-'+r,'attempt_id':'attempt-'+r,'output_version':'output-'+r,
                       'manifest':{'action_counts':copy.deepcopy(self.reports[r]['action_counts'])}} for r in self.reports}
        self.stack = []
        for target,fn in (
            ('publication_context',lambda r,s:(self.runs[r],self.pubs[r],None)),
            ('read_published_report',lambda r,**kw:(b'',self.reports[r],copy.deepcopy(self.pubs[r]))),
            ('history_analysis',lambda r,**kw:{'run_id':r,'publish_id':self.pubs[r]['publish_id'],
                'rows':dict.fromkeys(('users','movies','ratings'),2),
                'rating_distribution':[{'rating':5,'records':2}]})):
            p = patch('governance.history.'+target,side_effect=fn)
            self.stack.append(p.start())
            self.addCleanup(p.stop)

    def series(self):
        return history_series(['one','two'],store=Mock(),client=Mock())

    def test_full_quality_and_action_trends_and_null_dimensions(self):
        self.reports['two']['scores']['users']['clean']['accurate'] = 95
        self.reports['two']['action_counts']['users'] = {'repair:U1':3,'isolate:U2':1}
        self.pubs['two']['manifest']['action_counts'] = copy.deepcopy(self.reports['two']['action_counts'])
        result = self.series()
        transition = result['transitions'][0]
        metric = next(r for r in transition['quality_delta'] if (r['table'],r['phase'],r['metric'])==('users','clean','accurate'))
        self.assertEqual(metric['delta'],5)
        self.assertTrue(all(r['delta'] is None for r in transition['quality_delta'] if r['metric']=='up_to_date'))
        self.assertEqual(transition['action_delta'][0]['delta'],1)
        self.assertEqual(result['points'][1]['action_counts']['users']['repair:U1'],3)
        self.assertIn('formal report.json',result['points'][0]['summary_source'])

    def test_changed_metric_and_rule_versions_suppress_only_incompatible_deltas(self):
        self.runs['two']['metric_version'] = 'metrics-new'
        transition = self.series()['transitions'][0]
        self.assertFalse(transition['metric_compatible'])
        self.assertTrue(all(r['delta'] is None for r in transition['quality_delta']))
        self.assertEqual(transition['action_delta'][0]['delta'],0)
        self.runs['two']['rule_version'] = 'rules-new'
        transition = self.series()['transitions'][0]
        self.assertFalse(transition['rule_compatible'])
        self.assertTrue(all(r['delta'] is None for r in transition['action_delta']))

    def test_input_change_is_explicit_and_caller_order_is_preserved(self):
        self.runs['two']['input_version'] = 'raw-new'
        result = self.series()
        self.assertEqual([p['run_id'] for p in result['points']],['one','two'])
        self.assertEqual(result['transitions'][0]['changed_versions'],['input_version'])

    def test_malformed_or_manifest_mismatched_summary_is_rejected(self):
        original = copy.deepcopy(self.reports['two'])
        for mutation in (
            lambda r:r['action_counts']['users'].update({'repair:U1':9}),
            lambda r:r['scores']['users']['clean'].update({'accurate':float('nan')}),
            lambda r:r['scores']['movies'].pop('composite_raw'),
            lambda r:r['scores']['ratings']['raw'].pop('complete')):
            self.reports['two'] = copy.deepcopy(original)
            mutation(self.reports['two'])
            with self.assertRaises(HistoryUnavailable):
                self.series()

    def test_changed_publish_between_hive_and_report_is_rejected(self):
        self.stack[1].side_effect = lambda r,**kw:(b'',self.reports[r],{**self.pubs[r],'publish_id':'foreign'})
        with self.assertRaises(HistoryUnavailable):
            self.series()

    def test_foreign_dataset_or_unavailable_report_never_returns_partial_series(self):
        self.runs['two']['dataset_id'] = 'other'
        with self.assertRaises(ReportUnavailable):
            self.series()
        self.stack[1].assert_not_called()
        self.runs['two']['dataset_id'] = 'ml'
        self.stack[1].side_effect = ReportUnavailable('missing',503)
        with self.assertRaises(ReportUnavailable):
            self.series()
