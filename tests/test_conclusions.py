"""A correct number or a valid citation cannot endorse a fabricated reason."""
import copy
import json
import unittest
from unittest.mock import Mock

from agent.conclusions import ConclusionProposal, validate_conclusion
from agents.agent_output import AgentOutputSchema


class ConclusionTests(unittest.TestCase):
    def setUp(self):
        self.report = {"input_data_version":"raw-own", "rule_version":"rules-own", "metric_version":"metrics-own",
                       "scores":{"users":{"clean":{"unique":99.1,"complete":80.2}}},
                       "limitations":["样本不能代表全量"]}
        self.body = {"evidence_id":"a"*64,"source_record_id":"b"*64,"source_table":"users",
                     "rule_id":"U6","metric":None,"action":"dedup","before":"duplicate",
                     "after":"winner","final_disposition":"dedup"}
        self.proposal = {"run_id":"run-own","publish_id":"pub-own","input_version":"raw-own",
                         "rule_version":"rules-own","metric_version":"metrics-own",
                         "report_claims":[{"path":"/scores/users/clean/unique","value":99.1}],
                         "evidence_claims":[self.body.copy()],"hypotheses":[]}
        self.report_reader = Mock(return_value=(b'',self.report,{"publish_id":"pub-own"}))
        self.evidence_reader = Mock(return_value={"publish_id":"pub-own","items":[self.body]})

    def validate(self):
        return validate_conclusion('run-own',self.proposal,report_reader=self.report_reader,evidence_reader=self.evidence_reader)

    def test_valid_facts_are_rendered_with_versions_and_evidence(self):
        result = self.validate()
        self.assertEqual(result['validation_status'],'VERIFIED')
        self.assertEqual(len(result['accepted_claims']),2)
        self.assertIn('[evidence:'+'a'*64+']',result['answer'])
        self.assertIn('样本不能代表全量',result['answer'])

    def test_agents_sdk_accepts_and_decodes_the_strict_proposal_schema(self):
        schema = AgentOutputSchema(ConclusionProposal)
        proposal = schema.validate_json(json.dumps(self.proposal))
        self.assertIsInstance(proposal,ConclusionProposal)
        self.assertEqual(proposal.publish_id,'pub-own')

    def test_real_number_from_wrong_metric_is_rejected(self):
        self.proposal['report_claims'][0]['path'] = '/scores/users/clean/complete'
        result = self.validate()
        self.assertEqual(result['rejected_claims'][0]['reason'],'REPORT_VALUE_MISMATCH')
        self.assertNotIn('/complete',result['answer'])

    def test_valid_evidence_id_with_wrong_reason_action_or_before_is_rejected(self):
        for key,bad in [('rule_id','U0'),('action','repair'),('before','fabricated'),('metric','unique')]:
            self.proposal['evidence_claims'] = [{**self.body,key:bad}]
            with self.subTest(key=key):
                result = self.validate()
                self.assertEqual(result['rejected_claims'][0]['reason'],'EVIDENCE_FACT_MISMATCH')
                self.assertNotIn('[evidence:',result['answer'])

    def test_wrong_publish_or_versions_reject_entire_proposal(self):
        for key in ('run_id','publish_id','input_version','rule_version','metric_version'):
            old = self.proposal[key]
            self.proposal[key] = 'foreign'
            with self.subTest(key=key),self.assertRaisesRegex(ValueError,'BINDING'):
                self.validate()
            self.proposal[key] = old

    def test_missing_or_foreign_evidence_does_not_create_verified_claims(self):
        for page in ({'publish_id':'pub-own','items':[]},{'publish_id':'other','items':[self.body]}):
            self.evidence_reader.return_value = page
            self.assertEqual(self.validate()['rejected_claims'][0]['reason'],'EVIDENCE_NOT_FOUND_IN_PUBLICATION')

    def test_hypotheses_are_separate_and_never_formal_answer(self):
        self.proposal.update(report_claims=[],evidence_claims=[],hypotheses=['全量质量已达标'])
        result = self.validate()
        self.assertEqual(result['validation_status'],'REJECTED')
        self.assertFalse(result['hypotheses_verified'])
        self.assertNotIn('全量质量已达标',result['answer'])

    def test_unknown_or_composite_report_fields_cannot_be_claimed(self):
        for path in ('/scores/users/clean', '/request/password', '/scores/users/clean/missing'):
            self.proposal['report_claims'] = [{'path':path,'value':'bad'}]
            self.assertEqual(len(self.validate()['rejected_claims']),1)

    def test_bool_cannot_impersonate_numeric_claim(self):
        value = copy.deepcopy(self.proposal)
        value['report_claims'][0]['value'] = True
        with self.assertRaises(ValueError):
            ConclusionProposal.model_validate(value)


if __name__ == '__main__':
    unittest.main()
