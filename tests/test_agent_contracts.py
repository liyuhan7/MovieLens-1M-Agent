"""SDK tool invocation and answer dispatch without a model or live metadata."""
import asyncio
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch

from agents import RunContextWrapper
from agents.tool_context import ToolContext
from fastapi.testclient import TestClient

from agent import loop,server
from agent.conclusions import ConclusionProposal
from agent.published import ReportUnavailable
from agent.legacy_archive import LegacyUnavailable


class AgentContractTests(unittest.TestCase):
    def context(self):
        return RunContextWrapper(context=loop.AgentDeps('conversation','conversation','own'))

    def invoke(self,tool,context,arguments):
        raw = json.dumps(arguments)
        tool_context = ToolContext(context=context.context,tool_name=tool.name,
                                   tool_call_id='test-call',tool_arguments=raw)
        return json.loads(asyncio.run(tool.on_invoke_tool(tool_context,raw)))

    def test_sdk_read_tools_use_the_fixed_run_and_bounded_evidence(self):
        context = self.context()
        report = {'input_data_version':'raw','rule_version':'rules','metric_version':'metrics',
                  'scores':{'users':{}},'disposition':{'users':{'samples':{'clean':['own']}}}}
        with patch('agent.loop.read_published_report',return_value=(b'',report,{'publish_id':'pub'})) as reader:
            result = self.invoke(loop.read_latest_report,context,{})
            reader.assert_called_once_with('own')
            self.assertEqual(result['publication_identity']['run_id'],'own')
            self.assertEqual(result['publication_identity']['publish_id'],'pub')
        with patch('agent.loop._load_report',return_value=report) as reader:
            self.assertEqual(self.invoke(loop.read_report_field,context,{'tag':'latest','section':'scores'}),{'scores':report['scores']})
            self.assertEqual(self.invoke(loop.read_samples,context,{'tag':'own','kind':'clean'}),{'users':['own']})
            self.assertEqual(reader.call_args_list[0].args,('own',))
            self.assertEqual(reader.call_args_list[1].args,('own',))
        with patch('agent.results.read_evidence',return_value={'run_id':'own','items':[]}) as reader:
            self.invoke(loop.read_processing_evidence,context,{'rule_id':'U6','source_record_id':'a'*64,'source_table':'users'})
            reader.assert_called_once_with('own',limit=20,rule_id='U6',source_record_id='a'*64,source_table='users')
        with patch('metadata.store.Store') as store:
            store.return_value.list_runs.return_value = {'items':[]}
            self.invoke(loop.read_registry,context,{})
            store.return_value.list_runs.assert_called_once_with(status='PUBLISHED',limit=50)

    def test_write_tool_only_enqueues_and_rebinds_to_returned_run(self):
        context = self.context()
        with patch.dict(loop.TASKS,{'conversation':{}},clear=True), \
                patch('governance.service.submit_run',return_value=({'run_id':'queued-own','status':'QUEUED'},False)) as submit:
            result = self.invoke(loop.run_clean_data_pipeline,context,{'rule_pack':'default'})
            submit.assert_called_once_with('agent-conversation',run_id='conversation',parameters={'rule_pack':'default'})
            self.assertEqual(result['status'],'QUEUED')
            self.assertEqual(loop.TASKS['conversation']['status'],'queued')
            self.assertEqual(context.context.report_tag,'queued-own')

    def test_agent_factory_injects_write_tool_only_when_authorized(self):
        with patch('agent.loop._model',return_value=Mock()),patch('agent.loop.Agent') as factory:
            loop._build_agent(False,output_type=ConclusionProposal)
            arguments = factory.call_args.kwargs
            self.assertEqual(arguments['output_type'],ConclusionProposal)
            self.assertNotIn(loop.run_clean_data_pipeline,arguments['tools'])
            loop._build_agent(True)
            self.assertIn(loop.run_clean_data_pipeline,factory.call_args.kwargs['tools'])

    def test_read_agent_renders_validator_answer_instead_of_model_proposal(self):
        proposal = object()
        def run(*args,**kwargs):
            kwargs['context'].report_tag = 'own'
            return SimpleNamespace(final_output=proposal)
        async def runner(*args,**kwargs):
            return run(*args,**kwargs)
        with patch.dict(loop.TASKS,{'conversation':{}},clear=True),patch('agent.loop.SQLiteSession'), \
                patch('agent.loop._save_report_binding') as save, \
                patch('agent.loop._build_agent') as factory,patch('agent.loop.Runner.run',side_effect=runner), \
                patch('agent.conclusions.validate_conclusion',return_value={'answer':'checked','validation_status':'VERIFIED'}) as validate:
            loop._agent_loop('conversation','query',False)
            factory.assert_called_once_with(False,output_type=ConclusionProposal)
            validate.assert_called_once_with('own',proposal)
            self.assertEqual(loop.TASKS['conversation']['narrative'],'checked')
            self.assertEqual(loop.TASKS['conversation']['report_run_id'],'own')
            save.assert_called_once_with('conversation','own')

    def test_conversation_binding_survives_restart_and_cannot_change(self):
        with tempfile.TemporaryDirectory() as temporary,patch('agent.loop.AGENT_DB',str(Path(temporary)/'sessions.db')):
            self.assertIsNone(loop._read_report_binding('conversation'))
            loop._save_report_binding('conversation','own')
            loop._save_report_binding('conversation','own')
            with self.assertRaises(ValueError):
                loop._save_report_binding('conversation','other')
            self.assertEqual(loop._read_report_binding('conversation'),'own')
            proposal = object()
            with patch.dict(loop.TASKS,{},clear=True),patch('agent.loop._report_path') as path, \
                    patch('agent.loop.model_configured',return_value=True),patch('agent.loop._ask_loop',return_value=proposal) as model, \
                    patch('agent.conclusions.validate_conclusion',return_value={'answer':'checked'}) as validate:
                self.assertEqual(loop.ask_followup('conversation','why'),{'answer':'checked'})
                path.assert_called_once_with('own')
                model.assert_called_once_with('conversation','why',report_run_id='own')
                validate.assert_called_once_with('own',proposal)

    def test_followup_preflight_errors_keep_status_without_calling_model(self):
        with TestClient(server.app) as client,patch('agent.loop._ask_loop') as model,patch('agent.loop._report_path') as path:
            for error,status in ((FileNotFoundError(),404),(ReportUnavailable('candidate',409),409),
                                 (ReportUnavailable('corrupt',503),503),(LegacyUnavailable('ambiguous',409),409)):
                path.side_effect = error
                response = client.post('/api/tasks/own/ask',json={'message':'why'})
                self.assertEqual(response.status_code,status)
            model.assert_not_called()
