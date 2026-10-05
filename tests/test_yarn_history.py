"""Missing live records must use matching history, never duplicate a submission."""
import importlib.util
import os
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import httpx

from metadata.connection import ROOT


def candidate_module():
    path = os.environ.get("ML_YARN_CANDIDATE")
    if not path:
        from pipeline import yarn
        return yarn
    spec = importlib.util.spec_from_file_location("candidate_yarn_history", ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


yarn = candidate_module()
APPLICATION = "application_1791107599181_0075"
JOB = APPLICATION.replace("application_", "job_", 1)
SUBMISSION = "ml-history-acceptance"
REAL_CLIENT = httpx.Client


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.executor = yarn.YarnExecutor(Mock(), SimpleNamespace(
            run_id="run-history", attempt_id="attempt-history", work_path="outputs/history-tests"))
        self.executor.rm_url = "http://rm:8088"
        self.executor.history_url = "http://history:19888"
        self.job = {"id": JOB, "name": SUBMISSION, "state": "SUCCEEDED", "diagnostics": ""}

    def client(self, routes):
        # Real history discovery always confirms a full job response. Unit
        # fixtures default to the full listed object unless explicitly changed.
        routes = dict(routes)
        listed = routes.get(("history", "/ws/v1/history/mapreduce/jobs"), {})
        if isinstance(listed, dict):
            for job in (listed.get("jobs") or {}).get("job", []):
                routes.setdefault(("history", "/ws/v1/history/mapreduce/jobs/" + job["id"]), {"job": job})
        def respond(request):
            value = routes.get((request.url.host, request.url.path), 404)
            if isinstance(value, Exception):
                raise value
            return httpx.Response(value if isinstance(value, int) else 200,
                                  json={} if isinstance(value, int) else value)
        return patch.object(yarn.httpx, "Client", side_effect=lambda **kw:
                            REAL_CLIENT(transport=httpx.MockTransport(respond), **kw))

    def test_recorded_id_rm_expired_uses_matching_history(self):
        with self.client({("history", "/ws/v1/history/mapreduce/jobs/" + JOB): {"job": self.job}}):
            app = self.executor.application(APPLICATION, SUBMISSION)
        self.assertEqual((app["id"], app["finalStatus"], app["history_source"]),
                         (APPLICATION, "SUCCEEDED", "JobHistory"))

    def test_rm_temporarily_unavailable_uses_history(self):
        with self.client({("rm", "/ws/v1/cluster/apps/" + APPLICATION): 503,
                          ("history", "/ws/v1/history/mapreduce/jobs/" + JOB): {"job": self.job}}):
            self.assertEqual(self.executor.application(APPLICATION, SUBMISSION)["finalStatus"], "SUCCEEDED")

    def test_id_not_saved_resolves_exact_history_name(self):
        foreign = dict(self.job, id="job_1791107599181_0076", name="foreign")
        with self.client({("history", "/ws/v1/history/mapreduce/jobs"): {"jobs": {"job": [foreign, self.job]}}}):
            apps = self.executor.applications(SUBMISSION)
        self.assertEqual([app["id"] for app in apps], [APPLICATION])

    def test_same_job_in_both_services_is_one_candidate(self):
        live = {"id": APPLICATION, "name": SUBMISSION, "state": "FINISHED", "finalStatus": "SUCCEEDED"}
        with self.client({("rm", "/ws/v1/cluster/apps"): {"apps": {"app": [live]}},
                          ("history", "/ws/v1/history/mapreduce/jobs"): {"jobs": {"job": [self.job]}}}):
            self.assertEqual(len(self.executor.applications(SUBMISSION)), 1)

    def test_truncated_name_requires_full_name_confirmation(self):
        shortened = dict(self.job, name=SUBMISSION[:-1])
        collision = dict(shortened, id="job_1791107599181_0076")
        with self.client({("history", "/ws/v1/history/mapreduce/jobs"): {"jobs": {"job": [shortened, collision]}},
                          ("history", "/ws/v1/history/mapreduce/jobs/" + JOB): {"job": self.job},
                          ("history", "/ws/v1/history/mapreduce/jobs/" + collision["id"]):
                          {"job": dict(collision, name=SUBMISSION + "-foreign")}}):
            self.assertEqual([app["id"] for app in self.executor.applications(SUBMISSION)], [APPLICATION])

    def test_conflicting_terminal_history_is_uncertain(self):
        live = {"id": APPLICATION, "name": SUBMISSION, "state": "FAILED", "finalStatus": "FAILED"}
        with self.client({("rm", "/ws/v1/cluster/apps"): {"apps": {"app": [live]}},
                          ("history", "/ws/v1/history/mapreduce/jobs"): {"jobs": {"job": [self.job]}}}):
            with self.assertRaises(yarn.RecoveryPending):
                self.executor.applications(SUBMISSION)

    def test_unreadable_matching_candidate_cannot_be_ignored(self):
        foreign = dict(self.job, id="job_1791107599181_0076", name=SUBMISSION[:-1])
        with self.client({("history", "/ws/v1/history/mapreduce/jobs"): {"jobs": {"job": [self.job, foreign]}},
                          ("history", "/ws/v1/history/mapreduce/jobs/" + foreign["id"]): 503}):
            with self.assertRaises(yarn.RecoveryPending):
                self.executor.applications(SUBMISSION)

    def test_foreign_history_name_or_id_cannot_complete_recorded_job(self):
        for changed in (dict(self.job, name="foreign"), dict(self.job, id="job_1791107599181_0076")):
            with self.subTest(changed=changed), self.client({
                    ("history", "/ws/v1/history/mapreduce/jobs/" + JOB): {"job": changed}}):
                with self.assertRaises(yarn.RecoveryPending):
                    self.executor.application(APPLICATION, SUBMISSION)

    def test_nonterminal_history_is_not_success(self):
        with self.client({("history", "/ws/v1/history/mapreduce/jobs/" + JOB):
                          {"job": dict(self.job, state="RUNNING")}}):
            with self.assertRaises(yarn.RecoveryPending):
                self.executor.application(APPLICATION, SUBMISSION)

    def test_failed_killed_error_history_remain_failure(self):
        for state in ("FAILED", "KILLED", "ERROR"):
            with self.subTest(state=state), self.client({
                    ("history", "/ws/v1/history/mapreduce/jobs/" + JOB): {"job": dict(self.job, state=state)}}):
                app = self.executor.application(APPLICATION, SUBMISSION)
                self.assertEqual(app["finalStatus"], "FAILED" if state == "ERROR" else state)

    def test_no_service_confirms_id_remains_pending(self):
        with self.client({}):
            with self.assertRaises(yarn.RecoveryPending):
                self.executor.application(APPLICATION, SUBMISSION)

    def gap_run(self, history):
        store = self.executor.store
        store.get_run.return_value = {"input_version": "raw-history", "request": {"raw_paths": {"users": "data/users.dat"}}}
        store.input_locations.return_value = {"users": {"storage_uri": "/ml/raw/history/users.dat"}}
        store.prepare_job.return_value = {"status": "SUBMITTING", "created": False,
                                         "application_id": None, "submission_id": SUBMISSION}
        with self.client({("history", "/ws/v1/history/mapreduce/jobs"): {"jobs": {"job": history}}}), \
                patch.object(yarn.subprocess, "Popen", side_effect=AssertionError("Must not resubmit")):
            with self.assertRaises(yarn.RecoveryPending):
                self.executor.run("extractUsers", ROOT / "data/users.dat")
        store.update_job.assert_not_called()

    def test_missing_submission_is_not_proof_to_resubmit(self):
        self.gap_run([])

    def test_duplicate_submission_history_is_not_chosen_arbitrarily(self):
        self.gap_run([self.job, dict(self.job, id="job_1791107599181_0076")])

    def test_history_success_still_requires_hdfs_commit(self):
        self.executor.hdfs = Mock()
        self.executor.hdfs.exists.return_value = False
        with self.assertRaisesRegex(RuntimeError, "committed output marker"):
            self.executor.collect("/ml/missing-output", Path("absent-cache"))
        self.executor.hdfs.get.assert_not_called()


if __name__ == "__main__":
    unittest.main()
