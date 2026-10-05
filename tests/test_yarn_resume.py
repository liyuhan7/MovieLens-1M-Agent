"""Resumed jobs must produce the same serializable report shape as fresh jobs."""
import datetime
import json
import shutil
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import Mock

from metadata.connection import ROOT
from pipeline.yarn import YarnExecutor


class ResumeReportTests(unittest.TestCase):
    def test_durable_sql_timestamps_do_not_leak_into_report(self):
        work = ROOT / "outputs" / "tests" / uuid.uuid4().hex
        claim = SimpleNamespace(run_id="run-resume", attempt_id="attempt-resume", work_path=work.relative_to(ROOT).as_posix())
        store = Mock()
        store.get_run.return_value = {"input_version": "raw-test", "request": {"raw_paths": {"users": "raw/users.dat"}}}
        store.input_locations.return_value = {"users": {"storage_uri": "/ml/raw/users.dat"}}
        store.prepare_job.return_value = {"status": "SUCCEEDED", "stage": "extractUsers", "job_id": "job_1_1",
                                          "application_id": "application_1_1", "submission_id": "ml-test",
                                          "created_at": datetime.datetime.now(), "updated_at": datetime.datetime.now()}
        executor = YarnExecutor(store, claim)
        executor.hdfs = Mock()
        executor.collect = Mock(return_value=work / "jobs/extractUsers")
        try:
            executor.run("extractUsers", ROOT / "raw/users.dat")
            encoded = json.loads(json.dumps(executor.jobs))
            self.assertEqual(encoded[0]["submission_id"], "ml-test")
            self.assertNotIn("created_at", encoded[0])
            self.assertEqual(set(encoded[0]), {"stage", "mode", "job_id", "application_id", "submission_id", "hdfs_output"})
            store.update_job.assert_not_called()
            executor.hdfs.command.assert_not_called()
        finally:
            if not work.resolve().is_relative_to((ROOT / "outputs/tests").resolve()):
                raise RuntimeError("Test cleanup escaped workspace")
            shutil.rmtree(work)


if __name__ == "__main__":
    unittest.main()
