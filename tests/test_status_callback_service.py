# -*- coding: utf-8 -*-
import tempfile
import unittest
from unittest.mock import Mock, patch

from services.job_store import JobStore
from services.status_callback_service import StatusCallbackService


class StatusCallbackServiceTests(unittest.TestCase):
    def test_enqueue_success_callback_uses_success_template(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = JobStore(db_path=f"{tmpdir}/job_tasks.db")
            service = StatusCallbackService(
                job_store=store,
                success_callback_url_template="http://127.0.0.1:17000/app-api/knowledge-base-file/status-success/{guid}",
                fail_callback_url_template="http://127.0.0.1:17000/app-api/knowledge-base-file/status-fail/{guid}",
            )
            task = store.enqueue_job(
                job_type="ingest",
                knowledge_base_file_guid="kb-1",
                file_guid="file-1",
                payload={"knowledge_base_file_guid": "kb-1", "file_guid": "file-1"},
            )

            service.enqueue_task_callback(task, "ingest.success", payload={})
            callback = store.claim_next_callback(worker_id="worker-1")

            self.assertEqual(
                callback["target_url"],
                "http://127.0.0.1:17000/app-api/knowledge-base-file/status-success/kb-1",
            )

    def test_enqueue_failed_callback_uses_fail_template(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = JobStore(db_path=f"{tmpdir}/job_tasks.db")
            service = StatusCallbackService(
                job_store=store,
                success_callback_url_template="http://127.0.0.1:17000/app-api/knowledge-base-file/status-success/{guid}",
                fail_callback_url_template="http://127.0.0.1:17000/app-api/knowledge-base-file/status-fail/{guid}",
            )
            task = store.enqueue_job(
                job_type="ingest",
                knowledge_base_file_guid="kb-2",
                file_guid="file-2",
                payload={"knowledge_base_file_guid": "kb-2", "file_guid": "file-2"},
            )

            service.enqueue_task_callback(task, "ingest.failed", payload={})
            callback = store.claim_next_callback(worker_id="worker-1")

            self.assertEqual(
                callback["target_url"],
                "http://127.0.0.1:17000/app-api/knowledge-base-file/status-fail/kb-2",
            )

    def test_dispatch_uses_patch_without_json_for_empty_payload(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = JobStore(db_path=f"{tmpdir}/job_tasks.db")
            service = StatusCallbackService(
                job_store=store,
                app_id="app-test",
                app_secret="secret-test",
            )
            response = Mock()
            response.status_code = 200

            callback = {
                "callback_id": "cb-1",
                "task_id": "task-1",
                "target_url": "http://127.0.0.1:17000/app-api/knowledge-base-file/status-success/kb-1",
                "payload": {},
            }

            with patch("services.status_callback_service.requests.patch", return_value=response) as mock_patch:
                service.dispatch_pending_callback(callback)

            _, kwargs = mock_patch.call_args
            self.assertEqual(kwargs["timeout"], (10, 30))
            self.assertNotIn("json", kwargs)
            headers = kwargs["headers"]
            self.assertEqual(headers["X-App-Id"], "app-test")
            self.assertTrue(headers["X-Timestamp"])
            self.assertTrue(headers["X-Nonce"])
            self.assertEqual(len(headers["X-Signature"]), 64)


if __name__ == "__main__":
    unittest.main()
