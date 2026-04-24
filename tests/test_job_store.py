# -*- coding: utf-8 -*-
import tempfile
import unittest

from services.job_store import JobStore


class JobStoreTests(unittest.TestCase):
    def test_enqueue_deduplicates_pending_task(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = JobStore(db_path=f"{tmpdir}/job_tasks.db")
            first = store.enqueue_job(
                job_type="ingest",
                knowledge_base_file_guid="kb-1",
                file_guid="file-1",
                payload={"knowledge_base_file_guid": "kb-1", "file_guid": "file-1"},
            )
            second = store.enqueue_job(
                job_type="ingest",
                knowledge_base_file_guid="kb-1",
                file_guid="file-1",
                payload={"knowledge_base_file_guid": "kb-1", "file_guid": "file-1"},
            )

            self.assertEqual(first["task_id"], second["task_id"])
            self.assertEqual(second["status"], "pending")

    def test_failed_task_can_be_reactivated(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = JobStore(db_path=f"{tmpdir}/job_tasks.db")
            task = store.enqueue_job(
                job_type="ingest",
                knowledge_base_file_guid="kb-2",
                file_guid="file-2",
                payload={"knowledge_base_file_guid": "kb-2", "file_guid": "file-2"},
            )
            store.fail_task(task["task_id"], "boom", retryable=False)

            reactivated = store.enqueue_job(
                job_type="ingest",
                knowledge_base_file_guid="kb-2",
                file_guid="file-3",
                payload={"knowledge_base_file_guid": "kb-2", "file_guid": "file-3"},
            )

            self.assertEqual(task["task_id"], reactivated["task_id"])
            self.assertEqual(reactivated["status"], "pending")
            self.assertEqual(reactivated["file_guid"], "file-3")
            self.assertEqual(reactivated["retry_count"], 0)

    def test_claim_complete_and_callback_flow(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = JobStore(db_path=f"{tmpdir}/job_tasks.db")
            task = store.enqueue_job(
                job_type="delete",
                knowledge_base_file_guid="kb-3",
                file_guid=None,
                payload={"knowledge_base_file_guid": "kb-3"},
            )
            claimed = store.claim_next_job(worker_id="worker-1")
            self.assertEqual(task["task_id"], claimed["task_id"])
            self.assertEqual(claimed["status"], "running")

            store.complete_task(task["task_id"], {"deleted_count": 3})
            completed = store.get_task(task["task_id"])
            self.assertEqual(completed["status"], "success")
            self.assertEqual(completed["result"]["deleted_count"], 3)

            callback = store.enqueue_callback(
                task_id=task["task_id"],
                event_type="delete.success",
                target_url="http://callback.local",
                payload={"task_id": task["task_id"]},
            )
            claimed_callback = store.claim_next_callback(worker_id="worker-1")
            self.assertEqual(callback["callback_id"], claimed_callback["callback_id"])
            self.assertEqual(claimed_callback["status"], "running")

            store.complete_callback(callback["callback_id"])
            claimed_again = store.claim_next_callback(worker_id="worker-1")
            self.assertIsNone(claimed_again)

    def test_list_tasks_can_filter_by_knowledge_base_file_guid(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = JobStore(db_path=f"{tmpdir}/job_tasks.db")
            store.enqueue_job(
                job_type="ingest",
                knowledge_base_file_guid="kb-target",
                file_guid="file-1",
                payload={"knowledge_base_file_guid": "kb-target", "file_guid": "file-1"},
            )
            store.enqueue_job(
                job_type="delete",
                knowledge_base_file_guid="kb-target",
                file_guid=None,
                payload={"knowledge_base_file_guid": "kb-target"},
                max_retries=0,
            )
            store.enqueue_job(
                job_type="ingest",
                knowledge_base_file_guid="kb-other",
                file_guid="file-2",
                payload={"knowledge_base_file_guid": "kb-other", "file_guid": "file-2"},
            )

            filtered = store.list_tasks(knowledge_base_file_guid="kb-target")
            filtered_guids = {task["knowledge_base_file_guid"] for task in filtered}

            self.assertEqual(filtered_guids, {"kb-target"})
            self.assertEqual(len(filtered), 2)


if __name__ == "__main__":
    unittest.main()
