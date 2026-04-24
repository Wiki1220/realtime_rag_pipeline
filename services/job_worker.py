# -*- coding: utf-8 -*-
import os
import shutil
import socket
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

from services.file_download_service import FileDownloadService
from services.ingest_pipeline_service import IngestPipelineService
from services.job_store import JobStore
from services.milvus_delete_service import MilvusDeleteService
from services.status_callback_service import StatusCallbackService
from utils.logger import setup_logger


logger = setup_logger("JobWorker")

JOB_DB_PATH = os.getenv("JOB_STORE_DB_PATH", "logs/job_tasks.db")
JOB_WORKER_SLEEP_SECONDS = float(os.getenv("JOB_WORKER_SLEEP_SECONDS", "2"))
JOB_LEASE_SECONDS = int(os.getenv("JOB_LEASE_SECONDS", "900"))
JOB_HEARTBEAT_SECONDS = float(os.getenv("JOB_HEARTBEAT_SECONDS", "60"))


class _TaskHeartbeat:
    def __init__(
        self,
        *,
        job_store: JobStore,
        task_id: str,
        worker_id: str,
        lease_seconds: int,
        interval_seconds: float,
    ):
        self.job_store = job_store
        self.task_id = task_id
        self.worker_id = worker_id
        self.lease_seconds = lease_seconds
        self.interval_seconds = interval_seconds
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, name=f"job-heartbeat-{task_id}", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=max(1.0, self.interval_seconds))

    def _run(self) -> None:
        while not self._stop_event.wait(self.interval_seconds):
            try:
                self.job_store.heartbeat_task(
                    self.task_id,
                    self.worker_id,
                    lease_seconds=self.lease_seconds,
                )
            except Exception:
                logger.warning(
                    "Heartbeat failed task_id=%s worker_id=%s",
                    self.task_id,
                    self.worker_id,
                    exc_info=True,
                )


class JobWorker:
    def __init__(
        self,
        *,
        job_store: Optional[JobStore] = None,
        file_download_service: Optional[FileDownloadService] = None,
        ingest_service: Optional[IngestPipelineService] = None,
        milvus_delete_service: Optional[MilvusDeleteService] = None,
        callback_service: Optional[StatusCallbackService] = None,
    ):
        self.job_store = job_store or JobStore(db_path=JOB_DB_PATH)
        self.file_download_service = file_download_service or FileDownloadService()
        self.ingest_service = ingest_service or IngestPipelineService()
        self.milvus_delete_service = milvus_delete_service or MilvusDeleteService()
        self.callback_service = callback_service or StatusCallbackService(job_store=self.job_store)
        self.worker_id = f"{socket.gethostname()}-{os.getpid()}"

    def run_forever(self) -> None:
        logger.info("Starting job worker worker_id=%s", self.worker_id)
        while True:
            worked = self._process_one_job()
            callback_worked = self._process_one_callback()
            if not worked and not callback_worked:
                time.sleep(JOB_WORKER_SLEEP_SECONDS)

    def _process_one_job(self) -> bool:
        task = self.job_store.claim_next_job(
            worker_id=self.worker_id,
            lease_seconds=JOB_LEASE_SECONDS,
        )
        if not task:
            return False

        logger.info(
            "Claimed task task_id=%s job_type=%s knowledge_base_file_guid=%s retry_count=%s",
            task["task_id"],
            task["job_type"],
            task["knowledge_base_file_guid"],
            task["retry_count"],
        )

        heartbeat = _TaskHeartbeat(
            job_store=self.job_store,
            task_id=task["task_id"],
            worker_id=self.worker_id,
            lease_seconds=JOB_LEASE_SECONDS,
            interval_seconds=JOB_HEARTBEAT_SECONDS,
        )
        heartbeat.start()

        try:
            if task["job_type"] == "ingest":
                result = self._handle_ingest(task)
            elif task["job_type"] == "delete":
                result = self._handle_delete(task)
            else:
                raise RuntimeError(f"Unsupported job_type: {task['job_type']}")

            self.job_store.complete_task(task["task_id"], result)
            self.callback_service.enqueue_task_callback(
                task,
                event_type=f"{task['job_type']}.success",
                payload={},
            )
        except Exception as exc:
            logger.error(
                "Task failed task_id=%s knowledge_base_file_guid=%s error=%s",
                task["task_id"],
                task["knowledge_base_file_guid"],
                exc,
                exc_info=True,
            )
            retryable = task["job_type"] == "ingest"
            self.job_store.fail_task(
                task["task_id"],
                str(exc),
                retryable=retryable,
                retry_delay_seconds=30,
            )
            latest = self.job_store.get_task(task["task_id"])
            if latest and latest["status"] == "failed":
                self.callback_service.enqueue_task_callback(
                    latest,
                    event_type=f"{task['job_type']}.failed",
                    payload={},
                )
        finally:
            heartbeat.stop()
        return True

    def _handle_ingest(self, task: Dict[str, Any]) -> Dict[str, Any]:
        file_guid = task.get("file_guid")
        if not file_guid:
            raise RuntimeError("file_guid is required for ingest tasks.")

        download_info = self.file_download_service.get_file_info(file_guid)
        downloaded_path = download_info["path"]
        local_ingest_path = self._stage_downloaded_file(task["knowledge_base_file_guid"], file_guid, downloaded_path)
        pipeline_result = self.ingest_service.ingest_downloaded_file(
            local_path=local_ingest_path,
            knowledge_base_file_guid=task["knowledge_base_file_guid"],
            file_guid=file_guid,
            stored_filename=Path(local_ingest_path).name,
            source_file=download_info.get("source_file"),
        )
        if pipeline_result["status"] != "success":
            raise RuntimeError(f"Pipeline ingestion failed: {pipeline_result}")

        return {
            "knowledge_base_file_guid": task["knowledge_base_file_guid"],
            "file_guid": file_guid,
            "run_id": pipeline_result["run_id"],
            "doc_id": pipeline_result["doc_id"],
            "file_path": local_ingest_path,
            "source_file": download_info.get("source_file"),
            "stored_filename": Path(local_ingest_path).name,
            "pipeline_result": pipeline_result,
        }

    def _handle_delete(self, task: Dict[str, Any]) -> Dict[str, Any]:
        delete_result = self.milvus_delete_service.delete_document(task["knowledge_base_file_guid"])
        return {
            "knowledge_base_file_guid": task["knowledge_base_file_guid"],
            "delete_result": delete_result,
        }

    def _process_one_callback(self) -> bool:
        callback = self.job_store.claim_next_callback(worker_id=self.worker_id)
        if not callback:
            return False
        try:
            self.callback_service.dispatch_pending_callback(callback)
            self.job_store.complete_callback(callback["callback_id"])
        except Exception as exc:
            logger.warning(
                "Callback failed callback_id=%s task_id=%s error=%s",
                callback["callback_id"],
                callback["task_id"],
                exc,
            )
            self.job_store.fail_callback(callback["callback_id"], str(exc))
        return True

    def _stage_downloaded_file(self, knowledge_base_file_guid: str, file_guid: str, downloaded_path: str) -> str:
        suffix = Path(downloaded_path).suffix
        target_dir = Path("data/uploads")
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / f"{knowledge_base_file_guid}_{file_guid}{suffix}"
        if not target_path.exists():
            shutil.copy2(downloaded_path, target_path)
        return str(target_path.resolve())

def run_worker_forever() -> None:
    JobWorker().run_forever()
