# -*- coding: utf-8 -*-
import hashlib
import hmac
import os
import time
import uuid
from typing import Any, Dict, Optional

try:
    import requests
except ImportError:  # pragma: no cover
    class _RequestsStub:
        @staticmethod
        def patch(*args, **kwargs):
            raise RuntimeError("requests is required for status callback delivery.")

    requests = _RequestsStub()

from services.job_store import JobStore
from utils.logger import setup_logger


logger = setup_logger("StatusCallbackService")

CALLBACK_APP_ID = os.getenv("FILE_APP_ID", "")
CALLBACK_APP_SECRET = os.getenv("FILE_APP_SECRET", "")


class StatusCallbackService:
    def __init__(
        self,
        *,
        job_store: JobStore,
        success_callback_url_template: Optional[str] = None,
        fail_callback_url_template: Optional[str] = None,
        app_id: str = CALLBACK_APP_ID,
        app_secret: str = CALLBACK_APP_SECRET,
    ):
        self.job_store = job_store
        self.app_id = app_id
        self.app_secret = app_secret
        self.success_callback_url_template = (
            success_callback_url_template
            or os.getenv(
                "KB_STATUS_SUCCESS_CALLBACK_URL_TEMPLATE",
                "http://172.20.8.38:17000/app-api/knowledge-base-file/status-success/{guid}",
            ).strip()
        )
        self.fail_callback_url_template = (
            fail_callback_url_template
            or os.getenv(
                "KB_STATUS_FAIL_CALLBACK_URL_TEMPLATE",
                "http://172.20.8.38:17000/app-api/knowledge-base-file/status-fail/{guid}",
            ).strip()
        )

    def enqueue_task_callback(self, task: Dict[str, Any], event_type: str, payload: Dict[str, Any]) -> None:
        target_url = self._build_target_url(task, event_type)
        if not target_url:
            logger.warning(
                "Callback URL template is not configured. Skip callback for task_id=%s event_type=%s",
                task["task_id"],
                event_type,
            )
            return
        self.job_store.enqueue_callback(
            task_id=task["task_id"],
            event_type=event_type,
            target_url=target_url,
            payload=payload,
        )

    def dispatch_pending_callback(self, callback: Dict[str, Any]) -> None:
        request_kwargs = {
            "timeout": (10, 30),
            "headers": self._build_auth_headers(),
        }
        payload = callback.get("payload") or {}
        if payload:
            request_kwargs["json"] = payload
        response = requests.patch(callback["target_url"], **request_kwargs)
        response.raise_for_status()
        logger.info(
            "Delivered callback callback_id=%s task_id=%s status_code=%s",
            callback["callback_id"],
            callback["task_id"],
            response.status_code,
        )

    def _build_target_url(self, task: Dict[str, Any], event_type: str) -> str:
        if event_type.endswith(".success"):
            template = self.success_callback_url_template
        elif event_type.endswith(".failed"):
            template = self.fail_callback_url_template
        else:
            template = ""
        if not template:
            return ""
        return template.format(guid=task["knowledge_base_file_guid"])

    def _build_auth_headers(self) -> Dict[str, str]:
        timestamp = str(int(time.time() * 1000))
        nonce = uuid.uuid4().hex
        payload = f"{self.app_id}{timestamp}{nonce}"
        signature = hmac.new(
            self.app_secret.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return {
            "X-App-Id": self.app_id,
            "X-Timestamp": timestamp,
            "X-Nonce": nonce,
            "X-Signature": signature,
        }
