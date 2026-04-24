# -*- coding: utf-8 -*-
import json
import os
import sqlite3
import time
import uuid
from typing import Any, Dict, List, Optional


class JobStore:
    def __init__(self, db_path: str = "logs/job_tasks.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS job_tasks (
                    task_id TEXT PRIMARY KEY,
                    dedupe_key TEXT NOT NULL UNIQUE,
                    job_type TEXT NOT NULL,
                    knowledge_base_file_guid TEXT NOT NULL,
                    file_guid TEXT,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    result_json TEXT NOT NULL DEFAULT '{}',
                    error_message TEXT,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    max_retries INTEGER NOT NULL DEFAULT 3,
                    locked_by TEXT,
                    locked_until REAL,
                    next_run_at REAL NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS callback_outbox (
                    callback_id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    target_url TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    max_retries INTEGER NOT NULL DEFAULT 8,
                    locked_by TEXT,
                    locked_until REAL,
                    next_run_at REAL NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_job_tasks_status_next_run ON job_tasks(status, next_run_at, created_at)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_job_tasks_guid_type ON job_tasks(knowledge_base_file_guid, job_type)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_callback_outbox_status_next_run ON callback_outbox(status, next_run_at, created_at)"
            )

    def _row_to_dict(self, row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
        if row is None:
            return None
        record = dict(row)
        for key in ("payload_json", "result_json"):
            if key in record:
                try:
                    record[key] = json.loads(record[key] or "{}")
                except json.JSONDecodeError:
                    record[key] = {}
        if "payload_json" in record:
            record["payload"] = record.pop("payload_json")
        if "result_json" in record:
            record["result"] = record.pop("result_json")
        return record

    def _callback_row_to_dict(self, row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
        if row is None:
            return None
        record = dict(row)
        try:
            record["payload"] = json.loads(record.pop("payload_json") or "{}")
        except json.JSONDecodeError:
            record["payload"] = {}
        return record

    def enqueue_job(
        self,
        *,
        job_type: str,
        knowledge_base_file_guid: str,
        file_guid: Optional[str],
        payload: Dict[str, Any],
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        dedupe_key = f"{job_type}:{knowledge_base_file_guid}"
        now = time.time()
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute(
                "SELECT * FROM job_tasks WHERE dedupe_key = ?",
                (dedupe_key,),
            )
            existing = cursor.fetchone()
            if existing:
                if existing["status"] == "failed":
                    cursor.execute(
                        """
                        UPDATE job_tasks
                        SET file_guid = ?,
                            payload_json = ?,
                            status = 'pending',
                            retry_count = 0,
                            error_message = NULL,
                            locked_by = NULL,
                            locked_until = NULL,
                            next_run_at = ?,
                            updated_at = ?
                        WHERE dedupe_key = ?
                        """,
                        (
                            file_guid,
                            json.dumps(payload, ensure_ascii=False),
                            now,
                            now,
                            dedupe_key,
                        ),
                    )
                    conn.commit()
                    cursor.execute("SELECT * FROM job_tasks WHERE dedupe_key = ?", (dedupe_key,))
                    return self._row_to_dict(cursor.fetchone())
                conn.commit()
                return self._row_to_dict(existing)

            task_id = f"{job_type}-{uuid.uuid4().hex}"
            cursor.execute(
                """
                INSERT INTO job_tasks (
                    task_id, dedupe_key, job_type, knowledge_base_file_guid, file_guid,
                    status, payload_json, retry_count, max_retries, next_run_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'pending', ?, 0, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    dedupe_key,
                    job_type,
                    knowledge_base_file_guid,
                    file_guid,
                    json.dumps(payload, ensure_ascii=False),
                    max_retries,
                    now,
                    now,
                    now,
                ),
            )
            conn.commit()
            cursor.execute("SELECT * FROM job_tasks WHERE task_id = ?", (task_id,))
            return self._row_to_dict(cursor.fetchone())

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM job_tasks WHERE task_id = ?", (task_id,))
            return self._row_to_dict(cursor.fetchone())

    def list_tasks(
        self,
        *,
        status: Optional[str] = None,
        knowledge_base_file_guid: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        limit = max(1, min(limit, 200))
        with self._connect() as conn:
            cursor = conn.cursor()
            if status and knowledge_base_file_guid:
                cursor.execute(
                    """
                    SELECT * FROM job_tasks
                    WHERE status = ? AND knowledge_base_file_guid = ?
                    ORDER BY updated_at DESC LIMIT ?
                    """,
                    (status, knowledge_base_file_guid, limit),
                )
            elif status:
                cursor.execute(
                    "SELECT * FROM job_tasks WHERE status = ? ORDER BY updated_at DESC LIMIT ?",
                    (status, limit),
                )
            elif knowledge_base_file_guid:
                cursor.execute(
                    """
                    SELECT * FROM job_tasks
                    WHERE knowledge_base_file_guid = ?
                    ORDER BY updated_at DESC LIMIT ?
                    """,
                    (knowledge_base_file_guid, limit),
                )
            else:
                cursor.execute(
                    "SELECT * FROM job_tasks ORDER BY updated_at DESC LIMIT ?",
                    (limit,),
                )
            return [self._row_to_dict(row) for row in cursor.fetchall()]

    def claim_next_job(
        self,
        *,
        worker_id: str,
        job_types: Optional[List[str]] = None,
        lease_seconds: int = 900,
    ) -> Optional[Dict[str, Any]]:
        now = time.time()
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            base_sql = """
                SELECT * FROM job_tasks
                WHERE next_run_at <= ?
                  AND (
                    status = 'pending'
                    OR (status = 'running' AND COALESCE(locked_until, 0) < ?)
                  )
            """
            params: List[Any] = [now, now]
            if job_types:
                placeholders = ",".join("?" for _ in job_types)
                base_sql += f" AND job_type IN ({placeholders})"
                params.extend(job_types)
            base_sql += " ORDER BY created_at ASC LIMIT 1"
            cursor.execute(base_sql, tuple(params))
            row = cursor.fetchone()
            if not row:
                conn.commit()
                return None

            cursor.execute(
                """
                UPDATE job_tasks
                SET status = 'running',
                    locked_by = ?,
                    locked_until = ?,
                    updated_at = ?
                WHERE task_id = ?
                """,
                (worker_id, now + lease_seconds, now, row["task_id"]),
            )
            conn.commit()
            cursor.execute("SELECT * FROM job_tasks WHERE task_id = ?", (row["task_id"],))
            return self._row_to_dict(cursor.fetchone())

    def heartbeat_task(self, task_id: str, worker_id: str, lease_seconds: int = 900) -> None:
        now = time.time()
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE job_tasks
                SET locked_until = ?, updated_at = ?
                WHERE task_id = ? AND locked_by = ? AND status = 'running'
                """,
                (now + lease_seconds, now, task_id, worker_id),
            )
            conn.commit()

    def complete_task(self, task_id: str, result: Dict[str, Any]) -> None:
        now = time.time()
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE job_tasks
                SET status = 'success',
                    result_json = ?,
                    error_message = NULL,
                    locked_by = NULL,
                    locked_until = NULL,
                    updated_at = ?
                WHERE task_id = ?
                """,
                (json.dumps(result, ensure_ascii=False), now, task_id),
            )
            conn.commit()

    def fail_task(
        self,
        task_id: str,
        error_message: str,
        *,
        retryable: bool,
        retry_delay_seconds: int = 30,
        result: Optional[Dict[str, Any]] = None,
    ) -> None:
        now = time.time()
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT retry_count, max_retries FROM job_tasks WHERE task_id = ?", (task_id,))
            row = cursor.fetchone()
            if not row:
                return

            next_retry_count = int(row["retry_count"]) + 1
            max_retries = int(row["max_retries"])
            final_failed = (not retryable) or next_retry_count > max_retries
            status = "failed" if final_failed else "pending"
            next_run_at = now if final_failed else now + retry_delay_seconds
            cursor.execute(
                """
                UPDATE job_tasks
                SET status = ?,
                    retry_count = ?,
                    result_json = ?,
                    error_message = ?,
                    locked_by = NULL,
                    locked_until = NULL,
                    next_run_at = ?,
                    updated_at = ?
                WHERE task_id = ?
                """,
                (
                    status,
                    next_retry_count,
                    json.dumps(result or {}, ensure_ascii=False),
                    error_message,
                    next_run_at,
                    now,
                    task_id,
                ),
            )
            conn.commit()

    def enqueue_callback(
        self,
        *,
        task_id: str,
        event_type: str,
        target_url: str,
        payload: Dict[str, Any],
        max_retries: int = 8,
    ) -> Dict[str, Any]:
        now = time.time()
        callback_id = f"cb-{uuid.uuid4().hex}"
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO callback_outbox (
                    callback_id, task_id, event_type, target_url, payload_json,
                    status, retry_count, max_retries, next_run_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?, ?)
                """,
                (
                    callback_id,
                    task_id,
                    event_type,
                    target_url,
                    json.dumps(payload, ensure_ascii=False),
                    max_retries,
                    now,
                    now,
                    now,
                ),
            )
            conn.commit()
            cursor.execute("SELECT * FROM callback_outbox WHERE callback_id = ?", (callback_id,))
            return self._callback_row_to_dict(cursor.fetchone())

    def claim_next_callback(self, *, worker_id: str, lease_seconds: int = 300) -> Optional[Dict[str, Any]]:
        now = time.time()
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute(
                """
                SELECT * FROM callback_outbox
                WHERE next_run_at <= ?
                  AND (
                    status = 'pending'
                    OR (status = 'running' AND COALESCE(locked_until, 0) < ?)
                  )
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (now, now),
            )
            row = cursor.fetchone()
            if not row:
                conn.commit()
                return None

            cursor.execute(
                """
                UPDATE callback_outbox
                SET status = 'running',
                    locked_by = ?,
                    locked_until = ?,
                    updated_at = ?
                WHERE callback_id = ?
                """,
                (worker_id, now + lease_seconds, now, row["callback_id"]),
            )
            conn.commit()
            cursor.execute("SELECT * FROM callback_outbox WHERE callback_id = ?", (row["callback_id"],))
            return self._callback_row_to_dict(cursor.fetchone())

    def complete_callback(self, callback_id: str) -> None:
        now = time.time()
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE callback_outbox
                SET status = 'success',
                    error_message = NULL,
                    locked_by = NULL,
                    locked_until = NULL,
                    updated_at = ?
                WHERE callback_id = ?
                """,
                (now, callback_id),
            )
            conn.commit()

    def fail_callback(self, callback_id: str, error_message: str, retry_delay_seconds: int = 60) -> None:
        now = time.time()
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT retry_count, max_retries FROM callback_outbox WHERE callback_id = ?",
                (callback_id,),
            )
            row = cursor.fetchone()
            if not row:
                return
            next_retry_count = int(row["retry_count"]) + 1
            final_failed = next_retry_count > int(row["max_retries"])
            cursor.execute(
                """
                UPDATE callback_outbox
                SET status = ?,
                    retry_count = ?,
                    error_message = ?,
                    locked_by = NULL,
                    locked_until = NULL,
                    next_run_at = ?,
                    updated_at = ?
                WHERE callback_id = ?
                """,
                (
                    "failed" if final_failed else "pending",
                    next_retry_count,
                    error_message,
                    now if final_failed else now + retry_delay_seconds,
                    now,
                    callback_id,
                ),
            )
            conn.commit()
