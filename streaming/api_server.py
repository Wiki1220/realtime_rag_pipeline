# -*- coding: utf-8 -*-
"""
FastAPI Streaming Gateway (HTTP Webhook)
让整个 Pipeline 流水线变成一个常驻后台的 API 服务。
任何外部脚本、爬虫、业务系统只要向这个接口发送 POST 请求，
就可以立刻触发一条数据的端到端清洗与入库。
"""
import os
import logging
import json
import sqlite3
import uuid
from pathlib import Path
import yaml
import requests
from fastapi import FastAPI, HTTPException, BackgroundTasks, UploadFile, File, Form
from pydantic import BaseModel, Field, PrivateAttr
from typing import Dict, Any, Optional, List

from core.context import Context
from core.pipeline import Pipeline
from core.review_store import ReviewStore
from core.run_ops import (
    build_result_identifiers,
    expects_milvus_insert as pipeline_expects_milvus_insert,
    handle_pipeline_result,
)
from core.state_manager import global_state_manager
from services.job_store import JobStore
from utils.logger import setup_logger

logger = setup_logger("APIServer")

# 1. 初始化 FastAPI 应用
app = FastAPI(
    title="realtime_rag_pipeline API Gateway",
    description="支持人工审核的准实时大语言模型流水线接口",
    version="2.0.0"
)

# 保证本地日志文件夹存在，用于存储 SQLite DB
os.makedirs("logs", exist_ok=True)
review_store = ReviewStore(db_path="logs/pending_reviews.db")
job_store = JobStore(db_path=os.getenv("JOB_STORE_DB_PATH", "logs/job_tasks.db"))
UPLOAD_DIR = Path("data/uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
ALLOWED_EXTENSIONS = {"pdf", "docx", "doc", "txt", "md", "png", "jpg", "jpeg"}

# 2. 全局加载 Pipeline 实例
try:
    # 支持通过环境变量动态指定服务加载的流水线配置
    config_path = os.getenv("PIPELINE_CONFIG", "configs/pipeline_legacy_enhanced.yaml")
    logger.info(f"API Server: Loading pipeline configuration from {config_path}")
    
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    pipeline_name = os.path.basename(config_path)
    pipeline_engine = Pipeline(cfg.get("pipeline_steps", []), pipeline_name=pipeline_name)
    logger.info("API Server: Pipeline engine pre-loaded successfully.")
except Exception as e:
    logger.error(f"Failed to initialize Pipeline from {config_path}: {e}")
    raise RuntimeError("Cannot start API server without a valid pipeline configuration.")

# 3. 定义外部请求的数据契约 (Schema)
class DocumentEvent(BaseModel):
    """外部应用发过来的单条实体数据格式范例"""
    id: str
    name: Optional[str] = None
    raw_smiles: Optional[str] = None
    raw_selfies: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    raw_data: Dict[str, Any] = Field(default_factory=dict)
    _run_id: Optional[str] = PrivateAttr(default=None)

class ReviewSubmit(BaseModel):
    """人工审核回传修改过的数据"""
    action: Optional[str] = None
    raw_data: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    chunks_override: Optional[Dict[int, str]] = None # 用于单点覆盖 chunk: {0: "修好后的文本", 1: "..."}
    keep_indices: Optional[List[int]] = None
    drop_indices: Optional[List[int]] = None

class UrlIngestRequest(BaseModel):
    url: str
    id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    filename: Optional[str] = None


class KBFileIngestRequest(BaseModel):
    knowledge_base_file_guid: str
    file_guid: str


class KBFileDeleteRequest(BaseModel):
    knowledge_base_file_guid: str


def _new_run_id() -> str:
    return f"run-{uuid.uuid4().hex}"


def _ensure_run_id(event: DocumentEvent) -> str:
    if not event._run_id:
        event._run_id = _new_run_id()
    return event._run_id

def _is_allowed_filename(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

def _build_document_event(
    *,
    file_path: str,
    event_id: Optional[str] = None,
    filename: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> DocumentEvent:
    doc_id = event_id or f"upload-{uuid.uuid4().hex[:8]}"
    event_metadata = dict(metadata or {})
    if filename:
        event_metadata.setdefault("source_filename", filename)
    event = DocumentEvent(
        id=doc_id,
        metadata=event_metadata,
        raw_data={
            "file_path": file_path,
            "data_type": "document",
        },
    )
    _ensure_run_id(event)
    return event

def _create_context_from_event(event: DocumentEvent) -> Context:
    raw_data = dict(event.raw_data or {})
    raw_data["id"] = event.id
    if event.name is not None:
        raw_data["name"] = event.name
    if event.raw_smiles is not None:
        raw_data["raw_smiles"] = event.raw_smiles
    if event.raw_selfies is not None:
        raw_data["raw_selfies"] = event.raw_selfies
    ctx = Context(raw_data=raw_data)
    ctx.run_id = _ensure_run_id(event)
    ctx.pipeline_name = pipeline_name
    if event.metadata:
        ctx.metadata.update(event.metadata)
    return ctx


def _processor_order() -> List[str]:
    return [processor.__class__.__name__ for processor in pipeline_engine.processors]


def _build_processor_states(row_dict: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    ordered_states: Dict[str, Dict[str, Any]] = {}

    for proc_name in _processor_order():
        ordered_states[proc_name] = {
            "status": row_dict.get(f"{proc_name}_status", "pending") or "pending",
            "cost": row_dict.get(f"{proc_name}_cost", 0.0) or 0.0,
        }

    for key, value in row_dict.items():
        if key.endswith("_status") and key != "global_status":
            proc_name = key[:-7]
            if proc_name not in ordered_states:
                ordered_states[proc_name] = {
                    "status": value or "pending",
                    "cost": row_dict.get(f"{proc_name}_cost", 0.0) or 0.0,
                }

    return ordered_states


def _parse_run_metadata(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    raw = row_dict.get("run_metadata_json")
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _get_pipeline_status_row(run_id: str) -> Optional[Dict[str, Any]]:
    global_state_manager.ensure_management_columns(pipeline_name)
    table_name = global_state_manager._get_table_name(pipeline_name)
    with sqlite3.connect(global_state_manager.db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not cursor.fetchone():
            return None
        cursor.execute(f"SELECT * FROM {table_name} WHERE run_id = ?", (run_id,))
        row = cursor.fetchone()
        if not row:
            return None
    row_dict = dict(row)
    processor_states = _build_processor_states(row_dict)
    run_metadata = _parse_run_metadata(row_dict)
    return {
        "run_id": row_dict["run_id"],
        "doc_id": row_dict.get("doc_id"),
        "source_file": row_dict.get("source_file"),
        "global_status": row_dict.get("global_status"),
        "created_at": row_dict.get("created_at"),
        "updated_at": row_dict.get("updated_at"),
        "error_message": row_dict.get("error_message"),
        "pipeline_name": pipeline_name,
        "is_pending_review": review_store.has_pending_context(row_dict["run_id"]),
        "processor_states": processor_states,
        "run_metadata": run_metadata,
    }


def _delete_pending_review_if_exists(run_id: str) -> None:
    review_store.delete_context(run_id)

def _apply_chunk_review(ctx: Context, submission: ReviewSubmit) -> None:
    chunks = ctx.metadata.get("chunks", [])
    if not chunks:
        return

    if submission.chunks_override:
        for idx_key, new_text in submission.chunks_override.items():
            try:
                idx = int(idx_key)
            except (TypeError, ValueError):
                continue
            if 0 <= idx < len(chunks):
                chunks[idx]["text"] = new_text
                if "is_uncertain" in chunks[idx]:
                    chunks[idx]["is_uncertain"] = False

    if submission.keep_indices is not None and submission.drop_indices is not None:
        raise HTTPException(status_code=400, detail="Provide either keep_indices or drop_indices, not both.")

    if submission.keep_indices is not None:
        keep_set = set(submission.keep_indices)
        chunks = [chunk for idx, chunk in enumerate(chunks) if idx in keep_set]
    elif submission.drop_indices is not None:
        drop_set = set(submission.drop_indices)
        chunks = [chunk for idx, chunk in enumerate(chunks) if idx not in drop_set]

    ctx.metadata["chunks"] = chunks
    ctx.metadata["total_chunks"] = len(chunks)
    if not chunks:
        ctx.mark_invalid("All chunks were removed during human review.")


def _expects_milvus_insert() -> bool:
    return pipeline_expects_milvus_insert(pipeline_engine.processors)

def _resume_context(context_id: str, ctx: Context) -> dict:
    ctx.is_pending_review = False
    start_idx = ctx.paused_at_step if ctx.paused_at_step > 0 else 0
    ctx.paused_at_step = -1
    logger.info(f"Resuming pipeline for context {context_id} at step {start_idx}...")
    final_ctx = pipeline_engine.run(ctx, start_index=start_idx)
    result = _handle_pipeline_result(final_ctx)
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result)
    return result

def _list_pipeline_status_rows(limit: int = 20) -> List[Dict[str, Any]]:
    global_state_manager.ensure_management_columns(pipeline_name)
    table_name = global_state_manager._get_table_name(pipeline_name)
    with sqlite3.connect(global_state_manager.db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not cursor.fetchone():
            return []
        cursor.execute(
            f"""
            SELECT * FROM {table_name}
            WHERE COALESCE(is_hidden, 0) = 0
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cursor.fetchall()
    pending_records = review_store.find_existing_context_ids(row["run_id"] for row in rows)
    results = []
    for row in rows:
        row_dict = dict(row)
        processor_states = _build_processor_states(row_dict)
        run_metadata = _parse_run_metadata(row_dict)
        results.append({
            "run_id": row_dict["run_id"],
            "doc_id": row_dict.get("doc_id"),
            "source_file": row_dict.get("source_file"),
            "global_status": row_dict.get("global_status"),
            "created_at": row_dict.get("created_at"),
            "updated_at": row_dict.get("updated_at"),
            "error_message": row_dict.get("error_message"),
            "pipeline_name": pipeline_name,
            "is_pending_review": row_dict["run_id"] in pending_records,
            "processor_states": processor_states,
            "run_metadata": run_metadata,
        })
    return results

def _handle_pipeline_result(final_ctx: Context) -> dict:
    """内部通用函数，处理流水线跑完后的三个出口：通过、挂起待审、报错"""
    return handle_pipeline_result(
        final_ctx,
        fallback_run_id=final_ctx.run_id,
        review_store=review_store,
        state_manager=global_state_manager,
        fallback_pipeline_name=pipeline_name,
        expects_milvus_insert=_expects_milvus_insert(),
    )

def process_event_background(event: DocumentEvent):
    """【异步引擎】后台线程里慢吞吞跑"""
    ctx = _create_context_from_event(event)
         
    try:
        final_ctx = pipeline_engine.run(ctx)
        result = _handle_pipeline_result(final_ctx)
        if result["status"] == "success":
            logger.info(f"✅ Background job for run '{ctx.run_id}' completed successfully.")
        elif result["status"] == "pending_review":
            logger.info(f"⏸️ Background job for run '{ctx.run_id}' paused for manual review.")
        else:
            logger.warning(f"❌ Background job for run '{ctx.run_id}' failed. Errors: {result.get('errors')}")
    except Exception as e:
        global_state_manager.update_global_status(
            ctx.pipeline_name or pipeline_name,
            ctx.run_id,
            "failed",
            [str(e)],
        )
        logger.error(f"🔥 Catastrophic failure in background job {ctx.run_id}: {e}", exc_info=True)


def _build_task_response(task: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "task_id": task["task_id"],
        "job_type": task["job_type"],
        "knowledge_base_file_guid": task["knowledge_base_file_guid"],
        "file_guid": task.get("file_guid"),
        "status": task["status"],
        "retry_count": task["retry_count"],
        "max_retries": task["max_retries"],
        "error_message": task.get("error_message"),
        "result": task.get("result", {}),
        "created_at": task["created_at"],
        "updated_at": task["updated_at"],
    }

# ==================== 监控与可观测性 ====================
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response
from utils.metrics import PIPELINE_TOTAL, PROCESSOR_LATENCY

@app.get("/metrics")
def metrics():
    """导出 Prometheus 监控指标"""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

# ==================== 标准入库接口 ====================

@app.post("/api/v1/ingest/sync")
def ingest_document_sync(event: DocumentEvent):
    """
    【同步接口】: 一直卡住直到流水线出结果（或被挂起审核）。
    """
    with PROCESSOR_LATENCY.labels(processor_name="full_pipeline_sync").time():
        run_id = _ensure_run_id(event)
        logger.info(f"Received sync ingestion request for doc_id={event.id}, run_id={run_id}")
        ctx = _create_context_from_event(event)
            
        final_ctx = pipeline_engine.run(ctx)
        result = _handle_pipeline_result(final_ctx)
        
        # 埋点记录
        PIPELINE_TOTAL.labels(pipeline_type="preset4", status=result["status"]).inc()
        
        if result["status"] == "error":
            raise HTTPException(status_code=400, detail=result)
            
        return result

@app.post("/api/v1/ingest/async")
def ingest_document_async(event: DocumentEvent, background_tasks: BackgroundTasks):
    """
    【异步接口】: 接口秒回 OK，复杂的提取丢到后台，随后可以通过审核接口检视。
    """
    run_id = _ensure_run_id(event)
    logger.info(f"Received async ingestion request for doc_id={event.id}, run_id={run_id}")
    PIPELINE_TOTAL.labels(pipeline_type="preset4", status="accepted").inc()
    background_tasks.add_task(process_event_background, event)
    return {
        "status": "accepted",
        "message": "Pipeline processing started in background.",
        "id": run_id,
        "run_id": run_id,
        "doc_id": event.id,
    }

@app.post("/api/v1/files/upload")
async def upload_file_async(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    id: Optional[str] = Form(None),
    metadata_json: Optional[str] = Form(None),
):
    """
    上传单个文件到服务器，并以异步方式启动预设 4 管道。
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename.")
    if not _is_allowed_filename(file.filename):
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {file.filename}")

    try:
        metadata = json.loads(metadata_json) if metadata_json else {}
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid metadata_json: {exc}") from exc

    safe_name = Path(file.filename).name
    target_path = UPLOAD_DIR / safe_name
    if target_path.exists():
        target_path = UPLOAD_DIR / f"{target_path.stem}_{uuid.uuid4().hex[:8]}{target_path.suffix}"

    contents = await file.read()
    target_path.write_bytes(contents)

    event = _build_document_event(
        file_path=str(target_path.resolve()),
        event_id=id,
        filename=safe_name,
        metadata=metadata,
    )
    run_id = _ensure_run_id(event)
    background_tasks.add_task(process_event_background, event)
    return {
        "status": "accepted",
        "message": "File uploaded and pipeline processing started in background.",
        "id": run_id,
        "run_id": run_id,
        "doc_id": event.id,
        "file_path": str(target_path.resolve()),
        "filename": safe_name,
    }

@app.post("/api/v1/files/from-url")
def ingest_file_from_url(payload: UrlIngestRequest, background_tasks: BackgroundTasks):
    """
    由服务端下载远程文件后，再以异步方式启动预设 4 管道。
    """
    parsed_filename = payload.filename or os.path.basename(payload.url.split("?", 1)[0]) or f"downloaded_{uuid.uuid4().hex[:6]}.pdf"
    safe_name = Path(parsed_filename).name
    if not _is_allowed_filename(safe_name):
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {safe_name}")

    target_path = UPLOAD_DIR / safe_name
    if target_path.exists():
        target_path = UPLOAD_DIR / f"{target_path.stem}_{uuid.uuid4().hex[:8]}{target_path.suffix}"

    try:
        response = requests.get(payload.url, stream=True, timeout=30)
        response.raise_for_status()
        with open(target_path, "wb") as fh:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    fh.write(chunk)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to download file: {exc}") from exc

    event = _build_document_event(
        file_path=str(target_path.resolve()),
        event_id=payload.id,
        filename=safe_name,
        metadata=payload.metadata,
    )
    run_id = _ensure_run_id(event)
    background_tasks.add_task(process_event_background, event)
    return {
        "status": "accepted",
        "message": "File downloaded and pipeline processing started in background.",
        "id": run_id,
        "run_id": run_id,
        "doc_id": event.id,
        "file_path": str(target_path.resolve()),
        "filename": safe_name,
    }


@app.post("/api/v1/kb-files/ingest")
def enqueue_kb_file_ingest(payload: KBFileIngestRequest):
    knowledge_base_file_guid = payload.knowledge_base_file_guid.strip()
    file_guid = payload.file_guid.strip()
    if not knowledge_base_file_guid or not file_guid:
        raise HTTPException(status_code=400, detail="knowledge_base_file_guid and file_guid are required.")

    task = job_store.enqueue_job(
        job_type="ingest",
        knowledge_base_file_guid=knowledge_base_file_guid,
        file_guid=file_guid,
        payload={
            "knowledge_base_file_guid": knowledge_base_file_guid,
            "file_guid": file_guid,
        },
        max_retries=3,
    )
    accepted = task["status"] in {"pending", "running"}
    return {
        "status": "accepted" if accepted else task["status"],
        "message": "KB file ingest task accepted." if accepted else "KB file ingest task already exists.",
        "data": _build_task_response(task),
    }


@app.delete("/api/v1/kb-files/{knowledge_base_file_guid}")
def enqueue_kb_file_delete(knowledge_base_file_guid: str):
    normalized_guid = knowledge_base_file_guid.strip()
    if not normalized_guid:
        raise HTTPException(status_code=400, detail="knowledge_base_file_guid is required.")

    task = job_store.enqueue_job(
        job_type="delete",
        knowledge_base_file_guid=normalized_guid,
        file_guid=None,
        payload={"knowledge_base_file_guid": normalized_guid},
        max_retries=0,
    )
    if task["status"] == "success":
        return {
            "status": "success",
            "message": "KB file already deleted.",
            "data": _build_task_response(task),
        }

    accepted = task["status"] in {"pending", "running"}
    return {
        "status": "accepted" if accepted else task["status"],
        "message": "KB file delete task accepted." if accepted else "KB file delete task already exists.",
        "data": _build_task_response(task),
    }


@app.get("/api/v1/tasks")
def list_job_tasks(
    status: Optional[str] = None,
    knowledge_base_file_guid: Optional[str] = None,
    limit: int = 50,
):
    normalized_guid = knowledge_base_file_guid.strip() if knowledge_base_file_guid else None
    tasks = job_store.list_tasks(
        status=status,
        knowledge_base_file_guid=normalized_guid,
        limit=limit,
    )
    return {
        "status": "success",
        "count": len(tasks),
        "data": [_build_task_response(task) for task in tasks],
    }


@app.get("/api/v1/tasks/{task_id}")
def get_job_task(task_id: str):
    task = job_store.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found.")
    return {
        "status": "success",
        "data": _build_task_response(task),
    }

@app.get("/api/v1/runs")
def list_runs(limit: int = 20):
    """
    获取当前预设最近的任务状态列表。
    """
    limit = max(1, min(limit, 100))
    return {
        "status": "success",
        "count": len(rows),
        "data": rows,
    } if (rows := _list_pipeline_status_rows(limit)) is not None else {
        "status": "success",
        "count": 0,
        "data": [],
    }

@app.get("/api/v1/runs/{run_id}")
def get_run_status(run_id: str):
    """
    获取单个任务的详细状态。
    """
    row = _get_pipeline_status_row(run_id)
    if not row:
        raise HTTPException(status_code=404, detail="Run not found.")
    return {
        "status": "success",
        "data": row,
    }


@app.delete("/api/v1/runs/{run_id}")
def delete_run(run_id: str):
    """
    删除单个任务状态记录；若任务仍在待审核队列，也一并移除。
    """
    global_state_manager.ensure_management_columns(pipeline_name)
    deleted_state = global_state_manager.delete_run(pipeline_name, run_id)
    deleted_pending = review_store.delete_context(run_id)
    if not deleted_state and not deleted_pending:
        raise HTTPException(status_code=404, detail="Run not found.")
    return {
        "status": "success",
        "message": "Run deleted.",
        "run_id": run_id,
    }


@app.post("/api/v1/runs/actions/delete-failed")
def delete_failed_runs():
    """
    删除所有失败任务。
    """
    global_state_manager.ensure_management_columns(pipeline_name)
    deleted = global_state_manager.delete_runs_by_status(pipeline_name, "failed")
    return {
        "status": "success",
        "message": "Failed runs deleted.",
        "deleted_count": deleted,
    }


@app.post("/api/v1/runs/actions/hide-completed")
def hide_completed_runs():
    """
    将所有已完成任务标记为隐藏，后续不再出现在默认列表中。
    """
    global_state_manager.ensure_management_columns(pipeline_name)
    hidden = global_state_manager.hide_runs_by_status(pipeline_name, "completed")
    return {
        "status": "success",
        "message": "Completed runs hidden.",
        "hidden_count": hidden,
    }

# ==================== API 人工审核专用接口 ====================

@app.post("/api/v1/review/chunks/{context_id}")
def submit_chunk_review_sync(context_id: str, submission: ReviewSubmit):
    """
    【切片级审核专用接口】: 针对被 `ChunkLevelReviewPause` 拦下的特定不确定片段进行人工重写覆盖。
    submission.chunks_override 示例: {"12": "这句人工优化过了", "45": "另一个句子"}
    提交后自动清除这些片段的 is_uncertain 标志，如果所有片段都已审毕，则放通流水线往下执行。
    """
    logger.info(f"Received chunk-level review override for id: {context_id}")
    
    ctx = review_store.get_context(context_id)
    if not ctx:
        raise HTTPException(status_code=404, detail="Context not found or already submitted/deleted in DB.")
        
    if not submission.chunks_override:
         raise HTTPException(status_code=400, detail="Missing chunks_override data in payload.")
         
    chunks = ctx.metadata.get("chunks", [])
    
    # 覆盖人工提供的所有文本
    for idx_str, new_text in submission.chunks_override.items():
        try:
            idx = int(idx_str)
            if 0 <= idx < len(chunks):
                chunks[idx]["text"] = new_text
                chunks[idx]["is_uncertain"] = False # 已解除威胁
        except ValueError:
            pass
            
    # 复查：是否还有剩余的 “不确定切片” 没被覆盖？
    remaining_uncertain = [i for i, c in enumerate(chunks) if c.get("is_uncertain", False)]
    if remaining_uncertain:
        # 还有未解决的疑虑切片，重新冻结它
        logger.info(f"Pending chunks remaining {remaining_uncertain}. Re-freezing context {context_id}.")
        review_store.save_pending_context(context_id, ctx)
        identifiers = build_result_identifiers(ctx, context_id)
        return {
            "status": "pending_review",
            **identifiers,
            "message": f"Updated chunks. However, there are still remaining uncertain chunks: {remaining_uncertain}",
        }
    
    logger.info("All uncertain chunks resolved! Proceeding with pipeline embedder layer...")
    review_store.save_pending_context(context_id, ctx)
    result = _resume_context(context_id, ctx)
    if result["status"] != "pending_review":
        review_store.delete_context(context_id)
    return result

@app.get("/api/v1/review/pending")
def get_pending_reviews(limit: int = 50):
    """
    【审核接口 1】获取队列中所有被 "HumanReviewPause" 锁住等待人工审核的数据。
    """
    records = review_store.get_pending_records(limit=limit)
    return {
        "status": "success",
        "count": len(records),
        "data": records
    }

@app.post("/api/v1/review/submit/{context_id}")
def submit_review_sync(context_id: str, submission: ReviewSubmit):
    """
    【审核接口 2】人员完成审核/修改后，将数据合并回原 Context，并恢复后半段 Pipeline 执行。
    """
    logger.info(f"Received review submission for id: {context_id}")
    # 1. 把压在 SQLite 里的原始 Context 挖出来
    ctx = review_store.get_context(context_id)
    if not ctx:
        raise HTTPException(status_code=404, detail="Context not found or already submitted/deleted.")
        
    if submission.action in {"reject", "reject_all"}:
        global_state_manager.update_global_status(ctx.pipeline_name or pipeline_name, ctx.run_id or context_id, "rejected")
        review_store.delete_context(context_id)
        return {
            "status": "rejected",
            **build_result_identifiers(ctx, context_id),
            "message": "Run rejected and will not continue.",
        }

    # 2. 合并人工订正产生的新数据修改
    if submission.raw_data:
        # 如果 raw_data 是字典，选择性更新
        if isinstance(ctx.raw_data, dict):
            ctx.raw_data.update(submission.raw_data)
        else:
            ctx.raw_data = submission.raw_data
    if submission.metadata:
        ctx.metadata.update(submission.metadata)
    _apply_chunk_review(ctx, submission)
    if not ctx.is_valid:
        review_store.save_pending_context(context_id, ctx)
        raise HTTPException(
            status_code=400,
            detail={
                "status": "error",
                **build_result_identifiers(ctx, context_id),
                "errors": ctx.errors,
                "metadata_snapshot": ctx.metadata,
            },
        )
    review_store.save_pending_context(context_id, ctx)
    result = _resume_context(context_id, ctx)
    if result["status"] != "pending_review":
        review_store.delete_context(context_id)
    return result

@app.post("/api/v1/review/reject/{context_id}")
def reject_review(context_id: str):
    """
    直接驳回整条挂起任务，不再继续执行后续步骤。
    """
    ctx = review_store.get_context(context_id)
    if not ctx:
        raise HTTPException(status_code=404, detail="Context not found or already submitted/deleted.")
    global_state_manager.update_global_status(ctx.pipeline_name or pipeline_name, ctx.run_id or context_id, "rejected")
    review_store.delete_context(context_id)
    return {
        "status": "rejected",
        **build_result_identifiers(ctx, context_id),
        "message": "Run rejected and removed from pending review queue.",
    }

@app.get("/health")
def health_check():
    """检测服务存活状态"""
    return {"status": "ok", "service": "realtime_rag_pipeline API Gateway"}

if __name__ == "__main__":
    import uvicorn
    # 本地开发测试启动命令：python streaming/api_server.py
    logger.info("Starting Uvicorn Server on http://0.0.0.0:8000")
    uvicorn.run("api_server:app", host="0.0.0.0", port=8000, reload=True)
