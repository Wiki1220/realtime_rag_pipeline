# -*- coding: utf-8 -*-
from typing import Any, Dict, Iterable

from core.context import Context
from core.review_store import ReviewStore
from core.state_manager import StateManager


MILVUS_LOADER_CLASS_NAMES = {
    "MilvusLoader",
    "MilvusTypeLoader",
    "MilvusPersonalLoader",
}


def expects_milvus_insert(processors: Iterable[Any]) -> bool:
    return any(
        processor.__class__.__name__ in MILVUS_LOADER_CLASS_NAMES
        for processor in processors
    )


def build_result_identifiers(context: Context, fallback_run_id: str) -> Dict[str, str]:
    run_id = context.run_id or fallback_run_id
    doc_id = run_id
    if isinstance(context.raw_data, dict) and context.raw_data.get("id") is not None:
        doc_id = str(context.raw_data["id"])
    return {
        "id": run_id,
        "run_id": run_id,
        "doc_id": doc_id,
    }


def handle_pipeline_result(
    final_ctx: Context,
    *,
    fallback_run_id: str,
    review_store: ReviewStore,
    state_manager: StateManager,
    fallback_pipeline_name: str,
    expects_milvus_insert: bool,
) -> Dict[str, Any]:
    identifiers = build_result_identifiers(final_ctx, fallback_run_id)
    run_id = identifiers["run_id"]

    if not final_ctx.is_valid:
        return {
            "status": "error",
            **identifiers,
            "errors": final_ctx.errors,
            "metadata_snapshot": final_ctx.metadata,
        }

    if getattr(final_ctx, "is_pending_review", False):
        review_store.save_pending_context(run_id, final_ctx)
        return {
            "status": "pending_review",
            **identifiers,
            "message": "Data requires human review. Saved to pending queue.",
        }

    if expects_milvus_insert and final_ctx.metadata.get("milvus_inserted_count", 0) <= 0:
        final_ctx.mark_invalid("Pipeline finished without a successful Milvus insertion.")
        state_manager.update_global_status(
            final_ctx.pipeline_name or fallback_pipeline_name,
            run_id,
            "failed",
            final_ctx.errors,
        )
        return {
            "status": "error",
            **identifiers,
            "errors": final_ctx.errors,
            "metadata_snapshot": final_ctx.metadata,
        }

    return {
        "status": "success",
        **identifiers,
        "message": "Data processed and stored successfully.",
        "metadata_snapshot": final_ctx.metadata,
    }
