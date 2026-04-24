# -*- coding: utf-8 -*-
import os
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from core.context import Context
from core.pipeline import Pipeline
from core.review_store import ReviewStore
from core.run_ops import expects_milvus_insert as pipeline_expects_milvus_insert, handle_pipeline_result
from core.state_manager import global_state_manager
from utils.logger import setup_logger


logger = setup_logger("IngestPipelineService")


class IngestPipelineService:
    def __init__(self, config_path: Optional[str] = None, review_db_path: str = "logs/pending_reviews.db"):
        self.config_path = config_path or os.getenv("PIPELINE_CONFIG", "configs/pipeline_legacy_enhanced.yaml")
        with open(self.config_path, "r", encoding="utf-8") as file_handle:
            cfg = yaml.safe_load(file_handle)
        self.pipeline_name = os.path.basename(self.config_path)
        self.pipeline = Pipeline(cfg.get("pipeline_steps", []), pipeline_name=self.pipeline_name)
        self.review_store = ReviewStore(db_path=review_db_path)

    def ingest_downloaded_file(
        self,
        *,
        local_path: str,
        knowledge_base_file_guid: str,
        file_guid: str,
        stored_filename: Optional[str] = None,
        source_file: Optional[str] = None,
    ) -> Dict[str, Any]:
        run_id = f"run-{uuid.uuid4().hex}"
        resolved_path = str(Path(local_path).resolve())
        ctx = Context(
            raw_data={
                "id": knowledge_base_file_guid,
                "file_path": resolved_path,
                "data_type": "document",
                "document_id": knowledge_base_file_guid,
            }
        )
        ctx.run_id = run_id
        ctx.pipeline_name = self.pipeline_name
        ctx.metadata.update(
            {
                "document_id": knowledge_base_file_guid,
                "knowledge_base_file_guid": knowledge_base_file_guid,
                "file_guid": file_guid,
                "source_file": source_file or "",
                "stored_filename": stored_filename or Path(local_path).name,
                # Keep this runtime alias for existing processors that still read source_filename.
                "source_filename": source_file or stored_filename or Path(local_path).name,
            }
        )

        logger.info(
            "Starting pipeline ingestion run_id=%s knowledge_base_file_guid=%s file_guid=%s path=%s",
            run_id,
            knowledge_base_file_guid,
            file_guid,
            resolved_path,
        )
        final_ctx = self.pipeline.run(ctx)
        result = handle_pipeline_result(
            final_ctx,
            fallback_run_id=run_id,
            review_store=self.review_store,
            state_manager=global_state_manager,
            fallback_pipeline_name=self.pipeline_name,
            expects_milvus_insert=pipeline_expects_milvus_insert(self.pipeline.processors),
        )
        logger.info(
            "Finished pipeline ingestion run_id=%s knowledge_base_file_guid=%s status=%s",
            run_id,
            knowledge_base_file_guid,
            result["status"],
        )
        return result
