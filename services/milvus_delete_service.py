# -*- coding: utf-8 -*-
import os
from typing import Any, Dict

from utils.logger import setup_logger

try:
    from pymilvus import Collection, connections
except ImportError:  # pragma: no cover - exercised in runtime only
    Collection = None
    connections = None


logger = setup_logger("MilvusDeleteService")


class MilvusDeleteService:
    def __init__(
        self,
        *,
        host: str = os.getenv("MILVUS_HOST", "47.98.227.81"),
        port: str = os.getenv("MILVUS_PORT", "8050"),
        collection_name: str = os.getenv("MILVUS_COLLECTION", "knowledge_personal_test"),
        connection_alias: str = "kb-delete",
    ):
        self.host = host
        self.port = port
        self.collection_name = collection_name
        self.connection_alias = connection_alias

    def delete_document(self, knowledge_base_file_guid: str) -> Dict[str, Any]:
        if Collection is None or connections is None:
            raise RuntimeError("pymilvus is not installed.")

        connections.connect(alias=self.connection_alias, host=self.host, port=self.port)
        collection = Collection(self.collection_name, using=self.connection_alias)
        collection.load()
        expr = f'document_id == "{knowledge_base_file_guid}"'
        before_count = len(collection.query(expr=expr, output_fields=["document_id"], limit=16384))
        delete_result = collection.delete(expr=expr)
        collection.flush()

        deleted_count = 0
        delete_count = getattr(delete_result, "delete_count", None)
        if isinstance(delete_count, int):
            deleted_count = delete_count
        elif before_count:
            deleted_count = before_count

        logger.info(
            "Deleted Milvus document knowledge_base_file_guid=%s collection=%s deleted_count=%s",
            knowledge_base_file_guid,
            self.collection_name,
            deleted_count,
        )
        return {
            "knowledge_base_file_guid": knowledge_base_file_guid,
            "milvus_collection": self.collection_name,
            "deleted_count": deleted_count,
        }
