# -*- coding: utf-8 -*-
import json
import logging
import uuid
from typing import Any, Dict, List, Tuple

from core.context import Context
from core.registry import registry
from processors.base import BaseProcessor

try:
    from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection, utility
    PYMILVUS_AVAILABLE = True
    PYMILVUS_IMPORT_ERROR = ""
except ImportError as e:
    connections = FieldSchema = CollectionSchema = DataType = Collection = utility = None
    PYMILVUS_AVAILABLE = False
    PYMILVUS_IMPORT_ERROR = str(e)
    logging.warning(f"PyMilvus is not installed. MilvusPersonalLoader will fail if used: {e}")


logger = logging.getLogger("MilvusPersonalLoader")

MAX_TEXT_LEN = 32768
MAX_TYPE_LEN = 256
MAX_DOCUMENT_ID_LEN = 256


@registry.register("MilvusPersonalLoader")
class MilvusPersonalLoader(BaseProcessor):
    """
    预设5专用 Milvus Loader。
    Schema:
      pk(INT64, auto_id, primary)
      id_1(INT64)
      type(VARCHAR)
      text(VARCHAR)
      metadata(JSON)
      document_id(VARCHAR)
      embedding(FLOAT_VECTOR)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.host = self.config.get("host", "127.0.0.1")
        self.port = str(self.config.get("port", "19530"))
        self.collection_name = self.config.get("collection", "knowledge_personal_test")
        self.batch_size = int(self.config.get("batch_size", 500))
        self.connection_alias = self.config.get("connection_alias", "default")
        self.verify_query_limit = int(self.config.get("verify_query_limit", 5))
        self.default_type = self.config.get("default_type", "document_chunk")
        self.document_id_metadata_key = self.config.get("document_id_metadata_key", "document_id")
        self.document_id_raw_key = self.config.get("document_id_raw_key", "document_id")
        self.collection_instance = None
        self._connect_milvus()

    def _connect_milvus(self) -> bool:
        if not PYMILVUS_AVAILABLE:
            logger.error("❌ PyMilvus unavailable: %s", PYMILVUS_IMPORT_ERROR)
            return False
        try:
            connections.connect(alias=self.connection_alias, host=self.host, port=self.port)
            logger.info(
                "✅ Connected to Milvus Service %s:%s (alias=%s)",
                self.host,
                self.port,
                self.connection_alias,
            )
            return True
        except Exception as exc:
            logger.error("❌ Failed to connect to Milvus: %s", exc)
            return False

    def _prepare_collection(self, embedding_dim: int) -> Any:
        if self.collection_instance is not None:
            try:
                if utility.has_collection(self.collection_name, using=self.connection_alias):
                    return self.collection_instance
                logger.warning(
                    "Cached Milvus collection instance became stale for %s. Recreating.",
                    self.collection_name,
                )
                self.collection_instance = None
            except Exception as exc:
                logger.warning(
                    "Failed to validate cached Milvus collection %s: %s. Recreating.",
                    self.collection_name,
                    exc,
                )
                self.collection_instance = None

        if not utility.has_collection(self.collection_name, using=self.connection_alias):
            logger.info("🆕 Creating Milvus Collection: %s (dim=%s)", self.collection_name, embedding_dim)
            fields = [
                FieldSchema(name="pk", dtype=DataType.INT64, is_primary=True, auto_id=True),
                FieldSchema(name="id_1", dtype=DataType.INT64),
                FieldSchema(name="type", dtype=DataType.VARCHAR, max_length=MAX_TYPE_LEN),
                FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=MAX_TEXT_LEN),
                FieldSchema(name="metadata", dtype=DataType.JSON),
                FieldSchema(name="document_id", dtype=DataType.VARCHAR, max_length=MAX_DOCUMENT_ID_LEN),
                FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=embedding_dim),
            ]
            schema = CollectionSchema(fields=fields, description="Preset5 personal knowledge collection")
            collection = Collection(name=self.collection_name, schema=schema, using=self.connection_alias)
            collection.create_index(
                field_name="embedding",
                index_params={
                    "index_type": "HNSW",
                    "metric_type": "COSINE",
                    "params": {"M": 16, "efConstruction": 200},
                },
            )
            self.collection_instance = collection
        else:
            logger.info("📦 Using existing Collection: %s", self.collection_name)
            self.collection_instance = Collection(self.collection_name, using=self.connection_alias)

        self.collection_instance.load()
        return self.collection_instance

    def _extract_insert_result(self, result: Any) -> Tuple[int, List[str]]:
        acknowledged = 0
        primary_keys: List[str] = []

        insert_count = getattr(result, "insert_count", None)
        if isinstance(insert_count, int):
            acknowledged = insert_count

        raw_primary_keys = getattr(result, "primary_keys", None)
        if raw_primary_keys:
            primary_keys = [str(key) for key in raw_primary_keys]
            if acknowledged <= 0:
                acknowledged = len(primary_keys)

        return acknowledged, primary_keys

    def _verify_inserted_sample(self, collection: Any, primary_keys: List[str]) -> bool:
        if not primary_keys:
            return False
        sample_pks = [int(pk) for pk in primary_keys[: self.verify_query_limit]]
        expr = f"pk in {json.dumps(sample_pks, ensure_ascii=False)}"
        rows = collection.query(expr=expr, output_fields=["pk"])
        found = {int(row.get("pk")) for row in rows if row.get("pk") is not None}
        return all(pk in found for pk in sample_pks)

    def _resolve_document_id(self, context: Context) -> str:
        metadata_value = context.metadata.get(self.document_id_metadata_key)
        if metadata_value:
            return str(metadata_value)[:MAX_DOCUMENT_ID_LEN]

        if isinstance(context.raw_data, dict):
            raw_value = context.raw_data.get(self.document_id_raw_key)
            if raw_value:
                return str(raw_value)[:MAX_DOCUMENT_ID_LEN]

        generated = str(uuid.uuid4())
        context.metadata[self.document_id_metadata_key] = generated
        return generated

    def _build_chunk_metadata(self, context: Context, chunk: Dict[str, Any], document_id: str) -> Dict[str, Any]:
        chunk_meta = dict(chunk.get("metadata") or {})
        chunk_meta.setdefault("chunk_id", chunk.get("chunk_id", ""))
        chunk_meta["source_file"] = context.metadata.get("source_file") or ""
        chunk_meta["stored_filename"] = (
            context.metadata.get("stored_filename")
            or context.metadata.get("source_filename")
            or ""
        )
        chunk_meta.setdefault("run_id", context.run_id or "")
        chunk_meta.setdefault("doc_id", context.raw_data.get("id") if isinstance(context.raw_data, dict) else "")
        chunk_meta.setdefault("document_id", document_id)
        return chunk_meta

    def _generate_id_1(self) -> int:
        return uuid.uuid4().int & ((1 << 63) - 1)

    def process(self, context: Context) -> Context:
        if not PYMILVUS_AVAILABLE:
            context.mark_invalid(f"PyMilvus dependency unavailable: {PYMILVUS_IMPORT_ERROR}")
            return context

        if not connections.has_connection(self.connection_alias) and not self._connect_milvus():
            context.mark_invalid(f"Failed to connect to Milvus {self.host}:{self.port}.")
            return context

        chunks = context.metadata.get("chunks", [])
        if not chunks:
            context.mark_invalid("No chunks available for Milvus insertion.")
            return context

        first_emb = chunks[0].get("embedding")
        if not first_emb:
            context.mark_invalid("Chunks missing 'embedding' field. Check YuanEmbedderProcessor.")
            return context

        document_id = self._resolve_document_id(context)
        context.metadata["milvus_expected_insert_count"] = len(chunks)
        context.metadata["document_id"] = document_id

        try:
            collection = self._prepare_collection(len(first_emb))
        except Exception as exc:
            context.mark_invalid(f"Failed to prepare Milvus collection: {exc}")
            logger.error("Milvus collection preparation failed.", exc_info=True)
            return context

        col_id_1: List[int] = []
        col_types: List[str] = []
        col_texts: List[str] = []
        col_metadatas: List[Dict[str, Any]] = []
        col_document_ids: List[str] = []
        col_embeddings: List[List[float]] = []
        inserted_count = 0
        inserted_primary_keys: List[str] = []

        for chunk in chunks:
            text = (chunk.get("text") or "")[:MAX_TEXT_LEN]
            if not text:
                continue

            chunk_metadata = self._build_chunk_metadata(context, chunk, document_id)
            col_id_1.append(self._generate_id_1())
            col_types.append(str(chunk_metadata.get("type", self.default_type))[:MAX_TYPE_LEN])
            col_texts.append(text)
            col_metadatas.append(chunk_metadata)
            col_document_ids.append(document_id)
            col_embeddings.append(chunk.get("embedding"))

            if len(col_texts) >= self.batch_size:
                result = collection.insert([col_id_1, col_types, col_texts, col_metadatas, col_document_ids, col_embeddings])
                ack_count, primary_keys = self._extract_insert_result(result)
                if ack_count <= 0:
                    context.mark_invalid("Milvus batch insert returned 0 acknowledged rows.")
                    return context
                inserted_count += ack_count
                inserted_primary_keys.extend(primary_keys)
                col_id_1.clear()
                col_types.clear()
                col_texts.clear()
                col_metadatas.clear()
                col_document_ids.clear()
                col_embeddings.clear()

        if col_texts:
            result = collection.insert([col_id_1, col_types, col_texts, col_metadatas, col_document_ids, col_embeddings])
            ack_count, primary_keys = self._extract_insert_result(result)
            if ack_count <= 0:
                context.mark_invalid("Milvus insert returned 0 acknowledged rows.")
                return context
            inserted_count += ack_count
            inserted_primary_keys.extend(primary_keys)

        try:
            collection.flush()
        except Exception as exc:
            context.mark_invalid(f"Milvus flush failed: {exc}")
            logger.error("Milvus flush failed.", exc_info=True)
            return context

        if inserted_primary_keys and not self._verify_inserted_sample(collection, inserted_primary_keys):
            context.mark_invalid("Milvus insert verification failed after flush.")
            return context

        context.metadata["milvus_inserted_count"] = inserted_count
        context.metadata["milvus_collection"] = self.collection_name
        context.metadata["milvus_host"] = self.host
        context.metadata["milvus_port"] = self.port
        context.metadata["milvus_document_id"] = document_id
        context.metadata["milvus_sample_primary_keys"] = inserted_primary_keys[: self.verify_query_limit]
        logger.info(
            "🎉 Inserted %s rows to Milvus collection=%s document_id=%s",
            inserted_count,
            self.collection_name,
            document_id,
        )
        return context
