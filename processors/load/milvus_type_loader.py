# -*- coding: utf-8 -*-
import json
import uuid
import logging
from typing import Dict, Any, List, Tuple

from core.context import Context
from core.registry import ProcessorRegistry
from processors.base import BaseProcessor

try:
    from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection, utility
    PYMILVUS_AVAILABLE = True
    PYMILVUS_IMPORT_ERROR = ""
except ImportError as e:
    connections = FieldSchema = CollectionSchema = DataType = Collection = utility = None
    PYMILVUS_AVAILABLE = False
    PYMILVUS_IMPORT_ERROR = str(e)
    logging.warning(f"PyMilvus is not installed. MilvusTypeLoader will crash if triggered: {e}")

logger = logging.getLogger("MilvusTypeLoader")

# 硬性规定参数
MAX_TEXT_LEN = 32768
MAX_TYPE_LEN = 256

@ProcessorRegistry.register("MilvusTypeLoader")
class MilvusTypeLoader(BaseProcessor):
    """
    将带有 Embedding 的 Chunks 数组落盘至 Milvus 的专门加载器。
    满足复杂的表结构要求： pk, id_1, type, text, embedding。
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.host = self.config.get("host", "127.0.0.1")
        self.port = str(self.config.get("port", "19530"))
        self.collection_name = self.config.get("collection", "default_docs")
        self.batch_size = self.config.get("batch_size", 500)
        self.connection_alias = self.config.get("connection_alias", "default")
        self.verify_query_limit = int(self.config.get("verify_query_limit", 5))
        
        # 动态检测连接是否可用，只警告不立刻抛出（因为本组件是在导入时注册的，防止应用主进程挂掉）
        self._connect_milvus()
        self.collection_instance = None
        
    def _connect_milvus(self):
        if not PYMILVUS_AVAILABLE:
            logger.error("❌ PyMilvus unavailable: %s", PYMILVUS_IMPORT_ERROR)
            return False
        try:
            # Pymilvus 的全局连接池注册
            connections.connect(alias=self.connection_alias, host=self.host, port=self.port)
            logger.info(
                "✅ Connected to Milvus Service %s:%s (alias=%s)",
                self.host,
                self.port,
                self.connection_alias,
            )
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Milvus: {e}")
            return False
            
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

        sample_ids = primary_keys[: self.verify_query_limit]
        expr = f"id in {json.dumps(sample_ids, ensure_ascii=False)}"
        rows = collection.query(expr=expr, output_fields=["id"])
        found_ids = {str(row.get("id")) for row in rows}
        missing_ids = [pk for pk in sample_ids if pk not in found_ids]
        if missing_ids:
            logger.error(
                "❌ Milvus sample verification failed for collection=%s missing_ids=%s",
                self.collection_name,
                missing_ids,
            )
            return False
        return True

    def _prepare_collection(self, embedding_dim: int):
        """
        初始化集合，若不存在则创建。Schema 匹配现有 test_wuji 的 7 字段结构。
        """
        if self.collection_instance:
             return self.collection_instance
             
        if not utility.has_collection(self.collection_name, using=self.connection_alias):
            logger.info(f"🆕 Creating Milvus Collection: {self.collection_name} (dim: {embedding_dim})")
            fields = [
                FieldSchema(name="id", dtype=DataType.VARCHAR, max_length=100, is_primary=True),
                FieldSchema(name="source", dtype=DataType.VARCHAR, max_length=500),
                FieldSchema(name="header_path", dtype=DataType.VARCHAR, max_length=200),
                FieldSchema(name="data_type", dtype=DataType.VARCHAR, max_length=50),
                FieldSchema(name="chunk_length", dtype=DataType.INT64),
                FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=8000),
                FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=embedding_dim)
            ]
            schema = CollectionSchema(fields=fields, description="Realtime Pipeline Chunks")
            collection = Collection(name=self.collection_name, schema=schema, using=self.connection_alias)

            collection.create_index(
                field_name="embedding", 
                index_params={
                    "index_type": "HNSW",
                    "metric_type": "COSINE",
                    "params": {"M": 16, "efConstruction": 200}
                }
            )
            logger.info("✅ Collection and indexes created.")
            self.collection_instance = collection
        else:
            logger.info(f"📦 Using existing Collection: {self.collection_name}")
            self.collection_instance = Collection(self.collection_name, using=self.connection_alias)
            
        self.collection_instance.load()
        return self.collection_instance

    def process(self, context: Context) -> Context:
        if not PYMILVUS_AVAILABLE:
            context.mark_invalid(f"PyMilvus dependency unavailable: {PYMILVUS_IMPORT_ERROR}")
            return context

        try:
            if not connections.has_connection(self.connection_alias):
               if not self._connect_milvus():
                   context.mark_invalid(f"Failed to connect to Milvus {self.host}:{self.port}.")
                   return context
               
            if not connections.has_connection(self.connection_alias):
               logger.warning("No active connection to Milvus, cannot insert chunks.")
               context.mark_invalid("No active connection to Milvus.")
               return context
        except Exception as e:
            logger.warning(f"Milvus connection check failed: {e}.")
            context.mark_invalid(f"Milvus connection check failed: {e}")
            return context

        chunks = context.metadata.get('chunks', [])
        
        if not chunks:
            logger.warning("No chunks to insert. Milvus push aborted.")
            context.mark_invalid("No chunks available for Milvus insertion.")
            return context
        context.metadata["milvus_expected_insert_count"] = len(chunks)
            
        first_emb = chunks[0].get('embedding')
        if not first_emb:
            context.mark_invalid("Chunks missing 'embedding' field. Check YuanEmbedderProcessor.")
            return context
        
        emb_dim = len(first_emb)
        try:
            collection = self._prepare_collection(emb_dim)
        except Exception as e:
            context.mark_invalid(f"Failed to prepare Milvus collection: {e}")
            logger.error("Milvus collection preparation failed.", exc_info=True)
            return context
        
        logger.info(f"📥 Flushing {len(chunks)} fragments to Milvus...")
        
        # 匹配现有 Schema 的 7 个字段
        col_ids = []
        col_sources = []
        col_header_paths = []
        col_data_types = []
        col_chunk_lengths = []
        col_texts = []
        col_embeddings = []
        
        inserted_count = 0
        inserted_primary_keys: List[str] = []
        source_name = context.metadata.get("source_filename", "unknown")
        
        for i, chunk in enumerate(chunks, 1):
             text = chunk.get('text', '')
             if not text:
                  continue
                  
             safe_text = text[:8000]
             meta = chunk.get('metadata', {}) or {}
             header_path = meta.get("header_path", "")[:200]
             data_type = meta.get("type", "document_ai_chunk")[:50]
             
             # 生成唯一 ID: source_chunkIndex_uuid (防止主键冲突)
             chunk_id = f"{source_name}_{i}_{uuid.uuid4().hex[:8]}"[:100]
             
             col_ids.append(chunk_id)
             col_sources.append(str(source_name)[:500])
             col_header_paths.append(header_path)
             col_data_types.append(data_type)
             col_chunk_lengths.append(len(text))
             col_texts.append(safe_text)
             col_embeddings.append(chunk.get('embedding'))
             
             if len(col_texts) >= self.batch_size:
                 try:
                     result = collection.insert([col_ids, col_sources, col_header_paths, col_data_types, col_chunk_lengths, col_texts, col_embeddings])
                 except Exception as e:
                     context.mark_invalid(f"Milvus batch insert failed: {e}")
                     logger.error("Milvus batch insert failed.", exc_info=True)
                     return context
                 ack_count, primary_keys = self._extract_insert_result(result)
                 if ack_count <= 0:
                     context.mark_invalid("Milvus batch insert returned 0 acknowledged rows.")
                     logger.error("Milvus batch insert returned 0 acknowledged rows.")
                     return context
                 inserted_count += ack_count
                 inserted_primary_keys.extend(primary_keys)
                 col_ids.clear(); col_sources.clear(); col_header_paths.clear()
                 col_data_types.clear(); col_chunk_lengths.clear()
                 col_texts.clear(); col_embeddings.clear()
                  
        # 扫尾剩余
        if col_texts:
            try:
                result = collection.insert([col_ids, col_sources, col_header_paths, col_data_types, col_chunk_lengths, col_texts, col_embeddings])
            except Exception as e:
                context.mark_invalid(f"Milvus insert failed: {e}")
                logger.error("Milvus final insert failed.", exc_info=True)
                return context
            ack_count, primary_keys = self._extract_insert_result(result)
            if ack_count <= 0:
                context.mark_invalid("Milvus insert returned 0 acknowledged rows.")
                logger.error("Milvus final insert returned 0 acknowledged rows.")
                return context
            inserted_count += ack_count
            inserted_primary_keys.extend(primary_keys)
            
        try:
            collection.flush()
        except Exception as e:
            context.mark_invalid(f"Milvus flush failed: {e}")
            logger.error("Milvus flush failed.", exc_info=True)
            return context

        if inserted_count <= 0:
            context.mark_invalid("Milvus insertion produced 0 rows.")
            return context

        if inserted_primary_keys and not self._verify_inserted_sample(collection, inserted_primary_keys):
            context.mark_invalid("Milvus insert verification failed after flush.")
            return context

        logger.info(
            "🎉 Success! Inserted %s vector rows to Milvus collection=%s target=%s:%s",
            inserted_count,
            self.collection_name,
            self.host,
            self.port,
        )
        
        context.metadata["milvus_inserted_count"] = inserted_count
        context.metadata["milvus_collection"] = self.collection_name
        context.metadata["milvus_host"] = self.host
        context.metadata["milvus_port"] = self.port
        if inserted_primary_keys:
            context.metadata["milvus_sample_primary_keys"] = inserted_primary_keys[: self.verify_query_limit]
        return context
