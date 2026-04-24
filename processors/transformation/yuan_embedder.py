# -*- coding: utf-8 -*-
import logging
import threading
from typing import Dict, Any

from core.context import Context
from core.registry import ProcessorRegistry
from processors.base import BaseProcessor

# 运行时需要 sentence_transformers
try:
    import torch
    from sentence_transformers import SentenceTransformer
    EMBEDDER_IMPORT_ERROR = ""
except ImportError as e:
    torch = None
    SentenceTransformer = None
    EMBEDDER_IMPORT_ERROR = str(e)
    logging.warning(f"Failed to import sentence-transformers or torch: {e}")

logger = logging.getLogger("YuanEmbedderProcessor")

@ProcessorRegistry.register("YuanEmbedderProcessor")
class YuanEmbedderProcessor(BaseProcessor):
    """
    接收来自语意切分器的 Chunk 列表，通过 SentenceTransformer
    批量将其进行文本编码。专门针对 `Yuan_embedding` 等模型优化。
    """
    
    _model_instance = None
    _lock = threading.Lock()
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.model_path = self.config.get("model_path", "/data/zhenrong/model/Yuan_embedding")
        self.batch_size = self.config.get("batch_size", 64)
        self.max_length = self.config.get("max_length", 512)
        
        # 为了兼容多种环境，提取指定的 device 兜底
        device_str = self.config.get("device", "")
        if device_str:
             self.device = device_str
        elif torch is not None:
             self.device = "cuda" if torch.cuda.is_available() else "cpu"
        else:
             self.device = "cpu"
             
        self._lazy_load_model()
        
    def _lazy_load_model(self):
        with YuanEmbedderProcessor._lock:
            if SentenceTransformer is None or torch is None:
                 logger.error("❌ Yuan embedding dependencies unavailable: %s", EMBEDDER_IMPORT_ERROR)
                 return
            if YuanEmbedderProcessor._model_instance is None and self.model_path:
                 logger.info(f"⏳ Loading Yuan_embedding Model into {self.device}...")
                 
                 # 与给定的示例代码保持一致：开启远程代码信任并使用 bfloat16 以节省显存
                 try:
                     YuanEmbedderProcessor._model_instance = SentenceTransformer(
                         self.model_path,
                         trust_remote_code=True,
                         model_kwargs={"torch_dtype": torch.bfloat16},
                         device=self.device
                     )
                     YuanEmbedderProcessor._model_instance.max_seq_length = self.max_length
                     logger.info("✅ Yuan_embedding model loaded successfully.")
                 except Exception as e:
                     logger.error(f"❌ Failed to load Yuan embedding model: {e}")
                 
    def process(self, context: Context) -> Context:
        chunks = context.metadata.get('chunks', [])
        if not chunks:
            logger.warning("No chunks found in context.metadata. Embedding cannot continue.")
            context.mark_invalid("No chunks available for YuanEmbedderProcessor.")
            return context
            
        if YuanEmbedderProcessor._model_instance is None:
            msg = "Yuan Embedding model is not loaded."
            if EMBEDDER_IMPORT_ERROR:
                msg = f"{msg} Dependency error: {EMBEDDER_IMPORT_ERROR}"
            logger.error(msg)
            context.mark_invalid(msg)
            return context
            
        logger.info(f"🧠 Encoding {len(chunks)} chunks using Yuan Estimator...")
        context.metadata["embedding_model_path"] = self.model_path
        context.metadata["embedding_batch_size"] = self.batch_size
        context.metadata["embedding_device"] = self.device
        
        # 提取所有切片的纯文本
        texts_to_encode = [ch.get('text', '') for ch in chunks]
        
        # 批量向量化 (归一化确保 L2 or Cosine 计算准确)
        try:
            embeddings = YuanEmbedderProcessor._model_instance.encode(
                texts_to_encode, 
                batch_size=self.batch_size, 
                normalize_embeddings=True
            )
        except Exception as e:
            msg = f"Failed to encode texts with Yuan Embedding: {str(e)}"
            logger.error(msg)
            context.mark_invalid(msg)
            return context
            
        # 根据原始列表重新注水挂载
        for idx, chunk in enumerate(chunks):
            # 将 numpy array 转换为标准的 python list 浮点数，便于后续 JSON 序列化与落库
            chunk['embedding'] = embeddings[idx].tolist()
            
        logger.info("✅ Encoding completed and mapped back to chunks.")
        
        # 更新上下文
        context.metadata['chunks'] = chunks
        context.metadata["embedded_chunk_count"] = len(chunks)
        context.metadata["embedding_dim"] = len(chunks[0]["embedding"]) if chunks and chunks[0].get("embedding") else 0
        
        return context
