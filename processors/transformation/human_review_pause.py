# -*- coding: utf-8 -*-
import logging
from core.context import Context
from core.registry import registry
from processors.base import BaseProcessor

@registry.register("HumanReviewPause")
class HumanReviewPause(BaseProcessor):
    """
    专门用于在管线中间进行拦截的“电子栏杆”算子。
    将此算子配置在 YAML 的中间步骤。当数据流经此地时，
    会被强行打上 is_pending_review = True 的标记，从而导致 Pipeline 暂停循环。
    随后由上层 api_server 负责将其存入本地 SQLite。
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger("HumanReviewPause")
        self.always_pause = self.config.get("always_pause", True)
        self.pause_on_uncertain_chunks = self.config.get("pause_on_uncertain_chunks", True)
        self.min_uncertain_chunks = int(self.config.get("min_uncertain_chunks", 1))
        self.use_kafka = self.config.get("use_kafka", False)
        self.kafka_topic = self.config.get("kafka_topic", "chemrag-pending-reviews")

    def process(self, context: Context) -> Context:
        should_pause = self.always_pause
        if not should_pause and self.pause_on_uncertain_chunks:
            uncertain_chunks_count = int(context.metadata.get("uncertain_chunks_count", 0) or 0)
            should_pause = uncertain_chunks_count >= self.min_uncertain_chunks

        if should_pause:
            reason = "always_pause"
            if not self.always_pause:
                reason = f"uncertain_chunks={context.metadata.get('uncertain_chunks_count', 0)}"
            context.metadata["pending_review_reason"] = reason
            self.logger.info(
                "⏸️ HumanReviewPause hit! Marking context %s as pending review (%s)...",
                context.run_id,
                reason,
            )
            context.is_pending_review = True
            context.metadata["review_triggered"] = True
            context.metadata["review_trigger_reason"] = reason
            
            # --- Kafka Integration ---
            if self.use_kafka:
                from utils.kafka_manager import kafka_manager
                try:
                    self.logger.info(f"📤 Publishing context {context.run_id} to Kafka topic: {self.kafka_topic}")
                    kafka_manager.produce(self.kafka_topic, key=context.run_id, value=context.to_dict())
                except Exception as exc:
                    self.logger.error("Failed to publish pending review event: %s", exc, exc_info=True)
                    context.metadata["review_notification_error"] = str(exc)
            # -------------------------
            
        return context
