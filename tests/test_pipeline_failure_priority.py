# -*- coding: utf-8 -*-
import unittest
import uuid

from core.context import Context
from core.pipeline import Pipeline
from core.registry import registry
from processors.base import BaseProcessor


class PipelineFailurePriorityTests(unittest.TestCase):
    def test_processor_exception_clears_pending_review_state(self):
        processor_name = f"TestPendingThenCrash_{uuid.uuid4().hex}"

        class PendingThenCrashProcessor(BaseProcessor):
            def process(self, context: Context) -> Context:
                context.is_pending_review = True
                raise RuntimeError("boom")

        registry.register(processor_name)(PendingThenCrashProcessor)

        pipeline = Pipeline(
            [{"name": processor_name, "params": {}}],
            pipeline_name=f"{processor_name}.yaml",
        )
        ctx = Context(raw_data={"id": "doc-1", "file_path": "/tmp/doc.pdf"})

        final_ctx = pipeline.run(ctx)

        self.assertFalse(final_ctx.is_valid)
        self.assertFalse(final_ctx.is_pending_review)
        self.assertEqual(final_ctx.paused_at_step, -1)
        self.assertTrue(any("Fatal error" in err for err in final_ctx.errors))


if __name__ == "__main__":
    unittest.main()
