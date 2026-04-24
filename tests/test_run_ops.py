# -*- coding: utf-8 -*-
import sqlite3
import tempfile
import unittest

from core.context import Context
from core.review_store import ReviewStore
from core.run_ops import expects_milvus_insert, handle_pipeline_result
from core.state_manager import StateManager


class RunOpsTests(unittest.TestCase):
    def test_personal_milvus_loader_requires_insert_success(self):
        MilvusPersonalLoader = type("MilvusPersonalLoader", (), {})
        processors = [MilvusPersonalLoader()]
        self.assertTrue(expects_milvus_insert(processors))

    def test_invalid_context_is_not_saved_back_to_pending_queue(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            review_store = ReviewStore(db_path=f"{tmpdir}/pending_reviews.db")
            state_manager = StateManager(db_path=f"{tmpdir}/pipeline_states.db")

            ctx = Context(raw_data={"id": "doc-1"})
            ctx.run_id = "run-1"
            ctx.pipeline_name = "test_pipeline.yaml"
            ctx.is_valid = False
            ctx.is_pending_review = True
            ctx.errors.append("bad data")

            result = handle_pipeline_result(
                ctx,
                fallback_run_id="run-1",
                review_store=review_store,
                state_manager=state_manager,
                fallback_pipeline_name="test_pipeline.yaml",
                expects_milvus_insert=False,
            )

            self.assertEqual(result["status"], "error")
            self.assertFalse(review_store.has_pending_context("run-1"))

    def test_missing_milvus_insert_marks_run_failed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            review_store = ReviewStore(db_path=f"{tmpdir}/pending_reviews.db")
            state_manager = StateManager(db_path=f"{tmpdir}/pipeline_states.db")
            pipeline_name = "test_pipeline.yaml"

            state_manager.sync_table(pipeline_name, [])
            state_manager.init_run(pipeline_name, "run-2", "doc-2", "/tmp/doc.pdf")

            ctx = Context(raw_data={"id": "doc-2"})
            ctx.run_id = "run-2"
            ctx.pipeline_name = pipeline_name

            result = handle_pipeline_result(
                ctx,
                fallback_run_id="run-2",
                review_store=review_store,
                state_manager=state_manager,
                fallback_pipeline_name=pipeline_name,
                expects_milvus_insert=True,
            )

            self.assertEqual(result["status"], "error")

            with sqlite3.connect(state_manager.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT global_status FROM state_test_pipeline WHERE run_id = ?",
                    ("run-2",),
                )
                row = cursor.fetchone()

            self.assertEqual(row[0], "failed")


if __name__ == "__main__":
    unittest.main()
