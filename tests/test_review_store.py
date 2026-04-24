# -*- coding: utf-8 -*-
import tempfile
import unittest

from core.context import Context
from core.review_store import ReviewStore


class ReviewStoreTests(unittest.TestCase):
    def test_get_context_does_not_delete_record(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = ReviewStore(db_path=f"{tmpdir}/pending_reviews.db")
            ctx = Context(raw_data={"id": "doc-1"})
            ctx.run_id = "run-1"

            store.save_pending_context("run-1", ctx)

            loaded = store.get_context("run-1")

            self.assertIsNotNone(loaded)
            self.assertTrue(store.has_pending_context("run-1"))
            self.assertEqual(store.find_existing_context_ids(["run-1", "run-2"]), {"run-1"})

            self.assertTrue(store.delete_context("run-1"))
            self.assertFalse(store.has_pending_context("run-1"))


if __name__ == "__main__":
    unittest.main()
