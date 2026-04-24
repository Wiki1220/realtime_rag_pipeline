# -*- coding: utf-8 -*-
import tempfile
import unittest
from unittest.mock import Mock, patch

from services.file_download_service import FileDownloadError, FileDownloadService


class FileDownloadServiceTests(unittest.TestCase):
    def test_json_404_is_non_retryable(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = FileDownloadService(storage_dir=tmpdir, max_retries=3)
            response = Mock()
            response.status_code = 404
            response.headers = {"Content-Type": "application/json"}
            response.json.return_value = {"code": 0, "message": "file not found"}

            with patch("services.file_download_service.requests.get", return_value=response):
                with self.assertRaises(FileDownloadError) as ctx:
                    service.get_file_path("missing-guid")

            self.assertFalse(ctx.exception.retryable)
            self.assertEqual(ctx.exception.status_code, 404)
            self.assertIn("file not found", str(ctx.exception))

    def test_429_uses_rate_limit_reset(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = FileDownloadService(storage_dir=tmpdir, max_retries=1)
            response = Mock()
            response.status_code = 429
            response.headers = {
                "Content-Type": "application/json",
                "X-RateLimit-Limit": "10",
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Reset": "1700000005",
            }
            response.json.return_value = {"code": 0, "message": "too many requests"}

            with patch("services.file_download_service.requests.get", return_value=response), \
                 patch("services.file_download_service.time.time", return_value=1700000000), \
                 patch("services.file_download_service.time.sleep") as mock_sleep:
                with self.assertRaises(FileDownloadError) as ctx:
                    service.get_file_path("rate-limited-guid")

            self.assertFalse(ctx.exception.retryable)
            self.assertIn("too many requests", str(ctx.exception))
            self.assertTrue(mock_sleep.called)
            self.assertEqual(mock_sleep.call_args[0][0], 5)

    def test_successful_download_writes_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = FileDownloadService(storage_dir=tmpdir, max_retries=0)
            response = Mock()
            response.status_code = 200
            response.headers = {
                "Content-Type": "application/pdf",
                "Content-Disposition": 'attachment; filename="demo.pdf"',
            }
            response.iter_content.return_value = [b"abc", b"def"]

            with patch("services.file_download_service.requests.get", return_value=response):
                file_info = service.get_file_info("ok-guid")
                file_path = file_info["path"]

            with open(file_path, "rb") as file_handle:
                self.assertEqual(file_handle.read(), b"abcdef")
            self.assertTrue(file_path.endswith(".pdf"))
            self.assertEqual(file_info["original_filename"], "demo.pdf")

    def test_cached_file_info_returns_original_filename(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = FileDownloadService(storage_dir=tmpdir, max_retries=0)
            response = Mock()
            response.status_code = 200
            response.headers = {
                "Content-Type": "application/pdf",
                "Content-Disposition": 'attachment; filename="cached.pdf"',
            }
            response.iter_content.return_value = [b"cached"]

            with patch("services.file_download_service.requests.get", return_value=response):
                first = service.get_file_info("cached-guid")

            with patch("services.file_download_service.requests.get") as mock_get:
                second = service.get_file_info("cached-guid")

            self.assertEqual(first["original_filename"], "cached.pdf")
            self.assertEqual(second["original_filename"], "cached.pdf")
            mock_get.assert_not_called()


if __name__ == "__main__":
    unittest.main()
