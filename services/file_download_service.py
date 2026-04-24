# -*- coding: utf-8 -*-
import glob
import hashlib
import hmac
import json
import mimetypes
import os
import re
import time
import uuid
from contextlib import contextmanager
from email.header import decode_header
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import unquote

try:
    import requests
except ImportError:  # pragma: no cover - exercised only in minimal test envs
    class _RequestsStub:
        class RequestException(Exception):
            pass

        class Response:
            pass

        @staticmethod
        def get(*args, **kwargs):
            raise RuntimeError("requests is not installed.")

    requests = _RequestsStub()

from utils.logger import setup_logger


logger = setup_logger("FileDownloadService")

FILE_DOWNLOAD_BASE_URL = os.getenv(
    "FILE_DOWNLOAD_BASE_URL",
    "http://172.20.8.38:17000/app-api/file/download",
)
FILE_APP_ID = os.getenv("FILE_APP_ID", "")
FILE_APP_SECRET = os.getenv("FILE_APP_SECRET", "")
FILE_STORAGE_DIR = os.getenv("FILE_STORAGE_DIR", "data/file_cache")
FILE_CONNECT_TIMEOUT = int(os.getenv("FILE_CONNECT_TIMEOUT_SECONDS", "30"))
FILE_TOTAL_TIMEOUT = int(os.getenv("FILE_TOTAL_TIMEOUT_SECONDS", "300"))
FILE_MAX_RETRIES = int(os.getenv("FILE_DOWNLOAD_MAX_RETRIES", "3"))
DEFAULT_EXTENSION = ".bin"
NON_RETRYABLE_STATUS_CODES = {400, 401, 403, 404}
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class FileDownloadError(RuntimeError):
    def __init__(self, message: str, *, retryable: bool, status_code: Optional[int] = None):
        super().__init__(message)
        self.retryable = retryable
        self.status_code = status_code


class FileDownloadService:
    def __init__(
        self,
        *,
        base_url: str = FILE_DOWNLOAD_BASE_URL,
        storage_dir: str = FILE_STORAGE_DIR,
        app_id: str = FILE_APP_ID,
        app_secret: str = FILE_APP_SECRET,
        max_retries: int = FILE_MAX_RETRIES,
    ):
        self.base_url = base_url.rstrip("/")
        self.storage_dir = storage_dir
        self.app_id = app_id
        self.app_secret = app_secret
        self.max_retries = max_retries
        os.makedirs(self.storage_dir, exist_ok=True)

    def get_file_path(self, file_guid: str) -> str:
        return self.get_file_info(file_guid)["path"]

    def get_file_info(self, file_guid: str) -> Dict[str, Optional[str]]:
        normalized_guid = (file_guid or "").strip()
        if not normalized_guid:
            raise FileDownloadError("file_guid is required.")

        cached = self._find_cached(normalized_guid)
        if cached:
            logger.info("Using cached file for file_guid=%s path=%s", normalized_guid, cached["path"])
            return cached

        with self._file_lock(normalized_guid):
            cached = self._find_cached(normalized_guid)
            if cached:
                logger.info("Using cached file after waiting for file_guid=%s path=%s", normalized_guid, cached["path"])
                return cached

            url = f"{self.base_url}/{normalized_guid}"
            last_error = ""
            last_exception: Optional[FileDownloadError] = None
            for attempt in range(1, self.max_retries + 2):
                try:
                    return self._download_once(url, normalized_guid)
                except FileDownloadError as exc:
                    last_exception = exc
                    last_error = str(exc)
                    logger.warning(
                        "Download failed for file_guid=%s attempt=%s/%s error=%s",
                        normalized_guid,
                        attempt,
                        self.max_retries + 1,
                        exc,
                    )
                    self._cleanup_partial(normalized_guid)
                    if not exc.retryable:
                        break
                    if attempt <= self.max_retries:
                        time.sleep(self._calculate_retry_delay(exc, attempt))
                except requests.RequestException as exc:
                    last_error = str(exc)
                    logger.warning(
                        "Download request exception for file_guid=%s attempt=%s/%s error=%s",
                        normalized_guid,
                        attempt,
                        self.max_retries + 1,
                        exc,
                    )
                    self._cleanup_partial(normalized_guid)
                    if attempt <= self.max_retries:
                        time.sleep(min(2 ** attempt, 10))

            if last_exception is not None:
                raise FileDownloadError(
                    f"Failed to download file for file_guid={normalized_guid}: {last_error}",
                    retryable=False,
                    status_code=last_exception.status_code,
                ) from last_exception
            raise FileDownloadError(
                f"Failed to download file for file_guid={normalized_guid}: {last_error}",
                retryable=False,
            )

    def _download_once(self, url: str, file_guid: str) -> Dict[str, Optional[str]]:
        try:
            response = requests.get(
                url,
                headers=self._build_auth_headers(),
                stream=True,
                timeout=(FILE_CONNECT_TIMEOUT, FILE_TOTAL_TIMEOUT),
            )
        except requests.RequestException as exc:
            raise FileDownloadError(
                f"Request failed for file_guid={file_guid}: {exc}",
                retryable=True,
            ) from exc

        if response.status_code >= 400:
            self._raise_http_error(response, file_guid)

        content_type = (response.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        if content_type == "application/json":
            body = self._parse_json_body(response)
            message = self._format_platform_error(
                file_guid=file_guid,
                status_code=response.status_code,
                body=body,
            )
            raise FileDownloadError(
                message,
                retryable=response.status_code in RETRYABLE_STATUS_CODES,
                status_code=response.status_code,
            )

        extension = self._parse_extension_from_headers(dict(response.headers))
        original_filename = self._parse_original_filename(dict(response.headers))
        formatted_source_file = self._format_source_file(original_filename)
        final_path = os.path.join(self.storage_dir, f"{file_guid}{extension}")
        temp_path = f"{final_path}.part-{uuid.uuid4().hex}"

        with open(temp_path, "wb") as file_handle:
            for chunk in response.iter_content(chunk_size=128 * 1024):
                if chunk:
                    file_handle.write(chunk)

        os.replace(temp_path, final_path)
        self._write_cached_metadata(file_guid, original_filename=original_filename)
        logger.info("Downloaded file_guid=%s to %s", file_guid, final_path)
        return {
            "path": os.path.abspath(final_path),
            "source_file": formatted_source_file,
        }

    def _raise_http_error(self, response: requests.Response, file_guid: str) -> None:
        body = self._parse_json_body(response)
        message = self._format_platform_error(
            file_guid=file_guid,
            status_code=response.status_code,
            body=body,
            headers=dict(response.headers),
        )
        retryable = response.status_code in RETRYABLE_STATUS_CODES
        if response.status_code in NON_RETRYABLE_STATUS_CODES:
            retryable = False
        raise FileDownloadError(
            message,
            retryable=retryable,
            status_code=response.status_code,
        )

    def _parse_json_body(self, response: requests.Response) -> Dict[str, object]:
        try:
            parsed = response.json()
        except (ValueError, json.JSONDecodeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _format_platform_error(
        self,
        *,
        file_guid: str,
        status_code: int,
        body: Optional[Dict[str, object]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> str:
        body = body or {}
        headers = headers or {}
        code = body.get("code")
        message = body.get("message") or body.get("msg") or "unknown error"
        rate_limit = self._extract_rate_limit(headers)
        if rate_limit:
            logger.warning(
                "Rate limit encountered for file_guid=%s status=%s limit=%s remaining=%s reset=%s",
                file_guid,
                status_code,
                rate_limit.get("limit"),
                rate_limit.get("remaining"),
                rate_limit.get("reset"),
            )
        suffix = ""
        if rate_limit:
            suffix = (
                f" rate_limit(limit={rate_limit.get('limit')},"
                f" remaining={rate_limit.get('remaining')}, reset={rate_limit.get('reset')})"
            )
        return (
            f"Download failed for file_guid={file_guid}: "
            f"http_status={status_code} code={code} message={message}{suffix}"
        )

    def _extract_rate_limit(self, headers: Dict[str, str]) -> Dict[str, Optional[int]]:
        def _to_int(value: Optional[str]) -> Optional[int]:
            if value in (None, ""):
                return None
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

        limit = _to_int(headers.get("X-RateLimit-Limit"))
        remaining = _to_int(headers.get("X-RateLimit-Remaining"))
        reset = _to_int(headers.get("X-RateLimit-Reset"))
        if limit is None and remaining is None and reset is None:
            return {}
        return {
            "limit": limit,
            "remaining": remaining,
            "reset": reset,
        }

    def _calculate_retry_delay(self, exc: FileDownloadError, attempt: int) -> int:
        if exc.status_code == 429:
            match = re.search(r"reset=(\d+)", str(exc))
            if match:
                reset_at = int(match.group(1))
                now = int(time.time())
                return max(1, min(reset_at - now, 60))
        return min(2 ** attempt, 10)

    def _build_auth_headers(self) -> Dict[str, str]:
        timestamp = str(int(time.time() * 1000))
        nonce = uuid.uuid4().hex
        data_to_sign = self.app_id + timestamp + nonce
        signature = hmac.new(
            self.app_secret.encode("utf-8"),
            data_to_sign.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return {
            "X-App-Id": self.app_id,
            "X-Timestamp": timestamp,
            "X-Nonce": nonce,
            "X-Signature": signature,
        }

    def _find_cached(self, file_guid: str) -> Optional[Dict[str, Optional[str]]]:
        pattern = os.path.join(self.storage_dir, f"{file_guid}.*")
        for path in glob.glob(pattern):
            if path.endswith(".lock") or path.endswith(".meta.json"):
                continue
            if os.path.splitext(os.path.basename(path))[0] == file_guid:
                return {
                    "path": os.path.abspath(path),
                    "source_file": self._read_cached_source_file(file_guid),
                }
        plain_path = os.path.join(self.storage_dir, file_guid)
        if os.path.isfile(plain_path):
            return {
                "path": os.path.abspath(plain_path),
                "source_file": self._read_cached_source_file(file_guid),
            }
        return None

    def _cleanup_partial(self, file_guid: str) -> None:
        for path in glob.glob(os.path.join(self.storage_dir, f"{file_guid}.*.part-*")):
            try:
                os.remove(path)
            except OSError:
                pass

    def _parse_extension_from_headers(self, headers: Dict[str, str]) -> str:
        original_filename = self._parse_original_filename(headers)
        if original_filename:
            _, extension = os.path.splitext(self._strip_title_marks(original_filename))
            if extension:
                return extension.lower()

        content_type = (headers.get("Content-Type") or "").split(";")[0].strip()
        guessed = mimetypes.guess_extension(content_type) if content_type else None
        return (guessed or DEFAULT_EXTENSION).lower()

    def _parse_original_filename(self, headers: Dict[str, str]) -> Optional[str]:
        disposition = headers.get("Content-Disposition", "")
        if disposition:
            match = re.search(r"filename\*\s*=\s*([^;]+)", disposition, re.IGNORECASE)
            if match:
                encoded_value = match.group(1).strip().strip("\"'")
                parts = encoded_value.split("''", 1)
                encoded_name = parts[1] if len(parts) == 2 else encoded_value
                return self._normalize_header_filename(unquote(encoded_name))

            match = re.search(r'filename=["\']?([^"\';]+)', disposition, re.IGNORECASE)
            if match:
                return self._normalize_header_filename(match.group(1).strip())
        return None

    def _normalize_header_filename(self, raw_name: str) -> Optional[str]:
        if not raw_name:
            return None

        decoded_name = self._decode_mime_header(raw_name)
        repaired_name = self._repair_utf8_mojibake(decoded_name)
        normalized_name = Path(repaired_name.strip().strip("\"'")).name
        return normalized_name or None

    def _format_source_file(self, filename: Optional[str]) -> Optional[str]:
        if not filename:
            return None
        normalized = self._strip_title_marks(filename.strip())
        if not normalized:
            return None
        return f"《{normalized}》"

    def _strip_title_marks(self, filename: str) -> str:
        if filename.startswith("《") and filename.endswith("》") and len(filename) >= 2:
            return filename[1:-1].strip()
        return filename

    def _decode_mime_header(self, raw_name: str) -> str:
        try:
            parts = decode_header(raw_name)
        except Exception:
            return raw_name

        decoded_chunks = []
        for value, charset in parts:
            if isinstance(value, bytes):
                for encoding in [charset, "utf-8", "gb18030", "latin1"]:
                    if not encoding:
                        continue
                    try:
                        decoded_chunks.append(value.decode(encoding))
                        break
                    except (LookupError, UnicodeDecodeError):
                        continue
                else:
                    decoded_chunks.append(value.decode("utf-8", errors="ignore"))
            else:
                decoded_chunks.append(value)
        return "".join(decoded_chunks)

    def _repair_utf8_mojibake(self, value: str) -> str:
        if not value:
            return value
        if re.search(r"[\u4e00-\u9fff]", value):
            return value
        if not any(ord(ch) > 127 for ch in value):
            return value

        try:
            repaired = value.encode("latin1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return value
        return repaired

    def _metadata_path(self, file_guid: str) -> str:
        return os.path.join(self.storage_dir, f"{file_guid}.meta.json")

    def _write_cached_metadata(self, file_guid: str, *, original_filename: Optional[str]) -> None:
        metadata = {"source_file": self._format_source_file(original_filename)}
        with open(self._metadata_path(file_guid), "w", encoding="utf-8") as file_handle:
            json.dump(metadata, file_handle, ensure_ascii=False)

    def _read_cached_source_file(self, file_guid: str) -> Optional[str]:
        metadata_path = self._metadata_path(file_guid)
        if not os.path.isfile(metadata_path):
            return None
        try:
            with open(metadata_path, "r", encoding="utf-8") as file_handle:
                metadata = json.load(file_handle)
        except (OSError, json.JSONDecodeError):
            return None
        source_file = metadata.get("source_file")
        if not source_file:
            return None
        return self._format_source_file(self._normalize_header_filename(str(source_file)))

    @contextmanager
    def _file_lock(self, file_guid: str):
        import fcntl

        lock_path = Path(self.storage_dir) / f"{file_guid}.lock"
        with open(lock_path, "w", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
