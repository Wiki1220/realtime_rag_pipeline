# -*- coding: utf-8 -*-
import os
import time
import requests
import uuid
import logging
from pathlib import Path
from typing import Any, Dict, Tuple
from urllib.parse import urlparse

from core.context import Context
from core.registry import ProcessorRegistry
from processors.base import BaseProcessor

logger = logging.getLogger("MinerUOCRProcessor")

@ProcessorRegistry.register("MinerUOCRProcessor")
class MinerUOCRProcessor(BaseProcessor):
    """
    PDF 提取器。
    当前仅支持通过 HTTP 调用远程 MinerU 解析服务。
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.output_dir = self.config.get("output_dir", "./data_process/result")
        self.base_url = self._resolve_base_url()
        self.request_timeout_seconds = int(self.config.get("request_timeout_seconds", 1800))

    def _resolve_base_url(self) -> str:
        base_url = self.config.get("base_url")
        if base_url:
            return str(base_url).rstrip("/")

        mineru_host = self.config.get("mineru_host")
        mineru_port = self.config.get("mineru_port", 8000)
        if mineru_host:
            return f"http://{mineru_host}:{mineru_port}"

        raise ValueError(
            "MinerUOCRProcessor requires remote MinerU configuration. "
            "Set 'base_url' or ('mineru_host' + 'mineru_port')."
        )
        
    def process(self, context: Context) -> Context:
        # 1. 获取输入 PDF 路径或 URL
        pdf_path = context.metadata.get("input_pdf_path")
        if not pdf_path and isinstance(context.raw_data, str):
            pdf_path = context.raw_data
        elif not pdf_path and isinstance(context.raw_data, dict):
            pdf_path = context.raw_data.get("pdf_path") or context.raw_data.get("file_path")
            
        if not pdf_path:
            context.mark_invalid("PDF file path or URL is missing in the payload")
            return context

        context.metadata["input_pdf_path"] = pdf_path
        source_name = context.metadata.get("source_filename") or os.path.basename(str(pdf_path))
        context.metadata["source_filename"] = source_name

        abs_output_dir = os.path.abspath(self.output_dir)
        os.makedirs(abs_output_dir, exist_ok=True)
            
        download_path = None
        # 1.5. 下载网络 URL（如果需要）
        if str(pdf_path).startswith("http://") or str(pdf_path).startswith("https://"):
            logger.info(f"🌐 Detected remote URL: {pdf_path}. Downloading...")
            try:
                response = requests.get(pdf_path, stream=True, timeout=60)
                response.raise_for_status()
                file_ext = ".md" if pdf_path.lower().endswith(".md") else ".pdf"
                tmp_filename = f"downloaded_{uuid.uuid4().hex[:8]}{file_ext}"
                download_path = os.path.join(abs_output_dir, tmp_filename)
                with open(download_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                pdf_path = download_path
                context.metadata["input_pdf_path"] = pdf_path
                downloaded_name = os.path.basename(str(pdf_path))
                context.metadata.setdefault("source_filename", downloaded_name)
            except Exception as e:
                context.mark_invalid(f"Failed to download remote file: {str(e)}")
                return context
                
        try:
            if not os.path.exists(pdf_path):
                context.mark_invalid(f"PDF file is missing or invalid: {pdf_path}")
                return context

            context.metadata["input_file_size_bytes"] = os.path.getsize(pdf_path)
                
            # 2. 如果输入就是 Markdown，直接读取
            if pdf_path.lower().endswith(".md") or pdf_path.lower().endswith(".markdown"):
                logger.info(f"Input is already a markdown file: {pdf_path}")
                with open(pdf_path, 'r', encoding='utf-8') as f:
                    context.metadata['raw_markdown'] = f.read()
                return context
    
            abs_pdf_path = os.path.abspath(pdf_path)
            logger.info(
                "🚀 Starting PDF extraction: %s (remote MinerU=%s)",
                os.path.basename(abs_pdf_path),
                self.base_url,
            )
            context.metadata["mineru_base_url"] = self.base_url
            remote_output_dir = self._build_remote_output_dir(abs_pdf_path)
            context.metadata["mineru_output_dir"] = remote_output_dir
            start_time = time.time()
            
            try:
                md_content, mineru_data = self._extract_remote_mineru(abs_pdf_path, remote_output_dir)
                
                # 备份结果
                base_name = os.path.splitext(os.path.basename(abs_pdf_path))[0]
                local_dir = os.path.join(abs_output_dir, base_name)
                os.makedirs(local_dir, exist_ok=True)
                local_md_path = os.path.join(local_dir, f"{base_name}.md")
                with open(local_md_path, "w", encoding="utf-8") as f:
                    f.write(md_content)
                    
                duration = time.time() - start_time
                logger.info(f"✅ PDF Extraction Complete! Cost: {duration:.2f}s")
                context.metadata['raw_markdown'] = md_content
                context.metadata['mineru_parse_duration'] = duration
                context.metadata['mineru_markdown_path'] = local_md_path
                context.metadata['mineru_markdown_chars'] = len(md_content)
                context.metadata['mineru_markdown_lines'] = md_content.count("\n") + (1 if md_content else 0)
                if mineru_data.get("stdout_tail"):
                    context.metadata['mineru_stdout_tail'] = mineru_data["stdout_tail"]
                if mineru_data.get("stderr_tail"):
                    context.metadata['mineru_stderr_tail'] = mineru_data["stderr_tail"]
                
            except Exception as e:
                context.mark_invalid(f"PDF Extraction Error: {str(e)}")
                logger.error("Error details:", exc_info=True)
                 
            return context
        finally:
            if download_path and os.path.exists(download_path):
                try:
                    os.remove(download_path)
                    logger.info(f"🗑️ Cleaned up temporary downloaded file: {download_path}")
                except Exception as cleanup_err:
                    logger.warning(f"Failed to clean up temporary file {download_path}: {cleanup_err}")
    
    def _build_remote_output_dir(self, pdf_path: str) -> str:
        base_name = Path(pdf_path).stem
        return os.path.join(os.path.abspath(self.output_dir), "_mineru_runs", f"{base_name}_{uuid.uuid4().hex[:8]}")

    def _extract_remote_mineru(self, pdf_path: str, remote_output_dir: str) -> Tuple[str, Dict[str, Any]]:
        """通过 HTTP 调用远程 MinerU 解析服务"""
        url = f"{self.base_url}/api/v1/parse"
        payload = {
            "pdf_path": pdf_path,
            "output_dir": remote_output_dir,
            "parse_method": self.config.get("parse_method", "auto")
        }
        logger.info(f"📡 Calling remote MinerU at {url}...")
        response = requests.post(
            url,
            json=payload,
            timeout=self.request_timeout_seconds,
        )
        response.raise_for_status()
        data = response.json()

        # 兼容两类 MinerU 返回协议：
        # 1. 本仓库自带 wrapper: {"code": 200, "msg": "success", "data": {...}}
        # 2. 服务器现网服务: {"status": "success", "output": "..."}
        if "code" in data:
            if data.get("code") != 200:
                raise RuntimeError(f"Remote MinerU failed: {data.get('msg')}")

            response_data = data.get("data", {}) or {}
            markdown_content = response_data.get("markdown_content") or response_data.get("markdown_text")
            if isinstance(markdown_content, str) and markdown_content.strip():
                return markdown_content, response_data

            md_path = response_data["markdown_path"]
            parsed = urlparse(self.base_url)
            remote_host = parsed.hostname
            if remote_host in {"127.0.0.1", "localhost", None} and os.path.exists(md_path):
                with open(md_path, "r", encoding="utf-8") as f:
                    return f.read(), response_data

            raise RuntimeError(
                "Remote MinerU returned a markdown path that is not readable from this host. "
                "The remote MinerU service must return markdown content directly or share storage with the pipeline host."
            )

        if data.get("status") == "success":
            output_root = data.get("output")
            if not output_root:
                raise RuntimeError("Remote MinerU succeeded but did not return an output directory.")

            md_path = self._find_markdown_under_output(output_root, pdf_path)
            with open(md_path, "r", encoding="utf-8") as f:
                return f.read(), {"markdown_path": md_path, "output_dir": output_root}

        raise RuntimeError(f"Remote MinerU failed: {data}")

    def _find_markdown_under_output(self, output_root: str, pdf_path: str) -> str:
        root = Path(output_root)
        if not root.exists():
            raise RuntimeError(f"Remote MinerU output directory does not exist: {output_root}")

        base_name = Path(pdf_path).stem
        candidates = []
        for md_path in root.rglob("*.md"):
            if md_path.name.startswith("."):
                continue
            candidates.append(md_path)

        if not candidates:
            raise RuntimeError(f"Remote MinerU did not produce markdown under: {output_root}")

        preferred_patterns = [
            root / base_name / "auto" / f"{base_name}.md",
            root / base_name / f"{base_name}.md",
        ]
        for preferred_path in preferred_patterns:
            if preferred_path.exists():
                return str(preferred_path)

        def rank(path: Path):
            rel_parts = path.relative_to(root).parts
            stem_match = 0 if path.stem == base_name else 1
            in_named_dir = 0 if base_name in rel_parts else 1
            auto_dir = 0 if "auto" in rel_parts else 1
            return (stem_match, in_named_dir, auto_dir, -path.stat().st_mtime)

        return str(sorted(candidates, key=rank)[0])
