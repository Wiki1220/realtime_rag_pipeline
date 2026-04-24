#!/usr/bin/env python3
"""
Web UI Gateway
纯前端展示与表单转发层，所有业务动作统一调用后端 API。
"""

import os
import json
import uuid
from typing import Any, Dict, List
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

import requests
from flask import Flask, flash, jsonify, redirect, render_template_string, request, url_for

app = Flask(__name__)
app.secret_key = os.getenv("WEB_UPLOAD_GATEWAY_SECRET_KEY", "dev-only-change-me")

API_BASE_URL = os.getenv("PIPELINE_API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
REQUEST_TIMEOUT = 60
DEFERRED_UPLOAD_DIR = Path("data/deferred_uploads")
DEFERRED_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
DEFERRED_QUEUE_PATH = Path("logs/deferred_routing_queue.json")
DEFERRED_QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)

PIPELINE_ROUTERS = [
    {
        "key": "preset4_pdf",
        "label": "预设 4 PDF 管道",
        "extensions": {"pdf"},
        "enabled": os.getenv("PIPELINE_PRESET4_PDF_ENABLED", "1") == "1",
        "upload_endpoint": "/api/v1/files/upload",
        "url_endpoint": "/api/v1/files/from-url",
    },
]


def _normalize_template(template: str) -> str:
    return (
        template
        .replace("{{%", "{%")
        .replace("%}}", "%}")
        .replace("{{{{%", "{%")
        .replace("%}}}}", "%}")
        .replace("{{{{", "{{")
        .replace("}}}}", "}}")
    )

BASE_STYLES = """
    <style>
        :root {
            --bg: #eef3f8;
            --panel: rgba(255, 255, 255, 0.9);
            --panel-strong: #ffffff;
            --border: rgba(19, 40, 67, 0.1);
            --text: #132843;
            --muted: #61758f;
            --accent: #0b57d0;
            --accent-strong: #083b8a;
            --accent-soft: rgba(11, 87, 208, 0.08);
            --success: #18794e;
            --success-soft: #e8f5ee;
            --warning: #9a6700;
            --warning-soft: #fff4d6;
            --danger: #b42318;
            --danger-soft: #fdecec;
            --neutral: #667085;
            --neutral-soft: #f2f4f7;
            --shadow: 0 18px 45px rgba(17, 38, 63, 0.08);
            --radius-lg: 24px;
            --radius-md: 16px;
            --radius-sm: 12px;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: "IBM Plex Sans", "Avenir Next", "Segoe UI", sans-serif;
            color: var(--text);
            background:
                radial-gradient(circle at top left, rgba(11, 87, 208, 0.12), transparent 28%),
                radial-gradient(circle at top right, rgba(15, 118, 110, 0.12), transparent 24%),
                linear-gradient(180deg, #f6f9fc 0%, var(--bg) 100%);
            min-height: 100vh;
            padding: 28px;
        }
        .shell {
            max-width: 1320px;
            margin: 0 auto;
        }
        .topbar {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 20px;
            margin-bottom: 24px;
        }
        .title-block h1 {
            font-size: 30px;
            line-height: 1.1;
            font-weight: 700;
            letter-spacing: -0.03em;
        }
        .title-block p {
            margin-top: 8px;
            color: var(--muted);
            font-size: 14px;
        }
        .topbar-meta {
            display: flex;
            gap: 12px;
            flex-wrap: wrap;
            justify-content: flex-end;
        }
        .metric-chip {
            min-width: 130px;
            padding: 14px 16px;
            border: 1px solid var(--border);
            border-radius: var(--radius-md);
            background: rgba(255,255,255,0.72);
            backdrop-filter: blur(12px);
            box-shadow: var(--shadow);
        }
        .metric-chip .label {
            display: block;
            color: var(--muted);
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }
        .metric-chip .value {
            display: block;
            margin-top: 6px;
            font-size: 24px;
            font-weight: 700;
        }
        .flash-stack {
            display: grid;
            gap: 10px;
            margin-bottom: 18px;
        }
        .flash {
            padding: 14px 16px;
            border-radius: var(--radius-sm);
            border: 1px solid transparent;
            font-size: 14px;
        }
        .banner-stack {
            display: grid;
            gap: 10px;
            margin-bottom: 18px;
        }
        .top-banner {
            display: none;
            align-items: center;
            justify-content: space-between;
            gap: 12px;
            padding: 13px 16px;
            border-radius: var(--radius-sm);
            border: 1px solid transparent;
            font-size: 14px;
            box-shadow: var(--shadow);
        }
        .top-banner.is-open {
            display: flex;
        }
        .top-banner-info {
            color: var(--accent-strong);
            background: rgba(11, 87, 208, 0.1);
            border-color: rgba(11, 87, 208, 0.14);
        }
        .top-banner button {
            width: auto;
            margin: 0;
            padding: 0;
            border: 0;
            background: transparent;
            color: inherit;
            font: inherit;
            cursor: pointer;
            opacity: 0.75;
        }
        .top-banner button:hover {
            opacity: 1;
        }
        .flash-error {
            color: var(--danger);
            background: var(--danger-soft);
            border-color: rgba(180, 35, 24, 0.12);
        }
        .flash-success {
            color: var(--success);
            background: var(--success-soft);
            border-color: rgba(24, 121, 78, 0.12);
        }
        .menu-wrap {
            position: relative;
        }
        .menu-trigger {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 40px;
            height: 40px;
            margin: 0;
            padding: 0;
            border-radius: 12px;
            border: 1px solid rgba(19, 40, 67, 0.14);
            background: #fff;
            color: var(--text);
            box-shadow: none;
            cursor: pointer;
            appearance: none;
        }
        .menu-trigger:hover {
            background: #fff;
            border-color: rgba(19, 40, 67, 0.24);
        }
        .menu-trigger:focus-visible {
            outline: 2px solid rgba(11, 87, 208, 0.2);
            outline-offset: 2px;
        }
        .menu-trigger-bars {
            display: inline-flex;
            flex-direction: column;
            gap: 4px;
        }
        .menu-trigger-bars span {
            display: block;
            width: 16px;
            height: 2px;
            border-radius: 999px;
            background: currentColor;
        }
        .menu-panel {
            position: absolute;
            top: calc(100% + 10px);
            right: 0;
            min-width: 220px;
            padding: 8px;
            border-radius: 14px;
            background: rgba(255,255,255,0.98);
            border: 1px solid var(--border);
            box-shadow: var(--shadow);
            display: none;
            z-index: 20;
        }
        .menu-panel.is-open {
            display: grid;
            gap: 2px;
        }
        .menu-action {
            display: block;
            width: 100%;
            margin: 0;
            padding: 10px 12px;
            white-space: nowrap;
            text-align: left;
            background: #fff;
            color: var(--text);
            border: 1px solid rgba(19, 40, 67, 0.1);
            box-shadow: none;
            border-radius: 10px;
            font-weight: 500;
            cursor: pointer;
            appearance: none;
        }
        .menu-action:hover {
            background: rgba(19, 40, 67, 0.06);
            border-color: rgba(19, 40, 67, 0.14);
        }
        .menu-action.btn-danger {
            background: #fff;
            color: var(--text);
        }
        .modal-overlay {
            position: fixed;
            inset: 0;
            background: rgba(10, 18, 30, 0.45);
            backdrop-filter: blur(4px);
            display: none;
            align-items: center;
            justify-content: center;
            padding: 20px;
            z-index: 100;
        }
        .modal-overlay.is-open {
            display: flex;
        }
        .modal-card {
            width: min(460px, 100%);
            background: rgba(255,255,255,0.98);
            border: 1px solid var(--border);
            border-radius: 20px;
            box-shadow: var(--shadow);
            padding: 24px;
        }
        .modal-card h3 {
            font-size: 18px;
            margin-bottom: 8px;
        }
        .modal-card p {
            color: var(--muted);
            font-size: 14px;
            line-height: 1.6;
        }
        .modal-actions {
            display: flex;
            gap: 12px;
            justify-content: flex-end;
            margin-top: 20px;
            flex-wrap: wrap;
        }
        .layout {
            display: grid;
            grid-template-columns: 360px minmax(0, 1fr);
            gap: 22px;
            align-items: start;
        }
        .panel {
            background: var(--panel);
            backdrop-filter: blur(14px);
            border: 1px solid var(--border);
            border-radius: var(--radius-lg);
            box-shadow: var(--shadow);
        }
        .panel-section {
            padding: 24px;
        }
        .panel + .panel {
            margin-top: 18px;
        }
        .panel-scroll {
            max-height: 720px;
            overflow-y: auto;
        }
        .panel h2 {
            font-size: 16px;
            font-weight: 700;
            margin-bottom: 6px;
        }
        .panel-subtitle {
            color: var(--muted);
            font-size: 13px;
            line-height: 1.5;
        }
        .upload-area {
            margin-top: 18px;
            border: 1.5px dashed rgba(19, 40, 67, 0.18);
            border-radius: 20px;
            background:
                linear-gradient(180deg, rgba(11, 87, 208, 0.04), rgba(11, 87, 208, 0.01)),
                var(--panel-strong);
            padding: 28px 20px;
            text-align: center;
            transition: border-color 0.2s ease, background 0.2s ease, transform 0.2s ease;
            cursor: pointer;
        }
        .upload-area:hover {
            border-color: rgba(11, 87, 208, 0.35);
            transform: translateY(-1px);
        }
        .upload-area strong {
            display: block;
            font-size: 15px;
            margin-bottom: 8px;
        }
        .upload-area span {
            color: var(--muted);
            font-size: 13px;
            line-height: 1.5;
        }
        .btn {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
            width: 100%;
            margin-top: 16px;
            border: 0;
            border-radius: 14px;
            background: linear-gradient(135deg, var(--accent) 0%, var(--accent-strong) 100%);
            color: #fff;
            padding: 12px 18px;
            font-size: 14px;
            font-weight: 600;
            cursor: pointer;
            text-decoration: none;
        }
        .btn-secondary {
            background: #fff;
            color: var(--text);
            border: 1px solid var(--border);
        }
        .btn-danger {
            background: linear-gradient(135deg, #d92d20 0%, #b42318 100%);
        }
        .btn-success {
            background: linear-gradient(135deg, #18794e 0%, #0f5b39 100%);
        }
        .btn-inline {
            width: auto;
            margin-top: 0;
        }
        .form-row {
            margin-top: 18px;
        }
        .form-row label {
            display: block;
            margin-bottom: 8px;
            font-size: 13px;
            font-weight: 600;
            color: var(--muted);
        }
        .text-input, textarea {
            width: 100%;
            border: 1px solid rgba(19, 40, 67, 0.14);
            border-radius: 14px;
            background: rgba(255,255,255,0.96);
            padding: 12px 14px;
            font: inherit;
            color: var(--text);
        }
        textarea {
            min-height: 110px;
            resize: vertical;
        }
        input[type="file"] { display: none; }
        .content-panel-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 16px;
            padding: 24px 24px 0;
        }
        .content-panel-header .actions {
            display: flex;
            align-items: center;
            gap: 10px;
            flex-wrap: wrap;
        }
        .job-list {
            padding: 10px 18px 20px;
            max-height: 760px;
            overflow-y: auto;
        }
        .job-item {
            display: grid;
            grid-template-columns: minmax(0, 1fr) auto;
            gap: 18px;
            padding: 18px;
            border-radius: 18px;
            border: 1px solid rgba(19, 40, 67, 0.08);
            background: rgba(255, 255, 255, 0.72);
        }
        .job-item + .job-item {
            margin-top: 12px;
        }
        .job-list.is-loading {
            opacity: 0.7;
            transition: opacity 0.18s ease;
        }
        .job-name {
            font-size: 15px;
            font-weight: 700;
            color: var(--text);
            word-break: break-all;
        }
        .job-id {
            margin-top: 6px;
            color: var(--muted);
            font-size: 12px;
            font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
        }
        .job-meta {
            margin-top: 8px;
            color: var(--muted);
            font-size: 12px;
        }
        .error-box {
            margin-top: 10px;
            padding: 10px 12px;
            border-radius: 12px;
            background: var(--danger-soft);
            color: var(--danger);
            font-size: 12px;
            line-height: 1.5;
        }
        .stage-details {
            display: flex;
            gap: 8px;
            margin-top: 12px;
            flex-wrap: wrap;
        }
        .stage-item,
        .status-badge {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 6px 10px;
            border-radius: 999px;
            font-size: 12px;
            font-weight: 600;
            white-space: nowrap;
        }
        .status-running, .stage-running {
            background: rgba(11, 87, 208, 0.1);
            color: var(--accent);
        }
        .status-completed, .stage-success {
            background: var(--success-soft);
            color: var(--success);
        }
        .status-failed, .stage-failed {
            background: var(--danger-soft);
            color: var(--danger);
        }
        .status-paused, .stage-pending, .stage-paused {
            background: var(--warning-soft);
            color: var(--warning);
        }
        .status-rejected {
            background: var(--neutral-soft);
            color: var(--neutral);
        }
        .status-deferred, .status-queued {
            background: var(--warning-soft);
            color: var(--warning);
        }
        .status-unsupported {
            background: var(--neutral-soft);
            color: var(--neutral);
        }
        .empty-state {
            padding: 72px 24px;
            text-align: center;
            color: var(--muted);
        }
        .empty-state h3 {
            font-size: 18px;
            color: var(--text);
            margin-bottom: 8px;
        }
        .review-list {
            display: grid;
            gap: 16px;
            margin-top: 18px;
            max-height: 720px;
            overflow-y: auto;
            padding-right: 4px;
        }
        .review-card {
            padding: 22px;
            border-radius: 20px;
            border: 1px solid rgba(19, 40, 67, 0.08);
            background: rgba(255,255,255,0.82);
        }
        .review-card h3 {
            font-size: 16px;
            font-weight: 700;
        }
        .review-meta {
            margin-top: 6px;
            color: var(--muted);
            font-size: 12px;
        }
        .chunk-box {
            margin-top: 14px;
            padding: 14px;
            border-radius: 16px;
            border: 1px solid rgba(19, 40, 67, 0.08);
            background: #fbfcfe;
        }
        .chunk-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 12px;
            margin-bottom: 10px;
        }
        .chunk-header label {
            font-size: 13px;
            font-weight: 700;
        }
        .chunk-toggle {
            width: auto;
            padding: 8px 12px;
            border-radius: 12px;
            margin: 0;
            font-size: 12px;
        }
        .review-actions {
            display: flex;
            gap: 12px;
            margin-top: 18px;
            flex-wrap: wrap;
        }
        @media (max-width: 1080px) {
            .layout {
                grid-template-columns: 1fr;
            }
            .topbar {
                flex-direction: column;
            }
            .topbar-meta {
                justify-content: flex-start;
            }
        }
        @media (max-width: 680px) {
            body {
                padding: 16px;
            }
            .content-panel-header {
                flex-direction: column;
                align-items: flex-start;
            }
            .job-item {
                grid-template-columns: 1fr;
            }
            .review-actions {
                flex-direction: column;
            }
        }
    </style>
"""

REVIEW_HTML_TEMPLATE = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>审核队列</title>
    {BASE_STYLES}
</head>
<body>
    <div class="shell">
        <div class="topbar">
            <div class="title-block">
                <h1>审核队列</h1>
                <p>查看已暂停的任务，编辑分块内容，并决定任务继续执行还是直接驳回。</p>
            </div>
            <div class="topbar-meta">
                <div class="metric-chip">
                    <span class="label">待审核任务</span>
                    <span class="value">{{{{ records|length }}}}</span>
                </div>
            </div>
        </div>

        {{{{% with messages = get_flashed_messages(with_categories=true) %}}}}
            {{{{% if messages %}}}}
                <div class="flash-stack">
                {{{{% for category, message in messages %}}}}
                    <div class="flash flash-{{{{ category }}}}">{{{{ message }}}}</div>
                {{{{% endfor %}}}}
                </div>
            {{{{% endif %}}}}
        {{{{% endwith %}}}}

        <div class="panel">
            <div class="content-panel-header">
                <div>
                    <h2>待审核列表</h2>
                    <p class="panel-subtitle">进入人工审核节点的任务会显示在这里，直到继续执行或被驳回。</p>
                </div>
                <div class="actions">
                    <a href="/" class="btn btn-secondary btn-inline">返回控制台</a>
                </div>
            </div>
            <div class="panel-section panel-scroll">
                {{{{% if records %}}}}
                    <div class="review-list">
                    {{{{% for record in records %}}}}
                        <div class="review-card">
                            <h3>{{{{ record.id }}}}</h3>
                            <div class="review-meta">创建时间 {{{{ record.created_at }}}}</div>
                            <form action="/review/action/{{{{ record.id }}}}" method="POST">
                                {{{{% set chunks = record.metadata.get('chunks', []) %}}}}
                                <div>
                                {{{{% for c in chunks %}}}}
                                    <div class="chunk-box" id="chunk_container_{{{{ loop.index0 }}}}">
                                        <div class="chunk-header">
                                            <label>分块 {{{{ loop.index }}}} · {{{{ c.text|length }}}} 字</label>
                                            <div>
                                                <input type="checkbox" name="keep_{{{{ loop.index0 }}}}" id="keep_{{{{ loop.index0 }}}}" value="yes" checked style="display:none;">
                                                <button type="button" class="btn btn-danger btn-inline chunk-toggle" onclick="toggleDiscard({{{{ loop.index0 }}}})" id="btn_toggle_{{{{ loop.index0 }}}}">移除此分块</button>
                                            </div>
                                        </div>
                                        <textarea name="chunk_{{{{ loop.index0 }}}}" id="textarea_{{{{ loop.index0 }}}}">{{{{ c.text }}}}</textarea>
                                    </div>
                                {{{{% endfor %}}}}
                                </div>
                                <div class="review-actions">
                                    <button type="submit" name="action" value="approve" class="btn btn-success btn-inline">保存并继续执行</button>
                                    <button type="submit" name="action" value="reject_all" class="btn btn-danger btn-inline">驳回任务</button>
                                </div>
                            </form>
                        </div>
                    {{{{% endfor %}}}}
                    </div>
                    <script>
                        function toggleDiscard(idx) {{
                            var chk = document.getElementById('keep_' + idx);
                            var btn = document.getElementById('btn_toggle_' + idx);
                            var txt = document.getElementById('textarea_' + idx);
                            var box = document.getElementById('chunk_container_' + idx);
                            if (chk.checked) {{
                                chk.checked = false;
                                btn.innerHTML = '恢复分块';
                                txt.style.opacity = '0.35';
                                txt.readOnly = true;
                                box.style.background = '#fdecec';
                            }} else {{
                                chk.checked = true;
                                btn.innerHTML = '移除此分块';
                                txt.style.opacity = '1';
                                txt.readOnly = false;
                                box.style.background = '#fbfcfe';
                            }}
                        }}
                    </script>
                {{{{% else %}}}}
                    <div class="empty-state">
                        <h3>当前没有待审核任务</h3>
                        <p>审核队列为空。</p>
                    </div>
                {{{{% endif %}}}}
            </div>
        </div>
    </div>
</body>
</html>
"""

HTML_TEMPLATE = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>realtime_rag_pipeline 控制台</title>
    {BASE_STYLES}
</head>
<body>
    <div class="shell">
        <div class="topbar">
            <div class="title-block">
                <h1>管道管理控制台</h1>
                <p>统一管理文件导入、任务状态和审核处理。</p>
            </div>
            <div class="topbar-meta">
                <div class="metric-chip">
                    <span class="label">当前任务</span>
                    <span class="value">{{{{ jobs|length }}}}</span>
                </div>
                <div class="metric-chip">
                    <span class="label">已暂停</span>
                    <span class="value">{{{{ jobs|selectattr('global_status', 'equalto', 'paused')|list|length }}}}</span>
                </div>
                <div class="metric-chip">
                    <span class="label">已完成</span>
                    <span class="value">{{{{ jobs|selectattr('global_status', 'equalto', 'completed')|list|length }}}}</span>
                </div>
            </div>
        </div>

        <div class="banner-stack">
            <div class="top-banner top-banner-info" id="statusBanner">
                <span id="statusBannerText"></span>
                <button type="button" id="statusBannerClose" aria-label="关闭">关闭</button>
            </div>
        </div>

        {{{{% with messages = get_flashed_messages(with_categories=true) %}}}}
            {{{{% if messages %}}}}
                <div class="flash-stack">
                {{{{% for category, message in messages %}}}}
                    <div class="flash flash-{{{{ category }}}}">{{{{ message }}}}</div>
                {{{{% endfor %}}}}
                </div>
            {{{{% endif %}}}}
        {{{{% endwith %}}}}

        <div class="layout">
            <div>
                <div class="panel">
                    <div class="panel-section">
                        <h2>文件导入</h2>
                        <p class="panel-subtitle">按文件类型自动路由到已启用管道。当前在线支持 PDF，其它格式会进入待路由队列。</p>
                        <form method="POST" action="/upload" enctype="multipart/form-data" id="uploadForm">
                            <div class="upload-area" id="dropZone" onclick="document.getElementById('fileInput').click()">
                                <strong>点击选择文件，或将文件拖拽到此处</strong>
                                <span id="uploadText">支持 PDF、DOCX、DOC、TXT、MD、PNG、JPG、JPEG。当前在线管道仅处理 PDF。</span>
                                <input type="file" name="files" id="fileInput" accept=".pdf,.docx,.doc,.txt,.md,.png,.jpg,.jpeg" multiple>
                            </div>
                            <button type="submit" class="btn" id="uploadSubmitButton">开始导入</button>
                        </form>
                    </div>
                </div>

                <div class="panel">
                    <div class="panel-section">
                        <h2>远程导入</h2>
                        <p class="panel-subtitle">系统会根据文件类型自动选择在线管道；未上线格式会先进入待路由队列。</p>
                        <form method="POST" action="/upload_url" id="urlUploadForm">
                            <div class="form-row">
                                <label for="url">文件地址</label>
                                <input class="text-input" type="url" name="url" id="url" placeholder="https://example.com/file.pdf" required>
                            </div>
                            <button type="submit" class="btn" id="urlSubmitButton">从地址导入</button>
                        </form>
                    </div>
                </div>

                <div class="panel">
                    <div class="panel-section">
                        <h2>审核队列</h2>
                        <p class="panel-subtitle">查看已暂停任务，处理人工审核后继续执行或直接驳回。</p>
                        <a href="/review" class="btn">打开审核队列</a>
                    </div>
                </div>
            </div>

            <div class="panel">
                <div class="content-panel-header">
                    <div>
                        <h2>任务状态</h2>
                        <p class="panel-subtitle">查看最近任务的处理状态、处理器进度、时间、错误信息，以及待路由文件。</p>
                    </div>
                    <div class="actions">
                        <div class="menu-wrap">
                            <button type="button" class="menu-trigger" id="maintenanceMenuButton" aria-label="任务菜单" title="任务菜单">
                                <span class="menu-trigger-bars" aria-hidden="true">
                                    <span></span>
                                    <span></span>
                                    <span></span>
                                </span>
                            </button>
                            <div class="menu-panel" id="maintenanceMenuPanel">
                                <button type="button" class="menu-action" data-action="/maintenance/discard-unsupported" data-title="丢弃所有待支持任务" data-message="该操作会移除所有当前未匹配到可用管道的待支持任务，并删除对应的暂存文件。">丢弃所有待支持任务</button>
                                <button type="button" class="menu-action" data-action="/maintenance/delete-failed" data-title="丢弃所有失败任务" data-message="该操作会从任务状态数据库中删除所有失败任务。">丢弃所有失败任务</button>
                                <button type="button" class="menu-action" data-action="/maintenance/hide-completed" data-title="清空已完成任务" data-message="该操作不会真正删除已完成任务，只会将其标记为隐藏，后续不再在前端展示。">清空已完成任务</button>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="job-list" id="jobList">
                    {{{{% if jobs %}}}}
                        {{{{% for job in jobs %}}}}
                            <div class="job-item">
                                <div class="job-info">
                                    <div class="job-name">{{{{ job.source_file or '未知来源' }}}}</div>
                                    <div class="job-id">任务 ID {{{{ job.run_id }}}}</div>
                                    <div class="job-meta">创建时间 {{{{ job.created_at }}}} · 更新时间 {{{{ job.updated_at }}}}</div>
                                    {{{{% if job.route_label %}}}}
                                        <div class="job-meta">目标管道 {{{{ job.route_label }}}}</div>
                                    {{{{% endif %}}}}
                                    <div class="stage-details">
                                        {{{{% for proc_name, proc_state in job.processor_states.items() %}}}}
                                            <div class="stage-item stage-{{{{ proc_state.status }}}}">
                                                {{{{ proc_name }}}}
                                                {{{{% if proc_state.cost > 0 %}}}}<span class="stage-cost">{{{{ "%.1f"|format(proc_state.cost) }}}}s</span>{{{{% endif %}}}}
                                            </div>
                                        {{{{% endfor %}}}}
                                    </div>
                                    {{{{% if job.error_message %}}}}
                                        <div class="error-box">{{{{ job.error_message }}}}</div>
                                    {{{{% endif %}}}}
                                </div>
                                <div>
                                    <span class="status-badge status-{{{{ job.status_class }}}}">{{{{ job.status_label }}}}</span>
                                </div>
                            </div>
                        {{{{% endfor %}}}}
                    {{{{% else %}}}}
                        <div class="empty-state">
                            <h3>当前没有任务</h3>
                            <p>新提交的文件会显示在这里。</p>
                        </div>
                    {{{{% endif %}}}}
                </div>
            </div>
        </div>
    </div>
    <div class="modal-overlay" id="confirmModal">
        <div class="modal-card">
            <h3 id="confirmModalTitle">确认操作</h3>
            <p id="confirmModalMessage"></p>
            <form method="POST" id="confirmModalForm">
                <div class="modal-actions">
                    <button type="button" class="btn btn-secondary btn-inline" id="confirmModalCancel">取消</button>
                    <button type="submit" class="btn btn-danger btn-inline">确认</button>
                </div>
            </form>
        </div>
    </div>
    <script>
        const statusBanner = document.getElementById('statusBanner');
        const statusBannerText = document.getElementById('statusBannerText');
        const statusBannerClose = document.getElementById('statusBannerClose');
        const dropZone = document.getElementById('dropZone');
        const fileInput = document.getElementById('fileInput');
        const uploadText = document.getElementById('uploadText');
        const uploadForm = document.getElementById('uploadForm');
        const urlUploadForm = document.getElementById('urlUploadForm');
        const uploadSubmitButton = document.getElementById('uploadSubmitButton');
        const urlSubmitButton = document.getElementById('urlSubmitButton');
        const jobList = document.getElementById('jobList');
        const maintenanceMenuButton = document.getElementById('maintenanceMenuButton');
        const maintenanceMenuPanel = document.getElementById('maintenanceMenuPanel');
        const confirmModal = document.getElementById('confirmModal');
        const confirmModalForm = document.getElementById('confirmModalForm');
        const confirmModalTitle = document.getElementById('confirmModalTitle');
        const confirmModalMessage = document.getElementById('confirmModalMessage');
        const confirmModalCancel = document.getElementById('confirmModalCancel');

        function escapeHtml(value) {{
            return String(value || '')
                .replaceAll('&', '&amp;')
                .replaceAll('<', '&lt;')
                .replaceAll('>', '&gt;')
                .replaceAll('"', '&quot;')
                .replaceAll("'", '&#39;');
        }}

        function showBanner(message) {{
            statusBannerText.textContent = message;
            statusBanner.classList.add('is-open');
        }}

        function hideBanner() {{
            statusBanner.classList.remove('is-open');
        }}

        function renderJobs(jobs) {{
            if (!jobs.length) {{
                jobList.innerHTML = `
                    <div class="empty-state">
                        <h3>当前没有任务</h3>
                        <p>新提交的文件会显示在这里。</p>
                    </div>
                `;
                return;
            }}
            jobList.innerHTML = jobs.map((job) => {{
                const stageEntries = Object.entries(job.processor_states || {{}});
                const stages = stageEntries.map(([name, state]) => {{
                    const cost = state.cost > 0 ? `<span class="stage-cost">${{Number(state.cost).toFixed(1)}}s</span>` : '';
                    return `<div class="stage-item stage-${{escapeHtml(state.status)}}">${{escapeHtml(name)}}${{cost}}</div>`;
                }}).join('');
                const routeLabel = job.route_label ? `<div class="job-meta">目标管道 ${{escapeHtml(job.route_label)}}</div>` : '';
                const errorBox = job.error_message ? `<div class="error-box">${{escapeHtml(job.error_message)}}</div>` : '';
                return `
                    <div class="job-item">
                        <div class="job-info">
                            <div class="job-name">${{escapeHtml(job.source_file || '未知来源')}}</div>
                            <div class="job-id">任务 ID ${{escapeHtml(job.run_id)}}</div>
                            <div class="job-meta">创建时间 ${{escapeHtml(job.created_at)}} · 更新时间 ${{escapeHtml(job.updated_at)}}</div>
                            ${{routeLabel}}
                            <div class="stage-details">${{stages}}</div>
                            ${{errorBox}}
                        </div>
                        <div>
                            <span class="status-badge status-${{escapeHtml(job.status_class)}}">${{escapeHtml(job.status_label)}}</span>
                        </div>
                    </div>
                `;
            }}).join('');
        }}

        async function refreshJobs() {{
            jobList.classList.add('is-loading');
            try {{
                const response = await fetch('/dashboard-data', {{ headers: {{ 'Accept': 'application/json' }} }});
                if (!response.ok) {{
                    throw new Error(`HTTP ${{response.status}}`);
                }}
                const payload = await response.json();
                renderJobs(payload.jobs || []);
            }} catch (error) {{
                console.error('刷新任务状态失败', error);
            }} finally {{
                jobList.classList.remove('is-loading');
            }}
        }}

        fileInput.addEventListener('change', () => {{
            if (fileInput.files.length > 0) {{
                const visibleNames = Array.from(fileInput.files).map(f => f.name).slice(0, 3).join(', ');
                uploadText.innerHTML = `已选择 ${{fileInput.files.length}} 个文件${{fileInput.files.length > 0 ? ` · ${{visibleNames}}${{fileInput.files.length > 3 ? ' ...' : ''}}` : ''}}`;
            }}
        }});

        uploadForm.addEventListener('submit', () => {{
            uploadSubmitButton.disabled = true;
            uploadSubmitButton.textContent = '提交中...';
            showBanner('正在提交文件，任务创建完成后会自动出现在下方状态列表。');
        }});

        urlUploadForm.addEventListener('submit', () => {{
            urlSubmitButton.disabled = true;
            urlSubmitButton.textContent = '提交中...';
            showBanner('正在提交地址，任务创建完成后会自动出现在下方状态列表。');
        }});

        dropZone.addEventListener('dragover', (e) => {{
            e.preventDefault();
            dropZone.style.borderColor = 'rgba(11, 87, 208, 0.35)';
            dropZone.style.background = 'linear-gradient(180deg, rgba(11, 87, 208, 0.08), rgba(11, 87, 208, 0.02)), #ffffff';
        }});

        dropZone.addEventListener('dragleave', (e) => {{
            e.preventDefault();
            dropZone.style.borderColor = 'rgba(19, 40, 67, 0.18)';
            dropZone.style.background = 'linear-gradient(180deg, rgba(11, 87, 208, 0.04), rgba(11, 87, 208, 0.01)), #ffffff';
        }});

        dropZone.addEventListener('drop', (e) => {{
            e.preventDefault();
            dropZone.style.borderColor = 'rgba(19, 40, 67, 0.18)';
            dropZone.style.background = 'linear-gradient(180deg, rgba(11, 87, 208, 0.04), rgba(11, 87, 208, 0.01)), #ffffff';
            if (e.dataTransfer.files.length > 0) {{
                fileInput.files = e.dataTransfer.files;
                fileInput.dispatchEvent(new Event('change'));
            }}
        }});

        maintenanceMenuButton.addEventListener('click', () => {{
            maintenanceMenuPanel.classList.toggle('is-open');
        }});

        document.addEventListener('click', (event) => {{
            if (!event.target.closest('.menu-wrap')) {{
                maintenanceMenuPanel.classList.remove('is-open');
            }}
        }});

        document.querySelectorAll('[data-action]').forEach((button) => {{
            button.addEventListener('click', () => {{
                maintenanceMenuPanel.classList.remove('is-open');
                confirmModalForm.action = button.dataset.action;
                confirmModalTitle.textContent = button.dataset.title || '确认操作';
                confirmModalMessage.textContent = button.dataset.message || '';
                confirmModal.classList.add('is-open');
            }});
        }});

        confirmModalCancel.addEventListener('click', () => {{
            confirmModal.classList.remove('is-open');
        }});

        confirmModal.addEventListener('click', (event) => {{
            if (event.target === confirmModal) {{
                confirmModal.classList.remove('is-open');
            }}
        }});

        statusBannerClose.addEventListener('click', () => {{
            hideBanner();
        }});

        setInterval(refreshJobs, 5000);
    </script>
</body>
</html>
"""

REVIEW_HTML_TEMPLATE = _normalize_template(REVIEW_HTML_TEMPLATE)
HTML_TEMPLATE = _normalize_template(HTML_TEMPLATE)


def _api_url(path: str) -> str:
    return f"{API_BASE_URL}{path}"


def _now_string() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _get_file_extension(name: str) -> str:
    suffix = Path(name).suffix.lower().lstrip(".")
    return suffix


def _resolve_router(filename: str) -> Dict[str, Any]:
    ext = _get_file_extension(filename)
    for router in PIPELINE_ROUTERS:
        if ext in router["extensions"]:
            return router
    return {}


def _load_deferred_queue() -> List[Dict[str, Any]]:
    if not DEFERRED_QUEUE_PATH.exists():
        return []
    try:
        return json.loads(DEFERRED_QUEUE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []


def _save_deferred_queue(items: List[Dict[str, Any]]) -> None:
    DEFERRED_QUEUE_PATH.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")


def _discard_deferred_by_status(status_label: str) -> int:
    items = _load_deferred_queue()
    kept: List[Dict[str, Any]] = []
    removed = 0
    for item in items:
        if item.get("status_label") == status_label:
            removed += 1
            stored_path = item.get("source_file", "")
            if stored_path and stored_path.startswith(str(DEFERRED_UPLOAD_DIR.resolve())):
                try:
                    Path(stored_path).unlink(missing_ok=True)
                except Exception:
                    pass
            continue
        kept.append(item)
    _save_deferred_queue(kept)
    return removed


def _enqueue_deferred_item(
    *,
    filename: str,
    route_label: str,
    reason: str,
    status_class: str,
    source_kind: str,
    stored_path: str = "",
    source_url: str = "",
) -> str:
    items = _load_deferred_queue()
    run_id = f"deferred-{uuid.uuid4().hex[:10]}"
    now = _now_string()
    items.insert(0, {
        "run_id": run_id,
        "source_file": stored_path or source_url or filename,
        "created_at": now,
        "updated_at": now,
        "route_label": route_label,
        "status_label": reason,
        "status_class": status_class,
        "processor_states": {},
        "error_message": "",
        "source_kind": source_kind,
        "filename": filename,
        "source_url": source_url,
    })
    _save_deferred_queue(items[:200])
    return run_id


def _status_meta(status: str) -> Dict[str, str]:
    mapping = {
        "running": {"label": "运行中", "class": "running"},
        "completed": {"label": "已完成", "class": "completed"},
        "failed": {"label": "失败", "class": "failed"},
        "paused": {"label": "待审核", "class": "paused"},
        "rejected": {"label": "已驳回", "class": "rejected"},
    }
    return mapping.get(status, {"label": status, "class": "queued"})


def _present_job(job: Dict[str, Any]) -> Dict[str, Any]:
    presented = dict(job)
    meta = _status_meta(job.get("global_status", ""))
    presented["status_label"] = meta["label"]
    presented["status_class"] = meta["class"]
    presented["route_label"] = "预设 4 PDF 管道"
    return presented


def _extract_error(response: requests.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return response.text or f"HTTP {response.status_code}"
    if isinstance(payload, dict):
        detail = payload.get("detail")
        if isinstance(detail, dict):
            return detail.get("message") or detail.get("errors") or str(detail)
        if detail:
            return str(detail)
        message = payload.get("message")
        if message:
            return str(message)
    return str(payload)


def _get_jobs() -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    try:
        response = requests.get(_api_url("/api/v1/runs"), params={"limit": 15}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        jobs = [_present_job(job) for job in payload.get("data", [])]
    except requests.RequestException as exc:
        flash(f"无法获取任务状态: {exc}", "error")
    deferred = _load_deferred_queue()
    combined = deferred + jobs
    combined.sort(key=lambda item: item.get("updated_at", ""), reverse=True)
    return combined[:30]


def _get_pending_reviews() -> List[Dict[str, Any]]:
    try:
        response = requests.get(_api_url("/api/v1/review/pending"), params={"limit": 20}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        return payload.get("data", [])
    except requests.RequestException as exc:
        flash(f"无法获取待审核任务: {exc}", "error")
        return []


@app.route("/")
def index():
    jobs = _get_jobs()
    return render_template_string(HTML_TEMPLATE, jobs=jobs)


@app.route("/dashboard-data")
def dashboard_data():
    return jsonify({"jobs": _get_jobs()})


@app.route("/review")
def review_page():
    records = _get_pending_reviews()
    return render_template_string(REVIEW_HTML_TEMPLATE, records=records)


@app.route("/review/action/<context_id>", methods=["POST"])
def review_action(context_id: str):
    action = request.form.get("action")
    if action in {"reject", "reject_all"}:
        try:
            response = requests.post(_api_url(f"/api/v1/review/reject/{context_id}"), timeout=REQUEST_TIMEOUT)
            if response.ok:
                flash("任务已驳回。", "success")
            else:
                flash(f"驳回失败: {_extract_error(response)}", "error")
        except requests.RequestException as exc:
            flash(f"驳回失败: {exc}", "error")
        return redirect(url_for("review_page"))

    keep_indices: List[int] = []
    chunks_override: Dict[str, str] = {}
    index = 0
    while True:
        keep_key = f"keep_{index}"
        chunk_key = f"chunk_{index}"
        if keep_key not in request.form and chunk_key not in request.form:
            break
        if request.form.get(keep_key) == "yes":
            keep_indices.append(index)
            chunks_override[str(index)] = request.form.get(chunk_key, "")
        index += 1

    payload: Dict[str, Any] = {
        "keep_indices": keep_indices,
        "chunks_override": chunks_override,
    }
    try:
        response = requests.post(_api_url(f"/api/v1/review/submit/{context_id}"), json=payload, timeout=REQUEST_TIMEOUT)
        if response.ok:
            flash("审核结果已提交。", "success")
        else:
            flash(f"提交审核失败: {_extract_error(response)}", "error")
    except requests.RequestException as exc:
        flash(f"提交审核失败: {exc}", "error")
    return redirect(url_for("review_page"))


@app.route("/upload", methods=["POST"])
def upload():
    files = request.files.getlist("files")
    if not files or files[0].filename == "":
        flash("请选择至少一个文件。", "error")
        return redirect(url_for("index"))

    success_count = 0
    deferred_count = 0
    for file in files:
        if not file or not file.filename:
            continue
        file.stream.seek(0)
        router = _resolve_router(file.filename)
        if not router:
            safe_name = Path(file.filename).name
            stored_path = DEFERRED_UPLOAD_DIR / f"{uuid.uuid4().hex[:8]}_{safe_name}"
            stored_path.write_bytes(file.read())
            deferred_count += 1
            _enqueue_deferred_item(
                filename=safe_name,
                route_label="未匹配到可用管道",
                reason="待支持",
                status_class="unsupported",
                source_kind="upload",
                stored_path=str(stored_path.resolve()),
            )
            continue
        if not router.get("enabled"):
            safe_name = Path(file.filename).name
            stored_path = DEFERRED_UPLOAD_DIR / f"{uuid.uuid4().hex[:8]}_{safe_name}"
            stored_path.write_bytes(file.read())
            deferred_count += 1
            _enqueue_deferred_item(
                filename=safe_name,
                route_label=router["label"],
                reason="待路由",
                status_class="deferred",
                source_kind="upload",
                stored_path=str(stored_path.resolve()),
            )
            continue
        try:
            response = requests.post(
                _api_url(router["upload_endpoint"]),
                files={"file": (file.filename, file.stream, file.mimetype or "application/octet-stream")},
                timeout=REQUEST_TIMEOUT,
            )
            if response.ok:
                success_count += 1
            else:
                flash(f"{file.filename} 上传失败: {_extract_error(response)}", "error")
        except requests.RequestException as exc:
            flash(f"{file.filename} 上传失败: {exc}", "error")

    if success_count:
        flash(f"已提交 {success_count} 个文件进入管道。", "success")
    if deferred_count:
        flash(f"有 {deferred_count} 个文件已进入待路由队列。", "success")
    return redirect(url_for("index"))


@app.route("/upload_url", methods=["POST"])
def upload_url():
    target_url = request.form.get("url")
    if not target_url:
        flash("请输入可访问的文件 URL。", "error")
        return redirect(url_for("index"))

    parsed_name = Path(urlparse(target_url).path).name or f"remote_{uuid.uuid4().hex[:6]}"
    router = _resolve_router(parsed_name)
    if not router:
        _enqueue_deferred_item(
            filename=parsed_name,
            route_label="未匹配到可用管道",
            reason="待支持",
            status_class="unsupported",
            source_kind="url",
            source_url=target_url,
        )
        flash("该格式当前没有可用管道，已加入待路由队列。", "success")
        return redirect(url_for("index"))
    if not router.get("enabled"):
        _enqueue_deferred_item(
            filename=parsed_name,
            route_label=router["label"],
            reason="待路由",
            status_class="deferred",
            source_kind="url",
            source_url=target_url,
        )
        flash("目标管道当前未启用，任务已加入待路由队列。", "success")
        return redirect(url_for("index"))

    try:
        response = requests.post(
            _api_url(router["url_endpoint"]),
            json={"url": target_url},
            timeout=REQUEST_TIMEOUT,
        )
        if response.ok:
            flash("URL 文件已提交进入管道。", "success")
        else:
            flash(f"URL 入库失败: {_extract_error(response)}", "error")
    except requests.RequestException as exc:
        flash(f"URL 入库失败: {exc}", "error")
    return redirect(url_for("index"))


@app.route("/maintenance/discard-unsupported", methods=["POST"])
def discard_unsupported():
    removed = _discard_deferred_by_status("待支持")
    flash(f"已丢弃 {removed} 个待支持任务。", "success")
    return redirect(url_for("index"))


@app.route("/maintenance/delete-failed", methods=["POST"])
def delete_failed():
    try:
        response = requests.post(_api_url("/api/v1/runs/actions/delete-failed"), timeout=REQUEST_TIMEOUT)
        if response.ok:
            deleted_count = response.json().get("deleted_count", 0)
            flash(f"已丢弃 {deleted_count} 个失败任务。", "success")
        else:
            flash(f"丢弃失败任务失败: {_extract_error(response)}", "error")
    except requests.RequestException as exc:
        flash(f"丢弃失败任务失败: {exc}", "error")
    return redirect(url_for("index"))


@app.route("/maintenance/hide-completed", methods=["POST"])
def hide_completed():
    try:
        response = requests.post(_api_url("/api/v1/runs/actions/hide-completed"), timeout=REQUEST_TIMEOUT)
        if response.ok:
            hidden_count = response.json().get("hidden_count", 0)
            flash(f"已隐藏 {hidden_count} 个已完成任务。", "success")
        else:
            flash(f"隐藏已完成任务失败: {_extract_error(response)}", "error")
    except requests.RequestException as exc:
        flash(f"隐藏已完成任务失败: {exc}", "error")
    return redirect(url_for("index"))


if __name__ == "__main__":
    print("API 驱动型 GUI 启动: http://0.0.0.0:5001")
    app.run(host="0.0.0.0", port=5001, debug=False)
