#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
realtime_rag_pipeline 一站式命令行启动工具 (Unified Entrypoint)
通过这一个脚本，您可以：
1. 启动常驻后台的 API 服务集群
2. 触发一次性的本地 PDF 跑批清洗任务
3. 跑一边本地仿真测试
"""

import os
import argparse
import sys
import uvicorn
import logging
from typing import Optional

# 禁用相对路径可能引发的深坑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.logger import setup_logger
logger = setup_logger("Main")

def start_api_server(config_path: str, host: str, port: int):
    """一站式启动 API Gateway"""
    logger.info(f"🚀 Starting API Webhook Server using config: {config_path}")
    # 注入全局环境变量，供 api_server.py 读取
    os.environ["PIPELINE_CONFIG"] = config_path
    
    # 强制在当前控制台启动 uvicorn
    # 注意这边的第一个参数是 "streaming.api_server:app"
    uvicorn.run("streaming.api_server:app", host=host, port=port, reload=False)

def start_pdf_batch(config_path: str, target_dir: str):
    """一站式启动 PDF 批处理发报机"""
    logger.info(f"📄 Starting local PDF batch ingestion on: {target_dir}")
    from streaming.pdf_feeder import PdfFeeder
    feeder = PdfFeeder(config_path=config_path)
    feeder.feed_directory(target_dir)

def start_simulation(config_path: str):
    """跑核心管线的 Mock 测试流"""
    logger.info(f"🧪 Starting simulation dummy pipeline run using: {config_path}")
    import yaml
    from core.context import Context
    from core.pipeline import Pipeline
    
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config {config_path}: {e}")
        return

    steps = cfg.get("pipeline_steps", [])
    pipeline_name = os.path.basename(config_path)
    pipeline = Pipeline(steps, pipeline_name=pipeline_name)
    
    mock_dirty_data = {
        "id": "mock-001",
        "file_path": "dummy.pdf"
    }
    
    logger.info("=" * 40)
    logger.info(f"Incoming Event: {mock_dirty_data}")
    logger.info("=" * 40)
    
    ctx = Context(raw_data=mock_dirty_data)
    try:
        final_ctx = pipeline.run(ctx)
        logger.info("=" * 40)
        if getattr(final_ctx, 'is_pending_review', False):
            logger.info("⏸️ Pipeline paused for human review! (Simulation Success)")
        elif final_ctx.is_valid:
            logger.info("✅ Pipeline processed successfully to the end!")
            logger.info(f"Extracted Metadata Keys: {list(final_ctx.metadata.keys())}")
        else:
            logger.warning(f"❌ Pipeline aborted. Errors: {final_ctx.errors}")
    except KeyboardInterrupt:
        logger.warning("Pipeline Simulation interrupted by user.")


def start_job_worker(config_path: str):
    logger.info(f"🧰 Starting persistent job worker using config: {config_path}")
    os.environ["PIPELINE_CONFIG"] = config_path
    from services.job_worker import run_worker_forever

    run_worker_forever()


def main():
    parser = argparse.ArgumentParser(description="realtime_rag_pipeline 统一中控台")
    
    # 子命令结构
    subparsers = parser.add_subparsers(dest="command", help="选择并启动系统中的某个指定微服务")
    
    # ==========================================
    # 命令 1: serve (启动核心 Webhook)
    # ==========================================
    parser_serve = subparsers.add_parser("serve", help="启动 FastAPI 实时数据提交流水线后台与人工审核网关")
    parser_serve.add_argument("-c", "--config", default="configs/pipeline_legacy_enhanced.yaml", help="指定 YAML 配置文件路径")
    parser_serve.add_argument("--host", default="0.0.0.0", help="监听地址")
    parser_serve.add_argument("--port", type=int, default=8000, help="监听端口 (默认 8000)")

    # ==========================================
    # 命令 2: batch_pdf (扫 PDF 目录入库)
    # ==========================================
    parser_pdf = subparsers.add_parser("batch_pdf", help="离线任务：批量将目标文件夹中的所有 PDF 抽取并入库")
    parser_pdf.add_argument("--dir", required=True, help="要扫描入库的本地绝对路径文件夹")
    parser_pdf.add_argument("-c", "--config", default="configs/pipeline_legacy_enhanced.yaml", help="默认使用预设4配置")

    # ==========================================
    # 命令 3: test (纯本地 Mock 运转测试)
    # ==========================================
    parser_test = subparsers.add_parser("test", help="测试环境能否不崩盘，执行一遍基础流")
    parser_test.add_argument("-c", "--config", default="configs/pipeline_legacy_enhanced.yaml", help="配置文件")

    # ==========================================
    # 命令 4: worker (启动持久化任务 Worker)
    # ==========================================
    parser_worker = subparsers.add_parser("worker", help="启动业务入库与删除任务 Worker")
    parser_worker.add_argument("-c", "--config", default="configs/pipeline_legacy_enhanced.yaml", help="指定 YAML 配置文件路径")

    args = parser.parse_args()

    # 路由选择分发
    if args.command == "serve":
        start_api_server(args.config, args.host, args.port)
    elif args.command == "batch_pdf":
        start_pdf_batch(args.config, args.dir)
    elif args.command == "test":
        start_simulation(args.config)
    elif args.command == "worker":
        start_job_worker(args.config)
    else:
        parser.print_help()
        print("\n\n请在末尾加上你需要的服务名。例如：")
        print("python main.py serve --config configs/pipeline_legacy_enhanced.yaml")
        sys.exit(1)

if __name__ == "__main__":
    main()
