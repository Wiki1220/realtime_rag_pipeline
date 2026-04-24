# realtime_rag_pipeline

`realtime_rag_pipeline` 是一个面向知识库文档处理的服务化项目，包含两层能力：

- 管道层：负责 OCR、切块、内容过滤、向量化和向量入库
- 中间层：负责接收后端任务、下载文件、驱动管道、回调处理结果

当前线上主链路运行在 `/data/realtime_rag_pipeline`，使用 PM2 托管 API 和 worker。

## 架构

整体链路：

1. 后端调用中间层入库接口，传入 `knowledge_base_file_guid` 和 `file_guid`
2. 中间层下载原文件，生成受控落盘文件名
3. 中间层调用管道处理本地文件
4. 管道完成 OCR、切块、过滤、Embedding、Milvus 入库
5. 中间层通过 HTTP 回调后端成功或失败状态

核心目录：

- `configs/`：管道配置
- `core/`：上下文、状态、管道执行核心
- `processors/`：各处理算子
- `services/`：中间层服务与任务调度
- `streaming/`：API 服务与 Web 入口
- `deploy/pm2/`：线上 PM2 托管配置
- `tests/`：单测

## 启停

### 本地开发

API：

```bash
python main.py serve --config configs/pipeline_preset5.yaml --host 0.0.0.0 --port 8011
```

Worker：

```bash
python main.py worker --config configs/pipeline_preset5.yaml
```

### 线上运行

当前线上使用 PM2：

```bash
pm2 start deploy/pm2/ecosystem.config.js
pm2 status
pm2 restart realtime-rag-api
pm2 restart realtime-rag-worker
pm2 logs realtime-rag-api
pm2 logs realtime-rag-worker
```

机器重启后通过 `crontab` 执行 `pm2 resurrect` 恢复进程。

### 同步到服务器

```bash
./scripts/sync_to_server.sh
```

默认同步目标目录是：

```bash
/data/realtime_rag_pipeline
```

## 主要 API

### 中间层 API

入库：

```http
POST /api/v1/kb-files/ingest
Content-Type: application/json

{
  "knowledge_base_file_guid": "xxx",
  "file_guid": "xxx"
}
```

删除：

```http
DELETE /api/v1/kb-files/{knowledge_base_file_guid}
```

查询任务：

```http
GET /api/v1/tasks/{task_id}
GET /api/v1/tasks?knowledge_base_file_guid=xxx&limit=10
```

健康检查：

```http
GET /health
```

### 后端状态回调

当前中间层会主动回调后端：

- 成功：`PATCH /app-api/knowledge-base-file/status-success/{guid}`
- 失败：`PATCH /app-api/knowledge-base-file/status-fail/{guid}`

其中 `{guid}` 对应 `knowledge_base_file_guid`。

## 关键约定

- `document_id = knowledge_base_file_guid`
- `file_guid` 只用于下载文件
- `stored_filename` 是系统受控文件名：`{knowledge_base_file_guid}_{file_guid}.{ext}`
- `source_file` 是来源文件名，当前会统一写成 `《文件名》`
- 删除操作直接按 `document_id` 对 Milvus 做物理删除

## 管道说明

当前线上主配置是：

- `configs/pipeline_preset5.yaml`

主流程包含：

1. `MinerUOCRProcessor`
2. `HybridAdvancedChunkerProcessor`
3. `MultiLLMFilterProcessor`
4. `YuanEmbedderProcessor`
5. `MilvusPersonalLoader`

中间层不会直接处理业务逻辑，只负责：

- 任务受理
- 文件下载
- 管道驱动
- 状态回调

## 关键环境变量

下载与回调：

- `FILE_DOWNLOAD_BASE_URL`
- `FILE_APP_ID`
- `FILE_APP_SECRET`
- `KB_STATUS_SUCCESS_CALLBACK_URL_TEMPLATE`
- `KB_STATUS_FAIL_CALLBACK_URL_TEMPLATE`

Milvus：

- `MILVUS_HOST`
- `MILVUS_PORT`

任务执行：

- `PIPELINE_CONFIG`
- `JOB_STORE_DB_PATH`
- `JOB_WORKER_SLEEP_SECONDS`
- `JOB_LEASE_SECONDS`
- `JOB_HEARTBEAT_SECONDS`

## 运维事项

日志与状态：

- PM2 状态：`pm2 status`
- API 健康检查：`curl http://127.0.0.1:8011/health`
- 任务库：`logs/job_tasks.db`
- 管道状态库：`logs/pipeline_states.db`
- 待审核库：`logs/pending_reviews.db`

排障优先顺序：

1. 先看 PM2 里 `realtime-rag-api` 和 `realtime-rag-worker` 是否在线
2. 再看 `8011`、`8000`、`8050` 端口是否监听
3. 再查 `job_tasks.db` 中任务是否为 `pending/running/failed`
4. 再看外部依赖：
   - 文件下载服务
   - MinerU OCR 服务
   - LLM 接口
   - Milvus

目录清理原则：

- `data/uploads/`、`data/file_cache/`、`data_process/result/` 属于运行态目录，不进 Git
- 历史脚本和旧模块统一归档到 `_archive/`
- 线上运行方式以 PM2 为准，不再使用手工 `nohup`

## 相关文档

- 中间层说明：[README_MIDDLEWARE.md](README_MIDDLEWARE.md)
- 后端对接文档：`configs/后端知识库中间层API文档.md`
