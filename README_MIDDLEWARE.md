# 知识库中间层说明

## 目标

本中间层负责承接后端发来的文件入库/删除请求，并把业务请求转换为当前文档解析与向量入库链路可以稳定执行的任务。

当前只面向两类业务动作：

- 根据 `knowledge_base_file_guid` 和 `file_guid` 发起入库
- 根据 `knowledge_base_file_guid` 发起向量物理删除

本中间层不直接依赖后端业务数据库，不直接由后端上传文件，也不把业务逻辑耦合进 Pipeline。

## 核心设计

### 1. API 与执行解耦

API 进程只负责：

- 接收请求
- 参数校验
- 生成持久化任务
- 返回受理结果

真正执行任务的是独立 worker 进程。这样可以避免：

- API 请求超时
- 进程重启导致任务丢失
- 高并发时大量内存态后台任务堆积

### 2. 持久化任务队列

任务和回调都持久化到 SQLite：

- `logs/job_tasks.db`

其中包含两张表：

- `job_tasks`：入库/删除主任务
- `callback_outbox`：状态回调投递任务

支持：

- 去重
- 抢占执行
- 失败重试
- 重启恢复

### 3. 文件下载服务独立实现

下载逻辑参考开放平台文件下载协议，但没有直接调用别的项目代码。

当前实现能力：

- HMAC-SHA256 鉴权
- 本地缓存
- 文件锁
- 原子落盘
- 错误分类重试
- 429 限流识别
- JSON 错误体解析

### 4. Pipeline 适配层独立

业务任务不会直接进入 Pipeline。

中间层会先把下载后的文件转成标准 `DocumentEvent`，并固定写入：

- `document_id = knowledge_base_file_guid`
- `metadata.knowledge_base_file_guid`
- `metadata.file_guid`

这样删除和回查都可以稳定按 `knowledge_base_file_guid` 闭环。

### 5. 删除按物理删除实现

删除不会做逻辑删除标记。

当前做法：

- 直接在 Milvus 中按 `document_id == knowledge_base_file_guid` 执行删除

## 目录与职责

核心文件：

- [services/job_store.py](/Users/regen-bio/Documents/New%20project/services/job_store.py)
  任务存储与 outbox
- [services/file_download_service.py](/Users/regen-bio/Documents/New%20project/services/file_download_service.py)
  文件下载与缓存
- [services/ingest_pipeline_service.py](/Users/regen-bio/Documents/New%20project/services/ingest_pipeline_service.py)
  业务入库到 Pipeline 的适配层
- [services/milvus_delete_service.py](/Users/regen-bio/Documents/New%20project/services/milvus_delete_service.py)
  Milvus 物理删除
- [services/status_callback_service.py](/Users/regen-bio/Documents/New%20project/services/status_callback_service.py)
  状态回调投递
- [services/job_worker.py](/Users/regen-bio/Documents/New%20project/services/job_worker.py)
  独立 worker
- [streaming/api_server.py](/Users/regen-bio/Documents/New%20project/streaming/api_server.py)
  对外业务接口
- [main.py](/Users/regen-bio/Documents/New%20project/main.py)
  启动入口

## 启动方式

### API

```bash
python main.py serve --config configs/pipeline_preset5.yaml --host 0.0.0.0 --port 8011
```

### Worker

```bash
python main.py worker --config configs/pipeline_preset5.yaml
```

生产环境建议 API 与 worker 分开守护。

## 关键环境变量

### 文件下载

- `FILE_DOWNLOAD_BASE_URL`
- `FILE_APP_ID`
- `FILE_APP_SECRET`
- `FILE_STORAGE_DIR`
- `FILE_CONNECT_TIMEOUT_SECONDS`
- `FILE_TOTAL_TIMEOUT_SECONDS`
- `FILE_DOWNLOAD_MAX_RETRIES`

### 回调

- `KB_STATUS_SUCCESS_CALLBACK_URL_TEMPLATE`
- `KB_STATUS_FAIL_CALLBACK_URL_TEMPLATE`

### 任务库

- `JOB_STORE_DB_PATH`

### Milvus

- `MILVUS_HOST`
- `MILVUS_PORT`
- `MILVUS_COLLECTION`

## 当前接口

### 业务接口

- `POST /api/v1/kb-files/ingest`
- `DELETE /api/v1/kb-files/{knowledge_base_file_guid}`

### 运维接口

- `GET /api/v1/tasks`
- `GET /api/v1/tasks/{task_id}`
- `GET /health`

## 当前状态

已完成：

- 中间层任务模型
- 下载服务
- 业务入库接口
- 删除接口
- worker 执行链路
- 任务状态查询

已验证：

- 失败路径联调
- 成功路径联调

当前回调协议：

- 成功：`PATCH http://172.20.8.38:17000/app-api/knowledge-base-file/status-success/{guid}`
- 失败：`PATCH http://172.20.8.38:17000/app-api/knowledge-base-file/status-fail/{guid}`

说明：

- `{guid}` 对应 `knowledge_base_file_guid`
- 当前回调不发送 JSON 请求体，只通过路径参数告知后端
- 回调请求会携带与文件下载接口相同的开放平台 HMAC 鉴权头

后续建议增强：

- 任务执行 heartbeat
- 删除接口的联调样例与验收脚本
