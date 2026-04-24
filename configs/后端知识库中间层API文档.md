# 后端知识库中间层 API 文档

本文档用于后端系统对接当前知识库中间层服务。

## 1. 服务说明

中间层职责：

- 接收后端发起的知识库文件入库请求
- 根据 `file_guid` 调开放平台文件下载接口获取文件
- 调用当前文档解析与向量入库链路
- 根据 `knowledge_base_file_guid` 触发 Milvus 物理删除

中间层不要求后端上传文件，不要求后端提供本地路径。

## 2. 字段语义

### `knowledge_base_file_guid`

知识库文件主键。

中间层内部会把它同时作为：

- 业务主键
- 任务主键的业务维度
- Milvus `document_id`

### `file_guid`

文件主键。

仅用于调用开放平台文件下载接口获取原始文件。

## 3. 接口清单

### 3.1 发起入库

`POST /api/v1/kb-files/ingest`

请求头：

- `Content-Type: application/json`

请求体：

```json
{
  "knowledge_base_file_guid": "kb-file-guid-001",
  "file_guid": "file-guid-001"
}
```

成功响应示例：

```json
{
  "status": "accepted",
  "message": "KB file ingest task accepted.",
  "data": {
    "task_id": "ingest-xxxxxxxx",
    "job_type": "ingest",
    "knowledge_base_file_guid": "kb-file-guid-001",
    "file_guid": "file-guid-001",
    "status": "pending",
    "retry_count": 0,
    "max_retries": 3,
    "error_message": null,
    "result": {},
    "created_at": 1776075410.10994,
    "updated_at": 1776075410.10994
  }
}
```

说明：

- `accepted` 表示任务已落库，不表示已经完成入库
- 同一个 `knowledge_base_file_guid` 的同类型任务会做去重

### 3.2 发起删除

`DELETE /api/v1/kb-files/{knowledge_base_file_guid}`

示例：

```bash
curl -X DELETE http://127.0.0.1:8011/api/v1/kb-files/kb-file-guid-001
```

成功响应示例：

```json
{
  "status": "accepted",
  "message": "KB file delete task accepted.",
  "data": {
    "task_id": "delete-xxxxxxxx",
    "job_type": "delete",
    "knowledge_base_file_guid": "kb-file-guid-001",
    "file_guid": null,
    "status": "pending",
    "retry_count": 0,
    "max_retries": 0,
    "error_message": null,
    "result": {},
    "created_at": 1776075410.10994,
    "updated_at": 1776075410.10994
  }
}
```

说明：

- 删除是异步任务
- 删除语义是 Milvus 物理删除

### 3.3 查询任务状态

`GET /api/v1/tasks/{task_id}`

成功响应示例：

```json
{
  "status": "success",
  "data": {
    "task_id": "ingest-xxxxxxxx",
    "job_type": "ingest",
    "knowledge_base_file_guid": "kb-file-guid-001",
    "file_guid": "file-guid-001",
    "status": "success",
    "retry_count": 0,
    "max_retries": 3,
    "error_message": null,
    "result": {
      "knowledge_base_file_guid": "kb-file-guid-001",
      "file_guid": "file-guid-001",
      "run_id": "run-xxxxxxxx",
      "doc_id": "kb-file-guid-001",
      "file_path": "/data/realtime_rag_pipeline/data/uploads/xxx.pdf",
      "pipeline_result": {
        "status": "success",
        "run_id": "run-xxxxxxxx",
        "doc_id": "kb-file-guid-001",
        "message": "Data processed and stored successfully."
      }
    },
    "created_at": 1776075410.10994,
    "updated_at": 1776075523.5293221
  }
}
```

### 3.4 查询任务列表

`GET /api/v1/tasks`

可选参数：

- `status`
- `limit`

示例：

```bash
curl 'http://127.0.0.1:8011/api/v1/tasks?status=running&limit=20'
```

## 4. 任务状态说明

业务任务状态：

- `pending`
  任务已受理，等待 worker 执行
- `running`
  任务正在执行
- `success`
  任务已成功完成
- `failed`
  任务已最终失败，不再继续重试

## 5. 错误语义

### 入库失败

如果下载失败、解析失败、切块失败、模型调用失败或 Milvus 入库失败，任务会进入：

- `pending`
  如果是可重试错误，等待重试
- `failed`
  如果是最终失败或不可重试错误

可通过 `error_message` 查看失败原因。

### 下载错误分类

当前下载层错误策略：

- `400/401/403/404`
  不重试，直接判失败
- `429/500/502/503/504`
  可重试
- 网络超时/连接异常
  可重试

## 6. 回调说明

中间层在任务最终完成后，会主动回调后端状态接口。

### 6.1 入库成功回调

`PATCH http://172.20.8.38:17000/app-api/knowledge-base-file/status-success/{guid}`

说明：

- `{guid}` = `knowledge_base_file_guid`
- 不发送 JSON 请求体
- 请求头会携带开放平台 HMAC 鉴权参数

### 6.2 入库失败回调

`PATCH http://172.20.8.38:17000/app-api/knowledge-base-file/status-fail/{guid}`

说明：

- `{guid}` = `knowledge_base_file_guid`
- 不发送 JSON 请求体
- 请求头会携带开放平台 HMAC 鉴权参数

### 6.3 部署配置

默认配置：

- `KB_STATUS_SUCCESS_CALLBACK_URL_TEMPLATE=http://172.20.8.38:17000/app-api/knowledge-base-file/status-success/{guid}`
- `KB_STATUS_FAIL_CALLBACK_URL_TEMPLATE=http://172.20.8.38:17000/app-api/knowledge-base-file/status-fail/{guid}`

联调阶段如果后端希望人工核对，也仍然可以：

- 发起任务
- 保存 `task_id`
- 轮询 `/api/v1/tasks/{task_id}`

## 7. 联调建议

### 入库联调顺序

1. 后端调用 `POST /api/v1/kb-files/ingest`
2. 保存返回的 `task_id`
3. 轮询 `GET /api/v1/tasks/{task_id}`
4. 等待中间层回调成功/失败状态，或轮询到 `status=success` / `status=failed`

### 删除联调顺序

1. 后端调用 `DELETE /api/v1/kb-files/{knowledge_base_file_guid}`
2. 保存返回的 `task_id`
3. 轮询 `GET /api/v1/tasks/{task_id}`
4. 等待 `status=success` 或 `status=failed`

## 8. 当前验证结果

当前版本已完成：

- 下载接口对接
- 任务入库主链路
- Milvus 删除主链路
- 失败重试
- 任务状态查询

已验证：

- 失败路径联调
- 成功路径联调
