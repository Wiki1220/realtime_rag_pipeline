# Preset 4 API Usage Without Frontend

本文档说明在不使用 Web 前端页面的情况下，如何直接调用预设 4 `configs/pipeline_legacy_enhanced.yaml` 对应的后端 API 完成任务。

适用服务：

- API 服务地址：`http://127.0.0.1:8000`
- 健康检查：`GET /health`

如果你当前通过本地 SSH 隧道访问服务器，请把下面示例里的主机统一理解为：

```bash
http://127.0.0.1:8000
```

如果你在服务器本机调用，也可以使用：

```bash
http://127.0.0.1:8000
```

## 1. 先确认服务可用

```bash
curl http://127.0.0.1:8000/health
```

预期返回：

```json
{"status":"ok","service":"realtime_rag_pipeline API Gateway"}
```

## 2. 预设 4 的执行特点

预设 4 包含 `HumanReviewPause`，因此任务不会自动一路跑到底。

标准流程是：

1. 调用入库接口启动任务
2. 任务运行到人工审核节点后返回 `pending_review`
3. 调用待审核列表接口获取待处理任务
4. 调用审核提交接口恢复执行，或者调用驳回接口结束任务

这意味着：

- 没有前端页面时，仍然可以通过 API 完成整条链路
- 但你需要自己准备输入文件，并自己处理审核阶段的数据修改与回提

## 3. 输入数据要求

当前 API 现在支持三种入参方式：

- 直接传服务器本地文件路径
- 通过 API 上传文件
- 通过 API 传 URL，让服务端下载文件

如果你选择“本地文件路径”模式，调用方仍然需要先确保目标文件已经存在于服务器本地。

最常见的输入方式是：

```json
{
  "id": "doc-001",
  "metadata": {
    "source": "manual-api"
  },
  "raw_data": {
    "file_path": "/home/wuteng/realtime_rag_pipeline/data/uploads/example.pdf",
    "data_type": "document"
  }
}
```

关键字段：

- `id`：任务唯一标识，建议由调用方生成
- `raw_data.file_path`：服务器上的文件绝对路径
- `raw_data.data_type`：建议传 `document`
- `metadata`：可选，附加业务元数据

## 4. 方式一：使用服务器本地文件路径触发任务

接口：

- `POST /api/v1/ingest/sync`

特点：

- 请求会阻塞到任务完成、失败，或进入人工审核挂起
- 对预设 4 来说，通常会返回 `pending_review`

示例：

```bash
curl -X POST http://127.0.0.1:8000/api/v1/ingest/sync \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "doc-001",
    "metadata": {
      "source": "manual-api"
    },
    "raw_data": {
      "file_path": "/home/wuteng/realtime_rag_pipeline/data/uploads/example.pdf",
      "data_type": "document"
    }
  }'
```

可能返回 1：进入人工审核

```json
{
  "status": "pending_review",
  "message": "Data requires human review. Saved to pending queue.",
  "id": "doc-001"
}
```

可能返回 2：直接成功

```json
{
  "status": "success",
  "message": "Data processed and stored successfully.",
  "id": "doc-001",
  "metadata_snapshot": {}
}
```

## 5. 方式二：通过 API 上传文件

接口：

- `POST /api/v1/files/upload`

特点：

- 使用 `multipart/form-data`
- 上传文件后会自动异步启动管道
- 返回 `id`，用于后续查询状态和审核

表单字段：

- `file`：文件本体，必填
- `id`：任务 ID，可选
- `metadata_json`：JSON 字符串，可选

示例：

```bash
curl -X POST http://127.0.0.1:8000/api/v1/files/upload \
  -F 'file=@/path/to/example.pdf' \
  -F 'id=doc-upload-001' \
  -F 'metadata_json={"source":"api-upload"}'
```

返回示例：

```json
{
  "status": "accepted",
  "message": "File uploaded and pipeline processing started in background.",
  "id": "doc-upload-001",
  "file_path": "/home/wuteng/realtime_rag_pipeline/data/uploads/example.pdf",
  "filename": "example.pdf"
}
```

## 6. 方式三：通过 URL 触发任务

接口：

- `POST /api/v1/files/from-url`

特点：

- 由服务端下载远程文件
- 下载成功后自动异步启动管道

示例：

```bash
curl -X POST http://127.0.0.1:8000/api/v1/files/from-url \
  -H 'Content-Type: application/json' \
  -d '{
    "url": "https://example.com/example.pdf",
    "id": "doc-url-001",
    "metadata": {
      "source": "api-url"
    }
  }'
```

## 7. 异步触发任务

接口：

- `POST /api/v1/ingest/async`

特点：

- 接口立即返回 `accepted`
- 后台线程继续执行
- 后续需要通过待审核接口检查是否进入 `pending_review`

示例：

```bash
curl -X POST http://127.0.0.1:8000/api/v1/ingest/async \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "doc-002",
    "metadata": {
      "source": "manual-api"
    },
    "raw_data": {
      "file_path": "/home/wuteng/realtime_rag_pipeline/data/uploads/example.pdf",
      "data_type": "document"
    }
  }'
```

预期返回：

```json
{
  "status": "accepted",
  "message": "Pipeline processing started in background."
}
```

## 8. 查看任务状态

接口：

- `GET /api/v1/runs`
- `GET /api/v1/runs/{run_id}`

用途：

- 查看最近任务列表
- 查询单个任务的详细状态
- 获取全局状态、各处理器状态、错误信息、更新时间

示例 1：查看最近任务

```bash
curl "http://127.0.0.1:8000/api/v1/runs?limit=10"
```

返回结构示例：

```json
{
  "status": "success",
  "count": 1,
  "data": [
    {
      "run_id": "doc-upload-001",
      "source_file": "/home/wuteng/realtime_rag_pipeline/data/uploads/example.pdf",
      "global_status": "paused",
      "pipeline_name": "pipeline_legacy_enhanced.yaml",
      "is_pending_review": true,
      "processor_states": {
        "MinerUOCRProcessor": {"status": "success", "cost": 4.2},
        "HybridAdvancedChunkerProcessor": {"status": "success", "cost": 0.1},
        "MultiLLMFilterProcessor": {"status": "success", "cost": 11.8},
        "HumanReviewPause": {"status": "success", "cost": 0.0},
        "YuanEmbedderProcessor": {"status": "pending", "cost": 0.0},
        "MilvusTypeLoader": {"status": "pending", "cost": 0.0}
      }
    }
  ]
}
```

示例 2：查看单个任务

```bash
curl http://127.0.0.1:8000/api/v1/runs/doc-upload-001
```

## 9. 查看待审核任务

接口：

- `GET /api/v1/review/pending`

用途：

- 获取所有被 `HumanReviewPause` 挂起的任务
- 返回任务 ID、创建时间、原始输入和 `metadata`
- `metadata` 中通常会包含 `chunks`，用于人工审核和回提

示例：

```bash
curl "http://127.0.0.1:8000/api/v1/review/pending?limit=20"
```

返回结构示例：

```json
{
  "status": "success",
  "count": 1,
  "data": [
    {
      "id": "doc-001",
      "created_at": "2026-03-10 09:00:00",
      "raw_data": {
        "file_path": "/home/wuteng/realtime_rag_pipeline/data/uploads/example.pdf",
        "data_type": "document"
      },
      "metadata": {
        "chunks": [
          {
            "text": "chunk text 1"
          },
          {
            "text": "chunk text 2"
          }
        ]
      }
    }
  ]
}
```

## 10. 审核后恢复执行

接口：

- `POST /api/v1/review/submit/{context_id}`

用途：

- 将挂起任务从待审核队列中取出
- 合并你提交的修改
- 从 `HumanReviewPause` 之后继续执行后续步骤

请求体可包含：

- `raw_data`：覆盖或补充原始输入
- `metadata`：覆盖或补充上下文元数据
- `chunks_override`：按下标覆盖某些 chunk 文本
- `keep_indices`：仅保留这些 chunk
- `drop_indices`：删除这些 chunk
- 不需要修改时也可以传空对象

最常见用法是直接覆盖 `metadata.chunks`

示例：

```bash
curl -X POST http://127.0.0.1:8000/api/v1/review/submit/doc-001 \
  -H 'Content-Type: application/json' \
  -d '{
    "metadata": {
      "chunks": [
        {
          "text": "人工修订后的 chunk 1"
        },
        {
          "text": "人工修订后的 chunk 2"
        }
      ]
    }
  }'
```

如果恢复后成功，可能返回：

```json
{
  "status": "success",
  "message": "Data processed and stored successfully.",
  "id": "doc-001",
  "metadata_snapshot": {}
}
```

如果恢复后仍再次进入审核，可能返回：

```json
{
  "status": "pending_review",
  "message": "Data requires human review. Saved to pending queue.",
  "id": "doc-001"
}
```

只保留指定 chunk 的示例：

```bash
curl -X POST http://127.0.0.1:8000/api/v1/review/submit/doc-001 \
  -H 'Content-Type: application/json' \
  -d '{
    "keep_indices": [0, 1, 3],
    "chunks_override": {
      "1": "人工修订后的第二块"
    }
  }'
```

## 11. 驳回整单任务

接口：

- `POST /api/v1/review/reject/{context_id}`

用途：

- 将挂起任务标记为 `rejected`
- 从待审核队列移除
- 不再继续执行后续嵌入和入库步骤

示例：

```bash
curl -X POST http://127.0.0.1:8000/api/v1/review/reject/doc-001
```

## 12. 不确定块覆盖接口

接口：

- `POST /api/v1/review/chunks/{context_id}`

用途：

- 针对 `chunks_override` 中指定的 chunk 下标做局部文本覆盖
- 设计上更适合 `ChunkLevelReviewPause`
- 对预设 4 当前的 `HumanReviewPause` 来说，这个接口通常不是首选

示例：

```bash
curl -X POST http://127.0.0.1:8000/api/v1/review/chunks/doc-001 \
  -H 'Content-Type: application/json' \
  -d '{
    "chunks_override": {
      "0": "人工重写后的第一个块",
      "3": "人工重写后的第四个块"
    }
  }'
```

## 13. 监控接口

接口：

- `GET /metrics`

用途：

- 导出 Prometheus 指标

示例：

```bash
curl http://127.0.0.1:8000/metrics
```

## 14. 推荐调用顺序

### 场景 A：直接上传文件

1. 调 `POST /api/v1/files/upload`
2. 记录返回的 `id`
3. 轮询 `GET /api/v1/runs/{id}` 或 `GET /api/v1/review/pending`
4. 任务进入 `pending_review` 后，人工处理 `chunks`
5. 调 `POST /api/v1/review/submit/{id}`，或调 `POST /api/v1/review/reject/{id}`

### 场景 B：同步调用本地服务器文件

1. 调 `POST /api/v1/ingest/sync`
2. 如果返回 `success`，流程结束
3. 如果返回 `pending_review`，调 `GET /api/v1/review/pending`
4. 取出对应 `id` 和 `metadata.chunks`
5. 人工修改后调 `POST /api/v1/review/submit/{id}`，或调 `POST /api/v1/review/reject/{id}`

### 场景 C：异步调用本地服务器文件

1. 调 `POST /api/v1/ingest/async`
2. 轮询 `GET /api/v1/review/pending`
3. 发现目标任务进入待审核后，取出 `chunks`
4. 人工修改后调 `POST /api/v1/review/submit/{id}`

## 15. 当前 API 能做什么

- 上传文件并启动任务
- 让服务端下载 URL 文件并启动任务
- 启动预设 4
- 获取任务状态
- 获取挂起审核任务
- 驳回整单任务
- 按块保留/删除后继续执行
- 修改审核内容并恢复执行
- 导出监控指标
- 做健康检查

## 16. 当前 API 不能做什么

下面这些能力目前仍然需要你自己在调用方侧补业务逻辑：

- 更细粒度的权限控制
- 用户级配额、审计、租户隔离
- 上传后的异步通知回调
- 文件病毒扫描、内容安全检查、对象存储归档

## 17. 常见问题

### 13.1 为什么请求成功了，但结果不是 `success`

因为预设 4 默认包含 `HumanReviewPause`，运行到该步骤后会先挂起，等待审核。

### 17.2 现在支持直接上传文件吗

支持，使用：

```bash
POST /api/v1/files/upload
```

### 17.3 没有前端时，最重要的输入字段是什么

是服务器上的绝对文件路径：

```json
"raw_data": {
  "file_path": "/home/wuteng/realtime_rag_pipeline/data/uploads/example.pdf",
  "data_type": "document"
}
```

### 17.4 如何查询文件处理状态

上传或提交任务后，使用：

```bash
GET /api/v1/runs/{run_id}
```

### 17.5 如何驳回任务或删除部分 chunk

驳回整单：

```bash
POST /api/v1/review/reject/{context_id}
```

按块保留：

```bash
POST /api/v1/review/submit/{context_id}
```

请求体可传：

```json
{
  "keep_indices": [0, 2, 3]
}
```

### 17.6 如何从本地电脑访问这些接口

如果服务器内网不可直连，可以先建立 SSH 转发：

```bash
ssh -fNT -L 8000:127.0.0.1:8000 wuteng@172.20.8.28
```

然后本地访问：

```bash
http://127.0.0.1:8000
```
