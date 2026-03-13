# HTTP 服务层（serve）

Rivus 内置基于 [Sanic](https://sanic.dev) 的 HTTP RPC 服务层，让你无需额外代码即可将流水线暴露为 REST API，支持同步/异步任务提交、任务查询与取消。

**源码位置**：`rivus/serve/`

---

## 依赖安装

HTTP 服务层需要额外依赖：

```bash
pip install sanic loguru
```

---

## 快速启动

### 单 Pipeline 服务

```python
import rivus

@rivus.node
def process(ctx: rivus.Context):
    return ctx.require("input").upper()

pipeline = rivus.Pipeline("demo") | process

# 方式一：pipeline.serve()（阻塞）
pipeline.serve(port=8080)

# 方式二：rivus.serve()（支持多 Pipeline）
rivus.serve(pipeline, port=8080)
```

### 多 Pipeline 服务

```python
import rivus

pipeline_a = rivus.Pipeline("text") | step_a
pipeline_b = rivus.Pipeline("image") | step_b

# 使用各 Pipeline 的 name 作为路由路径
rivus.serve(pipeline_a, pipeline_b, port=8080)

# 自定义路由路径名
rivus.serve({"v2-text": pipeline_a}, pipeline_b, port=8080)
# → /text-pipeline/tasks  → pipeline_b
# → /v2-text/tasks        → pipeline_a
```

---

## `rivus.serve()` — 顶层入口

```python
def serve(
    *pipelines: Pipeline | dict[str, Pipeline],
    host: str = "127.0.0.1",
    port: int = 8000,
    prefix: str = "",
    backend: str = "sanic",
    workers: int = 1,
    debug: bool = False,
    max_tasks: int = 100,
) -> None
```

### 参数详解

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `*pipelines` | `Pipeline \| dict` | — | Pipeline 实例或 `{name: pipeline}` 字典，可混合传入 |
| `host` | `str` | `"127.0.0.1"` | 监听地址；`"0.0.0.0"` 接受所有网卡 |
| `port` | `int` | `8000` | 监听端口 |
| `prefix` | `str` | `""` | 全局路由前缀，如 `"/api/v1"` |
| `backend` | `str` | `"sanic"` | HTTP 框架（当前仅支持 `"sanic"`） |
| `workers` | `int` | `1` | HTTP 工作进程数 |
| `debug` | `bool` | `False` | 开启调试模式（详细日志） |
| `max_tasks` | `int` | `100` | 每个 Pipeline 保留的最大任务记录数（LRU） |

---

## 路由布局

以 `prefix="/api/v1"`、`pipeline.name="demo"` 为例：

```
GET  /api/v1/heartbeat                    # 心跳检测
POST /api/v1/shutdown                     # 关闭服务

POST   /api/v1/demo/tasks                 # 提交任务（同步/异步）
GET    /api/v1/demo/tasks?status=...      # 列出任务
GET    /api/v1/demo/tasks/{task_id}       # 查询单个任务
DELETE /api/v1/demo/tasks/{task_id}       # 取消任务
```

---

## REST API 文档

### 统一响应格式

所有接口均返回以下 JSON 结构：

```json
{
  "code": 0,
  "msg": "ok",
  "data": { ... }
}
```

**业务错误码**：

| code | HTTP 状态码 | 含义 |
|------|------------|------|
| `0` | 200 | 成功 |
| `400` | 400 | 请求参数错误 |
| `404` | 404 | 资源不存在 |
| `409` | 409 | 冲突（如任务已完成无法取消） |
| `500` | 500 | 服务器内部错误 |

---

### `GET /heartbeat` — 心跳检测

检查服务状态及所有 Pipeline 的运行情况。

**响应示例**：

```json
{
  "code": 0,
  "msg": "ok",
  "data": {
    "uptime_s": 42.135,
    "pipelines": {
      "demo": {
        "nodes": 3,
        "total_tasks": 10,
        "running_tasks": 2
      }
    }
  }
}
```

---

### `POST /shutdown` — 关闭服务

优雅关闭 HTTP 服务器。

**响应示例**：

```json
{
  "code": 0,
  "msg": "ok",
  "data": {"msg": "server is shutting down"}
}
```

---

### `POST /{name}/tasks` — 提交任务

向指定 Pipeline 提交一个任务。

**请求体（JSON）**：

```json
{
  "input": "any value",
  "timeout": 30.0
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `input` | `any` | 否 | 传入 Pipeline 的输入数据 |
| `timeout` | `float` | 否 | 超时秒数；`0` 表示异步模式，`null` 或省略表示永不超时 |

**运行模式**：

| `timeout` 值 | 运行模式 | 行为 |
|-------------|----------|------|
| `> 0`（如 `30.0`） | **同步模式** | 阻塞等待，最多 `timeout` 秒，直接返回结果 |
| 省略 / `null` | **同步模式** | 阻塞等待，永不超时，直接返回结果 |
| `0` | **异步模式** | 立即返回 `task_id`，后台运行 |

**同步模式响应**：

```json
{
  "code": 0,
  "msg": "ok",
  "data": {
    "task_id": "a1b2c3d4-...",
    "pipeline_name": "demo",
    "status": "done",
    "create_at": 1709123456.789,
    "start_at": 1709123456.800,
    "finish_at": 1709123456.950,
    "results": ["HELLO", "WORLD"],
    "error": null
  }
}
```

**异步模式响应**（`timeout=0`）：

```json
{
  "code": 0,
  "msg": "ok",
  "data": {
    "task_id": "a1b2c3d4-..."
  }
}
```

---

### `GET /{name}/tasks` — 列出任务

查询任务列表，支持按状态过滤。

**查询参数**：

| 参数 | 可选值 | 默认值 | 说明 |
|------|--------|--------|------|
| `status` | `all \| pending \| running \| done \| failed \| cancelled` | `all` | 过滤条件 |

```bash
GET /api/v1/demo/tasks?status=running
```

**响应示例**：

```json
{
  "code": 0,
  "msg": "ok",
  "data": {
    "tasks": [
      {
        "task_id": "a1b2c3d4-...",
        "pipeline_name": "demo",
        "status": "done",
        "create_at": 1709123456.789,
        "start_at": 1709123456.800,
        "finish_at": 1709123456.950,
        "results": ["HELLO"],
        "error": null
      }
    ],
    "total": 1
  }
}
```

---

### `GET /{name}/tasks/{task_id}` — 查询单个任务

```bash
GET /api/v1/demo/tasks/a1b2c3d4-...
```

**响应**：同上，`data` 字段为单个 `TaskRecord`。

**任务不存在**：

```json
{
  "code": 404,
  "msg": "Task 'xxx' not found.",
  "data": null
}
```

---

### `DELETE /{name}/tasks/{task_id}` — 取消任务

```bash
DELETE /api/v1/demo/tasks/a1b2c3d4-...
```

**成功**：

```json
{
  "code": 0,
  "msg": "ok",
  "data": {"task_id": "a1b2c3d4-...", "status": "cancelled"}
}
```

**任务已完成（不可取消）**：

```json
{
  "code": 409,
  "msg": "Task 'xxx' is already done and cannot be cancelled.",
  "data": null
}
```

**取消机制**：通过调用 `ctx.cancel()` 注入取消信号，流水线中各节点检测 `ctx.need_stop` 后自动跳过后续处理。

---

## 任务状态机

```
                 submit_task()
                      │
                      ▼
                  pending
                      │
              (立即或后台启动)
                      │
                      ▼
                  running ──────────── cancel_task() ──→ cancelled
                      │
               ┌──────┴──────┐
               ▼             ▼
             done           failed
```

| 状态 | 说明 |
|------|------|
| `pending` | 任务已创建，等待启动 |
| `running` | 任务正在执行 |
| `done` | 任务成功完成 |
| `failed` | 任务执行出错 |
| `cancelled` | 任务被取消 |

---

## 使用示例

### cURL

```bash
# 提交同步任务
curl -X POST http://localhost:8080/demo/tasks \
  -H "Content-Type: application/json" \
  -d '{"input": "hello world"}'

# 提交异步任务
curl -X POST http://localhost:8080/demo/tasks \
  -H "Content-Type: application/json" \
  -d '{"input": "hello", "timeout": 0}'

# 查询任务
curl http://localhost:8080/demo/tasks/a1b2c3d4-...

# 查询运行中任务
curl "http://localhost:8080/demo/tasks?status=running"

# 取消任务
curl -X DELETE http://localhost:8080/demo/tasks/a1b2c3d4-...

# 心跳
curl http://localhost:8080/heartbeat
```

### Python requests

```python
import requests

BASE = "http://localhost:8080"

# 提交同步任务
resp = requests.post(f"{BASE}/demo/tasks", json={"input": "hello"})
data = resp.json()["data"]
print(data["results"])      # ['HELLO']
print(data["status"])       # 'done'

# 提交异步任务并轮询
resp = requests.post(f"{BASE}/demo/tasks", json={"input": "hello", "timeout": 0})
task_id = resp.json()["data"]["task_id"]

import time
while True:
    resp = requests.get(f"{BASE}/demo/tasks/{task_id}")
    record = resp.json()["data"]
    if record["status"] in ("done", "failed", "cancelled"):
        print(record["results"])
        break
    time.sleep(0.1)
```

---

## 架构组件

```
rivus.serve()
      │
      ▼
TaskManager (每个 Pipeline 一个)
  ├── pipeline: Pipeline          — 持有 Pipeline 引用
  ├── _records: OrderedDict       — LRU 任务记录表
  └── submit_task() / cancel_task() / list_tasks() / get_task()
      │
      ▼
SanicServer (AbstractServer 实现)
  ├── _app: Sanic                 — HTTP 应用实例
  ├── _managers: {name: TaskManager}
  └── _register_routes()          — 注册所有路由
```

### `TaskManager` — 任务生命周期管理

负责单条 Pipeline 的所有 HTTP 任务管理：

- **`submit_task(req, is_async)`** — 同步/异步执行 Pipeline
- **`cancel_task(task_id)`** — 通过 `ctx.cancel()` 注入取消信号
- **`list_tasks(status)`** — 过滤查询任务列表（从新到旧排序）
- **`get_task(task_id)`** — 查询单个任务记录

**LRU 淘汰**：`_records` 是 `OrderedDict`，当记录数超过 `max_tasks` 时，自动淘汰最旧的记录。

### `TaskRecord` — 任务记录数据模型

```python
@dataclass
class TaskRecord:
    task_id: str
    pipeline_name: str
    create_at: float      # 创建时间戳
    status: TaskStatus    # pending/running/done/failed/cancelled
    start_at: float | None
    finish_at: float | None
    results: list | None  # Pipeline 运行结果
    error: str | None     # 错误信息（failed 时）
    ctx: Context | None   # 持有 Context 引用，用于取消
```

---

## 配置参数汇总

| 参数 | 说明 | 推荐值 |
|------|------|--------|
| `host` | 监听地址 | 生产环境 `"0.0.0.0"` |
| `port` | 端口 | `8080` 或 `8000` |
| `prefix` | URL 前缀 | `"/api/v1"` |
| `max_tasks` | 每 Pipeline 最大任务记录数 | 根据内存适当调大 |
| `debug` | 调试模式 | 开发 `True`，生产 `False` |

---

## 注意事项

1. **同步任务阻塞 HTTP 线程**：Sanic 通过 `loop.run_in_executor` 在线程池中执行同步 Pipeline，不会阻塞事件循环。

2. **长时间运行任务**：Sanic 的 `RESPONSE_TIMEOUT` 默认设为 30 分钟，`REQUEST_TIMEOUT` 设为 5 分钟，适合大多数场景。如有更长的运行需求，可在自定义 `SanicServer` 中调整。

3. **多进程模式**：`single_process=True` 是当前的默认值，确保 `/shutdown` 信号和内存状态在单进程内工作正常。多进程模式下，每个进程拥有独立的 `TaskManager` 状态，任务查询可能跨进程不一致。

4. **任务记录持久化**：默认 `TaskManager` 使用内存 `OrderedDict`，重启后记录丢失。若需持久化，参见[自定义后端](../guides/custom-backend.md)。
