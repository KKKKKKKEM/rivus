# 自定义 HTTP 后端扩展

本文档介绍如何通过继承 `AbstractServer` 来实现自定义 HTTP 后端，以及如何替换任务管理逻辑。

**源码位置**：`rivus/serve/base.py`

---

## 架构概述

Rivus 的 HTTP 服务层遵循关注点分离设计：

```
rivus.serve()
      │
      ├── TaskManager × N    — 框架无关的任务生命周期管理
      └── AbstractServer     — HTTP 框架适配层
            └── SanicServer  — 当前唯一内置实现
```

**扩展点**：

1. **自定义 HTTP 后端**：继承 `AbstractServer`，实现 `run()` 方法
2. **自定义任务管理**：直接使用或继承 `TaskManager`

---

## 实现自定义 HTTP 后端

### 最小实现

```python
from rivus.serve.base import AbstractServer, ServerConfig
from rivus.serve.task_manager import TaskManager


class MyServer(AbstractServer):
    """基于 my-http-framework 的 Rivus 后端"""

    def __init__(
        self,
        managers: dict[str, TaskManager],
        cfg: ServerConfig,
    ) -> None:
        super().__init__(managers, cfg)
        self._app = MyHttpFramework()
        self._register_routes()

    def _register_routes(self) -> None:
        app = self._app

        # 1. 全局路由
        @app.route("GET", self._route("heartbeat"))
        def heartbeat(req):
            return json_response(self._handle_heartbeat())

        @app.route("POST", self._route("shutdown"))
        def shutdown(req):
            # 触发关闭逻辑
            return json_response({"code": 0, "msg": "ok"})

        # 2. 每条 Pipeline 的 CRUD 路由
        for name in self._managers:
            self._register_pipeline_routes(app, name)

    def _register_pipeline_routes(self, app, name: str) -> None:
        tasks_path = self._route(name, "tasks")
        task_path = self._route(name, "tasks", "{task_id}")

        @app.route("POST", tasks_path)
        def submit(req):
            body = req.json()
            mode = "async" if body.get("timeout") == 0 else "sync"
            return json_response(self._handle_submit(name, body, mode))

        @app.route("GET", tasks_path)
        def list_tasks(req):
            status = req.query.get("status", "all")
            return json_response(self._handle_list_tasks(name, status))

        @app.route("GET", task_path)
        def get_task(req, task_id: str):
            return json_response(self._handle_get_task(name, task_id))

        @app.route("DELETE", task_path)
        def cancel_task(req, task_id: str):
            return json_response(self._handle_cancel_task(name, task_id))

    def run(self) -> None:
        """启动 HTTP 服务，阻塞直到退出"""
        self._app.listen(
            host=self._cfg.host,
            port=self._cfg.port,
        )
```

### 注册到 `rivus.serve()`

当前版本的 `rivus.serve()` 固定使用 `"sanic"` 后端。如需使用自定义后端，可直接实例化并调用：

```python
from rivus.serve.base import ServerConfig
from rivus.serve.task_manager import TaskManager
from my_server import MyServer

import rivus

pipeline = rivus.Pipeline("demo") | step1 | step2

# 手动构建
managers = {"demo": TaskManager(pipeline, max_tasks=500)}
cfg = ServerConfig(host="0.0.0.0", port=8080, prefix="/api")

server = MyServer(managers, cfg)
server.run()
```

---

## `AbstractServer` 提供的工具方法

继承 `AbstractServer` 后，可直接使用以下方法处理路由逻辑，无需重复实现：

| 方法 | 说明 | 路由 |
|------|------|------|
| `_handle_heartbeat()` | 返回服务状态和 Pipeline 信息 | `GET /heartbeat` |
| `_handle_submit(name, body, mode)` | 提交同步/异步任务 | `POST /{name}/tasks` |
| `_handle_list_tasks(name, status)` | 列出任务（支持过滤） | `GET /{name}/tasks` |
| `_handle_get_task(name, task_id)` | 查询单个任务 | `GET /{name}/tasks/{id}` |
| `_handle_cancel_task(name, task_id)` | 取消任务 | `DELETE /{name}/tasks/{id}` |
| `_route(*parts)` | 拼接路由路径（含 prefix） | — |

所有 `_handle_*` 方法返回 `dict`，可直接序列化为 JSON。

---

## `_route()` 路径拼接

```python
# cfg.prefix = "/api/v1"
self._route("heartbeat")             # → "/api/v1/heartbeat"
self._route("demo", "tasks")         # → "/api/v1/demo/tasks"
self._route("demo", "tasks", "{id}") # → "/api/v1/demo/tasks/{id}"

# cfg.prefix = ""
self._route("heartbeat")             # → "/heartbeat"
```

---

## `ServerConfig` 参数

```python
from rivus.serve.base import ServerConfig

cfg = ServerConfig(
    host="0.0.0.0",
    port=8080,
    prefix="/api/v1",    # 自动标准化：去尾斜杠，补头斜杠
    backend="sanic",     # 仅元数据，自定义后端可忽略
    workers=1,
    debug=False,
    max_tasks=500,
)
```

---

## 自定义任务管理器

如需持久化任务记录（如写入 Redis、数据库），可继承或包装 `TaskManager`：

```python
import json
import redis
from rivus.serve.task_manager import TaskManager
from rivus.serve.models import TaskRecord


class RedisTaskManager(TaskManager):
    """将任务记录存储到 Redis 的 TaskManager"""

    def __init__(self, pipeline, max_tasks=500, redis_url="redis://localhost:6379"):
        super().__init__(pipeline, max_tasks=max_tasks)
        self._redis = redis.from_url(redis_url)
        self._key_prefix = f"rivus:tasks:{pipeline.name}:"

    def save_record(self, record: TaskRecord) -> None:
        # 同时保存到内存（用于快速查询）和 Redis（用于持久化）
        super().save_record(record)
        self._redis.setex(
            f"{self._key_prefix}{record.task_id}",
            3600,  # 1 小时 TTL
            json.dumps(record.to_dict()),
        )

    def get_task(self, task_id: str) -> TaskRecord | None:
        # 先查内存
        record = super().get_task(task_id)
        if record:
            return record

        # 再查 Redis
        data = self._redis.get(f"{self._key_prefix}{task_id}")
        if data:
            d = json.loads(data)
            return TaskRecord(
                task_id=d["task_id"],
                pipeline_name=d["pipeline_name"],
                status=d["status"],
                results=d.get("results"),
                error=d.get("error"),
            )
        return None
```

**使用**：

```python
from rivus.serve.base import ServerConfig
from rivus.serve.sanic_server import SanicServer

managers = {
    "demo": RedisTaskManager(pipeline, redis_url="redis://localhost:6379")
}
cfg = ServerConfig(host="0.0.0.0", port=8080)
server = SanicServer(managers, cfg)
server.run()
```

---

## Flask 后端示例

以下是一个基于 Flask 的自定义后端示例（仅供参考，需安装 Flask）：

```python
from flask import Flask, request, jsonify
from rivus.serve.base import AbstractServer, ServerConfig
from rivus.serve.task_manager import TaskManager


class FlaskServer(AbstractServer):

    def __init__(self, managers: dict[str, TaskManager], cfg: ServerConfig) -> None:
        super().__init__(managers, cfg)
        self._app = Flask("rivus-flask")
        self._register_routes()

    def _register_routes(self) -> None:
        app = self._app

        @app.get(self._route("heartbeat"))
        def heartbeat():
            return jsonify(self._handle_heartbeat())

        @app.post(self._route("shutdown"))
        def shutdown():
            func = request.environ.get("werkzeug.server.shutdown")
            if func:
                func()
            return jsonify({"code": 0, "msg": "ok"})

        for name in self._managers:
            self._register_pipeline_routes(app, name)

    def _register_pipeline_routes(self, app: Flask, name: str) -> None:
        tasks_path = self._route(name, "tasks")
        task_path = self._route(name, "tasks", "<task_id>")

        @app.post(tasks_path, endpoint=f"submit_{name}")
        def submit():
            body = request.get_json() or {}
            mode = "async" if body.get("timeout") == 0 else "sync"
            result = self._handle_submit(name, body, mode)
            return jsonify(result), result.get("code") or 200

        @app.get(tasks_path, endpoint=f"list_{name}")
        def list_tasks():
            status = request.args.get("status", "all")
            return jsonify(self._handle_list_tasks(name, status))

        @app.get(task_path, endpoint=f"get_{name}")
        def get_task(task_id: str):
            result = self._handle_get_task(name, task_id)
            return jsonify(result), result.get("code") or 200

        @app.delete(task_path, endpoint=f"cancel_{name}")
        def cancel_task(task_id: str):
            result = self._handle_cancel_task(name, task_id)
            return jsonify(result), result.get("code") or 200

    def run(self) -> None:
        self._app.run(
            host=self._cfg.host,
            port=self._cfg.port,
            debug=self._cfg.debug,
        )
```

---

## 扩展要点总结

| 扩展场景 | 方法 |
|----------|------|
| 使用不同的 HTTP 框架 | 继承 `AbstractServer`，实现 `run()` 和路由注册 |
| 持久化任务记录 | 继承 `TaskManager`，重写 `save_record` / `get_task` |
| 自定义响应格式 | 不调用 `_handle_*` 方法，直接操作 `TaskManager` |
| 添加认证中间件 | 在自定义后端的路由注册中包装 handler |
| 多种后端共存 | 为不同 Pipeline 使用不同 `TaskManager` 实例 |
