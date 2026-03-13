"""AbstractServer：HTTP 服务器抽象基类。

所有 backend 实现（Sanic 等）继承此类，
只需实现 ``run()`` 方法即可复用完整的参数模型。

路由命名规范（以 prefix="/api", pipeline.name="demo" 为例）::

    GET  /api/heartbeat              # 所有 pipeline 的心跳（全局，非 per-pipeline）
    POST /api/shutdown               # 关闭服务
    POST /api/demo/tasks             # 提交任务
    GET  /api/demo/tasks             # 列出任务
    GET  /api/demo/tasks/{task_id}    # 查询任务
    DELETE /api/demo/tasks/{task_id}  # 取消任务
"""

from __future__ import annotations

import abc
import time

from rivus.serve.models import ApiResponse, TaskRequest
from rivus.serve.task_manager import TaskManager


class ServerConfig:
    """HTTP 服务配置。

    Args:
        host:       监听地址，``"0.0.0.0"`` 表示接受所有网卡。
        port:       监听端口。
        prefix:     全局路由前缀，例如 ``"/api/v1"``。空字符串表示无前缀。
        backend:    HTTP 框架，``"sanic"``。
        workers:    HTTP 服务工作进程数（仅 Sanic / Gunicorn 生效）。
        debug:      是否开启调试模式（自动重载、详细日志等）。
        max_tasks:  每个 TaskManager 保留的最大任务记录数（LRU）。
    """

    host: str
    port: int
    prefix: str
    backend: str
    workers: int
    debug: bool
    max_tasks: int

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8000,
        prefix: str = "",
        backend: str = "sanic",
        workers: int = 1,
        debug: bool = False,
        max_tasks: int = 500,
    ) -> None:
        self.host = host
        self.port = port
        # 标准化 prefix：去掉末尾斜杠，确保以 "/" 开头（空字符串除外）
        prefix = prefix.rstrip("/")
        if prefix and not prefix.startswith("/"):
            prefix = "/" + prefix
        self.prefix = prefix
        self.backend = backend
        self.workers = workers
        self.debug = debug
        self.max_tasks = max_tasks


class AbstractServer(abc.ABC):
    """HTTP 服务器抽象基类。

    子类需实现：

    - ``run()``：启动 HTTP 服务器，阻塞直到服务退出。

    子类可选覆盖：

    - ``_build_routes()``：在 ``run()`` 内部调用，向 app 注册所有路由。
      默认实现已处理好所有路由逻辑；子类通常只需调用它，
      或在调用前后注册额外路由。

    标准路由（由 ``_managers`` 和 ``_cfg`` 驱动）::

        GET  {prefix}/heartbeat
        POST {prefix}/shutdown
        POST {prefix}/{name}/tasks[?mode=sync|async]
        GET  {prefix}/{name}/tasks[?status=all|running|done|failed|cancelled]
        GET  {prefix}/{name}/tasks/{task_id}
    """

    _managers: dict[str, TaskManager]
    _cfg: ServerConfig
    _start_at: float

    def __init__(
        self,
        managers: dict[str, TaskManager],
        cfg: ServerConfig,
    ) -> None:
        self._managers = managers   # {pipeline_name: TaskManager}
        self._cfg = cfg
        self._start_at = time.time()

    # ------------------------------------------------------------------
    # 路由工具
    # ------------------------------------------------------------------

    def _route(self, *parts: str) -> str:
        """拼接路由路径，自动处理前缀和多余斜杠。"""
        segments = [self._cfg.prefix] + [p.strip("/") for p in parts if p]
        path = "/".join(s for s in segments if s)
        return "/" + path if path else "/"

    # ------------------------------------------------------------------
    # 通用处理逻辑（与框架无关，供 backend 调用）
    # ------------------------------------------------------------------

    def _handle_heartbeat(self) -> dict[str, object]:
        """GET /heartbeat 处理逻辑。"""
        pipelines_info: dict[str, object] = {
            name: {
                "nodes": len(mgr.pipeline._nodes),
                "total_tasks": len(mgr.list_tasks()),
                "running_tasks": len(mgr.list_tasks("running")),
            }
            for name, mgr in self._managers.items()
        }
        return ApiResponse.ok(
            {
                "uptime_s": round(time.time() - self._start_at, 3),
                "pipelines": pipelines_info,
            }
        ).to_dict()

    def _handle_submit(
        self,
        pipeline_name: str,
        body: dict[str, object],
        mode: str,
    ) -> dict[str, object]:
        """POST /{name}/tasks 处理逻辑。"""
        mgr = self._managers.get(pipeline_name)
        if mgr is None:
            return ApiResponse.not_found(
                f"Pipeline '{pipeline_name}' not registered."
            ).to_dict()

        req = TaskRequest.from_dict(body)
        if req.timeout is not None and req.timeout <= 0:
            req.timeout = None  # 0 或负数表示永不超时

        resp = mgr.submit_task(req, is_async=mode == "async")
        return resp.to_dict()

    def _handle_list_tasks(
        self,
        pipeline_name: str,
        status: str = "all",
    ) -> dict[str, object]:
        """GET /{name}/tasks 处理逻辑。"""
        mgr = self._managers.get(pipeline_name)
        if mgr is None:
            return ApiResponse.not_found(
                f"Pipeline '{pipeline_name}' not registered."
            ).to_dict()

        tasks = mgr.list_tasks(status)
        return ApiResponse.ok(
            {"tasks": [t.to_dict() for t in tasks], "total": len(tasks)}
        ).to_dict()

    def _handle_get_task(
        self,
        pipeline_name: str,
        task_id: str,
    ) -> dict[str, object]:
        """GET /{name}/tasks/{task_id} 处理逻辑。"""
        mgr = self._managers.get(pipeline_name)
        if mgr is None:
            return ApiResponse.not_found(
                f"Pipeline '{pipeline_name}' not registered."
            ).to_dict()

        record = mgr.get_task(task_id)
        if record is None:
            return ApiResponse.not_found(f"Task '{task_id}' not found.").to_dict()
        return ApiResponse.ok(record.to_dict()).to_dict()

    def _handle_cancel_task(
        self,
        pipeline_name: str,
        task_id: str,
    ) -> dict[str, object]:
        """DELETE /{name}/tasks/{task_id} 处理逻辑。"""
        mgr = self._managers.get(pipeline_name)
        if mgr is None:
            return ApiResponse.not_found(
                f"Pipeline '{pipeline_name}' not registered."
            ).to_dict()

        return mgr.cancel_task(task_id).to_dict()
    # ------------------------------------------------------------------
    # 子类必须实现
    # ------------------------------------------------------------------

    @abc.abstractmethod
    def run(self) -> None:
        """启动 HTTP 服务器，阻塞直到服务退出。"""
