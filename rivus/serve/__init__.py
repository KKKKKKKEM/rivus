"""rivus.serve — HTTP RPC 服务层。

用法::

    import rivus

    pipeline = rivus.Pipeline("demo") | node_a | node_b

    # 单个 Pipeline（路径用 pipeline.name）
    rivus.serve(pipeline, port=8000)

    # 多个 Pipeline
    rivus.serve(pipeline_a, pipeline_b, port=8000)

    # 字典自定义路径名
    rivus.serve({"v2": pipeline_a}, pipeline_b, port=8000)

    # 完整参数
    rivus.serve(
        pipeline,
        host="0.0.0.0",
        port=8000,
        prefix="/api/v1",
        debug=False,
        max_tasks=500,
    )

路由布局（prefix="/api/v1", name="demo"）::

    GET  /api/v1/heartbeat
    POST /api/v1/shutdown
    POST /api/v1/demo/tasks?mode=sync|async
    GET  /api/v1/demo/tasks?status=all|running|done|failed|cancelled
    GET  /api/v1/demo/tasks/{task_id}

响应格式::

    {"code": 0, "msg": "ok", "data": {...}}
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from rivus.serve.base import ServerConfig
from rivus.serve.task_manager import TaskManager

if TYPE_CHECKING:
    from rivus.pipeline import Pipeline

__all__ = ["serve"]


def serve(
    *pipelines: "Pipeline | dict[str, Pipeline]",
    host: str = "127.0.0.1",
    port: int = 8000,
    prefix: str = "",
    backend: str = "sanic",
    workers: int = 1,
    debug: bool = False,
    max_tasks: int = 100,
) -> None:
    """启动 HTTP RPC 服务，阻塞直到服务退出。

    Args:
        *pipelines: Pipeline 实例或 ``{name: pipeline}`` 字典，可混合传入。
                    字典形式允许自定义路由路径名；直接传 Pipeline 时使用 ``pipeline.name``。
        host:       监听地址，``"0.0.0.0"`` 接受所有网卡，默认仅本机。
        port:       监听端口，默认 ``8000``。
        prefix:     全局路由前缀，例如 ``"/api/v1"``。
        backend:    HTTP 框架，目前仅支持 ``"sanic"``。
        workers:    工作进程数（Sanic single_process 模式下无效）。
        debug:      开启调试模式。
        max_tasks:  每个 Pipeline 的任务记录 LRU 上限。

    Raises:
        ValueError: 传入的 pipeline 参数类型不符合要求，或未传入任何 pipeline。
        ImportError: 所选 backend 未安装（如 ``sanic`` 未安装）。
    """
    managers: dict[str, TaskManager] = {}

    for item in pipelines:
        if isinstance(item, dict):
            for name, pipeline in item.items():
                managers[name] = TaskManager(pipeline, max_tasks=max_tasks)
        else:
            # 直接传入 Pipeline 实例，使用 pipeline.name 作为路径名
            managers[item.name] = TaskManager(item, max_tasks=max_tasks)

    if not managers:
        raise ValueError("serve() requires at least one Pipeline.")

    cfg = ServerConfig(
        host=host,
        port=port,
        prefix=prefix,
        backend=backend,
        workers=workers,
        debug=debug,
        max_tasks=max_tasks,
    )

    if backend == "sanic":
        from rivus.serve.sanic_server import SanicServer

        server = SanicServer(managers, cfg)
    else:
        raise ImportError(
            f"Unsupported backend '{backend}'. "
            "Currently only 'sanic' is supported. "
            "Install it with: pip install sanic"
        )

    server.run()
