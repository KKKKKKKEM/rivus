"""Sanic HTTP 服务器实现"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from sanic import Sanic
from sanic import json as sanic_json
from sanic.request import Request
from sanic.response import HTTPResponse

from rivus.serve.base import AbstractServer, ServerConfig
from rivus.serve.models import ApiResponse
from rivus.serve.task_manager import TaskManager

if TYPE_CHECKING:
    # Sanic is generic in stubs; alias avoids reportExplicitAny at class body level
    _SanicApp = Sanic[Any, Any]


class SanicServer(AbstractServer):
    """基于 Sanic 的 HTTP 服务器。"""

    _app: _SanicApp
    _shutdown_event: asyncio.Event

    def __init__(
        self,
        managers: dict[str, TaskManager],
        cfg: ServerConfig,
    ) -> None:
        super().__init__(managers, cfg)
        # 每个 SanicServer 实例使用独立的 app 名称，避免 Sanic 单例冲突
        self._app = Sanic(f"rivus-serve-{id(self)}")

        self._app.config.RESPONSE_TIMEOUT = 30 * 60  # 30分钟响应超时，适合长时间运行的 Pipeline
        # 如果有大 body 上传/慢客户端，再按需调大：
        self._app.config.REQUEST_TIMEOUT = 5 * 60  # 5分钟请求超时，适合上传大文件等场景
        self._shutdown_event = asyncio.Event()
        self._register_routes()

    # ------------------------------------------------------------------
    # 路由注册
    # ------------------------------------------------------------------

    def _register_routes(self) -> None:
        app = self._app

        # ── 全局路由 ───────────────────────────────────────────────────

        heartbeat_path = self._route("heartbeat")
        shutdown_path = self._route("shutdown")

        @app.get(heartbeat_path)
        # pyright: ignore[reportUnusedFunction]
        async def heartbeat(_req: Request) -> HTTPResponse:
            return sanic_json(self._handle_heartbeat())

        @app.post(shutdown_path)
        # pyright: ignore[reportUnusedFunction]
        async def shutdown(_req: Request) -> HTTPResponse:
            _ = asyncio.get_running_loop().call_soon(self._shutdown_event.set)
            return sanic_json(ApiResponse.ok({"msg": "server is shutting down"}).to_dict())

        # ── 每条 Pipeline 的 CRUD 路由 ─────────────────────────────────

        for name in self._managers:
            # 用默认参数捕获 name，避免闭包陷阱
            self._register_pipeline_routes(app, name)

    def _register_pipeline_routes(self, app: _SanicApp, name: str) -> None:
        """为单条 Pipeline 注册 4 个 CRUD 路由。"""

        tasks_path = self._route(name, "tasks")
        task_path = self._route(name, "tasks", "<task_id:str>")

        @app.post(tasks_path, name=f"submit_{name}")
        # pyright: ignore[reportUnusedFunction]
        async def submit(req: Request) -> HTTPResponse:
            body: dict[str, object] = req.json or {}
            timeout = body.get("timeout")

            if timeout == 0:
                mode_str = "async"
            else:
                mode_str = "sync"

            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self._handle_submit(name, body, mode_str),
            )
            status_code = self._http_status(result["code"])
            return sanic_json(result, status=status_code)

        @app.get(tasks_path, name=f"list_tasks_{name}")
        # pyright: ignore[reportUnusedFunction]
        async def list_tasks(req: Request) -> HTTPResponse:
            status_filter: str = str(req.args.get("status") or "all")
            result = self._handle_list_tasks(name, status_filter)
            return sanic_json(result)

        @app.get(task_path, name=f"get_task_{name}")
        # pyright: ignore[reportUnusedFunction]
        async def get_task(_req: Request, task_id: str) -> HTTPResponse:
            result = self._handle_get_task(name, task_id)
            status_code = self._http_status(result["code"])
            return sanic_json(result, status=status_code)

        @app.delete(task_path, name=f"cancel_task_{name}")
        # pyright: ignore[reportUnusedFunction]
        async def cancel_task(_req: Request, task_id: str) -> HTTPResponse:
            result = self._handle_cancel_task(name, task_id)
            status_code = self._http_status(result["code"])
            return sanic_json(result, status=status_code)

    # ------------------------------------------------------------------
    # 辅助
    # ------------------------------------------------------------------

    @staticmethod
    def _http_status(code: object) -> int:
        """将业务 code 映射到 HTTP 状态码。"""
        if code == 0:
            return 200
        http_map: dict[int, int] = {400: 400, 404: 404, 409: 409, 500: 500}
        if isinstance(code, int):
            return http_map.get(code, 200)
        return 200

    # ------------------------------------------------------------------
    # 启动
    # ------------------------------------------------------------------

    def run(self) -> None:
        """启动 Sanic 服务器，阻塞直到收到 /shutdown 请求或 SIGINT。"""
        from loguru import logger

        cfg = self._cfg
        names = list(self._managers.keys())
        logger.info(
            f"[rivus.serve] Sanic server starting on http://{cfg.host}:{cfg.port}"
        )
        logger.info(f"[rivus.serve] Pipelines: {names}")
        logger.info(
            f"[rivus.serve] Heartbeat: GET http://{cfg.host}:{cfg.port}{self._route('heartbeat')}"
        )

        self._app.run(  # pyright: ignore[reportUnknownMemberType]
            host=cfg.host,
            port=cfg.port,
            workers=cfg.workers,
            debug=cfg.debug,
            # single_process=True 使 shutdown 信号生效（多进程模式下需要额外处理）
            single_process=True,
            motd=False,
        )
