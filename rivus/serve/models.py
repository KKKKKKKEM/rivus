"""serve 层数据模型（无第三方依赖）。"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from rivus import Context

# ---------------------------------------------------------------------------
# 请求模型
# ---------------------------------------------------------------------------


@dataclass
class TaskRequest:
    """POST /tasks 请求体。

    Attributes:
        input:   传入 pipeline.run() 的单个 item。
        timeout: 仅 sync 模式有效，超时秒数；None 表示永不超时。
    """

    input: object = None
    timeout: float | None = None

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> TaskRequest:
        timeout_raw = data.get("timeout")
        timeout: float | None = (
            float(str(timeout_raw)) if timeout_raw is not None else None
        )
        return cls(
            input=data.get("input"),
            timeout=timeout,
        )


# ---------------------------------------------------------------------------
# 任务记录
# ---------------------------------------------------------------------------

TaskStatus = Literal["pending", "running", "done", "failed", "cancelled"]


@dataclass
class TaskRecord:
    """HTTP 层任务记录，由 TaskManager 独立管理。

    跟踪 HTTP 提交的每个任务的完整生命周期。
    """

    task_id: str
    pipeline_name: str
    create_at: float = field(default_factory=time.time)
    status: TaskStatus = "pending"
    start_at: float | None = None
    finish_at: float | None = None
    results: list[object] | None = None
    error: str | None = None
    ctx: Context | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "task_id": self.task_id,
            "pipeline_name": self.pipeline_name,
            "status": self.status,
            "create_at": self.create_at,
            "start_at": self.start_at,
            "finish_at": self.finish_at,
            "results": self.results,
            "error": self.error,
        }


# ---------------------------------------------------------------------------
# 统一响应
# ---------------------------------------------------------------------------

# 业务错误码
CODE_OK = 0
CODE_BAD_REQUEST = 400
CODE_NOT_FOUND = 404
CODE_CONFLICT = 409          # 任务已完成/不可取消
CODE_INTERNAL_ERROR = 500


@dataclass
class ApiResponse:
    """所有接口的统一响应体。

    JSON 序列化格式::

        {"code": 0, "msg": "ok", "data": {...}}
    """

    code: int = CODE_OK
    msg: str = "ok"
    data: object = None

    def to_dict(self) -> dict[str, object]:
        return {"code": self.code, "msg": self.msg, "data": self.data}

    @classmethod
    def ok(cls, data: object = None) -> ApiResponse:
        return cls(code=CODE_OK, msg="ok", data=data)

    @classmethod
    def error(cls, code: int, msg: str, data: object = None) -> ApiResponse:
        return cls(code=code, msg=msg, data=data)

    @classmethod
    def not_found(cls, msg: str = "task not found") -> ApiResponse:
        return cls(code=CODE_NOT_FOUND, msg=msg, data=None)

    @classmethod
    def bad_request(cls, msg: str) -> ApiResponse:
        return cls(code=CODE_BAD_REQUEST, msg=msg, data=None)

    @classmethod
    def conflict(cls, msg: str) -> ApiResponse:
        return cls(code=CODE_CONFLICT, msg=msg, data=None)
