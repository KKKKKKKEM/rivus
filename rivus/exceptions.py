"""Rivus 异常体系。"""

from __future__ import annotations


class RivusError(Exception):
    """所有 Rivus 异常的基类。"""


class NodeError(RivusError):
    """单个 Node 处理某 item 时失败。"""

    def __init__(self, node_name: str, cause: BaseException) -> None:
        self.node_name = node_name
        self.cause = cause
        super().__init__(f"Node '{node_name}' failed: {cause}")


class PipelineError(RivusError):
    """Pipeline 级别的执行失败。"""

    def __init__(self, pipeline_name: str, cause: BaseException) -> None:
        self.pipeline_name = pipeline_name
        self.cause = cause
        super().__init__(f"Pipeline '{pipeline_name}' failed: {cause}")


class PipelineTimeoutError(RivusError):
    """Pipeline 执行超过了允许的最大时间。"""

    def __init__(self, pipeline_name: str, timeout: float) -> None:
        self.pipeline_name = pipeline_name
        self.timeout = timeout
        super().__init__(
            f"Pipeline '{pipeline_name}' timed out after {timeout}s"
        )


class PipelineStop(RivusError):
    """节点内主动触发的优雅停止信号（不计为失败）。"""

    def __init__(self, message: str = "pipeline stop requested") -> None:
        self.message = message
        super().__init__(message)


class ContextKeyError(RivusError):
    """访问 Context 中不存在的必选键。"""

    def __init__(self, key: str) -> None:
        self.key = key
        super().__init__(f"Required context key '{key}' is missing")
