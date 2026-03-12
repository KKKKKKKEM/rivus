"""Rivus — 基于 Queue 的数据流水线框架。

核心思想：每个 Node 只接受 ctx，返回值自动派生为新 ctx，多个 Node 通过 Queue 串联::

    items → wrap(ctx) → Q → [Node A, workers=W] → Q → [Node B, workers=W] → Q → results

公共 API::

    from rivus import (
        # 函数式节点
        Node, node,
        # 类式节点
        BaseNode,
        # 流水线
        Pipeline, PipelineStatus, PipelineReport, NodeReport,
        # 共享上下文
        Context,
        # 异常
        RivusError, NodeError, PipelineError, PipelineTimeoutError, ContextKeyError,
    )
"""

from rivus.context import Context
from rivus.exceptions import (
    ContextKeyError,
    NodeError,
    NodeSkip,
    PipelineError,
    PipelineStop,
    PipelineTimeoutError,
    RivusError,
)
from rivus.node import BaseNode, Node, node
from rivus.pipeline import (
    InMemoryStorage,
    NodeReport,
    Pipeline,
    PipelineReport,
    PipelineStatus,
    RunStorage,
    RunStorageBackend,
)

__all__ = [
    # node
    "Node",
    "node",
    "BaseNode",
    # pipeline
    "Pipeline",
    "PipelineStatus",
    "PipelineReport",
    "NodeReport",
    # storage
    "RunStorage",
    "RunStorageBackend",
    "InMemoryStorage",
    # context
    "Context",
    # exceptions
    "RivusError",
    "NodeError",
    "NodeSkip",
    "PipelineError",
    "PipelineStop",
    "PipelineTimeoutError",
    "ContextKeyError",
]

__version__ = "0.4.0"
