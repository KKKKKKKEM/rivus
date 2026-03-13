"""Rivus — 基于 Queue 的数据流水线框架。

核心思想：每个 Node 只接受 ctx，返回值自动派生为新 ctx，多个 Node 通过 Queue 串联::

    items → wrap(ctx) → Q → [Node A, workers=W] → Q → [Node B, workers=W] → Q → results
"""

from rivus import serve
from rivus.context import Context
from rivus.node import Node, node
from rivus.pipeline import (
    Pipeline,
)

__all__ = [
    # node
    "Node",
    "node",
    # pipeline
    "Pipeline",
    # serve
    "serve",
    # context
    "Context",
]

__version__ = "0.4.0"
