"""Node：流水线的基本处理单元。

每个 Node 持有一个处理函数 ``fn(ctx) -> Any``，以及并发度配置。
多个 Node 串联后，由 Pipeline 通过 Queue 自动连接，形成数据流水线::

    items → Q → [Node A, workers=4] → Q → [Node B, workers=2] → Q → results

节点函数签名
------------
- **只接收 ctx**：``fn(ctx: Context) -> Any``
- **返回值**：框架自动将返回值派生为子 Context（``ctx.derive(input=value)``）
  并传递给下游节点；返回 ``None`` 表示直接传递当前 ctx（pass-through）
- **生成器模式**：``yield`` 多个值，每个 yield 产生一个独立的下游 item（fan-out）

节点间传递模式
--------------
默认（流式）：上游每产出一个 item 立即流入下游，全程并发流水线。

Gather 模式（``gather=True``）：在该节点前插入屏障，等上游 **所有** item
全部完成后，将其 ``input`` 值收集为 ``list``，以单个 item 形式传入该节点。
适合需要聚合上游并发结果的 reduce / merge 场景。

设计原则
--------
- **单一职责**：Node 只描述「如何处理单个 item」和「用多少线程并发」
- **零配置协调**：节点间的生产者-消费者关系由队列自动处理
- **均匀适用**：workers=1 → 串行；workers=N → 并发消费，适合所有场景
"""

from __future__ import annotations

import functools
import inspect
from abc import ABC, abstractmethod
from typing import Any, Callable

from rivus.context import Context


# ---------------------------------------------------------------------------
# 函数式节点
# ---------------------------------------------------------------------------


class Node:
    """流水线节点（函数式）。

    Args:
        fn: 处理函数，签名为 ``(ctx: Context) -> Any``。

            - 返回普通值       → 框架以 ``ctx.derive(input=value)`` 打包后传下游
            - 返回 ``None``    → 当前 ctx 原样传下游（pass-through / 副作用节点）
            - 返回 ``Context`` → 直接传下游（用户手动控制派生）
            - ``yield`` 多个值 → 每个值派生为独立 item（fan-out）

        name: 节点名称；缺省取 ``fn.__name__``。
        workers: 并发消费线程数，默认 1（串行）。
        queue_size: 输入队列最大容量，0 表示无上限（背压控制）。
        setup_fn: 可选初始化函数，签名 ``(ctx: Context) -> None``；
                  在 Pipeline 启动时以 **root ctx** 调用一次，适合加载模型等重型初始化。
        gather: 若为 ``True``，在本节点前插入同步屏障：等待上游所有 item
                全部处理完毕，将结果值收集为 ``list`` 后以单个 item 传入本节点。
                节点函数通过 ``ctx.require("input")`` 取到的即是该列表。
                适用于 reduce / merge 等需要全量上游结果的场景。

    Examples::

        @node(workers=4)
        def preprocess(ctx: Context):
            image = ctx.require("input")
            return transform(image)

        @node(workers=1, name="ChunkDoc")
        def chunk_doc(ctx: Context):
            doc = ctx.require("input")
            for chunk in split(doc):
                yield chunk  # fan-out：一篇文章 → 多个 chunk

        # gather 模式：等上游全部完成，合并为列表后处理
        @node(workers=4)
        def process(ctx: Context):
            return heavy_work(ctx.require("input"))

        @node(gather=True)
        def reduce(ctx: Context):
            all_results = ctx.require("input")  # list
            return merge(all_results)

        pipeline = Pipeline("rag") | preprocess | chunk_doc | process | reduce
    """

    def __init__(
        self,
        fn: Callable[[Context], Any],
        name: str | None = None,
        *,
        workers: int = 1,
        queue_size: int = 0,
        setup_fn: Callable[[Context], None] | None = None,
        concurrency_type: str = "thread",
        gather: bool = False,
    ) -> None:
        if workers < 1:
            raise ValueError(f"workers must be >= 1, got {workers}")
        if concurrency_type not in ("thread", "process"):
            raise ValueError(
                f"concurrency_type must be 'thread' or 'process', got {concurrency_type!r}"
            )
        self.fn = fn
        self.name = name or getattr(fn, "__name__", "node")
        self.workers = workers
        self.queue_size = queue_size
        self.setup_fn = setup_fn
        self.concurrency_type = concurrency_type
        self.gather = gather
        self.is_generator = inspect.isgeneratorfunction(fn)

    def __repr__(self) -> str:
        return (
            f"Node(name={self.name!r}, workers={self.workers}, "
            f"concurrency_type={self.concurrency_type!r})"
        )


# ---------------------------------------------------------------------------
# 类式节点基类
# ---------------------------------------------------------------------------


class BaseNode(ABC):
    """类式节点基类。继承并实现 :meth:`process` 即可。

    类属性
    ------
    name : str
        节点名称；缺省取类名。
    workers : int
        并发消费线程数，默认 1。
    queue_size : int
        输入队列最大容量，默认无限。

    重写 :meth:`setup` 可以在流水线启动时加载模型等重型资源（调用一次）。
    :meth:`process` 支持普通返回值和生成器（yield）两种模式。

    设置 ``gather = True`` 可在本节点前插入同步屏障，等上游所有 item 全部
    完成后，将其结果值收集为 ``list`` 作为单个 item 传入 :meth:`process`。

    Examples::

        class Preprocess(BaseNode):
            workers = 4

            def setup(self, ctx: Context) -> None:
                # 流水线启动时调用一次，ctx 是 root Context
                self.model = ctx.require("model")

            def process(self, ctx: Context) -> dict:
                image = ctx.require("input")
                return self.model.transform(image)

        class ChunkDoc(BaseNode):
            name = "chunk"

            def process(self, ctx: Context):
                doc = ctx.require("input")
                for chunk in doc.split("\\n\\n"):
                    yield chunk  # fan-out

        class Aggregate(BaseNode):
            gather = True  # 等上游全量结果，合并后处理

            def process(self, ctx: Context):
                chunks = ctx.require("input")  # list
                return "\\n".join(chunks)

        pipeline = Pipeline("rag") | Preprocess() | ChunkDoc() | Aggregate()
    """

    name: str = ""
    workers: int = 1
    queue_size: int = 0
    concurrency_type: str = "thread"  # "thread" 或 "process"
    gather: bool = False  # True → 本节点前插入屏障，等上游全量结果后聚合为列表传入

    def setup(self, ctx: Context) -> None:
        """流水线启动时调用一次（可选重写）。

        Args:
            ctx: Pipeline 的 root Context，包含 ``initial`` 中的全部初始数据。
        """

    @abstractmethod
    def process(self, ctx: Context) -> Any:
        """处理单个 item（必须实现）。

        读取 ``ctx.require("input")`` 获取当前 item，
        返回结果（或 yield 多个结果）写入下游。
        """

    def _to_node(self) -> Node:
        """转换为 :class:`Node`，供 Pipeline 内部使用。"""
        name = self.name or self.__class__.__name__
        return Node(
            fn=self.process,
            name=name,
            workers=self.workers,
            queue_size=self.queue_size,
            setup_fn=self.setup,
            concurrency_type=self.concurrency_type,
            gather=getattr(self, "gather", False),
        )


# ---------------------------------------------------------------------------
# 装饰器
# ---------------------------------------------------------------------------


def node(
    fn: Callable[[Context], Any] | None = None,
    *,
    name: str | None = None,
    workers: int = 1,
    queue_size: int = 0,
    setup: Callable[[Context], None] | None = None,
    concurrency_type: str = "thread",
    gather: bool = False,
) -> Any:
    """将函数装饰为 :class:`Node` 实例。

    用法一（无参数直接装饰）::

        @node
        def preprocess(ctx: Context):
            return transform(ctx.require("input"))

    用法二（带参数）::

        @node(workers=4, name="Inference")
        def inference(ctx: Context):
            return ctx.require("model").predict(ctx.require("input"))

    用法三（生成器，fan-out）::

        @node(workers=1, name="ChunkDoc")
        def chunk_doc(ctx: Context):
            for chunk in split(ctx.require("input")):
                yield chunk

    用法四（带初始化函数）::

        def load_model(ctx: Context):
            ctx.set("model", Model.load(ctx.require("model_path")))

        @node(workers=4, setup=load_model)
        def infer(ctx: Context):
            return ctx.require("model").predict(ctx.require("input"))

    用法五（gather 模式，等上游全量结果后聚合处理）::

        @node(workers=4)
        def scatter(ctx: Context):
            return process(ctx.require("input"))

        @node(gather=True)          # 等 scatter 全部完成后触发，收到 list
        def reduce(ctx: Context):
            all_results = ctx.require("input")  # list
            return merge(all_results)

    Args:
        fn: 被装饰的函数，签名为 ``(ctx: Context) -> Any``。
        name: 节点名称，缺省取函数名。
        workers: 并发 worker 数量，默认 1。
        queue_size: 输入队列容量，默认无限。
        setup: 可选初始化函数，流水线启动时以 root ctx 调用一次。
        concurrency_type: ``"thread"``（默认）或 ``"process"``。
            进程模式下节点函数必须可 pickle（模块级函数或可 pickle 的
            可调用对象），适合 CPU 密集型任务；线程模式适合 I/O 密集型。
        gather: 若为 ``True``，在本节点前插入同步屏障：收集上游所有 item
                的结果值组成 ``list``，以单个 item 传入本节点。

    Returns:
        :class:`Node` 实例（可直接用于 ``Pipeline | node_instance``）。
    """

    def decorator(f: Callable) -> Node:
        n = Node(f, name=name, workers=workers,
                 queue_size=queue_size, setup_fn=setup,
                 concurrency_type=concurrency_type, gather=gather)
        functools.update_wrapper(n, f)  # type: ignore[arg-type]
        return n

    if fn is not None:
        return decorator(fn)
    return decorator
