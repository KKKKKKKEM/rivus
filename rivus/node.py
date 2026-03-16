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
- **路由模式**：返回 ``(branch_key, value)`` tuple，框架按 branch_key 路由到指定分支

节点间传递模式
--------------
默认（流式）：上游每产出一个 item 立即流入下游，全程并发流水线。

Gather 模式（``gather=True``）：在该节点前插入屏障，等上游 **所有** item
全部完成后，将其 ``input`` 值收集为 ``list``，以单个 item 形式传入该节点。
适合需要聚合上游并发结果的 reduce / merge 场景。

分叉与合并
----------
广播分叉（broadcast）：上游每产出一个 item，同时推送到所有下游分支，
每个分支独立处理同一份 ctx。适合并行加工同一数据。

路由分叉（route）：节点函数返回 ``(branch_key, value)``，框架按 branch_key
将 item 路由到对应的下游节点，只有一个分支会收到该 item。

Join 合并：收集多个上游分支的 done 信号，等所有上游分支都完成后才向下
传播结束信号。配合 ``gather=True`` 可聚合所有分支的输出结果。

设计原则
--------
- **单一职责**：Node 只描述「如何处理单个 item」和「用多少线程并发」
- **零配置协调**：节点间的生产者-消费者关系由队列自动处理
- **均匀适用**：workers=1 → 串行；workers=N → 并发消费，适合所有场景
"""

from __future__ import annotations

import functools
import inspect
import queue
import threading
from typing import Any, Callable, Literal, Optional

from rivus import signals
from rivus.context import Context

# ---------------------------------------------------------------------------
# 函数式节点
# ---------------------------------------------------------------------------
_DONE = object()   # 正常流结束


class ERROR(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class Node:
    """流水线节点"""

    def __init__(
        self,
        fn: Callable[[Context], Any],
        name: str | None = None,
        *,
        workers: int = 1,
        queue_size: int = 0,
        gather: bool = False,
    ) -> None:
        if workers < 1:
            raise ValueError(f"workers must be >= 1, got {workers}")

        self.fn = fn
        self.name = name or getattr(fn, "__name__", "node")
        self.workers = workers
        self.running_workers = 0
        self.queue_size = queue_size
        self.gather = gather
        self.task_queue: queue.Queue[Context] = queue.Queue(maxsize=queue_size)
        self.lock = threading.Lock()
        self.is_generator = inspect.isgeneratorfunction(fn)
        self.shutdown = threading.Event()
        self.shutdown_signal = False

        # DAG 多下游支持（替代原 self.next）
        self.nexts: list[Node] = []

        # 分叉模式：broadcast = 广播所有下游；route = 按返回值路由
        self.branch_mode: Literal["broadcast", "route"] = "broadcast"

        # join 节点：需要等待几个上游发出 done 信号（由 Pipeline._connect 累加）
        self._upstream_count: int = 0
        self._done_received: int = 0
        self._done_lock = threading.Lock()

        # gather 模式：收集上游所有 item，done 时一起推给自己
        self._gather_buffer: list[Any] = []
        self._gather_lock = threading.Lock()

    # ------------------------------------------------------------------
    # 运行
    # ------------------------------------------------------------------



    def run(self):
        while True:
            ctx: Context = self.task_queue.get()
            if ctx is _DONE:
                with self.lock:
                    self.running_workers -= 1
                    if self.running_workers == 0:
                        self.shutdown.set()
                        # gather 模式：所有 worker 退出时，把缓冲区一次性推给自己再处理
                        if self.gather and self._gather_buffer:
                            with self._gather_lock:
                                gathered = list(self._gather_buffer)
                                self._gather_buffer.clear()
                                
                            # 构造一个聚合 ctx 并重新入队自己处理
                            ctx = Context(initial={"input": gathered})
                            try:
                                stuff = self.fn(ctx)
                                self._dispatch(ctx, stuff)
                            except signals.SKIP:
                                pass
                            except Exception as exc:
                                self.follow2next(ctx, exc)

                        self._notify_nexts_done()

                    return

            if ctx.error:
                self.follow2next(ctx, ctx.error)
                continue

            try:
                if self.gather:
                    # gather 模式：先缓冲，不立即向下游发送
                    with self._gather_lock:
                        self._gather_buffer.append(ctx.input)
                else:
                    stuff = self.fn(ctx)
                    self._dispatch(ctx, stuff)

            except signals.SKIP:
                pass

            except signals.STOP:
                self.done()

            except Exception as exc:
                self.follow2next(ctx, exc)

    def _dispatch(self, ctx: Context, stuff: Any) -> None:
        """将 fn 返回值派生 ctx 并路由到下游。"""
        if self.is_generator:
            for item in stuff:
                derived = self._derive(ctx, item)
                self.follow2next(derived)
        else:
            derived = self._derive(ctx, stuff)
            self.follow2next(derived)

    def _derive(self, ctx: Context, value: Any) -> Context:
        if value is None:
            return ctx
        elif isinstance(value, Context):
            return value
        else:
            return ctx.derive(input=value)

    def _notify_nexts_done(self) -> None:
        """向所有下游发送 done 信号。"""
        for nxt in self.nexts:
            nxt.done()

    def start(self):
        self.running_workers = self.workers
        self.shutdown.clear()
        self.shutdown_signal = False
        self._done_received = 0
        self._gather_buffer.clear()

        for _ in range(self.workers):
            t = threading.Thread(target=self.run, daemon=True)
            t.start()

    def wait(self, timeout: Optional[float] = None):
        self.shutdown.wait(timeout=timeout)
        if timeout is not None and not self.shutdown.is_set():
            raise TimeoutError(
                f"Node '{self.name}' timed out after {timeout}s"
            )

    def done(self):
        """上游通知本节点：没有更多数据了。

        join 节点需要等到所有上游都调用 done() 后，才向本节点的 workers 发送 _DONE。
        """
        with self._done_lock:
            self._done_received += 1
            if self._done_received < self._upstream_count:
                return  # 还有上游未完成，继续等待

            # 所有上游已完成，向 workers 发送 _DONE
            with self.lock:
                if self.running_workers == 0 or self.shutdown_signal:
                    return
                for _ in range(self.running_workers):
                    self.submit(_DONE)
                self.shutdown_signal = True

    def submit(self, item, block=True, timeout=None):
        self.task_queue.put(item, block=block, timeout=timeout)

    def snapshot(self) -> "Node":
        """创建当前 Node 的快照副本（不含运行状态和 DAG 连接）。

        DAG 连接（nexts、_upstream_count、branch_mode）由 Pipeline._build_workers
        在快照集合上统一重建。
        """
        n = Node(
            fn=self.fn,
            name=self.name,
            workers=self.workers,
            queue_size=self.queue_size,
            gather=self.gather,
        )
        n.branch_mode = self.branch_mode
        # nexts 和 _upstream_count 由 Pipeline 重建，不在这里复制
        return n

    def follow2next(self, ctx: Context, err: Exception | None = None):
        if err is not None:
            ctx.set("error", err)

        if not self.nexts:
            return

        if self.branch_mode == "broadcast":
            for nxt in self.nexts:
                nxt.submit(item=ctx)

        elif self.branch_mode == "route":
            # route 模式：ctx.input 应为 (branch_key, real_value) 或仅 branch_key
            raw = ctx.input
            if isinstance(raw, tuple) and len(raw) == 2:
                branch_key, real_value = raw
                routed_ctx = ctx.derive(input=real_value)
            else:
                branch_key = raw
                routed_ctx = ctx

            # 按 nexts 顺序查找：node.name == branch_key
            target: Optional[Node] = None
            for nxt in self.nexts:
                if nxt.name == branch_key:
                    target = nxt
                    break

            if target is not None:
                target.submit(item=routed_ctx)
            # 未匹配到分支：静默丢弃（可通过 ctx.set("error", ...) 改为抛出）

    def __repr__(self) -> str:
        nxt_names = [n.name for n in self.nexts]
        return (
            f"Node(name={self.name!r}, workers={self.workers}, nexts={nxt_names})"
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
    gather: bool = False,
) -> Any:

    def decorator(f: Callable) -> Node:
        n = Node(f, name=name, workers=workers,
                 queue_size=queue_size, gather=gather)
        functools.update_wrapper(n, f)  # type: ignore[arg-type]
        return n

    if fn is not None:
        return decorator(fn)
    return decorator
