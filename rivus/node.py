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
import queue
import threading
from typing import Any, Callable, Optional

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
    ) -> None:
        if workers < 1:
            raise ValueError(f"workers must be >= 1, got {workers}")

        self.fn = fn
        self.name = name or getattr(fn, "__name__", "node")
        self.workers = workers
        self.running_workers = 0
        self.queue_size = queue_size
        self.task_queue: queue.Queue[Context] = queue.Queue(maxsize=queue_size)
        self.lock = threading.Lock()
        self.is_generator = inspect.isgeneratorfunction(fn)
        self.next: Optional[Node] = None
        self.shutdown = threading.Event()
        self.shutdown_signal = False

    def run(self):
        while True:
            ctx: Context = self.task_queue.get()
            if ctx is _DONE:
                with self.lock:
                    self.running_workers -= 1
                    if self.running_workers == 0:
                        self.shutdown.set()
                        if self.next:
                            self.next.done()
                    return

            elif ctx.need_stop:
                continue

            try:
                stuff = self.fn(ctx)

                if not self.is_generator:
                    stuff = [stuff]

                for item in stuff:
                    if item is None:
                        item = ctx  # pass-through
                    elif not isinstance(item, Context):
                        item = ctx.derive(input=item)
                    self.follow2next(item)

            except signals.SKIP:
                pass

            except signals.STOP:
                self.done()

            except Exception as exc:
                self.follow2next(ctx, err=exc)

    def start(self):
        self.running_workers = self.workers
        self.shutdown.clear()
        self.shutdown_signal = False

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
        with self.lock:
            if self.running_workers == 0 or self.shutdown_signal:
                return

            for _ in range(self.running_workers):
                self.submit(_DONE)

            self.shutdown_signal = True

    def submit(self, item, block=True, timeout=None):
        self.task_queue.put(item, block=block, timeout=timeout)

    def snapshot(self) -> Node:
        """创建当前 Node 的快照副本，包含 fn、name、workers、queue_size 等配置，但不包含状态（task_queue、shutdown 等）。"""
        return Node(
            fn=self.fn,
            name=self.name,
            workers=self.workers,
            queue_size=self.queue_size,
        )

    def follow2next(self, ctx: Context, err: Exception | None = None):
        if err is not None:
            ctx.set("error", err)

        if self.next:
            self.next.submit(item=ctx)

    def __repr__(self) -> str:
        return (
            f"Node(name={self.name!r}, workers={self.workers}, next: {self.next})"
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
) -> Any:

    def decorator(f: Callable) -> Node:
        n = Node(f, name=name, workers=workers, queue_size=queue_size)
        functools.update_wrapper(n, f)  # type: ignore[arg-type]
        return n

    if fn is not None:
        return decorator(fn)
    return decorator
