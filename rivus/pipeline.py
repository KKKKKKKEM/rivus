"""Pipeline：基于 Queue 的数据流水线。

核心模型
--------
队列中流淌的是 **Context 对象**（而非裸 item）。
每个 Node 持有一个 ``fn(ctx)`` 函数和并发度 ``workers``:::

    items → wrap → Q₀ → [Node₀, w=W₀] → Q₁ → [Node₁, w=W₁] → Q₂ → … → results
             ↑                ↑ fn(ctx)→derive
       derive(input=item)   节点读 ctx，返回值自动派生新 ctx

节点返回值约定
--------------
- 普通值     → ``ctx.derive(input=value)`` 传下游
- ``None``   → 当前 ctx 原样传下游（pass-through / 副作用节点）
- ``Context``→ 直接传下游（用户手动控制派生）
- ``yield``  → 每个值派生为独立 item（fan-out，一进多出）
- ``(key, value)`` → 路由模式，按 key 选择下游分支

节点间传递模式
--------------
默认模式（流式）
    上游每产出一个 item 就立即流入下游，形成全流水线并发。

Gather 模式（``gather=True``）
    在该节点前插入一道屏障（barrier）：等待上游所有 item 全部处理完毕，
    将它们的 ``input`` 值收集成一个 **list** 后，作为单个 item 传入该节点。
    节点函数通过 ``ctx.require("input")`` 得到的就是这个合并列表，适合
    需要聚合/归约上游并发结果的场景（如 reduce、排序、合并写入等）。

    示例::

        @node(workers=4)
        def scatter(ctx):        # 并发处理，每条独立输出
            return process(ctx.require("input"))

        @node(gather=True)       # 等 scatter 全部完成再处理
        def reduce(ctx):
            all_results = ctx.require("input")  # list
            return summarize(all_results)

分叉与合并
----------
广播分叉（broadcast，默认）
    上游每产出一个 item，同时推送到所有分支，每个分支独立处理同一份 ctx。
    适合并行加工同一数据::

        pipeline.add_node(parse).branch(enrich_a, enrich_b).join(summarize)

路由分叉（route）
    节点函数返回 ``(branch_key, value)``，框架按 branch_key 路由到对应分支。
    适合按条件分流::

        @node
        def classify(ctx):
            if is_pdf(ctx.input):
                return ("pdf", ctx.input)
            return ("txt", ctx.input)

        pipeline.add_node(classify).branch(handle_pdf, handle_txt, mode="route")

Join 合并
    将多个并行分支汇入同一下游节点。配合 ``gather=True`` 可等所有分支完成后
    将结果聚合成列表再处理::

        pipeline.branch(a, b).join(reduce_node, gather=True)

两种运行模式
------------
- 批量：``run(*items)``
- 流式：``start() → rs.submit() × N → rs.close()``，返回 :class:`Task`，调用方自行线程化

生命周期：IDLE → RUNNING → SUCCESS / FAILED / STOPPED / TIMEOUT
"""

from __future__ import annotations

import contextlib
import threading
from collections import deque
from typing import Any, Callable, Generator, Iterable, Literal

from rivus.context import Context
from rivus.node import Node

# ---------------------------------------------------------------------------
# 单次运行状态（完全独立，每次 run 创建一个新实例）
# ---------------------------------------------------------------------------


class Pipeline:

    def __init__(
        self,
        name: str = "pipeline",
        max_workers: int | None = 10,
        *,
        initial: dict[str, Any] | None = None,
        nodes: list[Node] | None = None,
        on_result: Callable[['Pipeline', deque[Context],
                             Context], Iterable[Any]] | None = None,
    ) -> None:
        self.name = name
        self._nodes: list[Node] = nodes or []
        self.initial = initial or {}

        self._vars: dict[str, Any] = {}
        self._vars_lock = threading.Lock()
        self.semaphore = threading.Semaphore(
            max_workers) if max_workers is not None else contextlib.nullcontext()

        self._on_startup: list[Callable[["Pipeline"], None]] = []
        self._on_run_start: list[Callable[["Context"], None]] = []
        self._on_run_end: list[Callable[["Context"], None]] = []
        self._started = False
        self._startup_lock = threading.Lock()
        self.on_result = on_result or (lambda _, results, ctx: [
                                       i.input for i in results])

        # DAG builder 状态
        self._tail: Node | None = None                  # 当前链尾
        self._pending_branches: list[Node] = []         # 等待 join 的分支末端
        self._edges: list[tuple[Node, Node]] = []       # 有向边列表 (src, dst)

    # ------------------------------------------------------------------
    # 全局变量 API
    # ------------------------------------------------------------------

    def set(self, key: str, value: Any) -> None:
        with self._vars_lock:
            self._vars[key] = value

    from typing import TypeVar as _TV
    from typing import overload as _overload
    _T = _TV("_T")

    @_overload
    def get(self, key: str) -> Any: ...
    @_overload
    def get(self, key: str, default: "_T") -> "_T": ...

    def get(self, key: str, default: Any = None) -> Any:
        with self._vars_lock:
            return self._vars.get(key, default)

    # ------------------------------------------------------------------
    # 生命周期钩子 API
    # ------------------------------------------------------------------

    def on_startup(self, fn: Callable[["Pipeline"], None]) -> Callable[["Pipeline"], None]:
        self._on_startup.append(fn)
        return fn

    def on_run_start(self, fn: Callable[["Context"], None]) -> Callable[["Context"], None]:
        self._on_run_start.append(fn)
        return fn

    def on_run_end(self, fn: Callable[["Context"], None]) -> Callable[["Context"], None]:
        self._on_run_end.append(fn)
        return fn

    def _fire_startup(self) -> None:
        with self._startup_lock:
            if not self._started:
                self._started = True
                for fn in self._on_startup:
                    fn(self)

    def _fire_run_end(self, ctx: Context) -> None:
        for fn in self._on_run_end:
            fn(ctx)

    # ------------------------------------------------------------------
    # Builder API
    # ------------------------------------------------------------------

    def _connect(self, src: Node, dst: Node) -> None:
        self._edges.append((src, dst))

    def add_node(self, n: "Node") -> "Pipeline":
        """追加节点，返回 self 支持链式调用。"""
        if not isinstance(n, Node):
            raise TypeError(
                f"Expected Node, got {type(n).__name__!r}. "
                "Use @node decorator."
            )
        if self._pending_branches:
            raise RuntimeError(
                "There are pending branches waiting for join(). "
                "Call join() before add_node()."
            )

        if self._tail is not None:
            self._connect(self._tail, n)
        # else: 第一个节点，_upstream_count 由 _build_workers 设为 1

        self._nodes.append(n)
        self._tail = n
        return self

    def add_nodes(self, *nodes: "Node") -> "Pipeline":
        """追加多个节点，返回 self 支持链式调用。"""
        for n in nodes:
            self.add_node(n)
        return self

    def branch(
        self,
        *args: "Node | Pipeline",
        mode: Literal["broadcast", "route"] = "broadcast",
    ) -> "Pipeline":
        """从当前尾节点开叉，创建多个并行分支。

        Args:
            *args:  分支，至少 2 个。每个分支可以是：

                    - :class:`Node` — 单节点分支；
                    - :class:`Pipeline` — 子流水线分支（支持任意深度嵌套）。

                    当传入 ``Pipeline`` 时，子流水线必须已完全定义（无 pending branches）。
                    子流水线的所有节点会被平展合并到主流水线中。

            mode:   ``"broadcast"`` 每个 item 广播到所有分支；
                    ``"route"`` 节点函数返回 ``(branch_key, value)`` 按名称路由。

        Examples::

            # 普通节点分支
            pipeline.add_node(parse).branch(enrich_a, enrich_b).join(merge)

            # Pipeline 子图分支（支持嵌套）
            branch_a = Pipeline().add_nodes(enrich_a, clean_a)
            branch_b = Pipeline().add_nodes(enrich_b, clean_b)
            pipeline.add_node(parse).branch(branch_a, branch_b).join(merge)

            # 混合 Node / Pipeline
            pipeline.add_node(parse).branch(enrich_a, branch_b).join(merge)
        """
        if self._tail is None:
            raise RuntimeError(
                "branch() requires at least one preceding node.")
        if self._pending_branches:
            raise RuntimeError(
                "There are pending branches waiting for join(). "
                "Call join() before branch()."
            )
        if len(args) < 2:
            raise ValueError("branch() requires at least 2 branch arguments.")

        self._tail.branch_mode = mode

        for arg in args:
            if isinstance(arg, Node):
                head_node = arg
                tail_node = arg
                self._nodes.append(arg)

            elif isinstance(arg, Pipeline):
                if not arg._nodes:
                    raise ValueError(
                        "Branch Pipeline must have at least one node.")
                if arg._pending_branches:
                    raise RuntimeError(
                        "Branch Pipeline has unjoined branches. "
                        "Call join() to complete it before using as a branch."
                    )
                if arg._tail is None:
                    raise RuntimeError(
                        "Branch Pipeline has no tail node. "
                        "Make sure it has at least one node and all branches are joined."
                    )
                # 子图头节点：_upstream_count == 0 的第一个节点
                head_node = next(
                    (n for n in arg._nodes if n not in {
                     dst for _, dst in arg._edges}),
                    arg._nodes[0],
                )
                tail_node = arg._tail
                self._nodes.extend(arg._nodes)
                self._edges.extend(arg._edges)

            else:
                raise TypeError(
                    f"Expected Node or Pipeline, got {type(arg).__name__!r}."
                )

            self._connect(self._tail, head_node)
            self._pending_branches.append(tail_node)

        self._tail = None
        return self

    def join(
        self,
        n: "Node",
        *,
        gather: bool = False,
    ) -> "Pipeline":
        """将所有待合并分支汇入节点 n。

        Args:
            n:       合并目标节点。
            gather:  ``True`` 时等所有分支完成后将结果收集为 list 再处理；
                     ``False`` 时流式接收（先完成先处理）。
        """
        if not self._pending_branches:
            raise RuntimeError("join() requires a preceding branch().")
        if not isinstance(n, Node):
            raise TypeError(
                f"Expected Node, got {type(n).__name__!r}. "
                "Use @node decorator."
            )

        if gather:
            n.gather = True

        for branch_end in self._pending_branches:
            self._connect(branch_end, n)

        self._nodes.append(n)
        self._pending_branches = []
        self._tail = n
        return self

    # ------------------------------------------------------------------
    # 运行
    # ------------------------------------------------------------------

    def make_context(self, **kwargs) -> Context:
        """创建一个新的 Context 实例，预设初始值。"""
        return Context(initial={**self.initial, **kwargs, "pipeline": self})

    def _build_workers(self, item: object) -> tuple[Context, Node, Node, Node]:
        if not self._nodes:
            raise RuntimeError(f"Pipeline '{self.name}' has no nodes.")
        if self._pending_branches:
            raise RuntimeError(
                f"Pipeline '{self.name}' has unjoined branches. "
                "Call join() to complete the DAG."
            )

        # collector 是被动存储节点：不启动 worker，数据直接 submit 进 task_queue
        # 当所有叶节点都 done() 后，直接 set shutdown event
        collector = Node(lambda x: x, name="__collector__")

        def _collector_done(self=collector) -> None:
            with collector._done_lock:
                collector._done_received += 1
                if collector._done_received >= collector._upstream_count:
                    collector.shutdown.set()

        collector.done = _collector_done  # type: ignore[method-assign]

        # 1. 为所有节点创建快照（不含 DAG 连接）
        orig_to_snap: dict[Node, Node] = {
            n: n.snapshot() for n in self._nodes
        }

        # 2. 从 _edges 重建 DAG 连接（在快照上）
        for src_orig, dst_orig in self._edges:
            src_snap = orig_to_snap[src_orig]
            dst_snap = orig_to_snap[dst_orig]
            src_snap.nexts.append(dst_snap)
            dst_snap._upstream_count += 1

        # 复制各节点的 branch_mode
        for orig, snap in orig_to_snap.items():
            snap.branch_mode = orig.branch_mode

        # 3. 找出度为 0 的节点（叶节点），接到 collector
        dst_set = {dst_orig for _, dst_orig in self._edges}
        src_set = {src_orig for src_orig, _ in self._edges}
        leaves: list[Node] = [
            orig_to_snap[n] for n in self._nodes if n not in src_set
        ]
        for leaf in leaves:
            leaf.nexts.append(collector)
            collector._upstream_count += 1

        # 4. 头节点（无入边）的 _upstream_count 来自外部 submit，固定为 1
        heads_orig = [n for n in self._nodes if n not in dst_set]
        for orig in heads_orig:
            snap = orig_to_snap[orig]
            if snap._upstream_count == 0:
                snap._upstream_count = 1

        if collector._upstream_count == 0:
            collector._upstream_count = 1

        # 5. 启动所有快照节点
        for snap in orig_to_snap.values():
            snap.start()

        # 6. 找入口节点（head）
        if heads_orig:
            head = orig_to_snap[heads_orig[0]]
        else:
            head = orig_to_snap[self._nodes[0]]

        # 7. 找 tail（出度为 0 的最后一个节点）
        tail_orig = next(
            (n for n in reversed(self._nodes) if n not in src_set),
            self._nodes[-1]
        )
        tail = orig_to_snap[tail_orig]

        # 8. 构造初始 ctx 并提交
        if isinstance(item, Context):
            ctx = item
        else:
            ctx = self.make_context(input=item)

        for fn in self._on_run_start:
            fn(ctx)

        head.submit(ctx)
        head.done()
        return ctx, head, tail, collector

    def run(self, item: object, timeout: float | None = None) -> Iterable[Any]:
        self._fire_startup()
        with self.semaphore:
            ctx, _, tail, collector = self._build_workers(item)
            try:
                collector.wait(timeout=timeout)
                return self.on_result(self, collector.task_queue.queue, ctx)
            finally:
                self._fire_run_end(ctx)

    def stream(self, item: object) -> Generator[Context, Any, None]:
        self._fire_startup()
        with self.semaphore:
            ctx, _, tail, collector = self._build_workers(item)
            try:
                while True:
                    try:
                        yield collector.task_queue.get(timeout=1)
                    except Exception:
                        if not collector.shutdown.is_set():
                            continue
                        else:
                            break
            finally:
                self._fire_run_end(ctx)

    def __repr__(self) -> str:
        return (
            f"Pipeline(name={self.name!r}, "
            f"nodes={[n.name for n in self._nodes]})"
        )

    # ------------------------------------------------------------------
    # HTTP 服务
    # ------------------------------------------------------------------

    def serve(
        self,
        host: str = "0.0.0.0",
        port: int = 8080,
        *,
        prefix: str = "",
    ) -> None:
        """启动 HTTP 服务（阻塞当前线程）。

        开启后可通过 RESTful 接口远程提交、查询、取消任务。

        接口列表（假设 ``prefix="/api/v1"``）
        ---------------------------------------
        GET  {prefix}/health                — 心跳 / 服务状态
        POST {prefix}/shutdown              — 退出服务
        POST {prefix}/tasks                 — 提交任务（同步 / 异步）
        GET  {prefix}/tasks[?status=...]    — 任务列表（可过滤）
        GET  {prefix}/tasks/{task_id}       — 查询单个任务
        DELETE {prefix}/tasks/{task_id}     — 取消任务

        任务提交请求体（JSON）:
            input    : any   — 待处理的 item 输入（可为空）
            timeout  : float  — 本次运行最大允许秒数（可选）

        Args:
            host:   监听地址，使用 ``"0.0.0.0"`` 可局域网访问（默认 ``"0.0.0.0"``）。
            port:   监听端口（默认 ``8080``）。
            prefix: URL 路径前缀，如 ``"/api/v1"``（默认为空）。

        Examples::

            # 基本用法
            pipeline = Pipeline("demo") | step1 | step2
            pipeline.serve(host="0.0.0.0", port=9000)

            # 自定义路径前缀
            pipeline.serve(host="0.0.0.0", port=9000, prefix="/api/v1")
        """
        from rivus.serve import serve

        serve(self, host=host, port=port, prefix=prefix)
