"""Pipeline：基于 Queue 的数据流水线。

核心模型
--------
队列中流淌的是 **Context 对象**（而非裸 item）。
每个 Node 持有一个 ``fn(ctx)`` 函数和并发度 ``workers``::

    items → wrap → Q₀ → [Node₀, w=W₀] → Q₁ → [Node₁, w=W₁] → Q₂ → … → results
             ↑                ↑ fn(ctx)→derive
       derive(input=item)   节点读 ctx，返回值自动派生新 ctx

节点返回值约定
--------------
- 普通值     → ``ctx.derive(input=value)`` 传下游
- ``None``   → 当前 ctx 原样传下游（pass-through / 副作用节点）
- ``Context``→ 直接传下游（用户手动控制派生）
- ``yield``  → 每个值派生为独立 item（fan-out，一进多出）

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

三种运行模式
------------
- 批量：``run(*items)``
- 流式：``start() → submit() × N → close() → wait()``
- 后台：``run_background(*items)``

生命周期：IDLE → RUNNING → SUCCESS / FAILED / STOPPED / TIMEOUT
"""

from __future__ import annotations

import inspect
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from rivus.context import Context
from rivus.exceptions import (
    NodeError,
    NodeSkip,
    PipelineError,
    PipelineStop,
    PipelineTimeoutError,
)
from rivus.log import LogConfig, RivusLogger, build_logger, default_logger
from rivus.node import BaseNode, Node

# 队列结束哨兵
_DONE = object()


# ---------------------------------------------------------------------------
# 状态常量
# ---------------------------------------------------------------------------


class PipelineStatus:
    """Pipeline 生命周期状态常量。"""

    IDLE = "idle"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    STOPPED = "stopped"
    TIMEOUT = "timeout"


# ---------------------------------------------------------------------------
# 运行报告
# ---------------------------------------------------------------------------


@dataclass
class NodeReport:
    """单个节点的运行统计。"""
    name: str
    workers: int
    concurrency_type: str = "thread"
    items_in: int = 0
    items_out: int = 0
    items_skipped: int = 0
    errors: int = 0
    total_time_s: float = 0.0
    min_time_s: float = float("inf")
    max_time_s: float = 0.0
    _times: list = field(default_factory=list, repr=False, compare=False)

    def __post_init__(self) -> None:
        # 兼容显式传参时 _times 未初始化的情况
        if not hasattr(self, "_times"):
            object.__setattr__(self, "_times", [])

    @property
    def avg_time_s(self) -> float:
        """单 item 平均处理耗时（秒）。"""
        return self.total_time_s / self.items_out if self.items_out > 0 else 0.0

    @property
    def p50_time_s(self) -> float:
        """处理耗时中位数（秒）。"""
        if not self._times:
            return 0.0
        s = sorted(self._times)
        return s[len(s) // 2]

    @property
    def p95_time_s(self) -> float:
        """处理耗时 P95（秒）。"""
        if not self._times:
            return 0.0
        s = sorted(self._times)
        return s[int(len(s) * 0.95)]

    def _update_timing(self, elapsed: float) -> None:
        """更新耗时统计（NOT thread-safe，需在锁内调用）。"""
        self.total_time_s += elapsed
        self._times.append(elapsed)
        if elapsed < self.min_time_s:
            self.min_time_s = elapsed
        if elapsed > self.max_time_s:
            self.max_time_s = elapsed


@dataclass
class PipelineReport:
    """Pipeline 运行完成后的汇总报告。"""
    pipeline_name: str
    status: str
    total_duration_s: float
    results: list[Any] = field(default_factory=list)
    """每个 item 最终的 ``ctx.get("input")`` 值。"""
    contexts: list[Context] = field(default_factory=list)
    """每个 item 的最终 Context，可访问完整派生链上的所有键。"""
    errors: list[BaseException] = field(default_factory=list)
    nodes: list[NodeReport] = field(default_factory=list)

    @property
    def succeeded(self) -> bool:
        return self.status == PipelineStatus.SUCCESS

    def values(self, key: str = "input") -> list[Any]:
        """从所有最终 Context 中提取指定 key 的值。"""
        return [c.get(key) for c in self.contexts]


# ---------------------------------------------------------------------------
# 进程工作函数（模块级，保证可 pickle）
# ---------------------------------------------------------------------------


def _mp_run(
    fn: "Callable",
    data_dict: dict,
    is_gen: bool,
) -> tuple:
    """在子进程中执行节点函数，返回 (result_type, result_data, elapsed_s)。

    result_type 取值：
    - ``"gen"``   : fn 是生成器，result_data 是 list[("value"|"ctx", data)]
    - ``"none"``  : fn 返回 None（pass-through），result_data 是修改后的 data_dict
    - ``"ctx"``   : fn 返回 Context，result_data 是 dict
    - ``"value"`` : fn 返回普通值，result_data 是该值
    """
    import inspect as _inspect
    import time as _time

    from rivus.context import Context as _Context
    from rivus.exceptions import NodeSkip as _NodeSkip
    ctx = _Context(data_dict)
    t0 = _time.perf_counter()
    try:
        result = fn(ctx)
    except _NodeSkip as _skip:
        return ("skip", _skip.message, _time.perf_counter() - t0)
    if is_gen or _inspect.isgenerator(result):
        items = []
        for v in result:
            if isinstance(v, _Context):
                items.append(("ctx", dict(v._data)))
            else:
                items.append(("value", v))
        return ("gen", items, _time.perf_counter() - t0)
    elif result is None:
        return ("none", dict(ctx._data), _time.perf_counter() - t0)
    elif isinstance(result, _Context):
        return ("ctx", dict(result._data), _time.perf_counter() - t0)
    else:
        return ("value", result, _time.perf_counter() - t0)


# ---------------------------------------------------------------------------
# 单次运行共享状态（传递给各 worker / relay / collector 构造器）
# ---------------------------------------------------------------------------


class _RunCtx:
    """封装 _boot_workers 一次运行内所有工作线程共享的可变状态。"""

    __slots__ = (
        "stop_event", "errors", "errors_lock",
        "ordered", "fail_fast", "new_seq", "root_ctx", "pipeline_name",
    )

    def __init__(
        self,
        stop_event: threading.Event,
        errors: list,
        errors_lock: threading.Lock,
        ordered: bool,
        fail_fast: bool,
        new_seq: Callable,
        root_ctx: "Context",
        pipeline_name: str,
    ) -> None:
        self.stop_event = stop_event
        self.errors = errors
        self.errors_lock = errors_lock
        self.ordered = ordered
        self.fail_fast = fail_fast
        self.new_seq = new_seq
        self.root_ctx = root_ctx
        self.pipeline_name = pipeline_name


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


class Pipeline:
    """基于 Queue 的数据流水线。

    节点注册
    --------
    支持函数式节点（``@node`` 装饰器）和类式节点（继承 :class:`~rivus.BaseNode`）::

        @node(workers=4)
        def preprocess(ctx: Context):
            return transform(ctx.require("input"))

        class Inference(BaseNode):
            workers = 2
            def setup(self, ctx): self.model = ctx.require("model")
            def process(self, ctx): return self.model(ctx.require("input"))

        pipeline = Pipeline("classify") | preprocess | Inference()

    节点函数签名
    ------------
    - 只接收 ``ctx: Context``，通过 ``ctx.require("input")`` 读取当前 item
    - 返回值自动包装为派生 Context；返回 ``None`` = pass-through
    - ``yield`` 多个值 = fan-out（一个 item 产生多个下游 item）

    gather 模式（屏障聚合）
    -----------------------
    给节点加 ``gather=True`` 可在该节点前设置同步屏障：上游所有并发 item
    全部处理完毕后，结果值被收集为 ``list`` 作为单个 item 送入该节点。
    适用于需要等待并汇聚上游全量结果的 reduce / merge 场景::

        @node(workers=4)
        def fan_out(ctx): return heavy_work(ctx.require("input"))

        @node(gather=True)          # 等 fan_out 全部完成后触发
        def aggregate(ctx):
            results = ctx.require("input")   # list
            return merge(results)

    三种运行方式
    ------------
    .. code-block:: python

        # 批量（最常用）
        report = (Pipeline("p") | node_a | node_b).run(items)

        # 流式（持续推送）
        p.start()
        for x in stream: p.submit(x)
        p.close()
        report = p.wait()

        # 后台
        p.run_background(items, timeout=60)
        report = p.wait()

    Args:
        name: 流水线名称。
        context: 预初始化的 root Context；不传时自动创建。
        log_config: 日志配置。
        fail_fast: 任意节点出错立即停止（默认 True）。
        ordered: 结果按提交顺序排列（有轻微额外开销）。
    """

    def __init__(
        self,
        name: str = "pipeline",
        *,
        context: Context | None = None,
        log_config: LogConfig | RivusLogger | None = None,
        fail_fast: bool = True,
        ordered: bool = False,
    ) -> None:
        self.name = name
        self.fail_fast = fail_fast
        self.ordered = ordered
        self._nodes: list[Node] = []

        if isinstance(log_config, RivusLogger):
            _logger = log_config
        elif log_config is None:
            _logger = default_logger(f"rivus.{name}")
        else:
            from dataclasses import replace as _dc_replace
            cfg = (
                _dc_replace(log_config, name=f"rivus.{name}")
                if log_config.name == "rivus"
                else log_config
            )
            _logger = build_logger(cfg)

        self._ctx: Context = context or Context()
        self._ctx._attach_logger(_logger)

        self._state_lock = threading.Lock()
        self._status: str = PipelineStatus.IDLE
        self._stop_event = threading.Event()
        self._done_event = threading.Event()
        self._report: PipelineReport | None = None
        self._exc: BaseException | None = None
        self._q_submit: queue.Queue | None = None
        self._seq_counter: int = 0
        self._seq_lock = threading.Lock()
        self._start_time: float = 0.0

        # once 节点：记录已执行过一次的节点索引
        self._once_executed: set[int] = set()

        # 生命周期钩子
        self._init_done: bool = False
        self._hooks_init: list[Callable] = []
        self._hooks_start: list[Callable] = []
        self._hooks_end: list[Callable] = []
        self._hooks_failure: list[Callable] = []

        self._ctx._attach_stop_event(self._stop_event)

    # ------------------------------------------------------------------
    # Builder API
    # ------------------------------------------------------------------

    def add_node(self, n: "Node | BaseNode") -> "Pipeline":
        """追加节点，返回 self 支持链式调用。

        接受 :class:`~rivus.Node`（函数式）或 :class:`~rivus.BaseNode`（类式）。
        """
        if self._status == PipelineStatus.RUNNING:
            raise RuntimeError("Cannot add nodes while pipeline is running.")
        if isinstance(n, BaseNode):
            self._nodes.append(n._to_node())
        elif isinstance(n, Node):
            self._nodes.append(n)
        else:
            raise TypeError(
                f"Expected Node or BaseNode, got {type(n).__name__!r}. "
                "Use @node decorator or inherit from BaseNode."
            )
        return self

    def add_nodes(self, *nodes: "Node | BaseNode") -> "Pipeline":
        """追加多个节点，返回 self 支持链式调用。"""
        for n in nodes:
            self.add_node(n)
        return self

    def __or__(self, n: "Node | BaseNode") -> "Pipeline":
        """管道符语法：``pipeline | node_or_base_node``。"""
        return self.add_node(n)

    # ------------------------------------------------------------------
    # 生命周期钩子
    # ------------------------------------------------------------------

    def on_init(self, fn: Callable) -> Callable:
        """注册初始化钩子，在首次 ``run()`` 前调用一次。

        可用作装饰器或直接调用::

            @pipeline.on_init
            def setup(ctx: Context):
                ctx.set("model", load_model())

            # 或
            pipeline.on_init(lambda ctx: ctx.set("model", load_model()))

        Args:
            fn: 钩子函数，签名 ``(ctx: Context) -> None``。
        """
        self._hooks_init.append(fn)
        return fn

    def on_start(self, fn: Callable) -> Callable:
        """注册运行开始钩子，每次 ``run()`` 开始时调用。

        Args:
            fn: 钩子函数，签名 ``(ctx: Context) -> None``。
        """
        self._hooks_start.append(fn)
        return fn

    def on_end(self, fn: Callable) -> Callable:
        """注册运行成功结束钩子，每次 ``run()`` 成功完成后调用。

        Args:
            fn: 钩子函数，签名 ``(report: PipelineReport) -> None``。
        """
        self._hooks_end.append(fn)
        return fn

    def on_failure(self, fn: Callable) -> Callable:
        """注册运行失败钩子，每次 ``run()`` 失败后调用。

        Args:
            fn: 钩子函数，签名 ``(report: PipelineReport) -> None``。
        """
        self._hooks_failure.append(fn)
        return fn

    def _fire_start_hooks(self) -> None:
        """触发 init（首次）和 start（每次）钩子。"""
        if not self._init_done:
            self._init_done = True
            for h in self._hooks_init:
                h(self._ctx)
        for h in self._hooks_start:
            h(self._ctx)

    # ------------------------------------------------------------------
    # 状态查询
    # ------------------------------------------------------------------

    @property
    def context(self) -> Context:
        return self._ctx

    @property
    def nodes(self) -> list[Node]:
        return list(self._nodes)

    @property
    def status(self) -> str:
        return self._status

    @property
    def result(self) -> PipelineReport | None:
        return self._report

    @property
    def is_running(self) -> bool:
        return self._status == PipelineStatus.RUNNING

    # ------------------------------------------------------------------
    # 控制
    # ------------------------------------------------------------------

    def stop(self) -> "Pipeline":
        """发出停止信号（当前 item 处理完后生效）。"""
        self._stop_event.set()
        self._ctx.log.warning("Pipeline '%s': stop requested.", self.name)
        return self

    def wait(self, timeout: float | None = None) -> PipelineReport:
        """阻塞直到流水线完成。"""
        finished = self._done_event.wait(timeout=timeout)
        if not finished:
            raise TimeoutError(
                f"Pipeline '{self.name}' did not finish within {timeout}s. "
                "Call stop() to cancel."
            )
        if self._exc is not None:
            raise self._exc
        return self._report  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # 运行
    # ------------------------------------------------------------------

    def run(
        self,
        *items,
        initial: dict[str, Any] | None = None,
        timeout: float | None = None,
        repeat: int = 1,
    ) -> "PipelineReport | list[PipelineReport]":
        """同步批量运行。

        Args:
            items: 待处理的 item 可迭代对象。每个 item 会被包装为
                   ``root_ctx.derive(input=item)`` 传入第一个节点。
                   若 item 本身是 :class:`Context`，则直接使用。
                     当未提供任何 item 时，会自动投递一个
                     ``root_ctx.derive()``，确保流水线仍执行一次。
            initial: 运行前写入 root Context 的初始数据（模型、配置等）。
            timeout: 最长执行时间（秒）；超时后自动 stop 并抛出
                     :class:`~rivus.exceptions.PipelineTimeoutError`。
            repeat: 重复运行次数（默认 1）。
                    当 ``repeat > 1`` 时，依次运行并返回 ``list[PipelineReport]``；
                    每次都会触发 ``on_start`` / ``on_end`` / ``on_failure`` 钩子，
                    ``on_init`` 仅在整个 Pipeline 生命周期内触发一次。

        Returns:
            ``repeat=1`` 时返回 :class:`PipelineReport`；
            ``repeat>1`` 时返回 ``list[PipelineReport]``。
        """
        if repeat < 1:
            raise ValueError(f"repeat must be >= 1, got {repeat}")
        if repeat > 1:
            return [
                self.run(*items, initial=initial, timeout=timeout)
                for _ in range(repeat)
            ]
        self._assert_idle()
        self._reset_state()
        if initial:
            self._ctx.update(initial)
        self._fire_start_hooks()
        if timeout is not None:
            return self._run_with_timeout(items, timeout)
        self._boot_workers()
        with self._state_lock:
            self._status = PipelineStatus.RUNNING
        self._push_all(items)
        self._q_submit.put(_DONE)  # type: ignore[union-attr]
        return self.wait()

    def run_background(
        self,
        *items,
        initial: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> "Pipeline":
        """后台运行，立即返回。"""
        self._assert_idle()
        self._reset_state()
        if initial:
            self._ctx.update(initial)
        self._fire_start_hooks()

        def _target() -> None:
            self._boot_workers()
            with self._state_lock:
                self._status = PipelineStatus.RUNNING
            self._push_all(items)
            self._q_submit.put(_DONE)  # type: ignore[union-attr]

        threading.Thread(
            target=_target, daemon=True, name=f"rivus-bg-{self.name}"
        ).start()

        if timeout is not None:
            self._start_watchdog(timeout)
        return self

    # ------------------------------------------------------------------
    # 流式 API
    # ------------------------------------------------------------------

    def start(self, initial: dict[str, Any] | None = None) -> "Pipeline":
        """启动工作线程（流式模式）。之后调用 :meth:`submit` 逐条推送 item。"""
        self._assert_idle()
        self._reset_state()
        if initial:
            self._ctx.update(initial)
        self._fire_start_hooks()
        self._boot_workers()
        with self._state_lock:
            self._status = PipelineStatus.RUNNING
        self._ctx.log.info(
            "Pipeline '%s' started (%d nodes).", self.name, len(self._nodes)
        )
        return self

    def submit(self, item: Any) -> "Pipeline":
        """提交单个 item（需先调用 :meth:`start`）。"""
        if self._q_submit is None:
            raise RuntimeError("Call start() before submit().")
        if self._stop_event.is_set():
            raise RuntimeError(f"Pipeline '{self.name}' has been stopped.")
        item_ctx = item if isinstance(
            item, Context) else self._ctx.derive(input=item)
        self._q_submit.put(self._tag(item_ctx))
        return self

    def close(self) -> "Pipeline":
        """通知流水线不再有新 item（流式模式）。"""
        if self._q_submit is None:
            raise RuntimeError("Call start() before close().")
        self._q_submit.put(_DONE)
        return self

    # ------------------------------------------------------------------
    # 内部：状态管理
    # ------------------------------------------------------------------

    def _assert_idle(self) -> None:
        if self._status == PipelineStatus.RUNNING:
            raise RuntimeError(
                f"Pipeline '{self.name}' is already running. "
                "Call wait() or stop() first."
            )

    def _reset_state(self) -> None:
        self._stop_event.clear()
        self._done_event.clear()
        self._report = None
        self._exc = None
        self._q_submit = None
        self._seq_counter = 0
        with self._state_lock:
            self._status = PipelineStatus.IDLE

    def _tag(self, item_ctx: Context) -> Any:
        """ordered 模式：给 ctx 打序号；非 ordered 模式直传。"""
        if not self.ordered:
            return item_ctx
        with self._seq_lock:
            seq = self._seq_counter
            self._seq_counter += 1
        return (seq, item_ctx)

    def _new_seq(self) -> int:
        """分配一个新的序号（fan-out 时给每个 yielded item 分配新序号）。"""
        with self._seq_lock:
            seq = self._seq_counter
            self._seq_counter += 1
        return seq

    # ------------------------------------------------------------------
    # 内部：工作线程引导 —— 各组件构造器
    # ------------------------------------------------------------------

    @staticmethod
    def _make_relay(
        in_q: queue.Queue,
        out_q: queue.Queue,
        expect: int,
        forward: int,
        stop_event: threading.Event,
        label: str = "",
    ) -> threading.Thread:
        """relay 线程：等待上游 expect 个 _DONE 后，向下游发出 forward 个 _DONE。"""
        def _relay() -> None:
            done_count = 0
            while True:
                try:
                    item = in_q.get(timeout=0.05)
                except queue.Empty:
                    if stop_event.is_set():
                        for _ in range(forward):
                            out_q.put(_DONE)
                        return
                    continue
                if item is _DONE:
                    done_count += 1
                    if done_count >= expect:
                        for _ in range(forward):
                            out_q.put(_DONE)
                        return
                else:
                    out_q.put(item)

        return threading.Thread(target=_relay, daemon=True, name=f"rivus-relay-{label}")

    @staticmethod
    def _make_gather_relay(
        in_q: queue.Queue,
        out_q: queue.Queue,
        expect: int,
        forward: int,
        ordered: bool,
        stop_event: threading.Event,
        root_ctx: "Context",
        label: str = "",
    ) -> threading.Thread:
        """gather relay：收集上游全部 item，合并为单个 list 后传入下游节点。

        下游节点收到的 ``ctx.require("input")`` 将是所有上游结果值组成的列表。
        """
        def _relay() -> None:
            collected: list = []
            done_count = 0
            while True:
                try:
                    item = in_q.get(timeout=0.05)
                except queue.Empty:
                    if stop_event.is_set():
                        for _ in range(forward):
                            out_q.put(_DONE)
                        return
                    continue
                if item is _DONE:
                    done_count += 1
                    if done_count >= expect:
                        merged_ctx = root_ctx.derive(input=collected)
                        out_q.put((0, merged_ctx) if ordered else merged_ctx)
                        for _ in range(forward):
                            out_q.put(_DONE)
                        return
                else:
                    ctx_item = item[1] if ordered else item
                    collected.append(ctx_item.get("input"))

        return threading.Thread(target=_relay, daemon=True, name=f"rivus-gather-{label}")

    @staticmethod
    def _ctx_from_dict(data: dict, rc: "_RunCtx") -> "Context":
        """从子进程返回的 dict 重建 Context，附加日志器和停止信号。"""
        ctx = Context(data)
        ctx._attach_logger(rc.root_ctx._logger)
        ev = getattr(rc.root_ctx, "_stop_event_ref", None)
        if ev is not None:
            ctx._attach_stop_event(ev)
        return ctx

    def _make_thread_worker(
        self,
        fn: Callable,
        nname: str,
        is_gen: bool,
        in_q: queue.Queue,
        out_q: queue.Queue,
        nr: NodeReport,
        rc: "_RunCtx",
    ) -> Callable:
        """返回线程 worker 函数（供同节点 N 个 Thread 复用同一闭包）。"""
        def _worker() -> None:
            while True:
                try:
                    tagged = in_q.get(timeout=0.05)
                except queue.Empty:
                    if rc.stop_event.is_set():
                        out_q.put(_DONE)
                        return
                    continue

                if tagged is _DONE:
                    out_q.put(_DONE)
                    return
                if rc.stop_event.is_set():
                    out_q.put(_DONE)
                    return

                seq, item_ctx = tagged if rc.ordered else (None, tagged)

                with rc.errors_lock:
                    nr.items_in += 1

                _t0 = time.perf_counter()
                try:
                    result = fn(item_ctx)

                    if is_gen or inspect.isgenerator(result):
                        _count = 0
                        for val in result:
                            out_ctx = val if isinstance(
                                val, Context) else item_ctx.derive(input=val)
                            out_q.put((rc.new_seq(), out_ctx)
                                      if rc.ordered else out_ctx)
                            _count += 1
                        with rc.errors_lock:
                            nr.items_out += _count
                            nr._update_timing(time.perf_counter() - _t0)

                    elif result is None:
                        out_q.put((seq, item_ctx) if rc.ordered else item_ctx)
                        with rc.errors_lock:
                            nr.items_out += 1
                            nr._update_timing(time.perf_counter() - _t0)

                    elif isinstance(result, Context):
                        out_q.put((seq, result) if rc.ordered else result)
                        with rc.errors_lock:
                            nr.items_out += 1
                            nr._update_timing(time.perf_counter() - _t0)

                    else:
                        out_ctx = item_ctx.derive(input=result)
                        out_q.put((seq, out_ctx) if rc.ordered else out_ctx)
                        with rc.errors_lock:
                            nr.items_out += 1
                            nr._update_timing(time.perf_counter() - _t0)

                except NodeSkip as skip_exc:
                    with rc.errors_lock:
                        nr.items_skipped += 1
                    rc.root_ctx.log.debug(
                        "Pipeline '%s' node '%s' skipped item: %s",
                        rc.pipeline_name, nname, skip_exc.message,
                    )

                except PipelineStop as stop_exc:
                    rc.root_ctx.log.info(
                        "Pipeline '%s' stop requested by node '%s': %s",
                        rc.pipeline_name, nname, stop_exc,
                    )
                    rc.stop_event.set()
                    out_q.put(_DONE)
                    return

                except Exception as exc:
                    with rc.errors_lock:
                        nr.errors += 1
                        nr._update_timing(time.perf_counter() - _t0)
                        rc.errors.append(NodeError(nname, exc))
                    if rc.fail_fast:
                        rc.stop_event.set()
                        out_q.put(_DONE)
                        return

        return _worker

    def _make_process_coordinator(
        self,
        fn: Callable,
        nname: str,
        is_gen: bool,
        n_workers: int,
        in_q: queue.Queue,
        out_q: queue.Queue,
        nr: NodeReport,
        rc: "_RunCtx",
    ) -> Callable:
        """返回进程模式协调函数（单线程驱动 ProcessPoolExecutor）。"""
        def _coordinator() -> None:
            from concurrent.futures import ProcessPoolExecutor

            pending: dict = {}   # future → (seq, item_ctx)
            done_count = 0
            input_exhausted = False

            with ProcessPoolExecutor(max_workers=n_workers) as executor:
                while True:
                    # ── 处理已完成的 future ───────────────────────────
                    for future in [f for f in list(pending) if f.done()]:
                        seq, item_ctx = pending.pop(future)
                        try:
                            res_type, res_data, elapsed = future.result()
                            with rc.errors_lock:
                                nr._update_timing(elapsed)
                            if res_type == "skip":
                                with rc.errors_lock:
                                    nr.items_skipped += 1
                                rc.root_ctx.log.debug(
                                    "Pipeline '%s' node '%s' skipped item: %s",
                                    rc.pipeline_name, nname, res_data,
                                )
                            elif res_type == "gen":
                                for vtype, vdata in res_data:
                                    out_ctx = (
                                        self._ctx_from_dict(vdata, rc)
                                        if vtype == "ctx"
                                        else item_ctx.derive(input=vdata)
                                    )
                                    out_q.put((rc.new_seq(), out_ctx)
                                              if rc.ordered else out_ctx)
                                    with rc.errors_lock:
                                        nr.items_out += 1
                            else:
                                out_ctx = (
                                    self._ctx_from_dict(res_data, rc)
                                    if res_type in ("none", "ctx")
                                    else item_ctx.derive(input=res_data)
                                )
                                out_q.put((seq, out_ctx)
                                          if rc.ordered else out_ctx)
                                with rc.errors_lock:
                                    nr.items_out += 1
                        except Exception as exc:
                            with rc.errors_lock:
                                nr.errors += 1
                                rc.errors.append(NodeError(nname, exc))
                            if rc.fail_fast:
                                rc.stop_event.set()

                    if rc.stop_event.is_set():
                        for f in list(pending):
                            f.cancel()
                        pending.clear()
                        break

                    if input_exhausted and not pending:
                        break
                    if input_exhausted:
                        time.sleep(0.005)
                        continue

                    # ── 读取下一个 input ──────────────────────────────
                    try:
                        tagged = in_q.get(timeout=0.05)
                    except queue.Empty:
                        continue

                    if tagged is _DONE:
                        done_count += 1
                        if done_count >= n_workers:
                            input_exhausted = True
                        continue

                    seq, item_ctx = tagged if rc.ordered else (None, tagged)
                    with rc.errors_lock:
                        nr.items_in += 1
                    pending[executor.submit(_mp_run, fn, dict(
                        item_ctx._data), is_gen)] = (seq, item_ctx)

            # relay 期望收到 n_workers 个 _DONE
            for _ in range(n_workers):
                out_q.put(_DONE)

        return _coordinator

    def _make_collector(
        self,
        q_results: queue.Queue,
        node_reports: list,
        rc: "_RunCtx",
    ) -> threading.Thread:
        """创建结果收集线程，负责排序、生成报告、设置最终状态。"""
        def _collect() -> None:
            raw: list = []
            while True:
                try:
                    item = q_results.get(timeout=0.05)
                except queue.Empty:
                    continue
                if item is _DONE:
                    break
                raw.append(item)

            if rc.ordered:
                raw.sort(key=lambda x: x[0])
                final_ctxs = [ctx for _, ctx in raw]
            else:
                final_ctxs = raw

            total_duration = time.perf_counter() - self._start_time

            with self._state_lock:
                if self._status == PipelineStatus.TIMEOUT:
                    final_status = PipelineStatus.TIMEOUT
                elif rc.stop_event.is_set() and not rc.errors:
                    final_status = PipelineStatus.STOPPED
                elif rc.errors and rc.fail_fast:
                    final_status = PipelineStatus.FAILED
                else:
                    final_status = PipelineStatus.SUCCESS
                self._status = final_status

            self._report = PipelineReport(
                pipeline_name=self.name,
                status=final_status,
                total_duration_s=total_duration,
                results=[c.get("input") for c in final_ctxs],
                contexts=final_ctxs,
                errors=list(rc.errors),
                nodes=node_reports,
            )

            log = rc.root_ctx.log
            if final_status == PipelineStatus.SUCCESS:
                log.info("Pipeline '%s' succeeded in %.3fs, %d results.",
                         self.name, total_duration, len(final_ctxs))
            elif final_status == PipelineStatus.FAILED:
                log.error("Pipeline '%s' failed in %.3fs.",
                          self.name, total_duration)
                self._exc = PipelineError(self.name, rc.errors[0])
            elif final_status == PipelineStatus.STOPPED:
                log.warning("Pipeline '%s' stopped after %.3fs.",
                            self.name, total_duration)
            else:
                log.error("Pipeline '%s' timed out after %.3fs.",
                          self.name, total_duration)

            # ── 生命周期钩子 ──────────────────────────────────────────
            hooks = (
                self._hooks_end
                if final_status == PipelineStatus.SUCCESS
                else self._hooks_failure
            )
            for h in hooks:
                try:
                    h(self._report)
                except Exception as hook_exc:
                    log.warning(
                        "Pipeline '%s': hook %r raised: %s",
                        self.name, getattr(h, "__name__", h), hook_exc,
                    )

            self._done_event.set()

        return threading.Thread(target=_collect, daemon=True, name=f"rivus-collector-{self.name}")

    # ------------------------------------------------------------------
    # 内部：工作线程引导 —— 编排入口
    # ------------------------------------------------------------------

    def _boot_workers(self) -> None:
        """创建并启动所有队列、relay / worker / collector 线程。"""
        if not self._nodes:
            raise RuntimeError(
                "No nodes in pipeline. Use add_node() or | operator.")

        nodes = self._nodes
        n = len(nodes)

        # ── 节点初始化 ────────────────────────────────────────────────
        # once 节点：首次运行时调用 setup_fn，后续跳过
        for i, nd in enumerate(nodes):
            if nd.setup_fn is not None and not (nd.once and i in self._once_executed):
                nd.setup_fn(self._ctx)

        node_reports = [
            NodeReport(name=nd.name, workers=nd.workers,
                       concurrency_type=nd.concurrency_type)
            for nd in nodes
        ]

        rc = _RunCtx(
            stop_event=self._stop_event,
            errors=[],
            errors_lock=threading.Lock(),
            ordered=self.ordered,
            fail_fast=self.fail_fast,
            new_seq=self._new_seq,
            root_ctx=self._ctx,
            pipeline_name=self.name,
        )

        # ── 队列 ──────────────────────────────────────────────────────
        q_submit: queue.Queue = queue.Queue()
        q_in = [queue.Queue(maxsize=nd.queue_size) for nd in nodes]
        q_out = [queue.Queue() for _ in nodes]
        q_results: queue.Queue = queue.Queue()
        self._q_submit = q_submit

        # ── Relay 线程 ────────────────────────────────────────────────
        relay_threads = [
            self._make_relay(
                q_submit, q_in[0], 1, nodes[0].workers, rc.stop_event, f"{self.name}/0"),
            *[
                self._make_gather_relay(
                    q_out[i], q_in[i + 1],
                    expect=nodes[i].workers,
                    forward=nodes[i + 1].workers,
                    ordered=rc.ordered,
                    stop_event=rc.stop_event,
                    root_ctx=rc.root_ctx,
                    label=f"{self.name}/{i}→{i+1}",
                ) if nodes[i + 1].gather else
                self._make_relay(q_out[i], q_in[i + 1], nodes[i].workers,
                                 nodes[i + 1].workers, rc.stop_event, f"{self.name}/{i}→{i+1}")
                for i in range(n - 1)
            ],
            self._make_relay(
                q_out[-1], q_results, nodes[-1].workers, 1, rc.stop_event, f"{self.name}/out"),
        ]

        # ── Worker 线程 ───────────────────────────────────────────────
        worker_threads: list[threading.Thread] = []
        for i, nd in enumerate(nodes):
            if nd.once and i in self._once_executed:
                # once 节点已在前次 run() 执行过，直接透传 item
                t = self._make_relay(
                    q_in[i], q_out[i],
                    expect=nd.workers, forward=nd.workers,
                    stop_event=rc.stop_event,
                    label=f"{self.name}/{nd.name}/once-skip",
                )
                worker_threads.append(t)
            else:
                if nd.once:
                    # 首次执行，标记为已完成；下次 run() 将透传
                    self._once_executed.add(i)
                if nd.concurrency_type == "process":
                    worker_threads.append(threading.Thread(
                        target=self._make_process_coordinator(
                            nd.fn, nd.name, nd.is_generator, nd.workers,
                            q_in[i], q_out[i], node_reports[i], rc,
                        ),
                        daemon=True, name=f"rivus-proc-{nd.name}",
                    ))
                else:
                    fn = self._make_thread_worker(
                        nd.fn, nd.name, nd.is_generator,
                        q_in[i], q_out[i], node_reports[i], rc,
                    )
                    for _ in range(nd.workers):
                        worker_threads.append(threading.Thread(
                            target=fn, daemon=True, name=f"rivus-{nd.name}"))

        # ── 启动 ──────────────────────────────────────────────────────
        collector = self._make_collector(q_results, node_reports, rc)
        self._start_time = time.perf_counter()
        for t in relay_threads + worker_threads + [collector]:
            t.start()

    # ------------------------------------------------------------------
    # 内部：提交辅助
    # ------------------------------------------------------------------

    def _push_all(self, items: Iterable[Any] | None) -> None:
        items = items or [None]

        for item in items:
            if self._stop_event.is_set():
                break
            item_ctx = item if isinstance(
                item, Context) else self._ctx.derive(input=item)
            self._q_submit.put(self._tag(item_ctx))  # type: ignore[union-attr]

    # ------------------------------------------------------------------
    # 内部：超时 & 看门狗
    # ------------------------------------------------------------------

    def _run_with_timeout(self, items: Iterable[Any] | None, timeout: float) -> PipelineReport:
        exc_box: list[BaseException] = []

        def _target() -> None:
            self._boot_workers()
            with self._state_lock:
                self._status = PipelineStatus.RUNNING
            self._push_all(items)
            self._q_submit.put(_DONE)  # type: ignore[union-attr]

        th = threading.Thread(target=_target, daemon=True,
                              name=f"rivus-{self.name}")
        th.start()
        th.join(timeout=timeout)

        if th.is_alive():
            self._stop_event.set()
            self._done_event.wait(timeout=2.0)
            with self._state_lock:
                if self._status == PipelineStatus.RUNNING:
                    self._status = PipelineStatus.TIMEOUT
            exc = PipelineTimeoutError(self.name, timeout)
            self._exc = exc
            self._done_event.set()
            self._ctx.log.error(
                "Pipeline '%s' timed out after %.1fs.", self.name, timeout
            )
            raise exc

        self._done_event.wait(timeout=2.0)
        if self._exc is not None:
            raise self._exc
        return self._report  # type: ignore[return-value]

    def _start_watchdog(self, timeout: float) -> None:
        def _watchdog() -> None:
            finished = self._done_event.wait(timeout=timeout)
            if not finished:
                self._stop_event.set()
                timed_out = False
                with self._state_lock:
                    if self._status == PipelineStatus.RUNNING:
                        self._status = PipelineStatus.TIMEOUT
                        self._exc = PipelineTimeoutError(self.name, timeout)
                        timed_out = True
                if timed_out:
                    self._done_event.set()
                    self._ctx.log.error(
                        "Pipeline '%s' timed out after %.1fs.", self.name, timeout
                    )

        threading.Thread(
            target=_watchdog, daemon=True, name=f"rivus-watchdog-{self.name}"
        ).start()

    def __repr__(self) -> str:
        return (
            f"Pipeline(name={self.name!r}, status={self._status!r}, "
            f"nodes={[n.name for n in self._nodes]})"
        )
