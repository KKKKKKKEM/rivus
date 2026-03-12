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

import abc
import inspect
import queue
import random
import threading
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from loguru import logger

from rivus.context import Context
from rivus.exceptions import (
    NodeError,
    NodeSkip,
    PipelineError,
    PipelineStop,
    PipelineTimeoutError,
)
from rivus.node import BaseNode, Node

# 队列哨兵
_DONE = object()   # 正常流结束
_STOP = object()   # 紧急停止（级联传播，唤醒所有阻塞线程）


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
    _lock: threading.Lock = field(
        default_factory=threading.Lock, repr=False, compare=False)

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
        """更新耗时统计（须在 _lock 内调用）。使用蓄水池采样，最多保留 10000 个样本。"""
        self.total_time_s += elapsed
        n = len(self._times)
        if n < 10_000:
            self._times.append(elapsed)
        else:
            # 蓄水池采样：以 10000/items_out 的概率替换随机位置
            idx = random.randint(0, self.items_out - 1)
            if idx < 10_000:
                self._times[idx] = elapsed
        if elapsed < self.min_time_s:
            self.min_time_s = elapsed
        if elapsed > self.max_time_s:
            self.max_time_s = elapsed


@dataclass
class PipelineReport:
    """Pipeline 单次运行完成后的汇总报告。"""
    pipeline_name: str
    status: str
    total_duration_s: float
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    results: list[Any] = field(default_factory=list)
    errors: list[BaseException] = field(default_factory=list)
    nodes: list[NodeReport] = field(default_factory=list)

    @property
    def succeeded(self) -> bool:
        return self.status == PipelineStatus.SUCCESS


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
# RunStorage —— 可插拔后端的 _RunState 存储层
# ---------------------------------------------------------------------------


class RunStorageBackend(abc.ABC):
    """RunStorage 后端接口。自定义存储时继承此类并实现所有抽象方法。"""

    @abc.abstractmethod
    def save(self, run_id: str, state: "_RunState") -> None:
        """Persist or cache a completed _RunState."""

    @abc.abstractmethod
    def get(self, run_id: str) -> "_RunState | None":
        """Return the _RunState for *run_id*, or None if not found."""

    @abc.abstractmethod
    def list(self) -> list[str]:
        """Return all known run_ids, ordered oldest → newest."""

    @abc.abstractmethod
    def clear(self) -> None:
        """Remove all stored runs."""


class InMemoryStorage(RunStorageBackend):
    """LRU 内存实现。超过 *max_runs* 后自动淘汰最旧的 run。"""

    def __init__(self, max_runs: int = 100) -> None:
        self._max = max_runs
        self._store: OrderedDict[str, "_RunState"] = OrderedDict()
        self._lock = threading.Lock()

    def save(self, run_id: str, state: "_RunState") -> None:
        with self._lock:
            self._store[run_id] = state
            self._store.move_to_end(run_id)
            while len(self._store) > self._max:
                self._store.popitem(last=False)

    def get(self, run_id: str) -> "_RunState | None":
        with self._lock:
            return self._store.get(run_id)

    def list(self) -> list[str]:
        with self._lock:
            return list(self._store.keys())

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


class RunStorage:
    """Pipeline 对外暴露的 run 状态管理接口。

    通过 ``pipeline.storage`` 访问。默认使用 :class:`InMemoryStorage` 后端。
    可在 ``Pipeline.__init__`` 中传入自定义 :class:`RunStorageBackend`。

    Examples::

        # 查询某次 run 的报告
        state = pipeline.storage.get(run_id)
        if state:
            print(state.report)

        # 列出所有 run_id
        for rid in pipeline.storage.list():
            print(rid)
    """

    def __init__(self, backend: RunStorageBackend) -> None:
        self._backend = backend

    def get(self, run_id: str) -> "_RunState | None":
        """Return the _RunState for *run_id*, or None if not found."""
        return self._backend.get(run_id)

    def list(self) -> list[str]:
        """Return all known run_ids, ordered oldest → newest."""
        return self._backend.list()

    def clear(self) -> None:
        """Remove all stored runs from the backend."""
        self._backend.clear()

    def __getitem__(self, run_id: str) -> "_RunState":
        state = self._backend.get(run_id)
        if state is None:
            raise KeyError(run_id)
        return state

    def __len__(self) -> int:
        return len(self._backend.list())

    def __contains__(self, run_id: object) -> bool:
        return isinstance(run_id, str) and self._backend.get(run_id) is not None


# ---------------------------------------------------------------------------
# 单次运行状态（完全独立，每次 run 创建一个新实例）
# ---------------------------------------------------------------------------


class _RunState:
    """封装一次 Pipeline.run() 内所有工作线程共享的可变状态。

    每次调用 run() / run_background() / start() 都会创建一个全新的
    _RunState 实例，使多次运行之间完全独立，互不影响。
    """

    __slots__ = (
        # 运行唯一标识
        "run_id",
        # 控制信号
        "stop_event", "done_event",
        # 提交队列
        "q_submit",
        # 序号（ordered 模式）
        "seq_counter", "seq_lock",
        # 结果与状态
        "status", "status_lock",
        "report", "exc",
        # 错误收集
        "errors", "errors_lock",
        # 计时
        "start_time",
        # 运行参数（从 Pipeline 复制，避免闭包捕获 self）
        "ordered", "fail_fast", "root_ctx", "pipeline_name",
    )

    def __init__(
        self,
        ordered: bool,
        fail_fast: bool,
        root_ctx: "Context",
        pipeline_name: str,
        run_id: str | None = None,
    ) -> None:
        self.run_id: str = run_id if run_id is not None else str(uuid.uuid4())
        self.stop_event = threading.Event()
        self.done_event = threading.Event()
        self.q_submit: queue.Queue | None = None
        self.seq_counter: int = 0
        self.seq_lock = threading.Lock()
        self.status: str = PipelineStatus.IDLE
        self.status_lock = threading.Lock()
        self.report: PipelineReport | None = None
        self.exc: BaseException | None = None
        self.errors: list = []
        self.errors_lock = threading.Lock()
        self.start_time: float = 0.0
        self.ordered = ordered
        self.fail_fast = fail_fast
        self.root_ctx = root_ctx
        self.pipeline_name = pipeline_name

    def new_seq(self) -> int:
        """分配一个新的序号（fan-out 时给每个 yielded item 分配新序号）。"""
        with self.seq_lock:
            seq = self.seq_counter
            self.seq_counter += 1
        return seq

    def tag(self, item_ctx: "Context") -> Any:
        """ordered 模式：给 ctx 打序号；非 ordered 模式直传。"""
        if not self.ordered:
            return item_ctx
        with self.seq_lock:
            seq = self.seq_counter
            self.seq_counter += 1
        return (seq, item_ctx)

    def inject_stop(self) -> None:
        """设置 stop_event 并向 q_submit 注入 _STOP 哨兵（级联停止）。"""
        self.stop_event.set()
        if self.q_submit is not None:
            try:
                self.q_submit.put_nowait(_STOP)
            except queue.Full:
                pass

    def wait(self, timeout: float | None = None) -> "PipelineReport":
        """阻塞直到本次运行完成，返回 PipelineReport。"""
        finished = self.done_event.wait(timeout=timeout)
        if not finished:
            raise TimeoutError(
                f"Pipeline '{self.pipeline_name}' did not finish within {timeout}s."
            )
        if self.exc is not None:
            raise self.exc
        return self.report  # type: ignore[return-value]


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
        fail_fast: 任意节点出错立即停止（默认 True）。
        ordered: 结果按提交顺序排列（有轻微额外开销）。
        timeout_grace: 超时触发后等待 collector 完成的宽限时间（秒，默认 5.0）。
    """

    def __init__(
        self,
        name: str = "pipeline",
        *,
        context: Context | None = None,
        fail_fast: bool = True,
        ordered: bool = False,
        timeout_grace: float = 5.0,
        storage: RunStorageBackend | None = None,
        max_runs: int = 100,
    ) -> None:
        self.name = name
        self.fail_fast = fail_fast
        self.ordered = ordered
        self.timeout_grace = timeout_grace
        self._nodes: list[Node] = []

        self._ctx: Context = context or Context()
        self._ctx._attach_pipeline(self)

        # ── per-pipeline 状态（跨 run 共享）──────────────────────────────
        # once 节点：记录已执行过一次的节点索引
        self._once_executed: set[int] = set()
        self._once_lock = threading.Lock()

        # 生命周期钉子
        self._init_done: bool = False
        self._hooks_init: list[Callable] = []
        self._hooks_start: list[Callable] = []
        self._hooks_end: list[Callable] = []
        self._hooks_failure: list[Callable] = []

        # 全局变量存储（Pipeline 生命周期内跨 run 共享）
        self._vars: dict[str, Any] = {}
        self._vars_lock = threading.Lock()

        # ── RunStorage ：关联所有 run 的 _RunState ────────────────────
        _backend = storage if storage is not None else InMemoryStorage(
            max_runs=max_runs)
        self.storage = RunStorage(_backend)

        # ── 当前活跃 run_id（用于 wait() / stop() / submit() / close() 委托）
        # run() 是同步的，不需要此字段；
        # run_background() / start() 会写入此字段。
        self._active_run_id: str | None = None
        self._active_run_lock = threading.Lock()

        # ── 并发运行控制：同一时刻最多一个 run ────────────────────────────
        self._running_lock = threading.Lock()
        self._is_running: bool = False

    # ------------------------------------------------------------------
    # 全局变量 API
    # ------------------------------------------------------------------

    def set(self, key: str, value: Any) -> None:
        """设置 Pipeline 级全局变量（线程安全）。

        适合存储 Pipeline 实例化后即创建、需在所有节点间复用的对象，
        如模型、数据库连接、共享配置等。

        Args:
            key:   变量名。
            value: 任意值。

        Examples::

            pipeline = Pipeline("infer")
            pipeline.set("model", load_model())
        """
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
        """获取 Pipeline 级全局变量（线程安全）。

        Args:
            key:     变量名。
            default: 键不存在时的默认值，默认为 ``None``。

        Returns:
            对应变量值，或 *default*。

        Examples::

            @node(workers=4)
            def infer(ctx: Context):
                model = ctx.pipeline.get("model")
                return model(ctx.require("input"))
        """
        with self._vars_lock:
            return self._vars.get(key, default)

    # ------------------------------------------------------------------
    # Builder API
    # ------------------------------------------------------------------

    def add_node(self, n: "Node | BaseNode") -> "Pipeline":
        """追加节点，返回 self 支持链式调用。

        接受 :class:`~rivus.Node`（函数式）或 :class:`~rivus.BaseNode`（类式）。
        """
        if self._is_running:
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
        """当前（或最近一次）运行的状态。"""
        with self._active_run_lock:
            rid = self._active_run_id
        if rid is None:
            ids = self.storage.list()
            if ids:
                state = self.storage.get(ids[-1])
                if state is not None:
                    with state.status_lock:
                        return state.status
            return PipelineStatus.IDLE
        state = self.storage.get(rid)
        if state is None:
            return PipelineStatus.IDLE
        with state.status_lock:
            return state.status

    @property
    def result(self) -> PipelineReport | None:
        """最近一次运行的报告（run_background 场景下，完成前为 None）。"""
        ids = self.storage.list()
        if ids:
            state = self.storage.get(ids[-1])
            if state is not None:
                return state.report
        return None

    @property
    def is_running(self) -> bool:
        with self._running_lock:
            return self._is_running

    # ------------------------------------------------------------------
    # 控制（委托给当前活跃 _RunState）
    # ------------------------------------------------------------------

    def stop(self) -> "Pipeline":
        """发出停止信号，立即唤醒所有阻塞中的工作线程。"""
        with self._active_run_lock:
            rid = self._active_run_id
        if rid is not None:
            state = self.storage.get(rid)
            if state is not None:
                state.inject_stop()
        return self

    def wait(self, timeout: float | None = None) -> PipelineReport:
        """阻塞直到当前后台/流式运行完成，返回本次 PipelineReport。"""
        with self._active_run_lock:
            rid = self._active_run_id
        if rid is None:
            raise RuntimeError(
                f"Pipeline '{self.name}': no active run. "
                "Call run_background() or start() first."
            )
        state = self.storage.get(rid)
        if state is None:
            raise RuntimeError(
                f"Pipeline '{self.name}': run {rid!r} not found in storage."
            )
        finished = state.done_event.wait(timeout=timeout)
        if not finished:
            raise TimeoutError(
                f"Pipeline '{self.name}' did not finish within {timeout}s. "
                "Call stop() to cancel."
            )
        if state.exc is not None:
            raise state.exc
        return state.report  # type: ignore[return-value]

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
        """同步批量运行，每次调用返回本次独立的 PipelineReport。

        多次调用 run() 完全并发安全——每次运行拥有独立的 _RunState，
        结果互不干扰。

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
            ]  # pyright: ignore[reportReturnType]

        self._assert_not_running()

        if initial:
            self._ctx.update(initial)
        self._fire_start_hooks()

        rs = self._new_run_state()

        if timeout is not None:
            return self._run_with_timeout(items, rs, timeout)

        self._boot_workers(rs)
        with rs.status_lock:
            rs.status = PipelineStatus.RUNNING
        self._push_all(items, rs)
        rs.q_submit.put(_DONE)  # type: ignore[union-attr]
        return rs.wait()

    def run_background(
        self,
        *items,
        initial: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> "Pipeline":
        """后台运行，立即返回。调用 wait() 获取本次 PipelineReport。"""
        self._assert_not_running()

        if initial:
            self._ctx.update(initial)
        self._fire_start_hooks()

        rs = self._new_run_state()

        def _target() -> None:
            self._boot_workers(rs)
            with rs.status_lock:
                rs.status = PipelineStatus.RUNNING
            self._push_all(items, rs)
            rs.q_submit.put(_DONE)  # type: ignore[union-attr]

        threading.Thread(target=_target, daemon=True,
                         name=f"rivus-bg-{self.name}").start()

        if timeout is not None:
            self._start_watchdog(rs, timeout)
        return self

    # ------------------------------------------------------------------
    # 流式 API
    # ------------------------------------------------------------------

    def start(self, initial: dict[str, Any] | None = None) -> "Pipeline":
        """启动工作线程（流式模式）。之后调用 :meth:`submit` 逐条推送 item。"""
        self._assert_not_running()

        if initial:
            self._ctx.update(initial)
        self._fire_start_hooks()

        rs = self._new_run_state()

        self._boot_workers(rs)
        with rs.status_lock:
            rs.status = PipelineStatus.RUNNING
        logger.info(
            f"Pipeline '{self.name}' started ({len(self._nodes)} nodes).")
        return self

    def submit(self, item: Any) -> "Pipeline":
        """提交单个 item（需先调用 :meth:`start`）。"""
        with self._active_run_lock:
            rid = self._active_run_id
        run = self.storage.get(rid) if rid else None
        if run is None or run.q_submit is None:
            raise RuntimeError("Call start() before submit().")
        if run.stop_event.is_set():
            raise RuntimeError(f"Pipeline '{self.name}' has been stopped.")
        item_ctx = item if isinstance(
            item, Context) else self._ctx.derive(input=item)
        run.q_submit.put(run.tag(item_ctx))
        return self

    def close(self) -> "Pipeline":
        """通知流水线不再有新 item（流式模式）。"""
        with self._active_run_lock:
            rid = self._active_run_id
        run = self.storage.get(rid) if rid else None
        if run is None or run.q_submit is None:
            raise RuntimeError("Call start() before close().")
        run.q_submit.put(_DONE)
        return self

    # ------------------------------------------------------------------
    # 内部：状态管理
    # ------------------------------------------------------------------

    def _assert_not_running(self) -> None:
        with self._running_lock:
            if self._is_running:
                raise RuntimeError(
                    f"Pipeline '{self.name}' is already running. "
                    "Call wait() or stop() first."
                )
            self._is_running = True

    def _mark_done(self) -> None:
        """由 collector 线程在本次运行结束后调用，释放运行锁，清除活跃 run_id。"""
        with self._active_run_lock:
            self._active_run_id = None
        with self._running_lock:
            self._is_running = False

    def _new_run_state(self) -> _RunState:
        """创建并返回一个全新的 _RunState，代表本次运行的完整独立状态。"""
        rs = _RunState(
            ordered=self.ordered,
            fail_fast=self.fail_fast,
            root_ctx=self._ctx,
            pipeline_name=self.name,
        )
        # 存入 storage，设置当前活跃 run_id
        self.storage._backend.save(rs.run_id, rs)
        with self._active_run_lock:
            self._active_run_id = rs.run_id
        return rs

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
        """relay 线程：等待上游 expect 个 _DONE 后，向下游发出 forward 个 _DONE。

        收到 _STOP 时立即转发并退出（级联停止）。
        """
        def _relay() -> None:
            done_count = 0
            while True:
                item = in_q.get()  # 真正阻塞，无轮询
                if item is _STOP:
                    out_q.put(_STOP)  # 立即向下游转发
                    return
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

        下游节点收到的 ``ctx.require("input")`` 将是所有上游结果値组成的列表。
        合并 ctx 以**第一个到达的上游 item ctx** 为基础派生，从而保留上游节点
        写入 ctx 的所有数据（``ctx.set(...)``）；若无 item 则回退到 root_ctx。

        收到 _STOP 时立即转发并退出（丢弃已收集的部分结果）。
        """
        def _relay() -> None:
            collected: list = []
            base_ctx: "Context | None" = None
            done_count = 0
            while True:
                item = in_q.get()  # 真正阻塞，无轮询
                if item is _STOP:
                    out_q.put(_STOP)  # 立即转发，丢弃已收集部分
                    return
                if item is _DONE:
                    done_count += 1
                    if done_count >= expect:
                        merged_ctx = (base_ctx if base_ctx is not None else root_ctx).derive(
                            input=collected)
                        out_q.put((0, merged_ctx) if ordered else merged_ctx)
                        for _ in range(forward):
                            out_q.put(_DONE)
                        return
                else:
                    ctx_item = item[1] if ordered else item
                    if base_ctx is None:
                        base_ctx = ctx_item
                    collected.append(ctx_item.get("input"))

        return threading.Thread(target=_relay, daemon=True, name=f"rivus-gather-{label}")

    @staticmethod
    def _ctx_from_dict(data: dict, rs: "_RunState") -> "Context":
        """从子进程返回的 dict 重建 Context，附加日志器和停止信号。"""
        ctx = Context(data)
        ev = getattr(rs.root_ctx, "_stop_event_ref", None)
        if ev is not None:
            ctx._attach_stop_event(ev)
        pl = getattr(rs.root_ctx, "_pipeline_ref", None)
        if pl is not None:
            ctx._attach_pipeline(pl)
        return ctx

    def _make_thread_worker(
        self,
        fn: Callable,
        nname: str,
        is_gen: bool,
        in_q: queue.Queue,
        out_q: queue.Queue,
        nr: NodeReport,
        rs: "_RunState",
    ) -> Callable:
        """返回线程 worker 函数（供同节点 N 个 Thread 复用同一闭包）。"""
        def _worker() -> None:
            while True:
                tagged = in_q.get()  # 真正阻塞，无轮询

                if tagged is _STOP:
                    in_q.put(_STOP)   # re-inject：唤醒同节点其他 worker
                    out_q.put(_STOP)   # 向下游转发
                    return
                if tagged is _DONE:
                    out_q.put(_DONE)
                    return
                if rs.stop_event.is_set():
                    out_q.put(_DONE)
                    return

                seq, item_ctx = tagged if rs.ordered else (None, tagged)

                with nr._lock:
                    nr.items_in += 1

                _t0 = time.perf_counter()
                try:
                    result = fn(item_ctx)

                    if is_gen or inspect.isgenerator(result):
                        _count = 0
                        for val in result:
                            out_ctx = val if isinstance(
                                val, Context) else item_ctx.derive(input=val)
                            out_q.put((rs.new_seq(), out_ctx)
                                      if rs.ordered else out_ctx)
                            _count += 1
                        with nr._lock:
                            nr.items_out += _count
                            nr._update_timing(time.perf_counter() - _t0)

                    elif result is None:
                        out_q.put((seq, item_ctx) if rs.ordered else item_ctx)
                        with nr._lock:
                            nr.items_out += 1
                            nr._update_timing(time.perf_counter() - _t0)

                    elif isinstance(result, Context):
                        out_q.put((seq, result) if rs.ordered else result)
                        with nr._lock:
                            nr.items_out += 1
                            nr._update_timing(time.perf_counter() - _t0)

                    else:
                        out_ctx = item_ctx.derive(input=result)
                        out_q.put((seq, out_ctx) if rs.ordered else out_ctx)
                        with nr._lock:
                            nr.items_out += 1
                            nr._update_timing(time.perf_counter() - _t0)

                except NodeSkip as skip_exc:
                    with nr._lock:
                        nr.items_skipped += 1
                    logger.debug(
                        f"Pipeline '{rs.pipeline_name}' node '{nname}' skipped item: {skip_exc.message}")

                except PipelineStop as stop_exc:
                    logger.debug(
                        f"Pipeline '{rs.pipeline_name}' stop requested by node '{nname}': {stop_exc}")
                    rs.inject_stop()
                    out_q.put(_DONE)
                    return

                except Exception as exc:
                    with nr._lock:
                        nr.errors += 1
                        nr._update_timing(time.perf_counter() - _t0)
                    with rs.errors_lock:
                        rs.errors.append(NodeError(nname, exc))
                    if rs.fail_fast:
                        rs.inject_stop()
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
        rs: "_RunState",
    ) -> Callable:
        """返回进程模式协调函数（单线程驱动 ProcessPoolExecutor）。"""
        def _coordinator() -> None:
            from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait

            pending: dict = {}   # future → (seq, item_ctx)
            done_count = 0
            input_exhausted = False

            with ProcessPoolExecutor(max_workers=n_workers) as executor:
                while True:
                    # ── 处理已完成的 future ───────────────────
                    for future in [f for f in list(pending) if f.done()]:
                        seq, item_ctx = pending.pop(future)
                        try:
                            res_type, res_data, elapsed = future.result()
                            with nr._lock:
                                nr._update_timing(elapsed)
                            if res_type == "skip":
                                with nr._lock:
                                    nr.items_skipped += 1
                                logger.debug(f"Pipeline '{rs.pipeline_name}' node '{nname}' skipped item: {res_data}")
                            elif res_type == "gen":
                                for vtype, vdata in res_data:
                                    out_ctx = (
                                        self._ctx_from_dict(vdata, rs)
                                        if vtype == "ctx"
                                        else item_ctx.derive(input=vdata)
                                    )
                                    out_q.put((rs.new_seq(), out_ctx)
                                              if rs.ordered else out_ctx)
                                    with nr._lock:
                                        nr.items_out += 1
                            else:
                                out_ctx = (
                                    self._ctx_from_dict(res_data, rs)
                                    if res_type in ("none", "ctx")
                                    else item_ctx.derive(input=res_data)
                                )
                                out_q.put((seq, out_ctx)
                                          if rs.ordered else out_ctx)
                                with nr._lock:
                                    nr.items_out += 1
                        except Exception as exc:
                            with nr._lock:
                                nr.errors += 1
                            with rs.errors_lock:
                                rs.errors.append(NodeError(nname, exc))
                            if rs.fail_fast:
                                rs.inject_stop()

                    if rs.stop_event.is_set():
                        for f in list(pending):
                            f.cancel()
                        pending.clear()
                        break

                    if input_exhausted and not pending:
                        break
                    if input_exhausted:
                        # 等待任意一个 future 完成，替代 sleep(0.005) 空转
                        wait(list(pending), timeout=0.1,
                             return_when=FIRST_COMPLETED)
                        continue

                    # ── 读取下一个 input ──────────────────
                    tagged = in_q.get()  # 真正阻塞，无轮询

                    if tagged is _STOP:
                        for f in list(pending):
                            f.cancel()
                        pending.clear()
                        out_q.put(_STOP)
                        return

                    if tagged is _DONE:
                        done_count += 1
                        if done_count >= n_workers:
                            input_exhausted = True
                        continue

                    seq, item_ctx = tagged if rs.ordered else (None, tagged)
                    with nr._lock:
                        nr.items_in += 1
                    pending[executor.submit(_mp_run, fn, item_ctx.snapshot(), is_gen)] = (
                        seq, item_ctx)

            # relay 期望收到 n_workers 个 _DONE
            for _ in range(n_workers):
                out_q.put(_DONE)

        return _coordinator

    def _make_collector(
        self,
        q_results: queue.Queue,
        node_reports: list,
        rs: "_RunState",
    ) -> threading.Thread:
        """创建结果收集线程，负责排序、生成报告、设置最终状态。"""
        def _collect() -> None:
            raw: list = []
            while True:
                item = q_results.get()  # 真正阻塞，无轮询
                if item is _DONE or item is _STOP:
                    break
                raw.append(item)

            if rs.ordered:
                raw.sort(key=lambda x: x[0])
                final_ctxs = [ctx for _, ctx in raw]
            else:
                final_ctxs = raw

            total_duration = time.perf_counter() - rs.start_time

            with rs.status_lock:
                if rs.status == PipelineStatus.TIMEOUT:
                    final_status = PipelineStatus.TIMEOUT
                elif rs.stop_event.is_set() and not rs.errors:
                    final_status = PipelineStatus.STOPPED
                elif rs.errors and rs.fail_fast:
                    final_status = PipelineStatus.FAILED
                else:
                    final_status = PipelineStatus.SUCCESS
                rs.status = final_status

            rs.report = PipelineReport(
                pipeline_name=rs.pipeline_name,
                status=final_status,
                total_duration_s=total_duration,
                run_id=rs.run_id,
                results=[c.get("input") for c in final_ctxs],
                errors=list(rs.errors),
                nodes=node_reports,
            )

            if final_status == PipelineStatus.SUCCESS:
                logger.info(
                    f"Pipeline '{self.name}' succeeded in {total_duration:.3f}s, {len(final_ctxs)} results.")
            elif final_status == PipelineStatus.FAILED:
                logger.error(
                    f"Pipeline '{self.name}' failed in {total_duration:.3f}s.")
                rs.exc = PipelineError(self.name, rs.errors[0])
            elif final_status == PipelineStatus.STOPPED:
                logger.warning(
                    f"Pipeline '{self.name}' stopped after {total_duration:.3f}s.")
            else:
                logger.error(
                    f"Pipeline '{self.name}' timed out after {total_duration:.3f}s.")

            # ── 生命周期钩子 ──────────────────────────────────────────
            hooks = (
                self._hooks_end
                if final_status == PipelineStatus.SUCCESS
                else self._hooks_failure
            )
            for h in hooks:
                try:
                    h(rs.report)
                except Exception as hook_exc:
                    logger.warning(
                        f"Pipeline '{self.name}': hook {getattr(h, '__name__', h)} raised: {hook_exc}"
                    )

            # 释放运行锁，允许下一次 run
            self._mark_done()
            rs.done_event.set()

        return threading.Thread(target=_collect, daemon=True, name=f"rivus-collector-{self.name}")

    # ------------------------------------------------------------------
    # 内部：工作线程引导 —— 编排入口
    # ------------------------------------------------------------------

    def _boot_workers(self, rs: "_RunState") -> None:
        """创建并启动所有队列、relay / worker / collector 线程。"""
        if not self._nodes:
            raise RuntimeError(
                "No nodes in pipeline. Use add_node() or | operator.")

        nodes = self._nodes
        n = len(nodes)

        # ── 节点初始化 ────────────────────────────────────────────────
        # once 节点：首次运行时调用 setup_fn，后续跳过
        with self._once_lock:
            for i, nd in enumerate(nodes):
                if nd.setup_fn is not None and not (nd.once and i in self._once_executed):
                    nd.setup_fn(self._ctx)

        node_reports = [
            NodeReport(name=nd.name, workers=nd.workers,
                       concurrency_type=nd.concurrency_type)
            for nd in nodes
        ]

        # ── 队列 ──────────────────────────────────────────────────────
        q_submit: queue.Queue = queue.Queue()
        q_in = [queue.Queue(maxsize=nd.queue_size) for nd in nodes]
        q_out = [queue.Queue() for _ in nodes]
        q_results: queue.Queue = queue.Queue()
        rs.q_submit = q_submit

        # ── 为本次 run 注入独立 stop_event ───────────────────────────
        # root_ctx 是 per-pipeline 的，stop_event 是 per-run 的；
        # 通过附加到 root_ctx，使 ctx.derive() 自动将本次 stop_event
        # 传播到所有 item ctx。
        self._ctx._attach_stop_event(rs.stop_event)

        # ── Relay 线程 ────────────────────────────────────────────────
        relay_threads = [
            self._make_relay(
                q_submit, q_in[0], 1, nodes[0].workers, rs.stop_event, f"{self.name}/0"),
            *[
                self._make_gather_relay(
                    q_out[i], q_in[i + 1],
                    expect=nodes[i].workers,
                    forward=nodes[i + 1].workers,
                    ordered=rs.ordered,
                    stop_event=rs.stop_event,
                    root_ctx=rs.root_ctx,
                    label=f"{self.name}/{i}→{i+1}",
                ) if nodes[i + 1].gather else
                self._make_relay(q_out[i], q_in[i + 1], nodes[i].workers,
                                 nodes[i + 1].workers, rs.stop_event, f"{self.name}/{i}→{i+1}")
                for i in range(n - 1)
            ],
            self._make_relay(
                q_out[-1], q_results, nodes[-1].workers, 1, rs.stop_event, f"{self.name}/out"),
        ]

        # ── Worker 线程 ───────────────────────────────────────────────
        worker_threads: list[threading.Thread] = []
        with self._once_lock:
            for i, nd in enumerate(nodes):
                if nd.once and i in self._once_executed:
                    # once 节点已在前次 run() 执行过，直接透传 item
                    t = self._make_relay(
                        q_in[i], q_out[i],
                        expect=nd.workers, forward=nd.workers,
                        stop_event=rs.stop_event,
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
                                q_in[i], q_out[i], node_reports[i], rs,
                            ),
                            daemon=True, name=f"rivus-proc-{nd.name}",
                        ))
                    else:
                        fn = self._make_thread_worker(
                            nd.fn, nd.name, nd.is_generator,
                            q_in[i], q_out[i], node_reports[i], rs,
                        )
                        for _ in range(nd.workers):
                            worker_threads.append(threading.Thread(
                                target=fn, daemon=True, name=f"rivus-{nd.name}"))

        # ── 启动 ──────────────────────────────────────────────────────
        collector = self._make_collector(q_results, node_reports, rs)
        rs.start_time = time.perf_counter()
        for t in relay_threads + worker_threads + [collector]:
            t.start()

    # ------------------------------------------------------------------
    # 内部：提交辅助
    # ------------------------------------------------------------------

    def _push_all(self, items: Iterable[Any] | None, rs: "_RunState") -> None:
        items = items or [None]

        for item in items:
            if rs.stop_event.is_set():
                break
            item_ctx = item if isinstance(
                item, Context) else self._ctx.derive(input=item)
            rs.q_submit.put(rs.tag(item_ctx))  # type: ignore[union-attr]

    # ------------------------------------------------------------------
    # 内部：超时 & 看门狗
    # ------------------------------------------------------------------

    def _run_with_timeout(
        self,
        items: Iterable[Any] | None,
        rs: "_RunState",
        timeout: float,
    ) -> PipelineReport:

        def _target() -> None:
            self._boot_workers(rs)
            with rs.status_lock:
                rs.status = PipelineStatus.RUNNING
            self._push_all(items, rs)
            rs.q_submit.put(_DONE)  # type: ignore[union-attr]

        th = threading.Thread(target=_target, daemon=True,
                              name=f"rivus-{self.name}")
        th.start()
        th.join(timeout=timeout)

        if th.is_alive():
            rs.stop_event.set()
            if rs.q_submit is not None:
                try:
                    rs.q_submit.put_nowait(_STOP)
                except queue.Full:
                    pass
            rs.done_event.wait(timeout=self.timeout_grace)
            with rs.status_lock:
                if rs.status == PipelineStatus.RUNNING:
                    rs.status = PipelineStatus.TIMEOUT
            exc = PipelineTimeoutError(self.name, timeout)
            rs.exc = exc
            rs.done_event.set()
            logger.error(
                f"Pipeline '{self.name}' timed out after {timeout:.1f}s."
            )
            raise exc

        rs.done_event.wait(timeout=self.timeout_grace)
        if rs.exc is not None:
            raise rs.exc
        return rs.report  # type: ignore[return-value]

    def _start_watchdog(self, rs: "_RunState", timeout: float) -> None:
        def _watchdog() -> None:
            finished = rs.done_event.wait(timeout=timeout)
            if not finished:
                rs.stop_event.set()
                if rs.q_submit is not None:
                    try:
                        rs.q_submit.put_nowait(_STOP)
                    except queue.Full:
                        pass
                timed_out = False
                with rs.status_lock:
                    if rs.status == PipelineStatus.RUNNING:
                        rs.status = PipelineStatus.TIMEOUT
                        rs.exc = PipelineTimeoutError(self.name, timeout)
                        timed_out = True
                if timed_out:
                    rs.done_event.set()
                    logger.error(
                        f"Pipeline '{self.name}' timed out after {timeout:.1f}s."
                    )

        threading.Thread(
            target=_watchdog, daemon=True, name=f"rivus-watchdog-{self.name}"
        ).start()

    def __repr__(self) -> str:
        return (
            f"Pipeline(name={self.name!r}, status={self.status!r}, "
            f"nodes={[n.name for n in self._nodes]})"
        )
