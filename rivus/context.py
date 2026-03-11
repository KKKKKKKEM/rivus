"""线程安全的统一状态容器，贯穿整条 Stage 链传递数据。"""

from __future__ import annotations

import copy
import threading
from threading import RLock
from typing import Any, Iterator, TypeVar, overload

from rivus.exceptions import ContextKeyError, PipelineStop
from rivus.log import RivusLogger, default_logger

T = TypeVar("T")


class Context:
    """在 Pipeline 中各 Node 之间共享的状态容器。

    Examples:
        >>> ctx = Context({"image_path": "/data/img.jpg"})
        >>> ctx.set("preprocess.resized", True)
        >>> ctx.require("image_path")
        '/data/img.jpg'
        >>> ctx.log.info("context ready")
    """

    def __init__(
        self,
        initial: dict[str, Any] | None = None,
        logger: RivusLogger | None = None,
    ) -> None:
        self._data: dict[str, Any] = dict(initial) if initial else {}
        self._logger: RivusLogger = logger or default_logger()
        self._lock = RLock()

    # ------------------------------------------------------------------
    # 读操作
    # ------------------------------------------------------------------

    @overload
    def get(self, key: str) -> Any: ...
    @overload
    def get(self, key: str, default: T) -> T: ...

    def get(self, key: str, default: Any = None) -> Any:
        """获取键对应的值，键不存在时返回 *default*。"""
        with self._lock:
            return self._data.get(key, default)

    def require(self, key: str) -> Any:
        """获取必须存在的值，不存在则抛出 :class:`ContextKeyError`。"""
        with self._lock:
            if key not in self._data:
                raise ContextKeyError(key)
            return self._data[key]

    def snapshot(self) -> dict[str, Any]:
        """返回当前所有数据的深拷贝快照（线程安全）。"""
        with self._lock:
            return copy.deepcopy(self._data)

    def keys(self) -> list[str]:
        with self._lock:
            return list(self._data.keys())

    # ------------------------------------------------------------------
    # 写操作
    # ------------------------------------------------------------------

    def set(self, key: str, value: Any) -> None:
        """写入单个键值对。"""
        with self._lock:
            self._data[key] = value

    def update(self, data: dict[str, Any]) -> None:
        """批量写入键值对，与 dict.update 语义相同。"""
        with self._lock:
            self._data.update(data)

    def delete(self, key: str) -> None:
        """删除一个键（键不存在时静默忽略）。"""
        with self._lock:
            self._data.pop(key, None)

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __contains__(self, key: object) -> bool:
        with self._lock:
            return key in self._data

    def __getitem__(self, key: str) -> Any:
        return self.require(key)

    def __setitem__(self, key: str, value: Any) -> None:
        self.set(key, value)

    def __iter__(self) -> Iterator[str]:
        with self._lock:
            return iter(list(self._data.keys()))

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)

    def __getstate__(self) -> dict:
        with self._lock:
            return {"_data": copy.deepcopy(self._data)}

    def __setstate__(self, state: dict) -> None:
        self._data = state["_data"]
        self._lock = RLock()
        self._logger = default_logger()

    # ------------------------------------------------------------------
    # 日志器
    # ------------------------------------------------------------------

    @property
    def log(self) -> RivusLogger:
        """内置日志器，通过 Pipeline 的 log_config 统一配置。

        Examples::

            ctx.log.info("loaded %d samples", n)
            ctx.log.warning("missing key, using default")
            ctx.log.bind(stage="inference").debug("logits=%s", logits)
        """
        return self._logger

    def _attach_logger(self, logger: RivusLogger) -> None:
        """由 Pipeline 内部调用，将配置好的日志器绑定到此 Context。"""
        self._logger = logger

    def _attach_pipeline(self, pipeline: Any) -> None:
        """由 Pipeline 内部调用，将自身引用绑定到此 Context。"""
        self._pipeline_ref = pipeline

    @property
    def pipeline(self) -> Any:
        """返回当前 Context 所属的 Pipeline 实例。

        若在 Pipeline 外独立使用 Context，则返回 ``None``。

        Examples::

            @node
            def my_node(ctx: Context):
                model = ctx.pipeline.get("model")
                return model(ctx.require("input"))
        """
        return getattr(self, "_pipeline_ref", None)

    def _attach_stop_event(self, event: threading.Event) -> None:
        """由 Pipeline 内部调用，注入停止信号事件。"""
        self._stop_event_ref: threading.Event = event

    def request_stop(self) -> None:
        """请求停止整个 Pipeline（协作式）。

        该方法适合在节点中检测到"已满足退出条件"时调用。
        当前正在执行的 item 会尽快结束，后续 item 将不再继续处理。
        """
        ev = getattr(self, "_stop_event_ref", None)
        if ev is not None:
            ev.set()

    def stop(self, message: str = "pipeline stop requested") -> None:
        """请求停止并抛出 :class:`PipelineStop` 以立刻结束当前节点执行。

        典型用法：在节点内判定已完成后，调用 ``ctx.stop("done")``。
        """
        self.request_stop()
        raise PipelineStop(message)

    @property
    def stop_requested(self) -> bool:
        """Task 内可轮询此属性实现协作式提前退出。

        由 Pipeline 在构造时注入；单独使用 Context 时始终返回 False。

        Examples::

            @task
            def heavy_compute(ctx: Context) -> None:
                for chunk in data_chunks:
                    if ctx.stop_requested:
                        ctx.log.warning("stop requested, exiting early")
                        return
                    process(chunk)
        """
        ev = getattr(self, "_stop_event_ref", None)
        return ev is not None and ev.is_set()

    # ------------------------------------------------------------------
    # Context 派生
    # ------------------------------------------------------------------

    def derive(self, **kwargs: Any) -> "Context":
        """创建子 Context，继承当前所有数据，并叠加 kwargs。

        类似 Go 的 ``context.WithValue``：在当前 Context 的基础上叠加新键值，
        原 Context 不受影响（immutable-style 派生）。

        子 Context 自动继承父 Context 的日志器和停止信号。

        Args:
            **kwargs: 在子 Context 中新增或覆盖的键值对。

        Returns:
            继承了父 Context 全部数据的子 Context。

        Examples::

            item_ctx = root_ctx.derive(input=image_path)
            result_ctx = item_ctx.derive(input=result, extra_flag=True)
        """
        with self._lock:
            child = Context(dict(self._data))
        child._attach_logger(self._logger)
        ev = getattr(self, "_stop_event_ref", None)
        if ev is not None:
            child._attach_stop_event(ev)
        pl = getattr(self, "_pipeline_ref", None)
        if pl is not None:
            child._attach_pipeline(pl)
        if kwargs:
            child.update(kwargs)
        return child

    def __repr__(self) -> str:
        with self._lock:
            return f"Context({self._data!r})"
