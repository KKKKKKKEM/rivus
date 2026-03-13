"""线程安全的统一状态容器，贯穿整条 Stage 链传递数据。"""

from __future__ import annotations

import threading
from threading import RLock
from typing import Any, TypeVar, overload

T = TypeVar("T")


class Context:
    """在 Pipeline 中各 Node 之间共享的状态容器"""

    def __init__(
        self,
        initial: dict[str, Any] | None = None,
        root: Context | None = None,
    ) -> None:
        # 子 Context 复用 root 的锁，减少 OS mutex 创建开销
        self._root: Context = root or self
        self._lock = root._lock if root is not None else RLock()
        self._id = id(self)  # unique identifier for this Context instance
        self.input: Any = None
        self.error: Exception | None = None
        # 每个 root Context 持有一个 Event，用于快速 need_stop 检测
        self._stop_event: threading.Event = root._stop_event if root is not None else threading.Event()

        for key, value in (initial or {}).items():
            setattr(self, key, value)

    # ------------------------------------------------------------------
    # 读操作
    # ------------------------------------------------------------------

    @overload
    def get(self, key: str) -> Any: ...
    @overload
    def get(self, key: str, default: T) -> T: ...

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            if hasattr(self, key):
                return getattr(self, key)
            elif self._root is not self:
                return self._root.get(key, default)
            else:
                return default

    def require(self, key: str) -> Any:
        with self._lock:
            if hasattr(self, key):
                return getattr(self, key)
            elif self._root is not self:
                return self._root.require(key)
            else:
                raise KeyError(key)

    # ------------------------------------------------------------------
    # 写操作
    # ------------------------------------------------------------------

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            setattr(self, key, value)

    def update(self, data: dict[str, Any]) -> None:
        with self._lock:
            for key, value in data.items():
                setattr(self, key, value)

    def delete(self, key: str) -> None:
        with self._lock:
            if hasattr(self, key):
                delattr(self, key)

    def cancel(self):
        with self._lock:
            self.error = Exception("Task cancelled")
            self._stop_event.set()

    @property
    def need_stop(self) -> bool:
        if self._stop_event.is_set():
            return True
        return self.error is not None
    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __getitem__(self, key: str) -> Any:
        return self.require(key)

    def __setitem__(self, key: str, value: Any) -> None:
        self.set(key, value)

    # ------------------------------------------------------------------
    # Context 派生
    # ------------------------------------------------------------------

    def derive(self, **kwargs: Any) -> "Context":
        return Context(root=self, initial=kwargs)
