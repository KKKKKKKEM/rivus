"""线程安全的统一状态容器，贯穿整条 Stage 链传递数据。"""

from __future__ import annotations

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
        self._lock = RLock()
        self._id = id(self)  # unique identifier for this Context instance
        self._root: Context = root or self
        self.input: Any = None
        self.error: Exception | None = None

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

    @property
    def need_stop(self) -> bool:
        with self._lock:
            need_stop = self.error is not None
            if not need_stop and self._root is not self:
                need_stop = self._root.need_stop

            return need_stop
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
