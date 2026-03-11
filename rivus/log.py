"""Rivus 内置日志体系。

主要类型：
- :class:`LogConfig`   — 声明式日志配置（等级、终端、文件、格式）
- :class:`RivusLogger` — 统一日志接口，支持 bind() 绑定上下文字段
- :func:`build_logger` — 根据 LogConfig 构建 RivusLogger

通过 ``ctx.log`` 访问日志器，支持标准五级方法和 ``bind(**extra)``。

Examples::

    # Pipeline 级别配置（写到文件 + 终端）
    Pipeline("my_pipeline", log_config=LogConfig(level="DEBUG", to_file="run.log"))

    # Task 内使用
    ctx.log.info("processing %s", img_path)
    ctx.log.bind(stage="inference", task="RunModel").debug("logits: %s", logits)
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

__all__ = ["LogConfig", "RivusLogger", "build_logger", "default_logger"]

DEFAULT_FMT = "%(asctime)s [%(levelname)-8s] %(name)s — %(message)s"
DEFAULT_DATE_FMT = "%Y-%m-%d %H:%M:%S"


# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------


@dataclass
class LogConfig:
    """日志配置，传入 :class:`~rivus.Pipeline` 以自定义日志行为。

    Args:
        level: 最低输出等级，接受字符串 ``"DEBUG"`` / ``"INFO"`` / ``"WARNING"`` /
               ``"ERROR"`` / ``"CRITICAL"`` 或 :mod:`logging` 整数常量。
        to_console: 是否输出到终端（stderr）。
        to_file: 日志文件路径；``None`` 表示不写文件。
        fmt: :mod:`logging` 格式串。
        date_fmt: 日期时间格式串。
        name: 底层 Logger 名称，默认由 Pipeline 自动设为 pipeline 名称。
        file_mode: 文件打开模式，``"a"`` 追加 / ``"w"`` 覆写。
    """

    level: int | str = "DEBUG"
    to_console: bool = True
    to_file: str | Path | None = None
    fmt: str = DEFAULT_FMT
    date_fmt: str = DEFAULT_DATE_FMT
    name: str = "rivus"
    file_mode: str = "a"

    def __post_init__(self) -> None:
        if isinstance(self.level, str):
            numeric = getattr(logging, self.level.upper(), None)
            if numeric is None:
                raise ValueError(f"Unknown log level: {self.level!r}")
            self.level = numeric


# ---------------------------------------------------------------------------
# 日志器
# ---------------------------------------------------------------------------


class RivusLogger:
    """Rivus 统一日志接口，封装 ``logging.Logger``。

    通常通过 ``ctx.log`` 获取，无需直接构造。

    Methods:
        debug / info / warning / error / critical / exception:
            与 :mod:`logging` 同名方法语义完全一致。
        bind(**extra):
            返回一个绑定了额外字段的子日志器，每条消息自动追加前缀
            ``[key=value ...]``。
        set_level(level):
            动态调整最低输出等级。
    """

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    # ------------------------------------------------------------------
    # 标准日志方法
    # ------------------------------------------------------------------

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.error(msg, *args, **kwargs)

    def critical(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.critical(msg, *args, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """记录 ERROR 级别消息并附加当前异常的 traceback。"""
        self._logger.exception(msg, *args, **kwargs)

    # ------------------------------------------------------------------
    # 绑定上下文字段
    # ------------------------------------------------------------------

    def bind(self, **extra: Any) -> "_BoundLogger":
        """返回绑定了额外字段的子日志器。

        每条日志消息前会自动加上 ``[key=value ...]`` 前缀，
        方便在 Task 内标注所属 Stage / Task 名称等信息。

        Examples::

            log = ctx.log.bind(stage="inference", task="RunModel")
            log.info("done in %.2fs", elapsed)
            # → [stage=inference task=RunModel] done in 0.15s
        """
        return _BoundLogger(self._logger, extra)

    # ------------------------------------------------------------------
    # 等级控制
    # ------------------------------------------------------------------

    @property
    def level(self) -> int:
        """当前最低输出等级（整数）。"""
        return self._logger.level

    def set_level(self, level: int | str) -> None:
        """动态调整最低输出等级。

        Args:
            level: 字符串 ``"DEBUG"``、``"INFO"`` 等，或 :mod:`logging` 整数常量。
        """
        if isinstance(level, str):
            level = getattr(logging, level.upper(), logging.INFO)
        self._logger.setLevel(level)
        for h in self._logger.handlers:
            h.setLevel(level)

    def __repr__(self) -> str:
        return (
            f"RivusLogger(name={self._logger.name!r}, "
            f"level={logging.getLevelName(self._logger.level)})"
        )


class _BoundLogger(RivusLogger):
    """绑定了额外字段的日志器（由 :meth:`RivusLogger.bind` 返回）。"""

    def __init__(self, base_logger: logging.Logger, extra: dict[str, Any]) -> None:
        super().__init__(base_logger)
        self._extra = extra
        self._prefix = " ".join(f"{k}={v}" for k, v in extra.items())

    def _log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None:
        """统一分发入口：自动添加 [field=value ...] 前缀，消除各方法重复逻辑。"""
        self._logger.log(level, f"[{self._prefix}] {msg}", *args, **kwargs)

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log(logging.WARNING, msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log(logging.ERROR, msg, *args, **kwargs)

    def critical(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log(logging.CRITICAL, msg, *args, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("exc_info", True)
        self._log(logging.ERROR, msg, *args, **kwargs)

    def bind(self, **extra: Any) -> "_BoundLogger":
        """在当前绑定字段基础上追加更多字段。"""
        return _BoundLogger(self._logger, {**self._extra, **extra})


# ---------------------------------------------------------------------------
# 工厂函数
# ---------------------------------------------------------------------------


def build_logger(config: LogConfig) -> RivusLogger:
    """根据 :class:`LogConfig` 构建并返回 :class:`RivusLogger`。

    每次调用都会重置同名 Logger 的 Handler，确保配置生效且无重复输出。
    """
    logger = logging.getLogger(config.name)
    logger.setLevel(config.level)
    # 清除旧 handler，防止重复添加（多次 run 或测试场景）
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(fmt=config.fmt, datefmt=config.date_fmt)

    if config.to_console:
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(config.level)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    if config.to_file:
        os.makedirs(Path(config.to_file).parent, exist_ok=True)
        fh = logging.FileHandler(
            str(config.to_file), mode=config.file_mode, encoding="utf-8"
        )
        fh.setLevel(config.level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return RivusLogger(logger)


def default_logger(name: str = "rivus") -> RivusLogger:
    """返回一个仅输出到 stderr（INFO 级）的默认日志器。

    用于 Context 在 Pipeline 外单独使用时的兜底日志器。
    """
    return build_logger(LogConfig(name=name))
