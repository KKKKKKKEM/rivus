"""Rivus 内置日志体系，基于 loguru。

主要类型：
- :class:`LogConfig`   — 声明式日志配置（等级、终端、文件、格式）
- :class:`RivusLogger` — 统一日志接口，支持 bind() 绑定上下文字段
- :func:`build_logger` — 根据 LogConfig 构建 RivusLogger

通过 ``ctx.log`` 访问日志器，支持标准五级方法和 ``bind(**extra)``。

日志格式使用 loguru 风格的格式串（``{time}``、``{level}``、``{message}`` 等）。

Examples::

    # Pipeline 级别配置（写到文件 + 终端）
    Pipeline("my_pipeline", log_config=LogConfig(level="DEBUG", to_file="run.log"))

    # Task 内使用
    ctx.log.info("processing %s", img_path)
    ctx.log.bind(stage="inference", task="RunModel").debug("logits: %s", logits)
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loguru import logger as _loguru

_loguru.remove(0)

__all__ = ["LogConfig", "RivusLogger", "build_logger", "default_logger"]

# 终端 / 文件统一格式（colorize=False 时标签自动剥离，输出纯文本）
DEFAULT_FMT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> "
    "[<level>{level:^8}</level>] "
    "<cyan>{extra[_name]}</cyan> — {message}"
)


# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------


@dataclass
class LogConfig:
    """日志配置，传入 :class:`~rivus.Pipeline` 以自定义日志行为。

    Args:
        level: 最低输出等级，接受字符串 ``"DEBUG"`` / ``"INFO"`` / ``"WARNING"`` /
               ``"ERROR"`` / ``"CRITICAL"`` 或 :mod:`logging` 整数常量（向后兼容）。
        to_console: 是否输出到终端（stderr）。
        to_file: 日志文件路径；``None`` 表示不写文件。
        fmt: loguru 格式串，终端和文件共用；终端渲染颜色标签，文件自动剥离为纯文本。
        date_fmt: 已废弃，loguru 在 ``fmt`` 中直接控制日期格式，此字段被忽略。
        name: 日志标识名称，默认由 Pipeline 自动设为 pipeline 名称。
        file_mode: 文件打开模式，``"a"`` 追加 / ``"w"`` 覆写。
    """

    level: int | str = "DEBUG"
    to_console: bool = True
    to_file: str | Path | None = None
    fmt: str = DEFAULT_FMT
    date_fmt: str = ""   # 保留字段，兼容旧代码，已废弃
    name: str = "rivus"
    file_mode: str = "a"

    def __post_init__(self) -> None:
        if isinstance(self.level, int):
            # 兼容 logging.DEBUG / logging.INFO 等整数常量
            import logging as _stdlib
            lvl_name = _stdlib.getLevelName(self.level)
            self.level = lvl_name if not lvl_name.startswith(
                "Level") else "DEBUG"
        self.level = str(self.level).upper()
        valid = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if self.level not in valid:
            raise ValueError(f"Unknown log level: {self.level!r}")


# ---------------------------------------------------------------------------
# 日志器
# ---------------------------------------------------------------------------


class RivusLogger:
    """Rivus 统一日志接口，基于 loguru 实现。

    通常通过 ``ctx.log`` 获取，无需直接构造。

    Methods:
        debug / info / warning / error / critical / exception:
            与 loguru 同名方法语义完全一致，兼容 ``%s`` 格式串。
        bind(**extra):
            返回一个绑定了额外字段的子日志器，每条消息自动追加前缀
            ``[key=value ...]``。
        set_level(level):
            动态调整所有关联 sink 的最低输出等级（重建 sinks）。
    """

    def __init__(
        self,
        bound: Any,
        sink_ids: list[int],
        sink_params: list[dict],
        _prefix: str = "",
    ) -> None:
        self._bound = bound
        self._sink_ids: list[int] = list(sink_ids)
        self._sink_params: list[dict] = list(sink_params)
        self._prefix = _prefix

    # ------------------------------------------------------------------
    # 内部：格式化（兼容 % 格式串）
    # ------------------------------------------------------------------

    def _fmt(self, msg: str, args: tuple) -> str:
        text = msg % args if args else msg
        return f"{self._prefix} {text}" if self._prefix else text

    # ------------------------------------------------------------------
    # 标准日志方法
    # ------------------------------------------------------------------

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._bound.opt(depth=1).debug(self._fmt(msg, args), **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._bound.opt(depth=1).info(self._fmt(msg, args), **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._bound.opt(depth=1).warning(self._fmt(msg, args), **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._bound.opt(depth=1).error(self._fmt(msg, args), **kwargs)

    def critical(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._bound.opt(depth=1).critical(self._fmt(msg, args), **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """记录 ERROR 级别消息并附加当前异常的 traceback。"""
        self._bound.opt(depth=1, exception=True).error(
            self._fmt(msg, args), **kwargs)

    # ------------------------------------------------------------------
    # 绑定上下文字段
    # ------------------------------------------------------------------

    def bind(self, **extra: Any) -> "RivusLogger":
        """返回绑定了额外字段的子日志器。

        每条日志消息前会自动加上 ``[key=value ...]`` 前缀，
        方便在 Task 内标注所属 Stage / Task 名称等信息。

        Examples::

            log = ctx.log.bind(stage="inference", task="RunModel")
            log.info("done in %.2fs", elapsed)
            # → [stage=inference task=RunModel] done in 0.15s
        """
        part = " ".join(f"{k}={v}" for k, v in extra.items())
        new_prefix = f"{self._prefix} [{part}]" if self._prefix else f"[{part}]"
        return RivusLogger(
            self._bound.bind(
                **extra), self._sink_ids, self._sink_params, new_prefix
        )

    # ------------------------------------------------------------------
    # 等级控制
    # ------------------------------------------------------------------

    @property
    def level(self) -> int:
        """始终返回 0；loguru 不暴露 bound logger 的 level，请通过 LogConfig 配置。"""
        return 0

    def set_level(self, level: int | str) -> None:
        """动态调整所有关联 sink 的最低输出等级。

        Args:
            level: 字符串 ``"DEBUG"``、``"INFO"`` 等，或 :mod:`logging` 整数常量。

        Note:
            loguru 不支持原地修改 sink 级别，此方法会移除旧 sinks 并以新级别重建。
        """
        if isinstance(level, int):
            import logging as _l
            level = _l.getLevelName(level)
        level_str = str(level).upper()
        for sid in list(self._sink_ids):
            try:
                _loguru.remove(sid)
            except ValueError:
                pass
        self._sink_ids.clear()
        for params in self._sink_params:
            new_id = _loguru.add(**{**params, "level": level_str})
            self._sink_ids.append(new_id)

    def __repr__(self) -> str:
        return f"RivusLogger(sink_ids={self._sink_ids!r})"


# ---------------------------------------------------------------------------
# 工厂函数
# ---------------------------------------------------------------------------


def build_logger(config: LogConfig) -> RivusLogger:
    """根据 :class:`LogConfig` 构建并返回 :class:`RivusLogger`。

    每个 Pipeline 通过 ``filter`` 隔离各自的 sinks，多 Pipeline 并存时互不干扰。
    """
    name = config.name
    level = config.level
    sink_ids: list[int] = []
    sink_params: list[dict] = []

    # 只接收来自本 pipeline 的日志（通过 extra._name 过滤）
    def _filter(record: dict) -> bool:
        return record["extra"].get("_name") == name

    if config.to_console:
        p: dict = dict(
            sink=sys.stderr,
            level=level,
            format=config.fmt,
            filter=_filter,
            colorize=True,
        )
        sink_ids.append(_loguru.add(**p))
        sink_params.append(p)

    if config.to_file:
        path = Path(config.to_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        p = dict(
            sink=str(path),
            level=level,
            format=config.fmt,
            filter=_filter,
            mode=config.file_mode,
            encoding="utf-8",
            colorize=False,
        )
        sink_ids.append(_loguru.add(**p))
        sink_params.append(p)

    bound = _loguru.bind(_name=name)
    return RivusLogger(bound, sink_ids, sink_params)


def default_logger(name: str = "rivus") -> RivusLogger:
    """返回一个仅输出到 stderr（INFO 级）的默认日志器。

    用于 Context 在 Pipeline 外单独使用时的兜底日志器。
    """
    return build_logger(LogConfig(level="INFO", name=name))
