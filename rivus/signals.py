"""Rivus 异常体系。"""

from __future__ import annotations


class RivusError(Exception):
    """所有 Rivus 异常的基类。"""


class SKIP(RivusError):
    ...


class STOP(RivusError):
    ...
