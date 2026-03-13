"""TaskManager：框架无关的任务生命周期管理器。

职责
----
- 接收同步 / 异步任务提交，驱动 Pipeline 执行
- 维护所有 HTTP 层任务的 TaskRecord（状态、结果、时间戳）
- 支持按 task_id 取消正在运行的任务
- 支持按 status 过滤任务列表

并发模型
--------
Pipeline.create_run() 并发安全，每次 run 拥有独立的 Task 实例。

- 同步提交（mode=sync）：在当前调用线程中直接调用 pipeline.run()，HTTP handler 阻塞等待
- 异步提交（mode=async）：在独立 daemon 线程中调用 pipeline.run()，
  HTTP handler 立即返回 task_id

取消机制
--------
task_id → TaskRecord.task → Task.stop()
TaskManager 通过持有 Task 引用实现精准取消，无需额外状态。
"""

from __future__ import annotations

import threading
import time
import uuid
from collections import OrderedDict
from typing import TYPE_CHECKING

from rivus.serve.models import (
    ApiResponse,
    TaskRecord,
    TaskRequest,
)

if TYPE_CHECKING:
    from rivus.pipeline import Pipeline


class TaskManager:
    """单条 Pipeline 对应一个 TaskManager。

    Args:
        pipeline:  被管理的 Pipeline 实例。
        max_tasks: TaskRecord LRU 上限，防止内存无限增长。
    """

    _pipeline: Pipeline
    _max_tasks: int
    _records: OrderedDict[str, TaskRecord]
    _lock: threading.Lock

    def __init__(self, pipeline: Pipeline, max_tasks: int = 500) -> None:
        self._pipeline = pipeline
        self._max_tasks = max_tasks
        # OrderedDict 实现 LRU：新记录插尾，淘汰时去头
        self._records = OrderedDict()
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # 公共查询
    # ------------------------------------------------------------------

    @property
    def pipeline(self) -> Pipeline:
        return self._pipeline

    def get_task(self, task_id: str) -> TaskRecord | None:
        with self._lock:
            return self._records.get(task_id)

    def list_tasks(self, status: str = "all") -> list[TaskRecord]:
        """返回满足 status 过滤条件的任务列表（从新到旧）。

        Args:
            status: ``"all"`` | ``"running"`` | ``"pending"`` |
                    ``"done"`` | ``"failed"`` | ``"cancelled"``
        """
        with self._lock:
            records = list(reversed(self._records.values()))
        if status == "all":
            return records
        return [r for r in records if r.status == status]

    # ------------------------------------------------------------------
    # 任务提交
    # ------------------------------------------------------------------

    def submit_task(self, req: TaskRequest, is_async=False) -> ApiResponse:
        """同步提交：阻塞当前线程直到 pipeline 完成，返回结果。"""

        task_id = str(uuid.uuid4())
        record = self.new_record(task_id)
        ctx = self._pipeline.make_context(input=req.input)

        record.status = "running"
        record.start_at = time.time()
        record.ctx = ctx  # 持有 Context 引用以实现精准取消

        self.save_record(record)

        event = threading.Event()

        def start():
            try:
                record.results = self._pipeline.run(ctx, timeout=req.timeout)
                record.status = "done" if record.status != "cancelled" else "cancelled"
            except Exception as exc:
                record.status = "failed"
                record.error = str(exc)
            finally:
                record.finish_at = time.time()

            event.set()

        if is_async:
            threading.Thread(target=start, daemon=True).start()
            return ApiResponse.ok({"task_id": task_id})
        else:
            start()
            event.wait()
            self.delete_record(task_id)
            return ApiResponse.ok(record.to_dict())

    # ------------------------------------------------------------------
    # 记录
    # ------------------------------------------------------------------

    def new_record(self, task_id: str) -> TaskRecord:
        return TaskRecord(
            task_id=task_id,
            pipeline_name=self._pipeline.name,
        )

    def save_record(self, record: TaskRecord) -> None:
        """保存 / 更新任务记录，超出 max_tasks 时淘汰最旧的。"""
        with self._lock:
            self._records[record.task_id] = record
            self._records.move_to_end(record.task_id)
            while len(self._records) > self._max_tasks:
                _ = self._records.popitem(last=False)

    def delete_record(self, task_id: str) -> None:
        with self._lock:
            self._records.pop(task_id, None)

    def cancel_task(self, task_id: str) -> ApiResponse:
        """取消指定 task_id 的任务。

        - 若任务不存在 → 404
        - 若任务已完成 / 已取消 / 已失败 → 409
        - 若任务 pending / running → 注入 stop 信号，状态设为 cancelled
        """
        record = self.get_task(task_id)
        if record is None:
            return ApiResponse.not_found(f"Task '{task_id}' not found.")

        with self._lock:
            if record.status in ("done", "failed", "cancelled"):
                return ApiResponse.conflict(
                    f"Task '{task_id}' is already {record.status} and cannot be cancelled."
                )
            record.status = "cancelled"
            record.finish_at = time.time()

        # 如果任务正在运行，通过 task 精准发出停止信号
        ctx = record.ctx
        if ctx is not None:
            ctx.cancel()

        return ApiResponse.ok({"task_id": task_id, "status": "cancelled"})
