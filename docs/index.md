# Rivus 文档

**Rivus** 是一个基于 Python `queue.Queue` 的轻量级数据流水线（Pipeline）框架，专为并发批处理、流式处理及多阶段数据转换场景设计。

## 核心思想

每个 **Node**（节点）只接受一个 `Context` 对象、返回处理结果，多个节点通过内部 Queue 自动串联，形成数据流水线：

```
items → wrap(ctx) → Q → [Node A, workers=W] → Q → [Node B, workers=W] → Q → results
```

- **Context**：贯穿整条 Pipeline 的线程安全状态容器，携带当前 item 及共享元数据。
- **Node**：处理单个 item 的最小单元，支持多线程 / 多进程并发。
- **Pipeline**：将多个节点组合成完整流水线，负责调度、错误处理、结果收集和生命周期管理。

---

## 目录

### 快速上手
- [快速上手](getting-started.md) — 5 分钟内跑起第一条流水线

### API 参考
- [Context](api/context.md) — 线程安全状态容器
- [Node / @node / BaseNode](api/node.md) — 节点定义方式
- [Pipeline](api/pipeline.md) — 流水线核心类
- [日志（LogConfig / RivusLogger）](api/log.md) — 日志体系
- [异常（Exceptions）](api/exceptions.md) — 异常体系

### 进阶指南
- [使用模式与最佳实践](guides/patterns.md) — 常用模式、fan-out、gather、进程模式等

---

## 安装

Rivus 为纯 Python 标准库实现，无第三方依赖：

```bash
# 直接将 rivus/ 目录置于项目中，或通过包管理器安装
pip install rivus        # （如已发包）
```

## 最简示例

```python
import rivus

@rivus.node(workers=4)
def double(ctx: rivus.Context):
    return ctx.require("input") * 2

report = (rivus.Pipeline("demo") | double).run(1, 2, 3, 4, 5)
print(report.results)   # [2, 4, 6, 8, 10]
```

## 公共 API 一览

```python
from rivus import (
    # 节点
    Node, node, BaseNode,
    # 流水线
    Pipeline, PipelineStatus, PipelineReport, NodeReport,
    # 上下文
    Context,
    # 日志
    LogConfig, RivusLogger,
    # 异常
    RivusError, NodeError, PipelineError,
    PipelineTimeoutError, PipelineStop, ContextKeyError,
)
```
