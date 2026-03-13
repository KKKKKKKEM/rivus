# Rivus 文档

**Rivus** 是一个基于 Python `queue.Queue` 的轻量级数据流水线（Pipeline）框架，专为并发批处理、流式处理及多阶段数据转换场景设计。零依赖标准库实现，内置 HTTP 服务层。

```
items → wrap(ctx) → Q → [Node A, workers=W] → Q → [Node B, workers=W] → Q → results
```

**版本**: `0.4.0` | **Python**: `>= 3.10` | **License**: MIT

---

## 核心特性

| 特性 | 说明 |
|------|------|
| **零依赖** | 核心层仅使用 Python 标准库（`queue`、`threading`） |
| **简洁 API** | `@node` 装饰器 + `\|` 管道符，声明式组合节点 |
| **多并发模型** | 每个节点独立配置线程数（`workers=N`） |
| **生成器扇出** | 节点 `yield` 多个值，一对多输出 |
| **Gather 屏障** | `gather=True` 汇聚上游所有输出后整体处理 |
| **优雅停止** | `ctx.cancel()` / `signals.STOP` 安全终止流水线 |
| **流式输出** | `pipeline.stream(item)` 逐个消费结果 |
| **HTTP 服务** | `pipeline.serve()` 一键暴露 RESTful 接口 |
| **生命周期钩子** | `on_startup`、`on_run_start`、`on_run_end` |

---

## 架构概览

### 数据流模型

```
                     Pipeline.run(item)
                           │
                    make_context(input=item)
                           │
                           ▼
          ┌────────────────────────────────────────┐
          │  Node 0  (workers=W₀)                  │
          │  ┌──────┐ ┌──────┐ ┌──────┐           │
          │  │  T0  │ │  T1  │ │  T2  │  ...       │  ← W₀ 个工作线程
          │  └──┬───┘ └──┬───┘ └──┬───┘           │
          └─────┼─────────┼─────────┼──────────────┘
                │         │         │
                ▼         ▼         ▼
          ┌─────────── Queue ────────────┐
          │  ctx₁    ctx₂    ctx₃  ...  │
          └─────────────────────────────┘
                           │
          ┌────────────────────────────────────────┐
          │  Node 1  (workers=W₁)                  │
          │  ┌──────┐ ┌──────┐                    │
          │  │  T0  │ │  T1  │  ...               │  ← W₁ 个工作线程
          │  └──┬───┘ └──┬───┘                    │
          └─────┼─────────┼──────────────────────┘
                ...       ...
                           │
                    __collector__ Queue
                           │
                    pipeline.on_result(...)
                           │
                    return list[object]
```

### 核心组件

```
rivus/
├── context.py       # Context    — 线程安全状态容器
├── node.py          # Node       — 处理单元 + @node 装饰器
│                    # ERROR      — 节点级错误类
├── pipeline.py      # Pipeline   — 流水线编排与运行
├── signals.py       # SKIP/STOP  — 控制流信号
└── serve/
    ├── __init__.py  # serve()    — HTTP 服务入口
    ├── base.py      # AbstractServer / ServerConfig
    ├── models.py    # TaskRequest / TaskRecord / ApiResponse
    ├── task_manager.py  # TaskManager — 任务生命周期管理
    └── sanic_server.py  # SanicServer — Sanic 实现
```

### Context 传播模型

```
pipeline.run(item)
       │
       ▼
Context(input=item, **initial)  ← 根 Context（携带 pipeline 引用）
       │
  Node 0 fn(ctx) → value
       │
       ▼
ctx.derive(input=value)         ← 子 Context，root 指向根 Context
       │
  Node 1 fn(ctx) → value
       │
       ▼
ctx.derive(input=value)
       ...
```

---

## 文档目录

### 入门

- [快速上手](./getting-started.md) — 安装、5 分钟教程、核心概念

### 核心 API

- [Context](./core/context.md) — 状态容器完整 API
- [Node](./core/node.md) — 节点定义、装饰器、信号机制
- [Pipeline](./core/pipeline.md) — 流水线编排与运行

### HTTP 服务

- [serve 概览](./serve/overview.md) — HTTP RPC 服务层完整文档

### 使用指南

- [高级模式](./guides/patterns.md) — Gather、扇出、并发调优、优雅停止
- [自定义后端](./guides/custom-backend.md) — 扩展 HTTP 服务层

---

## 快速示例

```python
import rivus

@rivus.node
def parse(ctx: rivus.Context):
    raw = ctx.require("input")
    return raw.strip().upper()

@rivus.node(workers=4)
def process(ctx: rivus.Context):
    text = ctx.require("input")
    return f"processed: {text}"

pipeline = rivus.Pipeline("demo") | parse | process

# 批量运行
results = pipeline.run("hello")
print(results)  # ['processed: HELLO']

# 流式消费
for ctx in pipeline.stream("world"):
    print(ctx.input)  # 'processed: WORLD'

# 启动 HTTP 服务
pipeline.serve(port=8080)
```
