# Pipeline — 流水线编排与运行

`Pipeline` 是 Rivus 的核心编排器，负责组装节点、管理并发、执行数据流、触发生命周期钩子，以及暴露 HTTP 服务接口。

**源码位置**：`rivus/pipeline.py`

---

## 概述

`Pipeline` 的职责：

1. **节点组装**：通过 `|` 运算符或 `add_node()` 方法构建处理链
2. **运行调度**：每次 `run()` 创建独立的节点快照，互不干扰
3. **并发控制**：全局信号量限制最大并发 `run()` 数量
4. **生命周期钩子**：startup、run_start、run_end 三阶段回调
5. **共享变量**：跨 `run()` 调用的持久化键值存储
6. **HTTP 服务**：`serve()` 一键暴露 REST API

---

## 构造函数

```python
class Pipeline:
    def __init__(
        self,
        name: str = "pipeline",
        max_workers: int | None = 10,
        *,
        initial: dict[str, Any] | None = None,
        nodes: list[Node] | None = None,
        on_result: Callable | None = None,
    ) -> None
```

### 参数详解

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `name` | `str` | `"pipeline"` | 流水线名称，用于日志、HTTP 路由路径 |
| `max_workers` | `int \| None` | `10` | 最大并发 `run()` 数量；`None` 表示无限制 |
| `initial` | `dict \| None` | `{}` | 每次 `run()` 注入根 Context 的初始键值对 |
| `nodes` | `list[Node] \| None` | `[]` | 预设节点列表 |
| `on_result` | `Callable \| None` | 见下文 | 自定义结果提取函数 |

### `on_result` 参数

默认行为是提取所有收集到的 Context 的 `input` 字段：

```python
# 默认实现
on_result = lambda pipeline, results, ctx: [i.input for i in results]
```

自定义示例（返回完整 Context 而非 `input`）：

```python
pipeline = rivus.Pipeline(
    "demo",
    on_result=lambda p, results, ctx: list(results)
)
```

**签名**：`(pipeline: Pipeline, results: deque[Context], ctx: Context) -> list[object]`

---

## 构建 API

### `|` 运算符

```python
pipeline = rivus.Pipeline("demo") | step1 | step2 | step3
```

等价于：

```python
pipeline = rivus.Pipeline("demo")
pipeline.add_node(step1)
pipeline.add_node(step2)
pipeline.add_node(step3)
```

---

### `add_node(node)`

追加单个节点，返回 `self`（支持链式调用）。

```python
def add_node(self, n: Node) -> Pipeline
```

```python
pipeline = rivus.Pipeline("demo")
pipeline.add_node(step1).add_node(step2)
```

若传入非 `Node` 类型，抛 `TypeError`。

---

### `add_nodes(*nodes)`

追加多个节点，返回 `self`。

```python
def add_nodes(self, *nodes: Node) -> Pipeline
```

```python
pipeline = rivus.Pipeline("demo")
pipeline.add_nodes(step1, step2, step3)
```

---

## 运行 API

### `run(item, timeout=None)`

同步运行流水线，返回结果列表。

```python
def run(self, item: object, timeout: float | None = None) -> list[object]
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `item` | `object` | 输入数据，或已构造的 `Context` 实例 |
| `timeout` | `float \| None` | 最大等待秒数；超时抛 `TimeoutError` |

**返回值**：结果列表（由 `on_result` 函数决定内容，默认为各 Context 的 `.input` 值）。

```python
# 普通输入
results = pipeline.run("hello")

# 带超时
results = pipeline.run("hello", timeout=30.0)

# 传入已构造的 Context（用于 HTTP 服务层）
ctx = pipeline.make_context(input="hello", user_id=42)
results = pipeline.run(ctx)
```

**运行流程**：

```
1. _fire_startup()   — 首次调用时触发 on_startup 钩子（线程安全，只执行一次）
2. semaphore.acquire()  — 获取并发槽位（max_workers 限制）
3. _build_workers(item) — 为所有节点创建快照、启动线程、提交第一个 item
4. tail.wait(timeout)   — 等待最后一个节点完成
5. on_result(...)        — 提取收集队列中的结果
6. _fire_run_end(ctx)    — 触发 on_run_end 钩子
7. semaphore.release()   — 释放并发槽位
```

---

### `stream(item)`

流式运行，返回生成器，逐个 yield 结果 Context。

```python
def stream(self, item: object) -> Generator[Context, Any, None]
```

```python
for ctx in pipeline.stream("hello"):
    print(ctx.input)   # 逐个消费结果
```

**与 `run()` 的区别**：

| | `run()` | `stream()` |
|-|---------|-----------|
| 返回时机 | 所有结果就绪后 | 每个结果就绪时立即 yield |
| 返回类型 | `list[object]` | `Generator[Context]` |
| 内存占用 | 全部结果缓存在内存 | 逐个消费，内存友好 |
| 适用场景 | 批量处理、结果需要汇总 | 实时处理、结果需要流式消费 |

---

### `make_context(**kwargs)`

创建一个携带初始数据的根 Context。

```python
def make_context(self, **kwargs) -> Context
```

```python
ctx = pipeline.make_context(input="hello", user_id=42)
# ctx 已包含: input="hello", user_id=42, pipeline=pipeline, **initial
```

等价于：

```python
ctx = Context(initial={**pipeline.initial, **kwargs, "pipeline": pipeline})
```

---

## 生命周期钩子

### 三个阶段

```
Pipeline 生命周期
├── on_startup     — 首次 run() 时执行一次（初始化共享资源）
│
└── 每次 run()
    ├── on_run_start   — 处理开始前（item 刚进入）
    ├── ... 节点处理 ...
    └── on_run_end     — 处理结束后（无论成功/失败）
```

---

### `on_startup(fn)`

注册 startup 钩子，**只在第一次 `run()` 前执行一次**，线程安全。

```python
def on_startup(self, fn: Callable[["Pipeline"], None]) -> Callable
```

**用途**：初始化数据库连接池、预加载模型等重量级资源。

```python
pipeline = rivus.Pipeline("demo")

@pipeline.on_startup
def init_resources(p: rivus.Pipeline):
    db = create_db_connection()
    p.set("db", db)           # 存入 Pipeline 全局变量
    print("Resources initialized")

# 首次 run() 时触发 init_resources，后续 run() 不再触发
results = pipeline.run("item1")  # → init_resources 执行
results = pipeline.run("item2")  # → init_resources 不再执行
```

---

### `on_run_start(fn)`

注册 run_start 钩子，**每次 `run()` 开始时执行**。

```python
def on_run_start(self, fn: Callable[["Context"], None]) -> Callable
```

**用途**：记录请求日志、设置 trace ID、注入请求级元数据。

```python
import time

@pipeline.on_run_start
def trace(ctx: rivus.Context):
    ctx.set("start_time", time.time())
    ctx.set("trace_id", generate_trace_id())
```

---

### `on_run_end(fn)`

注册 run_end 钩子，**每次 `run()` 结束后执行**（无论成功/失败/超时）。

```python
def on_run_end(self, fn: Callable[["Context"], None]) -> Callable
```

**用途**：记录耗时、清理资源、发送监控指标。

```python
@pipeline.on_run_end
def record_metrics(ctx: rivus.Context):
    start = ctx.get("start_time", 0)
    elapsed = time.time() - start
    print(f"Run completed in {elapsed:.3f}s")
```

---

### 注册多个钩子

可以为同一阶段注册多个钩子，按注册顺序依次执行：

```python
@pipeline.on_run_start
def hook1(ctx): ...

@pipeline.on_run_start
def hook2(ctx): ...  # hook1 先于 hook2 执行
```

---

## 全局变量 API

`Pipeline` 提供跨 `run()` 调用的线程安全键值存储，适合存储共享资源（如连接池）。

### `set(key, value)`

写入全局变量。

```python
def set(self, key: str, value: Any) -> None
```

### `get(key, default=None)`

读取全局变量。

```python
def get(self, key: str, default: Any = None) -> Any
```

```python
# 在 on_startup 中初始化
@pipeline.on_startup
def init(p):
    p.set("db_pool", create_pool())

# 在节点中通过 ctx.get("pipeline") 访问
@rivus.node
def query(ctx: rivus.Context):
    pipeline = ctx.get("pipeline")
    pool = pipeline.get("db_pool")
    return pool.execute(ctx.require("input"))
```

> **提示**：`pipeline` 引用在创建 Context 时自动注入（`make_context` 内部实现），所有节点均可通过 `ctx.get("pipeline")` 获取 Pipeline 实例。

---

## HTTP 服务

### `serve(host, port, prefix)`

启动 HTTP RPC 服务，阻塞当前线程。

```python
def serve(
    self,
    host: str = "0.0.0.0",
    port: int = 8080,
    *,
    prefix: str = "",
) -> None
```

**参数**：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `host` | `"0.0.0.0"` | 监听地址 |
| `port` | `8080` | 监听端口 |
| `prefix` | `""` | URL 路径前缀，如 `"/api/v1"` |

```python
# 基本用法
pipeline.serve(port=9000)

# 自定义路径前缀
pipeline.serve(host="0.0.0.0", port=9000, prefix="/api/v1")
```

HTTP 接口（`pipeline.name="demo"`, `prefix="/api/v1"`）：

```
GET  /api/v1/heartbeat
POST /api/v1/shutdown
POST /api/v1/demo/tasks        # 提交任务
GET  /api/v1/demo/tasks        # 列出任务
GET  /api/v1/demo/tasks/{id}   # 查询任务
DELETE /api/v1/demo/tasks/{id} # 取消任务
```

更多详情见 [HTTP 服务文档](../serve/overview.md)。

---

## 内部运行机制

### `_build_workers(item)`

每次 `run()` 的核心内部方法：

```python
def _build_workers(self, item: object) -> tuple[Context, Node, Node, Node]
```

执行步骤：

1. 对所有 `self._nodes` 调用 `node.snapshot()` 创建独立副本
2. 将节点副本串联（`node.next = next_node`）
3. 创建 `__collector__` 收集节点（末尾）
4. 启动所有节点（`node.start()`）
5. 创建根 Context，触发 `on_run_start` 钩子
6. 将 Context 提交给第一个节点，并触发第一个节点的 `done()`
7. 返回 `(ctx, head, tail, collector)`

```
self._nodes (模板)        运行时快照（独立）
──────────────────────    ──────────────────────────────────────────
[step1, step2, step3]  →  [step1_snap, step2_snap, step3_snap, __collector__]
                                │            │            │
                             start()      start()      start()
                                ↑
                         head.submit(ctx)
                         head.done()
```

### 节点快照隔离

每次 `run()` 使用各节点的**快照副本**，而非原始节点。快照包含：

- `fn`（相同引用）
- `name`、`workers`、`queue_size`（相同值）
- 独立的 `task_queue`、`shutdown` 事件（不共享状态）

因此多次并发 `run()` 互不干扰。

---

## 完整示例

```python
import time
import rivus

# ── 节点定义 ──────────────────────────────────────────────────────────

@rivus.node
def preprocess(ctx: rivus.Context):
    item = ctx.require("input")
    return item.strip().lower()

@rivus.node(workers=4)
def enrich(ctx: rivus.Context):
    text = ctx.require("input")
    db = ctx.get("pipeline").get("db")
    extra = db.get(text) if db else {}
    return {"text": text, **extra}

@rivus.node
def format_output(ctx: rivus.Context):
    data = ctx.require("input")
    return f"[{data['text']}]"

# ── Pipeline 配置 ─────────────────────────────────────────────────────

pipeline = (
    rivus.Pipeline(
        "text-pipeline",
        max_workers=20,          # 最多 20 个并发 run()
        initial={"version": "1.0"},  # 所有节点可读
    )
    | preprocess
    | enrich
    | format_output
)

# ── 生命周期钩子 ──────────────────────────────────────────────────────

@pipeline.on_startup
def init_db(p: rivus.Pipeline):
    p.set("db", {"hello": {"lang": "en"}, "mundo": {"lang": "es"}})
    print("DB initialized")

@pipeline.on_run_start
def log_start(ctx: rivus.Context):
    ctx.set("ts", time.time())

@pipeline.on_run_end
def log_end(ctx: rivus.Context):
    elapsed = time.time() - ctx.get("ts", time.time())
    print(f"Completed in {elapsed:.3f}s")

# ── 运行 ──────────────────────────────────────────────────────────────

# 批量运行
results = pipeline.run("  Hello  ")
print(results)  # ['[hello]']

# 流式运行
for ctx in pipeline.stream("  World  "):
    print(ctx.input)   # '[world]'

# HTTP 服务
# pipeline.serve(port=8080)
```

---

## API 速查

| 方法 | 签名 | 说明 |
|------|------|------|
| `__init__` | `(name, max_workers, initial, nodes, on_result)` | 构造 |
| `\|` | `pipeline \| node → Pipeline` | 追加节点（链式） |
| `add_node` | `(node) → self` | 追加单节点 |
| `add_nodes` | `(*nodes) → self` | 追加多节点 |
| `run` | `(item, timeout) → list` | 同步运行 |
| `stream` | `(item) → Generator[Context]` | 流式运行 |
| `make_context` | `(**kwargs) → Context` | 创建根 Context |
| `on_startup` | `(fn) → fn` | 注册 startup 钩子（执行一次） |
| `on_run_start` | `(fn) → fn` | 注册每次运行开始钩子 |
| `on_run_end` | `(fn) → fn` | 注册每次运行结束钩子 |
| `set` | `(key, value)` | 写入 Pipeline 全局变量 |
| `get` | `(key, default) → Any` | 读取 Pipeline 全局变量 |
| `serve` | `(host, port, prefix)` | 启动 HTTP 服务 |
