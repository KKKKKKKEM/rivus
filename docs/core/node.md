# Node — 流水线处理单元

`Node` 是 Rivus 流水线的基本执行单元，封装处理函数、并发配置和节点间通信队列。

**源码位置**：`rivus/node.py`、`rivus/signals.py`

---

## 概述

每个节点持有：
- 一个处理函数 `fn(ctx: Context) -> Any`
- 一个工作线程池（`workers` 个 `threading.Thread`）
- 一个输入队列（`queue.Queue`）
- 指向下一个节点的引用（`next: Node | None`）

节点间通过队列自动连接，形成生产者-消费者链：

```
Node A (workers=2)          Node B (workers=4)
┌─────────────────┐        ┌─────────────────┐
│  Thread 0       │        │  Thread 0       │
│  Thread 1       │──Q──▶  │  Thread 1       │──Q──▶  ...
│                 │        │  Thread 2       │
│  task_queue     │        │  Thread 3       │
└─────────────────┘        └─────────────────┘
```

---

## `@node` 装饰器

最常用的节点定义方式，将普通函数包装为 `Node` 实例。

### 签名

```python
def node(
    fn: Callable[[Context], Any] | None = None,
    *,
    name: str | None = None,
    workers: int = 1,
    queue_size: int = 0,
) -> Node | Callable
```

### 参数详解

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `fn` | `Callable` | — | 被装饰的函数（直接使用 `@node` 时自动传入） |
| `name` | `str \| None` | 函数名 | 节点名称，用于调试和日志 |
| `workers` | `int` | `1` | 并发工作线程数，必须 `>= 1` |
| `queue_size` | `int` | `0` | 输入队列最大容量，`0` 表示无限 |

### 使用方式

**无参数装饰（最简）**：

```python
@rivus.node
def step(ctx: rivus.Context):
    return ctx.require("input").upper()
```

**带参数装饰**：

```python
@rivus.node(workers=4, name="parallel-step")
def step(ctx: rivus.Context):
    return heavy_work(ctx.require("input"))
```

**手动调用**：

```python
def my_fn(ctx):
    return ctx.require("input")

step = rivus.node(my_fn, workers=2)
# 或
step = rivus.node(workers=2)(my_fn)
```

---

## `Node` 类

### 构造函数

```python
class Node:
    def __init__(
        self,
        fn: Callable[[Context], Any],
        name: str | None = None,
        *,
        workers: int = 1,
        queue_size: int = 0,
    ) -> None
```

### 实例属性

| 属性 | 类型 | 说明 |
|------|------|------|
| `fn` | `Callable` | 处理函数 |
| `name` | `str` | 节点名称 |
| `workers` | `int` | 配置的工作线程数 |
| `running_workers` | `int` | 当前活跃工作线程数 |
| `queue_size` | `int` | 队列容量配置 |
| `task_queue` | `queue.Queue[Context]` | 输入队列 |
| `is_generator` | `bool` | 处理函数是否为生成器 |
| `next` | `Node \| None` | 下游节点引用 |
| `shutdown` | `threading.Event` | 所有工作线程完成时触发 |

---

## 节点函数签名约定

节点函数只接收一个参数 `ctx: Context`，返回值决定如何传递给下游：

| 返回值 | 框架行为 |
|--------|----------|
| 任意非 `None` 值 | `ctx.derive(input=value)` → 传给下游 |
| `None` | 原样传递当前 `ctx`（pass-through） |
| `Context` 实例 | 直接传给下游（手动控制） |
| `yield value`（生成器） | 每个 yield 值各自派生一个下游 item（扇出） |

### 普通返回

```python
@rivus.node
def transform(ctx: rivus.Context):
    item = ctx.require("input")
    return item * 2   # ctx.derive(input=item*2) 传下游
```

### Pass-through（副作用节点）

```python
@rivus.node
def log_node(ctx: rivus.Context):
    print(f"Processing: {ctx.require('input')}")
    return None   # 或直接省略 return，ctx 原样继续
```

### 生成器扇出

```python
@rivus.node
def split(ctx: rivus.Context):
    batch = ctx.require("input")   # 输入一个列表
    for item in batch:
        yield item   # 每个 yield 产生独立下游 item
```

一条输入经过生成器节点后，下游会收到多条 item。例如输入列表 `[1, 2, 3]`，扇出后下游各自处理 `1`、`2`、`3`。

---

## 并发模型

### `workers` 参数

每个节点启动 `workers` 个守护线程并发消费其输入队列：

```python
@rivus.node(workers=1)    # 串行（默认）
def serial_step(ctx): ...

@rivus.node(workers=4)    # 4 个线程并发
def parallel_step(ctx): ...

@rivus.node(workers=8)    # 8 个线程并发
def heavy_step(ctx): ...
```

**何时增加 workers**：
- I/O 密集型操作（网络请求、文件读写、数据库查询）
- 节点处理耗时较长，成为瓶颈

**何时保持 workers=1**：
- 顺序敏感操作（如需要按序写入文件）
- 轻量级操作（CPU 开销远小于线程调度开销）
- 聚合/收集节点（避免并发写入共享结构）

### `queue_size` 参数

控制输入队列的最大长度（背压机制）：

```python
# 无限队列（默认），上游可以无限积压
@rivus.node(workers=2, queue_size=0)
def step(ctx): ...

# 队列满时上游阻塞，实现背压
@rivus.node(workers=2, queue_size=100)
def step(ctx): ...
```

当 `queue_size > 0` 时，若该节点的输入队列已满，上游节点的 `submit()` 调用会阻塞，直到队列有空位。

---

## 控制流信号

通过抛出信号异常来控制节点行为：

### `signals.SKIP` — 跳过当前 item

```python
from rivus import signals

@rivus.node
def filter_node(ctx: rivus.Context):
    item = ctx.require("input")
    if not is_valid(item):
        raise signals.SKIP   # 当前 item 不传给下游，直接丢弃
    return item
```

- 当前 item 不会传给下游节点
- 其他 item 不受影响，流水线继续运行

### `signals.STOP` — 终止整条流水线

```python
from rivus import signals

@rivus.node
def sentinel_node(ctx: rivus.Context):
    item = ctx.require("input")
    if item == "STOP":
        raise signals.STOP   # 终止整条流水线
    return item
```

- 向下游节点发送 `done()` 信号，整条流水线停止处理
- 当前及后续 item 均不再处理

### 信号继承关系

```
Exception
└── RivusError
    ├── SKIP    — 跳过当前 item
    └── STOP    — 终止流水线
```

### 对比：`ctx.cancel()` vs `signals.STOP`

| 机制 | 作用范围 | 使用场景 |
|------|----------|----------|
| `raise signals.STOP` | 节点内触发，停止整条流水线 | 节点业务逻辑决定停止 |
| `ctx.cancel()` | 外部注入，取消特定 Context | HTTP 层取消任务 |
| `raise signals.SKIP` | 仅跳过当前 item | 数据过滤 |

---

## 异常处理

节点函数中未被捕获的非信号异常，框架会将其记录在 `ctx.error` 中并传递给下游节点：

```python
@rivus.node
def risky_step(ctx: rivus.Context):
    try:
        return dangerous_operation(ctx.require("input"))
    except ValueError as e:
        # 方式一：自行处理，返回 None 或默认值
        return None

# 方式二：让框架捕获，error 会传给下游
@rivus.node
def risky_step(ctx: rivus.Context):
    return dangerous_operation(ctx.require("input"))  # 可能抛异常

@rivus.node
def error_handler(ctx: rivus.Context):
    if ctx.error:
        print(f"上游出错: {ctx.error}")
        # 处理错误，或 raise signals.SKIP 跳过
    return ctx.require("input")
```

---

## `Node` 内部 API

以下方法通常由框架内部调用，了解它们有助于理解运行机制：

### `start()`

启动 `workers` 个工作线程，开始消费 `task_queue`。

```python
node.start()
```

每次 `pipeline.run()` 时，Pipeline 会对每个节点的快照调用 `start()`。

### `done()`

向队列中注入 `workers` 个终止哨兵（`_DONE`），通知所有工作线程停止。

```python
node.done()
```

当上游节点的所有工作线程完成后，会自动调用下游节点的 `done()`，形成级联停止。

### `wait(timeout=None)`

阻塞直到该节点的所有工作线程完成。

```python
node.wait()           # 无限等待
node.wait(timeout=10) # 最多等待 10 秒，超时抛 TimeoutError
```

### `submit(item, block=True, timeout=None)`

向节点的输入队列提交一个 Context。

```python
node.submit(ctx)
```

### `snapshot()`

创建节点的配置快照（新的 `Node` 实例，含相同的 `fn`、`name`、`workers`、`queue_size`，但状态独立）。

每次 `pipeline.run()` 都会对所有节点调用 `snapshot()`，确保每次运行互不干扰。

---

## 完整示例

```python
import time
import rivus
from rivus import signals

# 1. 过滤节点（SKIP 信号）
@rivus.node
def validate(ctx: rivus.Context):
    item = ctx.require("input")
    if not isinstance(item, int) or item < 0:
        raise signals.SKIP
    return item

# 2. 扇出节点（生成器）
@rivus.node
def expand(ctx: rivus.Context):
    n = ctx.require("input")
    for i in range(n):
        yield i

# 3. 并发处理节点
@rivus.node(workers=4)
def heavy_process(ctx: rivus.Context):
    i = ctx.require("input")
    time.sleep(0.05)  # 模拟 I/O
    return i * i

# 4. 副作用节点（pass-through）
@rivus.node
def audit(ctx: rivus.Context):
    result = ctx.require("input")
    print(f"audit: {result}")
    # 返回 None → ctx 原样传下游

# 5. 早停节点（STOP 信号）
@rivus.node
def early_stop(ctx: rivus.Context):
    item = ctx.require("input")
    if item > 100:
        raise signals.STOP
    return item

pipeline = (
    rivus.Pipeline("demo")
    | validate
    | expand
    | heavy_process
    | audit
    | early_stop
)

results = pipeline.run(5)
# 输出: audit: 0, audit: 1, audit: 4, audit: 9, audit: 16
print(results)  # [0, 1, 4, 9, 16]
```

---

## API 速查

| 方法/属性 | 说明 |
|-----------|------|
| `@node` | 将函数装饰为 Node |
| `@node(workers=N)` | 指定并发度 |
| `@node(name="...")` | 指定名称 |
| `@node(queue_size=N)` | 限制队列长度（背压） |
| `raise signals.SKIP` | 跳过当前 item |
| `raise signals.STOP` | 终止流水线 |
| `return None` | pass-through，ctx 原样传下游 |
| `yield value` | 扇出，一变多 |
| `node.snapshot()` | 创建配置快照（框架内部使用） |
| `node.submit(ctx)` | 手动提交 Context（框架内部使用） |
| `node.wait(timeout)` | 等待节点完成（框架内部使用） |
