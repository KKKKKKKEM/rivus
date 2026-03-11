# Context — 线程安全状态容器

`Context` 是贯穿整条 Pipeline 的核心数据载体，每个 item 在流水线中都以一个 `Context` 实例的形式流动。

**源码**：`rivus/context.py`

---

## 概述

```python
from rivus import Context

ctx = Context({"image_path": "/data/img.jpg"})
ctx.set("preprocess.resized", True)
value = ctx.require("image_path")   # '/data/img.jpg'
ctx.log.info("context ready")
```

- 所有读写操作均通过内部 `RLock` 保证线程安全。
- 节点函数通过 `ctx.require("input")` 获取当前 item 数据。
- 通过 `ctx.derive(...)` 创建子 Context（不可变风格派生）。

---

## 构造函数

```python
Context(
    initial: dict[str, Any] | None = None,
    logger: RivusLogger | None = None,
)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `initial` | `dict` \| `None` | 初始键值对，不传则创建空 Context |
| `logger` | `RivusLogger` \| `None` | 绑定日志器；不传时使用默认 stderr 日志器 |

---

## 读操作

### `get(key, default=None) → Any`

获取键对应的值，键不存在时返回 `default`（默认 `None`）。

```python
path = ctx.get("image_path")           # 键不存在返回 None
path = ctx.get("image_path", "/tmp/")  # 键不存在返回 "/tmp/"
```

### `require(key) → Any`

获取必须存在的值。键不存在时抛出 `ContextKeyError`。  
推荐在节点函数中用此方法获取 `"input"` 等关键键，让错误尽早暴露。

```python
item = ctx.require("input")  # 不存在 → ContextKeyError
```

### `snapshot() → dict[str, Any]`

返回所有数据的**深拷贝**快照（线程安全）。适合需要保存当前状态或传递给外部系统的场景。

```python
snap = ctx.snapshot()
```

### `keys() → list[str]`

返回所有键名列表（线程安全的副本）。

```python
print(ctx.keys())  # ['input', 'model', ...]
```

---

## 写操作

### `set(key, value) → None`

写入单个键值对（线程安全）。

```python
ctx.set("result", 42)
```

### `update(data: dict) → None`

批量写入键值对，语义与 `dict.update` 相同。

```python
ctx.update({"score": 0.95, "label": "cat"})
```

### `delete(key) → None`

删除一个键；键不存在时静默忽略。

```python
ctx.delete("temp_buffer")
```

---

## 运算符支持

| 语法 | 等价操作 |
|------|----------|
| `"key" in ctx` | `ctx.get("key") is not None`（精确：检查 key 存在性） |
| `ctx["key"]` | `ctx.require("key")` |
| `ctx["key"] = value` | `ctx.set("key", value)` |
| `for key in ctx` | 遍历所有键 |
| `len(ctx)` | 返回键数量 |

---

## 日志器

### `ctx.log → RivusLogger`

内置日志器，由 Pipeline 统一注入配置。支持标准五级方法及 `bind()`：

```python
ctx.log.info("processing %s", item_id)
ctx.log.warning("slow response, retrying")
ctx.log.error("failed to parse: %s", err)

# 绑定上下文字段
log = ctx.log.bind(stage="inference", worker_id=3)
log.debug("logits: %s", logits)
# 输出: [stage=inference worker_id=3] logits: ...
```

---

## 停止控制

### `ctx.request_stop() → None`

请求停止整个 Pipeline（协作式）。当前正在处理的 item 执行完毕后，后续 item 不再被处理。不抛出异常，适合在循环中设置退出标志后继续处理当前 item 的剩余逻辑。

```python
def process(ctx: Context):
    if some_condition:
        ctx.request_stop()   # 设置停止标志，当前函数继续执行
    return result
```

### `ctx.stop(message="pipeline stop requested") → Never`

请求停止并**立即**抛出 `PipelineStop` 异常，中断当前节点执行。Pipeline 收到此信号后标记为 `STOPPED`（不视为失败）。

```python
def process(ctx: Context):
    if done_condition:
        ctx.stop("task complete")   # 立即终止，不再处理后续逻辑
    return heavy_work(ctx.require("input"))
```

### `ctx.stop_requested → bool`

只读属性，轮询检测停止请求。适合在长时间循环中实现协作式提前退出。

```python
@node(workers=4)
def heavy_compute(ctx: Context):
    for chunk in data_chunks:
        if ctx.stop_requested:
            ctx.log.warning("stop requested, exiting early")
            return
        process(chunk)
```

> **注意**：单独使用 Context（不在 Pipeline 中）时，`stop_requested` 始终返回 `False`，`request_stop()` 和 `stop()` 的信号不会传播。

---

## Context 派生

### `ctx.derive(**kwargs) → Context`

创建子 Context，**继承当前所有数据**，并以 `kwargs` 覆盖或新增键值。原 Context 不受影响（不可变风格派生），子 Context 自动继承父 Context 的日志器和停止信号。

```python
item_ctx = root_ctx.derive(input="/data/img.jpg")
result_ctx = item_ctx.derive(input=result, extra_flag=True)
```

> **框架内部行为**：Pipeline 在将 item 送入第一个节点时，自动调用 `root_ctx.derive(input=item)` 创建每条 item 的个人 Context；节点处理后的返回值同样通过 `item_ctx.derive(input=return_value)` 传递给下游。

---

## Pickle 支持

Context 实现了 `__getstate__` / `__setstate__`，支持 pickle 序列化，用于 `concurrency_type="process"` 的跨进程传递。反序列化后日志器和锁会自动重置。

---

## 完整类图

```
Context
├── _data: dict[str, Any]       # 内部数据存储（受 RLock 保护）
├── _lock: RLock                # 可重入锁
├── _logger: RivusLogger        # 日志器（Pipeline 注入）
├── _stop_event_ref: Event      # 停止信号（Pipeline 注入，可选）
│
├── 读: get / require / snapshot / keys
├── 写: set / update / delete
├── 运算符: __contains__ / __getitem__ / __setitem__ / __iter__ / __len__
├── 日志: .log, ._attach_logger()
├── 停止: request_stop() / stop() / stop_requested
└── 派生: derive(**kwargs)
```
