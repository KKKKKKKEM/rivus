# Context — 线程安全状态容器

`Context` 是 Rivus 流水线中数据和状态的统一载体，贯穿每个节点的处理过程。

**源码位置**：`rivus/context.py`

---

## 概述

每次 `pipeline.run(item)` 调用都会创建一个根 `Context`，携带输入数据和初始元数据。当节点返回值时，框架自动调用 `ctx.derive(input=value)` 创建子 Context 传给下游节点——子 Context 与根 Context 形成继承链，读取不到的 key 会自动向上查找。

```
根 Context(input="hello", db=..., pipeline=...)
      │
  Node 0 → "HELLO"
      │
子 Context(input="HELLO", root→根)   ← 读 db 时自动委托给根
      │
  Node 1 → "processed: HELLO"
      │
子 Context(input="processed: HELLO", root→根)
```

所有读写操作均通过 `RLock` 保护，线程安全。

---

## 构造函数

```python
class Context:
    def __init__(
        self,
        initial: dict[str, Any] | None = None,
        root: Context | None = None,
    ) -> None
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `initial` | `dict \| None` | 初始键值对，作为实例属性设置 |
| `root` | `Context \| None` | 父 Context 引用；为 `None` 时以自身为 root |

> **注意**：通常不需要手动构造 Context，框架会自动创建。

---

## 内置属性

| 属性 | 类型 | 说明 |
|------|------|------|
| `input` | `Any` | 当前节点接收到的数据，由上游节点返回值设置 |
| `error` | `Exception \| None` | 若不为 `None`，表示流水线已处于错误/停止状态 |
| `pipeline` | `Pipeline` | 所属 Pipeline 实例（由 `make_context` 自动注入） |

---

## 读取 API

### `get(key, default=None)`

安全读取属性。若当前 Context 没有该 key，自动向 root Context 查找。

```python
def get(self, key: str, default: Any = None) -> Any
```

**行为**：
1. 检查当前实例是否有 `key` 属性
2. 若无，且 `root != self`，则委托 `root.get(key, default)`
3. 否则返回 `default`

```python
@rivus.node
def step(ctx: rivus.Context):
    # 安全读取，不存在返回 None
    db = ctx.get("db")

    # 带默认值
    timeout = ctx.get("timeout", 30)

    # 多层查找：先找当前 ctx，再找 root
    config = ctx.get("config", {})
```

---

### `require(key)`

强制读取属性。若找不到则抛出 `KeyError`。

```python
def require(self, key: str) -> Any
```

**行为**与 `get` 相同，但找不到时抛 `KeyError` 而非返回默认值。

```python
@rivus.node
def step(ctx: rivus.Context):
    # 必须存在，否则抛出 KeyError("input")
    item = ctx.require("input")
    return process(item)
```

---

### `__getitem__(key)` — 下标语法

等价于 `ctx.require(key)`，抛 `KeyError` 而非返回默认值。

```python
item = ctx["input"]          # 等价于 ctx.require("input")
```

---

## 写入 API

### `set(key, value)`

线程安全地写入一个属性。

```python
def set(self, key: str, value: Any) -> None
```

```python
@rivus.node
def enrich(ctx: rivus.Context):
    item = ctx.require("input")
    result = enrich_data(item)
    ctx.set("metadata", {"processed_at": time.time()})
    return result
```

---

### `update(data)`

批量写入多个属性。

```python
def update(self, data: dict[str, Any]) -> None
```

```python
ctx.update({
    "user_id": 42,
    "timestamp": time.time(),
    "source": "api",
})
```

---

### `delete(key)`

删除一个属性（若不存在则静默忽略）。

```python
def delete(self, key: str) -> None
```

---

### `__setitem__(key, value)` — 下标语法

等价于 `ctx.set(key, value)`。

```python
ctx["result"] = "computed"   # 等价于 ctx.set("result", "computed")
```

---

## 取消 API

### `cancel()`

注入取消信号。调用后，`ctx.need_stop` 返回 `True`，流水线中后续节点将跳过处理该 Context。

```python
def cancel(self) -> None
```

内部实现：

```python
def cancel(self):
    with self._lock:
        self.error = Exception("Task cancelled")
```

**使用场景**：HTTP 服务层的任务取消（`TaskManager.cancel_task`）会调用 `ctx.cancel()`。

---

### `need_stop` 属性

```python
@property
def need_stop(self) -> bool
```

读取当前 Context（或其 root）是否处于停止状态：

- `self.error is not None` → `True`
- 或者 `root.need_stop` → `True`

框架在每个节点的工作线程循环中检查 `ctx.need_stop`，为 `True` 时跳过该 item 的处理。

---

## 派生 API

### `derive(**kwargs)`

创建子 Context，继承当前 Context 作为 root。

```python
def derive(self, **kwargs: Any) -> Context
```

```python
# 框架内部自动调用：
child = ctx.derive(input=node_return_value)
# child.get("db") → 从 root 查找
# child.input     → node_return_value
```

> **注意**：一般不需要手动调用 `derive`，节点直接返回值即可。但如果需要完全控制 Context 传播，可以在节点中返回一个手动 `derive` 的 Context。

---

## 线程安全说明

所有读写操作均使用 `threading.RLock`（可重入锁）保护：

- `get` / `require` — 读锁
- `set` / `update` / `delete` — 写锁
- `cancel` / `need_stop` — 读/写锁

这意味着多个工作线程可以安全地并发读写同一个 Context（如 root Context 中的共享数据）。

> **提示**：由于 root Context 是所有子 Context 的共享状态，频繁的并发写入（如 `ctx.set(...)` 在高并发节点中）可能造成锁竞争。建议将共享写入操作移到单线程节点（`workers=1`）中执行。

---

## 完整示例

```python
import rivus

@rivus.node
def step_a(ctx: rivus.Context):
    item = ctx.require("input")       # 读取输入（必须存在）
    config = ctx.get("config", {})    # 读取初始配置（可选）

    result = process(item, config)
    ctx.set("step_a_done", True)      # 写入元数据，供后续节点读取
    return result

@rivus.node
def step_b(ctx: rivus.Context):
    result = ctx.require("input")     # 上游返回值
    done = ctx.get("step_a_done")     # 读取上游写入的元数据（从 root 查找）
    assert done is True

    return f"final: {result}"

pipeline = rivus.Pipeline("demo", initial={"config": {"mode": "fast"}})
pipeline.add_nodes(step_a, step_b)

results = pipeline.run("test")
print(results)  # ['final: ...']
```

---

## API 速查

| 方法/属性 | 签名 | 说明 |
|-----------|------|------|
| `get` | `(key, default=None) → Any` | 安全读取，自动向 root 查找 |
| `require` | `(key) → Any` | 强制读取，不存在抛 `KeyError` |
| `__getitem__` | `[key] → Any` | 等价于 `require(key)` |
| `set` | `(key, value) → None` | 写入属性 |
| `update` | `(data: dict) → None` | 批量写入 |
| `delete` | `(key) → None` | 删除属性 |
| `__setitem__` | `[key] = value` | 等价于 `set(key, value)` |
| `cancel` | `() → None` | 注入取消信号 |
| `need_stop` | `→ bool` | 是否处于停止状态 |
| `derive` | `(**kwargs) → Context` | 创建继承当前 Context 的子 Context |
| `input` | `Any` | 当前节点的输入数据 |
| `error` | `Exception \| None` | 错误/停止信号 |
