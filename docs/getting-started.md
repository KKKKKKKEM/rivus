# 快速上手

本文档帮助你在 5 分钟内运行第一个 Rivus 流水线。

---

## 安装

```bash
pip install -U git+https://github.com/KKKKKKKEM/rivus.git
```

**系统要求**：Python `>= 3.10`

**可选依赖**（HTTP 服务层）：

```bash
pip install sanic loguru  # 启用 pipeline.serve() 功能
```

---

## 第一个流水线

### 最简示例

```python
import rivus

@rivus.node
def greet(ctx: rivus.Context):
    name = ctx.require("input")
    return f"Hello, {name}!"

pipeline = rivus.Pipeline("hello")
pipeline.add_node(greet)

results = pipeline.run("Alice")
print(results)  # ['Hello, Alice!']
```

### 多节点链式处理

```python
import rivus

@rivus.node
def normalize(ctx: rivus.Context):
    text = ctx.require("input")
    return text.strip().lower()

@rivus.node(workers=2)
def transform(ctx: rivus.Context):
    text = ctx.require("input")
    return text.replace(" ", "_")

# 用管道符 | 组合节点
pipeline = rivus.Pipeline("text-proc") | normalize | transform

results = pipeline.run("  Hello World  ")
print(results)  # ['hello_world']
```

---

## 核心概念

### Context — 状态容器

`Context` 是贯穿整条流水线的数据载体。每当一个节点返回值，框架自动调用 `ctx.derive(input=value)` 生成子 Context 传递给下一个节点。

```python
@rivus.node
def my_node(ctx: rivus.Context):
    # 读取当前处理的数据（必须存在，否则抛 KeyError）
    item = ctx.require("input")

    # 读取共享元数据（可提供默认值）
    db = ctx.get("db_conn", None)

    # 写入共享元数据（对同一 root context 下的子 context 可见）
    ctx.set("processed", True)

    return item.upper()
```

**Context 继承链**：

```
make_context(input="hello")      ← 根 Context（含 pipeline 引用）
      │
   Node 0 fn(ctx) → "HELLO"
      │
ctx.derive(input="HELLO")        ← 子 Context，root → 根 Context
      │
   Node 1 fn(ctx) → "processed: HELLO"
      │
ctx.derive(input="processed: HELLO")
```

子 Context 调用 `ctx.get("key")` 时，若本级没有，会自动向上查找 root Context，因此 `pipeline.run(item, initial={...})` 设置的初始值在所有节点都可访问。

---

### Node — 处理单元

每个节点是一个接收 `ctx` 的函数，返回值自动流向下游。

#### 函数式节点（推荐）

```python
# 简单节点
@rivus.node
def step(ctx: rivus.Context):
    return ctx.require("input") * 2

# 配置并发度
@rivus.node(workers=4)
def parallel_step(ctx: rivus.Context):
    return heavy_work(ctx.require("input"))

# 命名节点（调试用）
@rivus.node(name="my-step", workers=2)
def named_step(ctx: rivus.Context):
    ...
```

#### 节点返回值约定

| 返回值 | 行为 |
|--------|------|
| 普通值 | 调用 `ctx.derive(input=value)` 传给下游 |
| `None` | 原样传递当前 `ctx`（pass-through，适合副作用节点） |
| `Context` 实例 | 直接传给下游（完全手动控制） |
| `yield value` | 每个 yield 生成一个独立下游 item（扇出） |

#### Pass-through 节点（副作用）

```python
@rivus.node
def log_step(ctx: rivus.Context):
    print(f"Processing: {ctx.require('input')}")
    # 返回 None → 当前 ctx 原样继续向下传
```

---

### Pipeline — 流水线

`Pipeline` 负责组装节点、管理并发、运行数据流。

#### 创建与组装

```python
# 方式一：管道符（推荐）
pipeline = rivus.Pipeline("demo") | step1 | step2 | step3

# 方式二：add_node / add_nodes
pipeline = rivus.Pipeline("demo")
pipeline.add_nodes(step1, step2, step3)

# 带初始共享数据
pipeline = rivus.Pipeline("demo", initial={"db": db_conn, "config": cfg})
```

#### 运行

```python
# 同步运行，返回结果列表
results = pipeline.run("input_item")

# 带超时（秒）
results = pipeline.run("input_item", timeout=30.0)

# 流式输出（逐个消费）
for ctx in pipeline.stream("input_item"):
    print(ctx.input)
```

#### 并发控制

`Pipeline` 的 `max_workers` 参数限制同时运行的 `run()` 调用数量（信号量），防止过载：

```python
# 最多允许 5 个 run() 同时执行（默认 10）
pipeline = rivus.Pipeline("demo", max_workers=5) | step1 | step2
```

---

### 生成器扇出（Fan-out）

节点用 `yield` 实现一对多输出：

```python
@rivus.node
def split(ctx: rivus.Context):
    batch = ctx.require("input")  # 输入一个列表
    for item in batch:
        yield item  # 每个 yield 生成独立的下游 item

@rivus.node(workers=4)
def process(ctx: rivus.Context):
    return ctx.require("input") * 2

pipeline = rivus.Pipeline("fan-out") | split | process
results = pipeline.run([1, 2, 3, 4, 5])
print(results)  # [2, 4, 6, 8, 10]（顺序不保证）
```

---

### 初始共享数据

通过 `initial` 注入全局共享数据（如数据库连接、配置），所有节点均可访问：

```python
import rivus

@rivus.node
def fetch(ctx: rivus.Context):
    db = ctx.get("db")       # 从初始数据读取
    item_id = ctx.require("input")
    return db.get(item_id)

pipeline = rivus.Pipeline("fetch", initial={"db": my_database})
results = pipeline.run(42)
```

---

### 控制流信号

```python
import rivus
from rivus import signals

@rivus.node
def filter_node(ctx: rivus.Context):
    item = ctx.require("input")

    if item is None:
        raise signals.SKIP   # 跳过本条，不向下游传递

    if item == "POISON":
        raise signals.STOP   # 终止整条流水线

    return item
```

| 信号 | 行为 |
|------|------|
| `signals.SKIP` | 丢弃当前 item，不传给下游 |
| `signals.STOP` | 立即终止整条流水线 |
| `ctx.cancel()` | 通过 Context 注入取消信号 |

---

## 完整示例

```python
import time
import rivus
from rivus import signals

# ── 节点定义 ──────────────────────────────────────────────────────────

@rivus.node
def validate(ctx: rivus.Context):
    """校验并过滤无效输入"""
    item = ctx.require("input")
    if not isinstance(item, str) or not item.strip():
        raise signals.SKIP
    return item.strip()

@rivus.node
def split_words(ctx: rivus.Context):
    """将句子拆分为单词（扇出）"""
    sentence = ctx.require("input")
    for word in sentence.split():
        yield word

@rivus.node(workers=4)
def process_word(ctx: rivus.Context):
    """并发处理每个单词"""
    word = ctx.require("input")
    time.sleep(0.01)  # 模拟 I/O
    return word.upper()

@rivus.node
def collect(ctx: rivus.Context):
    """收集结果（pass-through）"""
    result = ctx.require("input")
    print(f"Got: {result}")
    # 返回 None → pass-through，ctx.input 仍是 result

# ── 组装并运行 ────────────────────────────────────────────────────────

pipeline = (
    rivus.Pipeline("word-processor")
    | validate
    | split_words
    | process_word
    | collect
)

results = pipeline.run("hello world foo bar")
print(f"Total words: {len(results)}")
# Got: HELLO
# Got: WORLD
# Got: FOO
# Got: BAR
# Total words: 4
```

---

## 下一步

- [Context API 参考](./core/context.md) — 完整的状态容器文档
- [Node API 参考](./core/node.md) — 节点配置与信号机制
- [Pipeline API 参考](./core/pipeline.md) — 流水线高级配置
- [高级模式](./guides/patterns.md) — Gather、并发调优、生命周期钩子
- [HTTP 服务](./serve/overview.md) — 将流水线暴露为 REST API
