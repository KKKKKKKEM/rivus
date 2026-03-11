# Node / @node / BaseNode — 节点定义

节点（Node）是流水线的最小处理单元。Rivus 提供三种定义节点的方式：

| 方式 | 适用场景 |
|------|----------|
| `@node` 装饰器 | 函数式，最简洁 |
| `Node(fn, ...)` | 直接构造，适合组合/动态创建 |
| 继承 `BaseNode` | 类式，需要 `setup` 初始化或持有状态 |

**源码**：`rivus/node.py`

---

## 节点函数签名

所有节点函数均遵循统一签名：

```python
def fn(ctx: Context) -> Any
```

- **输入**：只接受一个 `ctx: Context` 参数
- **输出**约定见下表：

| 返回值类型 | 框架行为 |
|-----------|---------|
| 普通值（包括 `dict`, `str`, `int` 等） | 框架以 `ctx.derive(input=value)` 包装后传下游 |
| `None` | 当前 ctx 原样传下游（pass-through / 副作用节点） |
| `Context` 实例 | 直接传下游（用户手动控制派生） |
| `yield` 多个值 | 每个 yield 产生一个独立的下游 item（fan-out，一进多出） |

---

## `@node` 装饰器

```python
from rivus import node, Context

@node
def preprocess(ctx: Context):
    ...

@node(workers=4, name="Inference")
def inference(ctx: Context):
    ...
```

### 参数

```python
@node(
    fn=None,
    *,
    name: str | None = None,
    workers: int = 1,
    queue_size: int = 0,
    setup: Callable[[Context], None] | None = None,
    concurrency_type: str = "thread",
    gather: bool = False,
)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `name` | `str` \| `None` | 函数名 | 节点名称，用于日志和报告 |
| `workers` | `int` | `1` | 并发 worker 数量；`1` 为串行，`N>1` 为并发 |
| `queue_size` | `int` | `0` | 输入队列最大容量；`0` 表示无限（不背压） |
| `setup` | `Callable` \| `None` | `None` | 初始化函数，Pipeline 启动时以 root ctx 调用一次 |
| `concurrency_type` | `str` | `"thread"` | `"thread"` 或 `"process"`，见[并发模式](#并发模式) |
| `gather` | `bool` | `False` | 若为 `True`，本节点前插入同步屏障，见[Gather 模式](#gather-模式) |

### 用法示例

**用法一：无参数直接装饰**

```python
@node
def preprocess(ctx: Context):
    return transform(ctx.require("input"))
```

**用法二：带参数**

```python
@node(workers=4, name="Inference")
def inference(ctx: Context):
    return ctx.require("model").predict(ctx.require("input"))
```

**用法三：生成器 fan-out**

```python
@node(workers=1, name="ChunkDoc")
def chunk_doc(ctx: Context):
    for chunk in split(ctx.require("input")):
        yield chunk   # 一篇文章 → 多个 chunk
```

**用法四：带初始化函数**

```python
def load_model(ctx: Context):
    ctx.set("model", Model.load(ctx.require("model_path")))

@node(workers=4, setup=load_model)
def infer(ctx: Context):
    return ctx.require("model").predict(ctx.require("input"))
```

**用法五：gather 模式**

```python
@node(workers=4)
def scatter(ctx: Context):
    return process(ctx.require("input"))

@node(gather=True)          # 等 scatter 全部完成后触发，收到 list
def reduce(ctx: Context):
    results = ctx.require("input")   # list
    return merge(results)
```

---

## `Node` 类

`@node` 装饰器的底层类型，可直接构造：

```python
from rivus import Node, Context

def my_fn(ctx: Context):
    return ctx.require("input") * 2

my_node = Node(
    fn=my_fn,
    name="Double",
    workers=4,
    queue_size=100,
    setup_fn=None,
    concurrency_type="thread",
    gather=False,
)
```

### 构造参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `fn` | `Callable[[Context], Any]` | 必填 | 处理函数 |
| `name` | `str` \| `None` | `None` → 取 `fn.__name__` | 节点名称 |
| `workers` | `int` | `1` | 并发 worker 数量 |
| `queue_size` | `int` | `0` | 输入队列容量 |
| `setup_fn` | `Callable[[Context], None]` \| `None` | `None` | 初始化函数 |
| `concurrency_type` | `str` | `"thread"` | `"thread"` 或 `"process"` |
| `gather` | `bool` | `False` | 是否采用 gather 模式 |

### 属性

| 属性 | 类型 | 说明 |
|------|------|------|
| `fn` | `Callable` | 处理函数 |
| `name` | `str` | 节点名称 |
| `workers` | `int` | 并发度 |
| `queue_size` | `int` | 队列容量 |
| `setup_fn` | `Callable` \| `None` | 初始化函数 |
| `concurrency_type` | `str` | 并发类型 |
| `gather` | `bool` | gather 模式标志 |
| `is_generator` | `bool` | 处理函数是否为生成器（框架自动检测） |

---

## `BaseNode` 类

类式节点基类，继承并实现 `process` 即可。

```python
from rivus import BaseNode, Context

class Preprocess(BaseNode):
    workers = 4
    name = "Preprocess"              # 可选；缺省取类名
    queue_size = 0                   # 可选
    concurrency_type = "thread"      # 可选
    gather = False                   # 可选

    def setup(self, ctx: Context) -> None:
        # 流水线启动时调用一次，可加载模型等
        self.model = ctx.require("model")

    def process(self, ctx: Context) -> dict:
        image = ctx.require("input")
        return self.model.transform(image)
```

### 类属性

| 属性 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `name` | `str` | `""` → 取类名 | 节点名称 |
| `workers` | `int` | `1` | 并发 worker 数量 |
| `queue_size` | `int` | `0` | 输入队列容量 |
| `concurrency_type` | `str` | `"thread"` | 并发类型 |
| `gather` | `bool` | `False` | 是否采用 gather 模式 |

### 方法

#### `setup(ctx: Context) → None`

可选重写。Pipeline 启动时以 **root Context** 调用一次，适合加载模型、建立数据库连接等重型初始化。

```python
def setup(self, ctx: Context) -> None:
    self.tokenizer = load_tokenizer(ctx.require("model_name"))
    self.model = load_model(ctx.require("model_name"))
```

#### `process(ctx: Context) → Any`

**必须实现**。处理单个 item 的核心逻辑。通过 `ctx.require("input")` 获取当前 item，返回值规则与普通节点函数相同（支持 `yield` fan-out）。

```python
def process(self, ctx: Context) -> str:
    text = ctx.require("input")
    return self.tokenizer.decode(self.model(text))
```

### 支持生成器（fan-out）

```python
class ChunkDoc(BaseNode):
    name = "chunk"

    def process(self, ctx: Context):
        doc = ctx.require("input")
        for chunk in doc.split("\n\n"):
            yield chunk   # fan-out
```

### 支持 gather 模式

```python
class Aggregate(BaseNode):
    gather = True   # 等上游全量结果，合并后处理

    def process(self, ctx: Context):
        chunks = ctx.require("input")   # list
        return "\n".join(chunks)
```

---

## Gather 模式

在节点上设置 `gather=True` 时，Pipeline 会在该节点前插入一道**同步屏障**（barrier）：

1. 等待上游所有 item 全部处理完毕
2. 将每个 item 的 `ctx.get("input")` 值收集为一个 `list`
3. 以这个 `list` 作为 `input` 创建单个 item，传入 gather 节点

```
上游 [item1, item2, item3, item4] → 并发处理 → barrier → [result1, result2, result3, result4]（列表）→ gather 节点
```

**适用场景**：reduce、排序、合并写入数据库、计算统计信息等需要全量上游结果的操作。

```python
@node(workers=4)
def embed(ctx: Context):
    return compute_embedding(ctx.require("input"))

@node(gather=True)
def store(ctx: Context):
    embeddings = ctx.require("input")   # list of all embeddings
    database.batch_insert(embeddings)
    return len(embeddings)
```

---

## 并发模式

### `concurrency_type="thread"`（默认）

使用 `threading.Thread` 实现并发，适合 **I/O 密集型**任务（网络请求、文件读写、数据库查询等）。线程间共享内存，`Context` 对象直接传递。

```python
@node(workers=8, concurrency_type="thread")
def fetch(ctx: Context):
    return requests.get(ctx.require("input")).json()
```

### `concurrency_type="process"`

使用 `concurrent.futures.ProcessPoolExecutor` + 单协调线程实现并发，适合 **CPU 密集型**任务（图像处理、数值计算、模型推理等）。

**限制**：
- 节点函数 `fn` 必须可 pickle（模块级函数或可 pickle 的可调用对象）
- `Context` 以 `dict` 形式序列化传递（自动处理）
- `setup_fn` 在主进程运行，不会传入子进程

```python
import rivus

def cpu_heavy(ctx: rivus.Context):
    data = ctx.require("input")
    return heavy_computation(data)   # CPU 密集计算

heavy_node = rivus.Node(cpu_heavy, name="compute", workers=4, concurrency_type="process")
```

> **Tip**：进程模式下，Lambda 和闭包通常不可 pickle，请使用模块级函数。

---

## 队列背压（queue_size）

`queue_size > 0` 时，当输入队列满时上游 worker 会阻塞等待（背压），防止内存溢出：

```python
# 若上游产出速度远快于本节点，设置 queue_size 防止 OOM
@node(workers=2, queue_size=100)
def slow_writer(ctx: Context):
    write_to_db(ctx.require("input"))
```

`queue_size=0`（默认）表示无限队列，不做背压控制。
