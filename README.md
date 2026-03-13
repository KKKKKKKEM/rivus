# Rivus

**Rivus** 是一个基于 Python `queue.Queue` 的轻量级数据流水线（Pipeline）框架，专为并发批处理、流式处理及多阶段数据转换场景设计。

```
items → wrap(ctx) → Q → [Node A, workers=W] → Q → [Node B, workers=W] → Q → results
```

## 特性

- **零依赖** — 仅使用 Python 标准库
- **简洁 API** — `@node` 装饰器 + 管道符 `|` 组合节点
- **多并发模型** — 每个节点独立配置线程池或进程池
- **生成器支持** — 节点可 `yield` 多个结果，实现一对多扇出
- **gather 屏障** — `gather=True` 将上游所有输出汇聚后再处理
- **优雅停止** — 节点内调用 `ctx.stop()` 可安全终止流水线
- **结构化报告** — 含耗时统计（min/max/avg/p50/p95）的 `PipelineReport`
- **RunStorage** — 每次 `run()` 自动分配 `task_id`，所有历史运行状态可查可管理

## 安装

```bash
pip install -U git+https://github.com/KKKKKKKEM/rivus.git
```

## 快速上手

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

report = (rivus.Pipeline("demo") | parse | process).run("hello", "world")
print(report.results)
# ['processed: HELLO', 'processed: WORLD']
print(report.status)
# 'success'
```

## 核心概念

### Context

贯穿整条 Pipeline 的线程安全状态容器，携带当前 item 及共享元数据。

```python
@rivus.node
def my_node(ctx: rivus.Context):
    item = ctx.require("input")       # 读取当前 item（不存在则抛异常）
    value = ctx.get("key", default)   # 读取共享元数据
    ctx.set("key", value)             # 写入共享元数据
    return result
```

### Node 定义方式

**函数式（推荐）**

```python
@rivus.node
def step(ctx: rivus.Context):
    ...

@rivus.node(workers=4, concurrency_type="thread")  # concurrency_type="process" 亦可
def heavy_step(ctx: rivus.Context):
    ...
```

**类式**

```python
class MyNode(rivus.BaseNode):
    def process(self, ctx: rivus.Context):
        ...
```

### 生成器 / 扇出

```python
@rivus.node
def expand(ctx: rivus.Context):
    batch = ctx.require("input")
    for item in batch:
        yield item          # 每个 yield 产生独立的下游 item
```

### Gather 屏障

```python
@rivus.node(gather=True)    # 等待所有上游 item 到齐，合并成列表后整体处理
def aggregate(ctx: rivus.Context):
    all_items = ctx.require("input")  # list
    return sum(all_items)
```

### 优雅停止

```python
@rivus.node
def guard(ctx: rivus.Context):
    item = ctx.require("input")
    if item == "STOP":
        ctx.stop("received stop signal")   # 触发 PipelineStop，状态为 STOPPED
    return item
```

### 多进程节点

```python
@rivus.node(workers=4, concurrency_type="process")
def cpu_bound(ctx: rivus.Context):
    data = ctx.require("input")
    return heavy_computation(data)   # fn 必须可 pickle
```

## API 参考

| 类 / 函数              | 说明                                             |
| ---------------------- | ------------------------------------------------ |
| `@node` / `node(...)`  | 将函数包装为节点                                 |
| `BaseNode`             | 类式节点基类，重写 `process(ctx)`                |
| `Pipeline(name)`       | 创建流水线                                       |
| `Pipeline \| node`     | 添加节点                                         |
| `Pipeline.run(*items)` | 同步运行，返回 `PipelineReport`                  |
| `Pipeline.storage`     | `RunStorage` 实例，管理所有历史运行状态          |
| `Context`              | item + 共享元数据容器                            |
| `PipelineReport`       | 运行报告（含 `task_id`、状态、结果、错误、耗时） |
| `NodeReport`           | 单节点统计（处理数、错误数、耗时分布）           |
| `RunStorage`           | 历史运行状态管理接口（`get` / `list` / `clear`） |
| `RunStorageBackend`    | 可插拔存储后端抽象基类                           |
| `InMemoryStorage`      | 默认 LRU 内存后端（`max_runs=100`）              |

详细文档见 [docs/](docs/)。

## RunStorage — 历史运行管理

每次 `run()` / `run_background()` / `start()` 都会生成唯一的 `task_id`（UUID4），运行状态和报告自动存入 `pipeline.storage`：

```python
report1 = pipeline.run("hello")
report2 = pipeline.run("world")

# 每个报告携带唯一 task_id
print(report1.task_id)   # 'a1b2c3d4-...'
print(report2.task_id)   # 'e5f6g7h8-...'

# 列出所有历史 task_id（oldest → newest）
for rid in pipeline.storage.list():
    state = pipeline.storage.get(rid)
    print(rid, state.report.status)

# 按 task_id 查询
state = pipeline.storage[report1.task_id]
print(state.report.results)

# 自定义存储后端（如 SQLite、Redis）
class MyBackend(rivus.RunStorageBackend):
    def save(self, task_id, state): ...
    def get(self, task_id): ...
    def list(self): ...
    def clear(self): ...

pipeline = rivus.Pipeline("demo", storage=MyBackend())
```

## 示例

完整示例见 [examples/demo.py](examples/demo.py)。

## License

MIT
