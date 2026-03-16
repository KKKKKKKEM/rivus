# Rivus

**Rivus** 是一个基于 Python `queue.Queue` 的轻量级数据流水线（Pipeline）框架，专为并发批处理、流式处理及多阶段数据转换场景设计。

```
items → wrap(ctx) → Q → [Node A, workers=W] → Q → [Node B, workers=W] → Q → results
```

## 特性

- **简洁 API** — `@node` 装饰器 + `add_node()` / `add_nodes()` 组合节点
- **多线程并发** — 每个节点独立配置工作线程数（`workers`）
- **生成器支持** — 节点可 `yield` 多个结果，实现一对多扇出
- **流式输出** — `pipeline.stream(item)` 逐条消费结果，无需等待全部完成
- **优雅取消** — 调用 `ctx.cancel()` 可安全终止整条流水线
- **生命周期钩子** — `on_startup` / `on_run_start` / `on_run_end`
- **并发限流** — `max_workers` 控制同时运行的最大 run 数
- **HTTP 服务** — 内置 `pipeline.serve()` 开启 RESTful 任务接口

## 安装

```bash
pip install -U git+http://rdgit.300624.cn/wankai20/rivus
```

## 快速上手

```python
import rivus

@rivus.node
def parse(ctx: rivus.Context):
    raw = ctx.input
    return raw.strip().upper()

@rivus.node(workers=4)
def process(ctx: rivus.Context):
    text = ctx.input
    return f"processed: {text}"

pipeline = rivus.Pipeline("demo")
pipeline.add_nodes(parse, process)

results = pipeline.run("hello")
print(results)  # ['processed: HELLO']
```

## 核心概念

### Context

贯穿整条 Pipeline 的线程安全状态容器，携带当前 item 及共享元数据。每个节点收到的 `ctx` 是上游派生出的子 Context，`ctx.input` 始终是上游节点的返回值。

```python
@rivus.node
def my_node(ctx: rivus.Context):
    item = ctx.input              # 当前 item（上游返回值）
    item = ctx.require("input")   # 等价写法，key 不存在则抛 KeyError
    value = ctx.get("key", None)  # 读取共享元数据，不存在返回默认值
    ctx.set("key", value)         # 写入共享元数据（线程安全）
    return result
```

初始化时通过 `initial` 注入全局共享数据，所有节点均可读取：

```python
pipeline = rivus.Pipeline("demo", initial={"db": db_conn, "config": cfg})

@rivus.node
def step(ctx: rivus.Context):
    db = ctx.get("db")   # 读取全局共享数据
```

### Node 定义方式

**函数式（推荐）**

```python
@rivus.node
def step(ctx: rivus.Context):
    ...

@rivus.node(workers=4)
def heavy_step(ctx: rivus.Context):
    ...
```

**组合节点**

```python
pipeline = rivus.Pipeline("demo")
pipeline.add_node(step)           # 逐个添加
pipeline.add_nodes(step1, step2)  # 批量添加
```

### 生成器 / 扇出

节点 `yield` 多个值，每个值作为独立 item 流入下游：

```python
@rivus.node
def expand(ctx: rivus.Context):
    batch = ctx.input   # 假设上游传来一个列表
    for item in batch:
        yield item      # 每个 yield 产生一个独立的下游 item
```

### 流式输出

```python
for ctx in pipeline.stream("my_input"):
    print(ctx.input)   # 每条结果逐条输出，无需等待全部完成
```

### 优雅取消

```python
@rivus.node
def guard(ctx: rivus.Context):
    item = ctx.input
    if item == "POISON":
        ctx.cancel()   # 向所有节点广播取消信号，流水线安全终止
    return item
```

取消信号通过共享的 `threading.Event` 传播，所有子 Context 立即感知，无锁开销。

### 超时控制

```python
results = pipeline.run("my_input", timeout=30.0)  # 超过 30 秒抛 TimeoutError
```

### 生命周期钩子

```python
@pipeline.on_startup
def init(p: rivus.Pipeline):
    print("Pipeline 首次启动（只触发一次）")

@pipeline.on_run_start
def before(ctx: rivus.Context):
    print(f"开始处理: {ctx.input}")

@pipeline.on_run_end
def after(ctx: rivus.Context):
    print("处理完毕")
```

### 自定义结果收集

`on_result` 控制 `run()` 的返回值格式：

```python
pipeline = rivus.Pipeline(
    "demo",
    on_result=lambda pipeline, results, ctx: [c.input for c in results if c.error is None]
)
```

## API 参考

| 类 / 函数                                         | 说明                                |
| ------------------------------------------------- | ----------------------------------- |
| `@node` / `node(workers=N)`                       | 将函数包装为节点                    |
| `Pipeline(name, max_workers, initial, on_result)` | 创建流水线                          |
| `pipeline.add_node(node)`                         | 追加单个节点                        |
| `pipeline.add_nodes(*nodes)`                      | 追加多个节点                        |
| `pipeline.run(item, timeout)`                     | 同步运行单个 item，返回结果列表     |
| `pipeline.stream(item)`                           | 流式运行，返回 `Context` 生成器     |
| `pipeline.serve(host, port, prefix)`              | 启动 HTTP 服务（阻塞）              |
| `pipeline.set(key, value)`                        | 写入 Pipeline 级全局变量            |
| `pipeline.get(key, default)`                      | 读取 Pipeline 级全局变量            |
| `Context`                                         | item + 共享元数据容器               |
| `ctx.input`                                       | 当前 item 值                        |
| `ctx.error`                                       | 当前 item 的错误（`None` 表示正常） |
| `ctx.get(key, default)`                           | 读取元数据，自动向上查找 root       |
| `ctx.require(key)`                                | 读取元数据，不存在则抛 `KeyError`   |
| `ctx.set(key, value)`                             | 写入元数据（线程安全）              |
| `ctx.cancel()`                                    | 广播取消信号，终止整条流水线        |

## HTTP 服务

```python
pipeline.serve(host="0.0.0.0", port=8080, prefix="/api/v1")
```

启动后可通过 RESTful 接口远程提交和管理任务：

| 方法     | 路径                      | 说明                         |
| -------- | ------------------------- | ---------------------------- |
| `GET`    | `/api/v1/health`          | 心跳检测                     |
| `POST`   | `/api/v1/tasks`           | 提交任务（同步或异步）       |
| `GET`    | `/api/v1/tasks`           | 任务列表（可按 status 过滤） |
| `GET`    | `/api/v1/tasks/{task_id}` | 查询单个任务状态             |
| `DELETE` | `/api/v1/tasks/{task_id}` | 取消任务                     |
| `POST`   | `/api/v1/shutdown`        | 关闭服务                     |

提交任务请求体（JSON）：

```json
{
  "input": "your_data",
  "timeout": 30.0
}
```

异步提交时 `在 body 设置 timeout: 0`，立即返回 `task_id`，之后通过 `GET /tasks/{task_id}` 轮询结果。

## 示例

完整示例见 [example/demo1.py](example/demo1.py)。

## License

MIT
