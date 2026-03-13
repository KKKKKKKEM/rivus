# 高级使用模式

本文档收录 Rivus 的高级使用模式，涵盖数据流控制、并发调优、生命周期管理等场景。

---

## 1. 扇出（Fan-out）

### 基础扇出

一个节点产生多个输出，下游并发处理：

```python
import rivus

@rivus.node
def split_batch(ctx: rivus.Context):
    """将一个批次拆分为独立 item"""
    batch = ctx.require("input")  # 输入：[item1, item2, ...]
    for item in batch:
        yield item

@rivus.node(workers=4)
def process_item(ctx: rivus.Context):
    item = ctx.require("input")
    return expensive_operation(item)

pipeline = rivus.Pipeline("fan-out") | split_batch | process_item

# 输入 1 个列表，输出多个结果
results = pipeline.run([1, 2, 3, 4, 5])
print(len(results))  # 5（顺序不保证）
```

### 多级扇出

```python
@rivus.node
def level1(ctx: rivus.Context):
    for category in ctx.require("input"):
        yield category  # 5 个类别

@rivus.node
def level2(ctx: rivus.Context):
    category = ctx.require("input")
    for item in get_items(category):
        yield item      # 每个类别下 N 个 item

@rivus.node(workers=8)
def process(ctx: rivus.Context):
    return transform(ctx.require("input"))

# 1 个输入 → 5 × N 个输出，全程并发
pipeline = rivus.Pipeline("multi-fan-out") | level1 | level2 | process
```

---

## 2. 数据过滤（SKIP 信号）

### 基础过滤

```python
from rivus import signals

@rivus.node
def validate(ctx: rivus.Context):
    item = ctx.require("input")
    if not is_valid(item):
        raise signals.SKIP  # 丢弃，不传下游
    return item

@rivus.node
def filter_empty(ctx: rivus.Context):
    item = ctx.require("input")
    if item is None or item == "":
        raise signals.SKIP
    return item.strip()
```

### 条件路由（过滤 + 多管道）

由于 Rivus 是线性管道，条件路由可通过过滤器配合多条 Pipeline 实现：

```python
from rivus import signals

def make_type_filter(target_type: str):
    """生成只处理特定类型的过滤节点"""
    @rivus.node(name=f"filter-{target_type}")
    def _filter(ctx: rivus.Context):
        item = ctx.require("input")
        if item.get("type") != target_type:
            raise signals.SKIP
        return item
    return _filter

# 两条专用管道
text_pipeline = rivus.Pipeline("text") | make_type_filter("text") | process_text
image_pipeline = rivus.Pipeline("image") | make_type_filter("image") | process_image

# 根据 item 类型路由
def route(item):
    if item.get("type") == "text":
        return text_pipeline.run(item)
    return image_pipeline.run(item)
```

---

## 3. 优雅停止

### STOP 信号（节点主动停止）

```python
from rivus import signals

@rivus.node
def poison_pill_detector(ctx: rivus.Context):
    item = ctx.require("input")
    if item == "STOP":
        raise signals.STOP  # 终止整条流水线
    return item

pipeline = rivus.Pipeline("demo") | poison_pill_detector | process
pipeline.run("item1")   # 正常处理
pipeline.run("STOP")    # 触发停止（注意：run() 仍会返回，但流水线不再继续）
```

### ctx.cancel()（外部取消）

适用于 HTTP 服务层或超时场景：

```python
import threading
import rivus

@rivus.node(workers=4)
def slow_step(ctx: rivus.Context):
    item = ctx.require("input")
    time.sleep(10)  # 模拟慢操作
    return item

pipeline = rivus.Pipeline("demo") | slow_step
ctx = pipeline.make_context(input="data")

# 在另一个线程中取消
def cancel_after(seconds):
    time.sleep(seconds)
    ctx.cancel()

threading.Thread(target=cancel_after, args=(2,), daemon=True).start()

# run() 接受已构造的 Context
results = pipeline.run(ctx)  # 2 秒后被取消
```

### 超时控制

```python
from rivus.node import Node  # TimeoutError

try:
    results = pipeline.run("data", timeout=30.0)
except TimeoutError:
    print("Pipeline timed out!")
```

---

## 4. Gather 屏障（汇聚模式）

> **注意**：当前版本（0.4.0）的 `@node` 装饰器不直接支持 `gather=True` 参数（README 中提及，但实现中需手动实现 gather 逻辑）。以下展示如何用框架现有能力模拟 gather 行为。

### 手动 Gather 模式

使用 `stream()` 收集所有上游结果，再统一处理：

```python
import rivus

@rivus.node(workers=4)
def scatter(ctx: rivus.Context):
    """并发处理，每条独立输出"""
    n = ctx.require("input")
    return n * n

# 先跑 scatter 阶段
scatter_pipeline = rivus.Pipeline("scatter") | scatter

# 收集所有结果
def scatter_then_reduce(items: list, reduce_fn):
    scatter_results = []
    for item in items:
        results = scatter_pipeline.run(item)
        scatter_results.extend(results)
    return reduce_fn(scatter_results)

# 使用
total = scatter_then_reduce([1, 2, 3, 4, 5], sum)
print(total)  # 1+4+9+16+25 = 55
```

### 使用扇出 + 最终节点汇聚

```python
@rivus.node
def split(ctx: rivus.Context):
    for item in ctx.require("input"):
        yield item

@rivus.node(workers=4)
def transform(ctx: rivus.Context):
    return process(ctx.require("input"))

# 注意：所有结果由 pipeline.run() 统一收集后返回
pipeline = rivus.Pipeline("scatter-gather") | split | transform
results = pipeline.run([1, 2, 3, 4, 5])
total = sum(results)  # 在 run() 返回后汇聚
```

---

## 5. 并发调优

### 节点并发度设置原则

```
                    节点类型
┌─────────────────┬──────────────────────────────────────┐
│ I/O 密集型      │ workers = CPU 核数 × 2 ~ 10          │
│（网络、数据库）  │ 示例：@node(workers=8)               │
├─────────────────┼──────────────────────────────────────┤
│ CPU 密集型      │ workers = CPU 核数（超了反而更慢）    │
│（计算、加密）   │ 示例：@node(workers=4)               │
├─────────────────┼──────────────────────────────────────┤
│ 顺序敏感型      │ workers = 1（默认）                  │
│（写日志、排序） │ 示例：@node                          │
└─────────────────┴──────────────────────────────────────┘
```

### 瓶颈节点识别

通过在节点内记录耗时，找出瓶颈：

```python
import time
import rivus

@rivus.node(workers=4)
def maybe_bottleneck(ctx: rivus.Context):
    t0 = time.time()
    result = slow_operation(ctx.require("input"))
    elapsed = time.time() - t0
    if elapsed > 1.0:
        print(f"SLOW: {elapsed:.2f}s")  # 找出慢项目
    return result
```

### 背压控制（queue_size）

当下游处理速度慢于上游时，使用 `queue_size` 限制队列长度，阻塞上游避免内存爆炸：

```python
@rivus.node(workers=1)           # 快速生产者
def producer(ctx: rivus.Context):
    for chunk in read_large_file(ctx.require("input")):
        yield chunk

@rivus.node(workers=2, queue_size=50)  # 慢速消费者，队列满时上游阻塞
def consumer(ctx: rivus.Context):
    return upload_to_s3(ctx.require("input"))

pipeline = rivus.Pipeline("pipeline") | producer | consumer
```

### Pipeline 并发限制

`max_workers` 控制同时运行的 `run()` 调用数量：

```python
# 适合高 QPS HTTP 服务场景
pipeline = rivus.Pipeline("api", max_workers=50)

# CPU 密集型场景，限制并发避免过载
pipeline = rivus.Pipeline("cpu-heavy", max_workers=4)

# 无限制（适合控制量很小的场景）
pipeline = rivus.Pipeline("unlimited", max_workers=None)
```

---

## 6. 生命周期钩子高级用法

### 连接池模式

```python
import rivus
from contextlib import contextmanager

pipeline = rivus.Pipeline("db-pipeline", max_workers=20)

# startup：初始化连接池（只执行一次）
@pipeline.on_startup
def init_pool(p: rivus.Pipeline):
    pool = create_connection_pool(max_size=20)
    p.set("db_pool", pool)

# run_start：从连接池借用连接
@pipeline.on_run_start
def acquire_conn(ctx: rivus.Context):
    pool = ctx.get("pipeline").get("db_pool")
    conn = pool.acquire()
    ctx.set("db", conn)

# run_end：归还连接到池
@pipeline.on_run_end
def release_conn(ctx: rivus.Context):
    pool = ctx.get("pipeline").get("db_pool")
    conn = ctx.get("db")
    if conn:
        pool.release(conn)

@rivus.node
def query(ctx: rivus.Context):
    db = ctx.require("db")
    return db.query(ctx.require("input"))

pipeline.add_node(query)
```

### 分布式追踪

```python
import uuid
import time

@pipeline.on_run_start
def inject_trace(ctx: rivus.Context):
    ctx.set("trace_id", str(uuid.uuid4()))
    ctx.set("start_time", time.perf_counter())

@pipeline.on_run_end
def record_trace(ctx: rivus.Context):
    trace_id = ctx.get("trace_id", "?")
    elapsed = time.perf_counter() - ctx.get("start_time", 0)
    send_to_jaeger(trace_id=trace_id, duration_ms=elapsed * 1000)
```

### 错误监控

```python
@pipeline.on_run_end
def error_monitor(ctx: rivus.Context):
    if ctx.error:
        send_alert(
            title="Pipeline error",
            message=str(ctx.error),
            pipeline=ctx.get("pipeline").name,
        )
```

---

## 7. 自定义结果提取

通过 `on_result` 参数控制 `run()` 返回的内容：

```python
# 默认：返回 [ctx.input for ctx in results]
pipeline = rivus.Pipeline("demo")

# 返回完整 Context（适合需要访问元数据的场景）
pipeline = rivus.Pipeline(
    "demo",
    on_result=lambda p, results, ctx: list(results)
)

# 过滤错误并提取 input
pipeline = rivus.Pipeline(
    "demo",
    on_result=lambda p, results, ctx: [
        r.input for r in results if r.error is None
    ]
)

# 自定义聚合
pipeline = rivus.Pipeline(
    "sum-pipeline",
    on_result=lambda p, results, ctx: [sum(r.input for r in results)]
)
```

---

## 8. 流水线组合

### 串行组合（顺序执行多条流水线）

```python
import rivus

pipeline_a = rivus.Pipeline("a") | step_a1 | step_a2
pipeline_b = rivus.Pipeline("b") | step_b1 | step_b2

# 手动串联
items = ["item1", "item2", "item3"]
intermediate = []
for item in items:
    intermediate.extend(pipeline_a.run(item))

final = []
for item in intermediate:
    final.extend(pipeline_b.run(item))
```

### 并行处理同一批数据

```python
import concurrent.futures
import rivus

pipeline = rivus.Pipeline("parallel-runs", max_workers=10) | heavy_step

items = list(range(100))

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(pipeline.run, item): item for item in items}
    results = []
    for future in concurrent.futures.as_completed(futures):
        results.extend(future.result())
```

---

## 9. 流式处理大数据集

```python
import rivus

@rivus.node
def read_chunk(ctx: rivus.Context):
    """从文件读取一行"""
    line = ctx.require("input")
    return line.strip()

@rivus.node(workers=4)
def process_line(ctx: rivus.Context):
    return transform(ctx.require("input"))

pipeline = rivus.Pipeline("stream-proc") | read_chunk | process_line

# 流式处理，内存占用低
with open("large_file.txt") as f:
    for line in f:
        for ctx in pipeline.stream(line):
            write_output(ctx.input)
```

---

## 10. 调试技巧

### 节点日志

```python
import rivus
from loguru import logger

@rivus.node(name="debug-step")
def debug_step(ctx: rivus.Context):
    item = ctx.require("input")
    logger.debug(f"[{ctx._id}] Processing: {item!r}")
    result = process(item)
    logger.debug(f"[{ctx._id}] Result: {result!r}")
    return result
```

### 单节点测试

```python
import rivus
from rivus.context import Context

# 直接测试节点函数，无需 Pipeline
@rivus.node
def my_step(ctx: rivus.Context):
    return ctx.require("input").upper()

# 手动构造 Context 测试
ctx = Context(initial={"input": "hello"})
result = my_step.fn(ctx)
assert result == "HELLO"
```

### 检查中间结果

```python
import rivus

results_at_step2 = []

@rivus.node
def checkpoint(ctx: rivus.Context):
    """无侵入检查点，记录中间结果"""
    item = ctx.require("input")
    results_at_step2.append(item)   # 记录
    return None                     # pass-through

pipeline = (
    rivus.Pipeline("debug")
    | step1
    | checkpoint   # 插入检查点
    | step2
    | step3
)
pipeline.run("test")
print("After step1:", results_at_step2)
```
