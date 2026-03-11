# Pipeline — 流水线核心类

`Pipeline` 是 Rivus 的流水线调度核心，负责将多个 Node 串联，通过内部 `queue.Queue` 自动管理节点间的数据传递、并发调度、错误处理和生命周期管理。

**源码**：`rivus/pipeline.py`

---

## 架构模型

```
items → derive(input=item) → Q₀ → [Node₀, w=W₀] → Q₁ → [Node₁, w=W₁] → Q₂ → … → results
                                      ↑ fn(ctx) → derive(input=result)
```

- 每个节点前后各有一个 Queue，上下游通过 relay 线程连接。
- 每个节点启动 `workers` 个 worker 线程（或 1 个协调线程 + 进程池）。
- 结果收集线程负责汇总最终的 `PipelineReport`。

---

## 构造函数

```python
Pipeline(
    name: str = "pipeline",
    *,
    context: Context | None = None,
    log_config: LogConfig | RivusLogger | None = None,
    fail_fast: bool = True,
    ordered: bool = False,
)
```

| 参数         | 类型                                   | 默认值       | 说明                                       |
| ------------ | -------------------------------------- | ------------ | ------------------------------------------ |
| `name`       | `str`                                  | `"pipeline"` | 流水线名称，用于日志标识                   |
| `context`    | `Context` \| `None`                    | `None`       | 预初始化的 root Context；不传时自动创建    |
| `log_config` | `LogConfig` \| `RivusLogger` \| `None` | `None`       | 日志配置或日志器实例                       |
| `fail_fast`  | `bool`                                 | `True`       | 任意节点出错时立即停止整条流水线           |
| `ordered`    | `bool`                                 | `False`      | 结果按 item 提交顺序排列（有轻微额外开销） |

### 示例

```python
import rivus

# 最简构造
p = rivus.Pipeline("my_pipeline")

# 带日志配置
p = rivus.Pipeline(
    "classify",
    log_config=rivus.LogConfig(level="DEBUG", to_file="run.log"),
)

# 带预初始化 Context（共享模型等）
ctx = rivus.Context({"model": MyModel(), "threshold": 0.5})
p = rivus.Pipeline("infer", context=ctx)

# fail_fast=False：出错继续处理其他 item
p = rivus.Pipeline("safe", fail_fast=False)

# ordered=True：结果保持提交顺序
p = rivus.Pipeline("ordered_results", ordered=True)
```

---

## 节点注册

### `add_node(n) → Pipeline`

追加单个节点，返回 `self` 支持链式调用。接受 `Node` 或 `BaseNode` 实例。

```python
p.add_node(preprocess_node)
p.add_node(InferenceNode())
```

### `add_nodes(*nodes) → Pipeline`

追加多个节点，返回 `self`。

```python
p.add_nodes(step1, step2, step3)
```

### `__or__(n) → Pipeline`（管道符）

等价于 `add_node(n)`，支持 `|` 语法糖：

```python
p = rivus.Pipeline("demo") | node_a | node_b | NodeC()
```

> **注意**：Pipeline 运行期间不允许添加节点（`RUNNING` 状态下会抛出 `RuntimeError`）。

---

## 生命周期钩子

Pipeline 提供四个钩子，可用作装饰器或直接调用注册。

| 方法             | 触发时机                                                          | 函数签名                     |
| ---------------- | ----------------------------------------------------------------- | ---------------------------- |
| `on_init(fn)`    | **首次** `run()` / `start()` 唤坂，整个 Pipeline 生命周期内仅一次 | `fn(ctx: Context)`           |
| `on_start(fn)`   | 每次 `run()` / `start()` 开始时                                   | `fn(ctx: Context)`           |
| `on_end(fn)`     | 每次运行**成功结束**后                                            | `fn(report: PipelineReport)` |
| `on_failure(fn)` | 每次运行**失败**后                                                | `fn(report: PipelineReport)` |

**触发顺序**：`on_init`（首次）→ `on_start`（每次）→ [Pipeline 执行] → `on_end` 或 `on_failure`

### 用法示例

```python
pipeline = rivus.Pipeline("ml-pipeline") | load_model | process | aggregate

@pipeline.on_init
def setup(ctx: rivus.Context):
    # 首次运行前执行，适合连接数据库、加载配置等
    ctx.set("db", create_db_pool())
    ctx.log.info("初始化完成")

@pipeline.on_start
def on_start(ctx: rivus.Context):
    ctx.log.info("开始新一轮运行")

@pipeline.on_end
def on_end(report: rivus.PipelineReport):
    print(f"运行成功，耗时 {report.total_duration_s:.2f}s，共 {len(report.results)} 条结果")

@pipeline.on_failure
def on_failure(report: rivus.PipelineReport):
    for err in report.errors:
        send_alert(str(err))

# on_init 钩子在此处触发
report1 = pipeline.run(*batch1)
# on_init 不再触发，on_start 依然触发
report2 = pipeline.run(*batch2)
```

> 钩子内抛出的异常不会打断 Pipeline 运行，会被记录为警告日志。

---

## 运行模式

### 批量运行（最常用）

#### `run(*items, initial=None, timeout=None, repeat=1) → PipelineReport | list[PipelineReport]`

同步批量运行，阻塞直到所有 item 处理完毕。

```python
report = pipeline.run(item1, item2, item3)

# 传入初始数据
report = pipeline.run(*items, initial={"model": model, "config": cfg})

# 设置超时（秒）
report = pipeline.run(*items, timeout=30.0)

# 重复运行 5 次（适合定时任务、压测等场景）
reports = pipeline.run(*items, repeat=5)
for i, r in enumerate(reports):
    print(f"第 {i+1} 次：{r.total_duration_s:.2f}s，{len(r.results)} 条")
```

| 参数      | 类型              | 说明                                                                                                                                               |
| --------- | ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `*items`  | `Any`             | 待处理的 item，每个自动包装为 `root_ctx.derive(input=item)`；若 item 本身是 `Context` 则直接使用；**不传 item 时自动投递一个 `root_ctx.derive()`** |
| `initial` | `dict` \| `None`  | 运行前写入 root Context 的数据                                                                                                                     |
| `timeout` | `float` \| `None` | 最长执行时间（秒），超时抛出 `PipelineTimeoutError`                                                                                                |
| `repeat`  | `int`             | 重复运行次数，默认 `1`；`>1` 时依次运行并返回 `list[PipelineReport]`                                                                               |

**返回**：`repeat=1` 时返回 `PipelineReport`；`repeat>1` 时返回 `list[PipelineReport]`

**注意**：`run()` 使用 `*items` varargs，传入一个列表 `run([a, b, c])` 会将该列表视为**单个 item**。请使用 `run(*my_list)` 展开列表。

**`repeat` 模式的钩子行为**：每次重复都会触发 `on_start` / `on_end` / `on_failure`；`on_init` 仍只在整个 Pipeline 生命周期内触发一次；`once=True` 节点仅首次执行，后续重复直接透传。

---

### 流式运行

适合持续产生 item 的场景（消息队列、实时数据流等）：

#### `start(initial=None) → Pipeline`

启动 worker 线程，进入 `RUNNING` 状态。之后可调用 `submit()` 逐条推送。

```python
p.start(initial={"config": cfg})
```

#### `submit(item) → Pipeline`

提交单个 item（需先调用 `start()`）。

```python
for message in message_queue:
    p.submit(message)
```

#### `close() → Pipeline`

通知流水线不再有新 item（发出结束信号）。流水线处理完所有已提交的 item 后自动结束。

```python
p.close()
```

#### `wait(timeout=None) → PipelineReport`

阻塞直到流水线完成。

```python
report = p.wait(timeout=60.0)
```

**完整流式示例**：

```python
p = rivus.Pipeline("streaming") | process_node | enrich_node
p.start()

for item in data_generator():
    p.submit(item)

p.close()
report = p.wait()
```

---

### 后台运行

#### `run_background(*items, initial=None, timeout=None) → Pipeline`

非阻塞运行，立即返回。通过 `wait()` 获取结果。

```python
p.run_background(*items, timeout=60)

# 做其他事情...
do_other_work()

report = p.wait()
```

---

### 停止

#### `stop() → Pipeline`

发出停止信号，当前正在处理的 item 执行完毕后流水线停止，后续 item 不再处理。不抛出异常，最终状态为 `STOPPED`。

```python
import threading

p.run_background(*items)
timer = threading.Timer(5.0, p.stop)
timer.start()
report = p.wait()
```

---

## 状态查询

### `status → str`

当前状态，取值见 `PipelineStatus`：

| 状态        | 说明                                            |
| ----------- | ----------------------------------------------- |
| `"idle"`    | 未运行                                          |
| `"running"` | 正在运行                                        |
| `"success"` | 所有 item 成功处理                              |
| `"failed"`  | 出现错误（`fail_fast=True` 时触发）             |
| `"stopped"` | 被主动停止（`ctx.stop()` 或 `pipeline.stop()`） |
| `"timeout"` | 超时                                            |

```python
print(p.status)   # "success"
```

### `is_running → bool`

```python
if p.is_running:
    print("still working...")
```

### `context → Context`

root Context 实例。

### `nodes → list[Node]`

已注册的节点列表（副本）。

### `result → PipelineReport | None`

最近一次运行的报告，运行中时为 `None`。

---

## `PipelineStatus` 常量类

```python
from rivus import PipelineStatus

PipelineStatus.IDLE      # "idle"
PipelineStatus.RUNNING   # "running"
PipelineStatus.SUCCESS   # "success"
PipelineStatus.FAILED    # "failed"
PipelineStatus.STOPPED   # "stopped"
PipelineStatus.TIMEOUT   # "timeout"
```

---

## `PipelineReport` 运行报告

`run()` / `wait()` 的返回值，汇总完整运行统计。

```python
@dataclass
class PipelineReport:
    pipeline_name: str
    status: str
    total_duration_s: float
    results: list[Any]         # 每个 item 最终的 ctx.get("input") 值
    contexts: list[Context]    # 每个 item 的最终 Context
    errors: list[BaseException]
    nodes: list[NodeReport]
```

### 属性

| 属性               | 类型                  | 说明                                         |
| ------------------ | --------------------- | -------------------------------------------- |
| `pipeline_name`    | `str`                 | 流水线名称                                   |
| `status`           | `str`                 | 最终状态                                     |
| `total_duration_s` | `float`               | 总执行时长（秒）                             |
| `results`          | `list`                | 每个 item 的最终 `input` 值                  |
| `contexts`         | `list[Context]`       | 每个 item 的最终 Context，可访问所有中间数据 |
| `errors`           | `list[BaseException]` | 所有节点错误列表                             |
| `nodes`            | `list[NodeReport]`    | 每个节点的运行统计                           |
| `succeeded`        | `bool`（属性）        | `status == "success"`                        |

### 方法

#### `values(key="input") → list[Any]`

从所有最终 Context 中提取指定 key 的值。

```python
report.values("input")     # 等价于 report.results
report.values("score")     # 取每个 item 的 "score" 字段
```

### 示例

```python
report = pipeline.run(*items)

print(f"状态: {report.status}")
print(f"耗时: {report.total_duration_s:.2f}s")
print(f"结果: {report.results}")

if not report.succeeded:
    for err in report.errors:
        print(f"错误: {err}")

# 访问中间数据
for ctx in report.contexts:
    print(ctx.get("intermediate_result"))

# 节点统计
for nr in report.nodes:
    print(f"{nr.name}: 输入={nr.items_in} 输出={nr.items_out} 错误={nr.errors}")
    print(f"  平均={nr.avg_time_s:.3f}s P50={nr.p50_time_s:.3f}s P95={nr.p95_time_s:.3f}s")
```

---

## `NodeReport` 节点统计

```python
@dataclass
class NodeReport:
    name: str
    workers: int
    concurrency_type: str        # "thread" 或 "process"
    items_in: int                # 处理的 item 数量
    items_out: int               # 产出的 item 数量（fan-out 时可能大于 items_in）
    items_skipped: int           # 节点内 raise NodeSkip() 跳过的 item 数量
    errors: int                  # 发生错误的 item 数量
    total_time_s: float          # 所有 item 处理总耗时

    # 计算属性（property）
    avg_time_s: float            # = total_time_s / items_out
    p50_time_s: float            # 处理耗时中位数
    p95_time_s: float            # 处理耗时 P95
    min_time_s: float            # 最短单 item 耗时
    max_time_s: float            # 最长单 item 耗时
```

---

## 生命周期状态机

```
                    run() / start()
IDLE ──────────────────────────────→ RUNNING
                                         │
                    ┌────────────────────┤
                    ▼                    ▼
                 SUCCESS              FAILED
                    │                    │
              ctx.stop() /          fail_fast=True
           pipeline.stop()              │
                    ▼                    │
                STOPPED              ◄──┘
                    │
             timeout 到期
                    ▼
               TIMEOUT
```

- **IDLE → RUNNING**：调用 `run()` / `start()` / `run_background()`
- **RUNNING → SUCCESS**：所有 item 正常处理完毕
- **RUNNING → FAILED**：节点抛出异常且 `fail_fast=True`
- **RUNNING → STOPPED**：`ctx.stop()` / `pipeline.stop()` 被调用
- **RUNNING → TIMEOUT**：超时时间到

---

## `ordered` 模式说明

默认情况下，结果的顺序取决于各 item 的实际处理完成时间（并发不确定）。设置 `ordered=True` 后，框架给每个 item 分配序号，结果收集时按序号排序，**保证结果顺序与提交顺序一致**。

```python
p = rivus.Pipeline("ordered", ordered=True) | slow_node
report = p.run(3, 1, 2)   # 即使 1 先处理完，结果也是 [result_3, result_1, result_2]
```

---

## 错误处理策略

### `fail_fast=True`（默认）

任意节点抛出异常时：

1. 设置 stop 信号，停止接受新 item
2. 正在处理的 item 完成后 worker 退出
3. 报告状态为 `FAILED`，`PipelineError` 被抛出

### `fail_fast=False`

出错时继续处理其余 item，错误记录在 `report.errors` 中，最终状态仍为 `SUCCESS`（如果其他 item 正常）。

```python
p = rivus.Pipeline("tolerant", fail_fast=False) | risky_node
report = p.run(*items)
print(report.status)    # "success"（即使有部分 item 出错）
print(report.errors)    # 所有错误列表
```

### `PipelineStop`（节点主动停止）

当节点内调用 `ctx.stop()` 时，Pipeline 将最终状态设为 `STOPPED`（不视为失败），`report.errors` 为空。

---

## 并发模型示意

以 `Pipeline | node_A(workers=2) | node_B(workers=3)` 为例：

```
submit_queue → relay₀ → [worker_A₁, worker_A₂] → q_out_A
                                                        ↓
                                                  relay₁ (等 2 个 DONE)
                                                        ↓
                                          [worker_B₁, worker_B₂, worker_B₃] → q_out_B
                                                                                    ↓
                                                                              relay_out
                                                                                    ↓
                                                                             collector → PipelineReport
```
