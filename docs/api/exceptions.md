# 异常体系 — Exceptions

Rivus 定义了专用异常层级，方便区分框架错误、节点错误、超时和主动停止信号。

**源码**：`rivus/exceptions.py`

---

## 异常层级

```
BaseException
└── Exception
    └── RivusError                  # 所有 Rivus 异常的基类
        ├── NodeError               # 节点处理失败
        ├── PipelineError           # Pipeline 级别失败
        ├── PipelineTimeoutError    # Pipeline 执行超时
        ├── PipelineStop            # 节点主动触发的优雅停止（非失败）
        └── ContextKeyError         # 访问 Context 中不存在的必选键
```

---

## `RivusError`

所有 Rivus 异常的基类，可用于统一捕获框架内所有异常：

```python
try:
    report = pipeline.run(*items)
except rivus.RivusError as e:
    print(f"Rivus error: {e}")
```

---

## `NodeError`

单个 Node 处理某 item 时发生的错误。

```python
class NodeError(RivusError):
    node_name: str      # 出错的节点名称
    cause: BaseException  # 原始异常
```

**什么时候会抛出**：`fail_fast=True` 时，Pipeline 在 worker 捕获到节点异常后，会将 `NodeError` 包装在 `PipelineError` 中抛出。`fail_fast=False` 时，`NodeError` 记录在 `report.errors` 中，不抛出。

```python
# fail_fast=False 时遍历错误
for err in report.errors:
    if isinstance(err, rivus.NodeError):
        print(f"节点 '{err.node_name}' 失败: {err.cause}")
```

---

## `PipelineError`

Pipeline 级别的执行失败，由框架在 `fail_fast=True` 且节点出错时抛出。

```python
class PipelineError(RivusError):
    pipeline_name: str      # 流水线名称
    cause: BaseException    # 导致失败的第一个 NodeError
```

```python
try:
    report = pipeline.run(*items)
except rivus.PipelineError as e:
    print(f"Pipeline '{e.pipeline_name}' 失败")
    print(f"根因: {e.cause}")
    if isinstance(e.cause, rivus.NodeError):
        print(f"出错节点: {e.cause.node_name}")
        print(f"原始异常: {e.cause.cause}")
```

---

## `PipelineTimeoutError`

Pipeline 执行超过允许的最大时间后抛出。

```python
class PipelineTimeoutError(RivusError):
    pipeline_name: str  # 流水线名称
    timeout: float      # 配置的超时时间（秒）
```

```python
try:
    report = pipeline.run(*items, timeout=30.0)
except rivus.PipelineTimeoutError as e:
    print(f"Pipeline '{e.pipeline_name}' 超时（限制 {e.timeout}s）")
```

---

## `PipelineStop`

节点内主动触发的**优雅停止信号**。不视为错误，Pipeline 最终状态为 `STOPPED`，`report.errors` 为空。

```python
class PipelineStop(RivusError):
    message: str    # 停止原因
```

通常不需要直接构造此异常，而是通过 `ctx.stop()` 触发：

```python
@rivus.node
def process(ctx: rivus.Context):
    item = ctx.require("input")
    if item == "TERMINATE":
        ctx.stop("received terminate signal")   # 内部抛出 PipelineStop
    return transform(item)
```

若需要直接捕获（高级用法）：

```python
from rivus.exceptions import PipelineStop
try:
    result = pipeline.run(*items)
except PipelineStop:
    pass   # 不会发生：PipelineStop 由框架内部捕获，不会传播到外部
```

> **注意**：`PipelineStop` 由 Pipeline worker 线程内部捕获，**不会传播到 `run()` 调用处**。`run()` 正常返回 `PipelineReport`，`report.status == "stopped"`。

---

## `ContextKeyError`

访问 `Context` 中不存在的必选键时（调用 `ctx.require("key")` 或 `ctx["key"]`）抛出。

```python
class ContextKeyError(RivusError):
    key: str    # 不存在的键名
```

```python
try:
    value = ctx.require("model")
except rivus.ContextKeyError as e:
    print(f"必选键 '{e.key}' 不存在于 Context 中")
    print("请通过 initial= 或 ctx.set() 预先设置该键")
```

**常见原因**：
- 节点依赖的数据未通过 `initial=` 注入
- 上游节点未设置预期的输出键
- 误用了 `ctx.require()` 替代 `ctx.get()`（键确实可选时应用 `get`）

---

## 全局异常捕获示例

```python
import rivus

pipeline = rivus.Pipeline("my_pipeline", fail_fast=True) | node_a | node_b

try:
    report = pipeline.run(*items, timeout=60.0)

except rivus.PipelineTimeoutError as e:
    print(f"超时: {e.pipeline_name} 超过 {e.timeout}s")

except rivus.PipelineError as e:
    print(f"节点失败: {e}")
    if isinstance(e.cause, rivus.NodeError):
        print(f"  节点: {e.cause.node_name}")
        print(f"  原因: {e.cause.cause!r}")

except rivus.RivusError as e:
    print(f"其他 Rivus 错误: {e}")

else:
    if report.status == "stopped":
        print("流水线被主动停止")
    else:
        print(f"成功，共 {len(report.results)} 条结果")
```

---

## 异常与 Pipeline 状态对照

| 异常 | 触发条件 | `report.status` | 是否从 `run()` 抛出 |
|------|---------|----------------|---------------------|
| `NodeError` | 节点函数抛出任意异常 | `"failed"` | 仅 `fail_fast=True` 时包装在 `PipelineError` 中抛出 |
| `PipelineError` | `fail_fast=True` 且有 `NodeError` | `"failed"` | 是 |
| `PipelineTimeoutError` | 超过 `timeout` | `"timeout"` | 是 |
| `PipelineStop` | 节点内 `ctx.stop()` | `"stopped"` | 否（框架内部捕获） |
| `ContextKeyError` | `ctx.require(key)` 且 key 不存在 | `"failed"` | 同 `NodeError` |
