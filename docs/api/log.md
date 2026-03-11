# 日志系统 — LogConfig / RivusLogger

Rivus 内置完整的日志体系，支持终端输出、文件输出、动态等级调整和上下文字段绑定。

**源码**：`rivus/log.py`

---

## 快速使用

```python
import rivus

# 通过 Pipeline 配置日志
p = rivus.Pipeline(
    "my_pipeline",
    log_config=rivus.LogConfig(
        level="DEBUG",
        to_console=True,
        to_file="logs/run.log",
    )
)

# 节点内通过 ctx.log 使用
@rivus.node
def process(ctx: rivus.Context):
    ctx.log.info("processing item: %s", ctx.require("input"))
    ctx.log.bind(stage="process").debug("detail info")
    return result
```

---

## `LogConfig` — 日志配置

声明式日志配置，传入 `Pipeline` 的 `log_config` 参数。

```python
from rivus import LogConfig

config = LogConfig(
    level="DEBUG",
    to_console=True,
    to_file="pipeline.log",
    fmt="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
    date_fmt="%Y-%m-%d %H:%M:%S",
    name="rivus",
    file_mode="a",
)
```

### 参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `level` | `int \| str` | `"DEBUG"` | 最低输出等级：`"DEBUG"` / `"INFO"` / `"WARNING"` / `"ERROR"` / `"CRITICAL"` 或 `logging` 整数常量 |
| `to_console` | `bool` | `True` | 是否输出到终端（`stderr`） |
| `to_file` | `str \| Path \| None` | `None` | 日志文件路径；`None` 表示不写文件。父目录会自动创建 |
| `fmt` | `str` | 见下 | `logging` 格式串 |
| `date_fmt` | `str` | `"%Y-%m-%d %H:%M:%S"` | 日期时间格式串 |
| `name` | `str` | `"rivus"` | 底层 Logger 名称；Pipeline 会自动修改为 `"rivus.<pipeline_name>"` |
| `file_mode` | `str` | `"a"` | 文件打开模式：`"a"` 追加 / `"w"` 覆写 |

**默认格式串**：
```
%(asctime)s [%(levelname)-8s] %(name)s — %(message)s
```

### 示例

```python
# 仅输出 WARNING 及以上到终端，不写文件
LogConfig(level="WARNING", to_file=None)

# DEBUG 级别同时写满日志文件（覆写模式）和终端
LogConfig(level="DEBUG", to_file="debug.log", file_mode="w")

# 仅写文件，不输出终端
LogConfig(level="INFO", to_console=False, to_file="app.log")

# 自定义格式
LogConfig(
    level="INFO",
    fmt="%(asctime)s %(levelname)s %(message)s",
    date_fmt="%H:%M:%S",
)
```

---

## `RivusLogger` — 统一日志接口

封装 Python 标准库 `logging.Logger`，提供统一的日志方法和 `bind()` 上下文绑定。

通常通过 `ctx.log` 获取，无需直接构造。

### 标准日志方法

所有方法签名与 `logging.Logger` 完全一致：

```python
ctx.log.debug(msg, *args, **kwargs)
ctx.log.info(msg, *args, **kwargs)
ctx.log.warning(msg, *args, **kwargs)
ctx.log.error(msg, *args, **kwargs)
ctx.log.critical(msg, *args, **kwargs)
ctx.log.exception(msg, *args, **kwargs)  # ERROR 级 + 自动附加 traceback
```

支持 `%` 格式化占位符（与标准 logging 一致，延迟格式化，性能更好）：

```python
ctx.log.info("processing %d items from %s", count, source)
ctx.log.error("failed to parse item: %r", item)
```

### `ctx.log.exception(msg, ...)`

等价于 `ctx.log.error(msg, ..., exc_info=True)`，自动捕获并记录当前异常的完整 traceback，通常在 `except` 块中使用：

```python
try:
    result = risky_operation()
except Exception:
    ctx.log.exception("操作失败，item=%r", item)
    raise
```

### `ctx.log.bind(**extra) → _BoundLogger`

返回绑定了额外字段的子日志器。每条日志消息前自动加上 `[key=value ...]` 前缀，方便标注所属阶段、ID 等信息。

```python
log = ctx.log.bind(stage="inference", worker_id=3)
log.info("done in %.2fs", elapsed)
# 输出: [stage=inference worker_id=3] done in 0.15s
```

可以链式绑定：

```python
log = ctx.log.bind(pipeline="rag").bind(node="Embed").bind(batch_id=42)
log.debug("embedding started")
# 输出: [pipeline=rag node=Embed batch_id=42] embedding started
```

### `ctx.log.set_level(level)`

动态调整日志等级（影响所有 Handler）：

```python
ctx.log.set_level("WARNING")    # 字符串
ctx.log.set_level(logging.DEBUG) # 整数
```

### `ctx.log.level → int`

当前最低输出等级（整数）。

---

## `build_logger(config)` — 工厂函数

根据 `LogConfig` 构建并返回 `RivusLogger`。每次调用都会重置同名 Logger 的 Handler，确保多次 `run()` 不重复输出。

```python
from rivus.log import build_logger, LogConfig

logger = build_logger(LogConfig(level="DEBUG", to_file="test.log"))
logger.info("Hello, %s!", "world")
```

> **注意**：Pipeline 内部已自动调用此函数，通常不需要手动调用。

## `default_logger(name="rivus")` — 默认日志器

返回一个仅输出到 `stderr`（INFO 级）的默认日志器，用于 `Context` 在 Pipeline 外单独使用时的兜底。

---

## 日志等级说明

| 等级 | 整数值 | 说明 |
|------|--------|------|
| `DEBUG` | 10 | 详细调试信息，包括每个 item 的处理细节 |
| `INFO` | 20 | 流水线启动/结束、关键里程碑（推荐生产环境使用） |
| `WARNING` | 30 | 非致命问题（如 `stop` 被触发） |
| `ERROR` | 40 | 节点处理失败、流水线 FAILED/TIMEOUT |
| `CRITICAL` | 50 | 严重系统错误 |

**推荐**：
- 开发/调试时：`level="DEBUG"`
- 生产环境：`level="INFO"`，同时写文件

---

## 完整配置示例

```python
import rivus

# 生产环境：INFO 级写文件（每次覆写），不输出终端
pipeline = rivus.Pipeline(
    "production",
    log_config=rivus.LogConfig(
        level="INFO",
        to_console=False,
        to_file="logs/production.log",
        file_mode="a",
    )
)

# 调试环境：DEBUG 级输出终端
pipeline = rivus.Pipeline(
    "debug_run",
    log_config=rivus.LogConfig(level="DEBUG"),
)

# 传入已有的 RivusLogger（高级用法）
from rivus.log import build_logger, LogConfig
my_logger = build_logger(LogConfig(name="custom", level="INFO"))
pipeline = rivus.Pipeline("custom", log_config=my_logger)
```

---

## 节点内日志最佳实践

```python
@rivus.node(workers=4)
def process(ctx: rivus.Context):
    item = ctx.require("input")

    # 绑定 item 标识，方便过滤日志
    log = ctx.log.bind(item_id=item["id"])
    log.info("start processing")

    try:
        result = compute(item)
        log.debug("result: %r", result)
        return result
    except ValueError as e:
        log.exception("invalid input")
        raise
```
