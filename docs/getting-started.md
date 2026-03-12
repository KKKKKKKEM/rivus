# 快速上手

本文通过几个递进示例，帮助你在 5 分钟内理解并使用 Rivus。

---

## 1. 最简流水线

```python
import rivus

@rivus.node
def greet(ctx: rivus.Context):
    name = ctx.require("input")
    return f"Hello, {name}!"

report = (rivus.Pipeline("hello") | greet).run("Alice", "Bob", "Carol")
print(report.results)
# ['Hello, Alice!', 'Hello, Bob!', 'Hello, Carol!']
```

**要点**：
- `@rivus.node` 将函数变成一个节点实例。
- `Pipeline("name") | node` 将节点加入流水线（管道符语法）。
- `.run(*items)` 同步执行，返回 `PipelineReport`。
- 节点函数通过 `ctx.require("input")` 读取当前 item。

---

## 2. 多节点串联

```python
import rivus

@rivus.node
def parse(ctx: rivus.Context):
    return ctx.require("input").strip().upper()

@rivus.node
def tag(ctx: rivus.Context):
    text = ctx.require("input")
    return {"text": text, "length": len(text)}

report = (rivus.Pipeline("etl") | parse | tag).run("  hello  ", " world ")
for r in report.results:
    print(r)
# {'text': 'HELLO', 'length': 5}
# {'text': 'WORLD', 'length': 5}
```

上游节点的返回值会自动以 `ctx.derive(input=value)` 包装成新 Context，传递给下游节点。

---

## 3. 并发处理

通过 `workers` 参数启用多线程并发，适合 I/O 密集型任务：

```python
import time
import rivus

@rivus.node(workers=4)
def fetch(ctx: rivus.Context):
    url = ctx.require("input")
    time.sleep(0.5)          # 模拟网络请求
    return f"data from {url}"

urls = [f"http://example.com/{i}" for i in range(8)]
report = (rivus.Pipeline("crawler") | fetch).run(*urls)
# 4 workers 并发，约 1s 完成 8 条请求
```

---

## 4. 共享初始数据（initial）

可通过 `initial` 向所有节点注入共享数据（如模型、配置）：

```python
import rivus

@rivus.node(workers=2)
def predict(ctx: rivus.Context):
    model = ctx.require("model")
    item = ctx.require("input")
    return model.predict(item)

class FakeModel:
    def predict(self, x): return x * 10

report = (rivus.Pipeline("infer") | predict).run(
    1, 2, 3,
    initial={"model": FakeModel()}
)
print(report.results)   # [10, 20, 30]
```

---

## 5. Fan-out（一变多）

节点函数使用 `yield` 即可将一个 item 展开为多个下游 item：

```python
import rivus

@rivus.node
def split_sentences(ctx: rivus.Context):
    paragraph = ctx.require("input")
    for sentence in paragraph.split("."):
        s = sentence.strip()
        if s:
            yield s          # 每个句子作为独立 item 送入下游

@rivus.node(workers=4)
def count_words(ctx: rivus.Context):
    sentence = ctx.require("input")
    return len(sentence.split())

report = (rivus.Pipeline("nlp") | split_sentences | count_words).run(
    "Hello world. How are you. Fine thanks."
)
print(report.results)   # [2, 3, 2]
```

---

## 6. Gather（多合一）

`gather=True` 在节点前插入同步屏障，将上游所有结果收集为 `list` 后一次性传入：

```python
import rivus

@rivus.node(workers=4)
def process(ctx: rivus.Context):
    return ctx.require("input") ** 2

@rivus.node(gather=True)
def summarize(ctx: rivus.Context):
    results = ctx.require("input")   # list: [0, 1, 4, 9, 16]
    return sum(results)

report = (rivus.Pipeline("sum_squares") | process | summarize).run(0, 1, 2, 3, 4)
print(report.results)   # [30]
```

---

## 7. 类式节点（BaseNode）

需要在流水线启动时初始化重型资源（如加载 ML 模型）时，使用 `BaseNode`：

```python
import rivus

class Inference(rivus.BaseNode):
    workers = 2

    def setup(self, ctx: rivus.Context) -> None:
        # 流水线启动时调用一次
        self.threshold = ctx.get("threshold", 0.5)

    def process(self, ctx: rivus.Context):
        score = ctx.require("input")
        return "positive" if score >= self.threshold else "negative"

report = (
    rivus.Pipeline("classify", context=rivus.Context({"threshold": 0.6}))
    | Inference()
).run(0.3, 0.7, 0.5, 0.8)
print(report.results)   # ['negative', 'positive', 'negative', 'positive']
```

---

## 8. 流式模式

适合持续产生 item 的场景（如实时数据流）：

```python
import rivus

@rivus.node(workers=2)
def double(ctx: rivus.Context):
    return ctx.require("input") * 2

p = rivus.Pipeline("stream") | double
p.start()

# 逐条推送
for x in range(5):
    p.submit(x)

p.close()             # 通知不再有新 item
report = p.wait()     # 阻塞直到完成
print(report.results)
```

---

## 9. 后台运行

```python
import time
import rivus

@rivus.node(workers=2)
def slow(ctx: rivus.Context):
    time.sleep(1)
    return ctx.require("input") + 1

p = (rivus.Pipeline("bg") | slow).run_background(1, 2, 3, timeout=10)
# 做其他事情 ...
report = p.wait()
print(report.results)
```

---

## 10. 错误处理与报告

```python
import rivus

@rivus.node
def risky(ctx: rivus.Context):
    x = ctx.require("input")
    if x == 0:
        raise ValueError("zero not allowed")
    return 100 / x

# fail_fast=False：遇到错误继续处理其他 item
p = rivus.Pipeline("safe", fail_fast=False) | risky
report = p.run(1, 0, 2, 4)

print("status:", report.status)       # success（非 fail_fast 模式）
print("errors:", report.errors)       # [NodeError('risky', ValueError(...))]
print("results:", report.results)     # [100.0, None, 50.0, 25.0]
for nr in report.nodes:
    print(f"  {nr.name}: in={nr.items_in} out={nr.items_out} err={nr.errors}")
```

---

## 11. 优雅停止

节点内可主动触发流水线停止（不计为失败）：

```python
import rivus

count = 0

@rivus.node
def until_ten(ctx: rivus.Context):
    global count
    count += 1
    if count >= 3:
        ctx.stop("reached limit")    # 立即抛出 PipelineStop，不再处理后续 item
    return ctx.require("input")

report = (rivus.Pipeline("stopper") | until_ten).run(1, 2, 3, 4, 5)
print(report.status)    # stopped
print(report.results)   # [1, 2]（第 3 条触发 stop）
```

---

## 完整示例（参考 `examples/demo.py`）

```python
import time
import rivus

@rivus.node
def step1(ctx: rivus.Context):
    return "result from step 1"

@rivus.node
def step2(ctx: rivus.Context):
    for i in range(20):
        yield {"id": i}       # fan-out

@rivus.node(workers=4)
def step3(ctx: rivus.Context):
    item = ctx.require("input")
    time.sleep(1)
    return f"result for {item['id']}"

@rivus.node(gather=True)
def step4(ctx: rivus.Context):
    items = ctx.require("input")   # list of 20 results
    return f"merged: {len(items)} items"

pipeline = rivus.Pipeline("demo")
pipeline.add_nodes(step1, step2, step3, step4)
report = pipeline.run()
print(report.results)
```
