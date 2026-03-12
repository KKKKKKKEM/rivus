# 使用模式与最佳实践

本文档整理了 Rivus 中常见的设计模式与使用技巧。

---

## 目录

- [基础模式](#基础模式)
  - [线性流水线](#线性流水线)
  - [并发 I/O 密集型](#并发-io-密集型)
  - [CPU 密集型（进程模式）](#cpu-密集型进程模式)
- [数据流模式](#数据流模式)
  - [Fan-out：一变多](#fan-out一变多)
  - [Gather：多合一](#gather多合一)
  - [Fan-out + Gather：分散-聚合](#fan-out--gather分散-聚合)
- [初始化模式](#初始化模式)
  - [共享模型/配置](#共享模型配置)
  - [per-worker 初始化（BaseNode.setup）](#per-worker-初始化basenodesetup)
  - [Once 节点（生命周期内只执行一次）](#once-节点生命周期内只执行一次)
  - [Pipeline 生命周期钩子](#pipeline-生命周期钩子)
- [流控模式](#流控模式)
  - [NodeSkip：过滤/丢弃 item](#nodeskip过滤丢弃-item)
  - [背压控制（queue_size）](#背压控制queue_size)
  - [优雅停止](#优雅停止)
  - [超时保护](#超时保护)
- [结果处理模式](#结果处理模式)
  - [有序结果](#有序结果)
  - [访问中间数据](#访问中间数据)
  - [容错收集（fail_fast=False）](#容错收集fail_fastfalse)
- [流式模式](#流式模式)
  - [持续数据流](#持续数据流)
  - [后台运行 + 超时监控](#后台运行--超时监控)
- [高级模式](#高级模式)
  - [Dynamic context 传递](#dynamic-context-传递)
  - [节内协作式停止](#节内协作式停止)
  - [多阶段 RAG 流水线](#多阶段-rag-流水线)

---

## 基础模式

### 线性流水线

最基本的串行处理形式：每个节点依次处理上游产出。

```python
import rivus

@rivus.node
def load(ctx: rivus.Context):
    path = ctx.require("input")
    return open(path).read()

@rivus.node
def parse(ctx: rivus.Context):
    text = ctx.require("input")
    return {"words": text.split(), "length": len(text)}

@rivus.node
def store(ctx: rivus.Context):
    data = ctx.require("input")
    database.insert(data)
    # 返回 None → pass-through，ctx 原样传下游（副作用节点）

pipeline = rivus.Pipeline("etl") | load | parse | store
report = pipeline.run("file1.txt", "file2.txt", "file3.txt")
```

### 并发 I/O 密集型

网络请求、数据库查询等 I/O 密集任务，用多线程提高吞吐量：

```python
import requests
import rivus

@rivus.node(workers=16)          # 16 线程并发
def fetch(ctx: rivus.Context):
    url = ctx.require("input")
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()

@rivus.node(workers=4)
def extract(ctx: rivus.Context):
    data = ctx.require("input")
    return {"title": data["title"], "content": data["body"]}

pipeline = rivus.Pipeline("crawler") | fetch | extract
report = pipeline.run(*urls)
```

### CPU 密集型（进程模式）

数值计算、图像处理等 CPU 密集任务，绕过 GIL：

```python
import rivus

# 注意：进程模式函数必须是模块级，不能是 lambda 或闭包
def resize_image(ctx: rivus.Context):
    from PIL import Image
    path = ctx.require("input")
    img = Image.open(path)
    return img.resize((224, 224))

resize_node = rivus.Node(
    resize_image,
    name="Resize",
    workers=4,
    concurrency_type="process",  # 多进程，绕过 GIL
)

pipeline = rivus.Pipeline("vision") | resize_node
report = pipeline.run(*image_paths)
```

---

## 数据流模式

### Fan-out：一变多

一条记录展开为多条下游记录：

```python
import rivus

@rivus.node
def split_document(ctx: rivus.Context):
    """将文档拆分为段落"""
    doc = ctx.require("input")
    for para in doc.split("\n\n"):
        para = para.strip()
        if para:
            yield para           # 每个段落作为独立 item

@rivus.node(workers=8)
def embed_paragraph(ctx: rivus.Context):
    """向量化每个段落"""
    para = ctx.require("input")
    return embedding_model.encode(para)

pipeline = rivus.Pipeline("index") | split_document | embed_paragraph
report = pipeline.run(*documents)    # 10 篇文档 → N 个段落 → N 个向量
```

### Gather：多合一

等待所有上游并发结果，合并后统一处理：

```python
import rivus

@rivus.node(workers=8)
def score_item(ctx: rivus.Context):
    item = ctx.require("input")
    return {"id": item["id"], "score": model.score(item)}

@rivus.node(gather=True)
def rank_and_store(ctx: rivus.Context):
    """等所有评分完成后，统一排序写入"""
    scored = ctx.require("input")          # list of dicts
    ranked = sorted(scored, key=lambda x: x["score"], reverse=True)
    database.batch_insert(ranked)
    return len(ranked)

pipeline = rivus.Pipeline("ranking") | score_item | rank_and_store
report = pipeline.run(*candidates)
```

### Fan-out + Gather：分散-聚合

经典的 map-reduce 模式：

```python
import rivus

@rivus.node
def split(ctx: rivus.Context):
    """将批次拆分为单条"""
    batch = ctx.require("input")
    for item in batch:
        yield item

@rivus.node(workers=8)
def process(ctx: rivus.Context):
    return heavy_compute(ctx.require("input"))

@rivus.node(gather=True)
def aggregate(ctx: rivus.Context):
    """汇总所有结果"""
    results = ctx.require("input")    # list
    return {"count": len(results), "total": sum(r["score"] for r in results)}

pipeline = rivus.Pipeline("map_reduce") | split | process | aggregate
report = pipeline.run(*batches)
```

---

## 初始化模式

### 共享模型/配置

通过 `initial=` 向所有节点注入共享数据（模型、配置、数据库连接等）：

```python
import rivus

@rivus.node(workers=4)
def inference(ctx: rivus.Context):
    model = ctx.require("model")        # 从共享 Context 读取
    tokenizer = ctx.require("tokenizer")
    text = ctx.require("input")
    tokens = tokenizer.encode(text)
    return model.generate(tokens)

model = load_model("bert-base")
tokenizer = load_tokenizer("bert-base")

pipeline = rivus.Pipeline("nlp") | inference
report = pipeline.run(
    *texts,
    initial={"model": model, "tokenizer": tokenizer}
)
```

或通过预初始化 Context：

```python
ctx = rivus.Context({
    "model": load_model("resnet50"),
    "threshold": 0.8,
    "labels": LABEL_MAP,
})
pipeline = rivus.Pipeline("classify", context=ctx) | preprocess | inference | postprocess
```

### per-worker 初始化（BaseNode.setup）

`setup()` 在流水线启动时调用**一次**，所有 workers 共享同一个 `BaseNode` 实例，因此 `setup()` 中初始化的资源被所有 workers 共享。

```python
import rivus

class InferenceNode(rivus.BaseNode):
    workers = 4
    name = "Inference"

    def setup(self, ctx: rivus.Context) -> None:
        """每次 pipeline.run() 前调用一次，初始化共享资源"""
        model_path = ctx.require("model_path")
        self.model = ModelLoader.load(model_path)
        self.threshold = ctx.get("threshold", 0.5)

    def process(self, ctx: rivus.Context):
        data = ctx.require("input")
        score = self.model.predict(data)
        return {"data": data, "score": score, "label": score > self.threshold}

pipeline = rivus.Pipeline("classify") | InferenceNode()
report = pipeline.run(*items, initial={"model_path": "/models/v2.pt"})
```

> **注意**：多 worker 并发调用 `process()` 时，若 `self.model` 非线程安全，需加锁或为每个 worker 创建独立模型实例（在 setup 中初始化 `self._lock = threading.Lock()`）。

### Once 节点（生命周期内只执行一次）

`once=True` 让节点在整个 **Pipeline 对象生命周期**内只执行一次。首次 `run()` 正常执行，后续 `run()` 直接透传 item。

适合：模型加载、数据库连接建立、配置文件读取等**幂等且昂贵**的初始化操作。

```python
import rivus

@rivus.node(once=True)
def load_model(ctx: rivus.Context):
    ctx.set("model", Model.load(ctx.require("model_path")))
    return ctx.get("input")   # pass-through，让 input 继续流转

@rivus.node(workers=4)
def infer(ctx: rivus.Context):
    model = ctx.require("model")
    return model.predict(ctx.require("input"))

pipeline = rivus.Pipeline("ml") | load_model | infer

report1 = pipeline.run(*batch1, initial={"model_path": "/models/v2.pt"})
# load_model 已执行，model 存在 ctx 中
report2 = pipeline.run(*batch2)   # load_model 跳过，直接推理
```

### Pipeline 生命周期钩子

生命周期钩子让你在 Pipeline 运行的不同阶段插入逻辑，而无需修改任何节点。

| 钩子         | 触发时机                                |
| ------------ | --------------------------------------- |
| `on_init`    | 整个生命周期第一次 `run()` 前（仅一次） |
| `on_start`   | 每次 `run()` / `start()` 开始时         |
| `on_end`     | 每次运行**成功**后                      |
| `on_failure` | 每次运行**失败**后                      |

```python
import rivus

pipeline = rivus.Pipeline("etl") | extract | transform | load

@pipeline.on_init
def setup_resources(ctx: rivus.Context):
    ctx.set("db", create_connection_pool())
    ctx.set("cache", RedisCache())

@pipeline.on_start
def log_start(ctx: rivus.Context):
    pass

@pipeline.on_end
def cleanup(report: rivus.PipelineReport):
    print(f"完成：{len(report.results)} 条，耗时 {report.total_duration_s:.2f}s")

@pipeline.on_failure
def alert_on_failure(report: rivus.PipelineReport):
    for err in report.errors:
        send_alert(str(err))

# on_init 在首次 run() 前触发
report1 = pipeline.run(*batch1)
# on_init 不再触发，on_start/on_end 依然触发
report2 = pipeline.run(*batch2)
```

---

## 流控模式

### NodeSkip：过滤/丢弃 item

在节点内 `raise NodeSkip()` 可以**静默丢弃当前 item**，不产生错误，item 不会流向下游。

```python
import rivus

@rivus.node
def filter_quality(ctx: rivus.Context):
    item = ctx.require("input")
    if item.get("quality_score", 0) < 0.7:
        raise rivus.NodeSkip(f"低质量 item 已过滤: score={item['quality_score']}")
    return item

@rivus.node(workers=4)
def process_high_quality(ctx: rivus.Context):
    # 只有 quality_score >= 0.7 的 item 才会到达这里
    return enrich(ctx.require("input"))

pipeline = rivus.Pipeline("quality-filter") | filter_quality | process_high_quality
report = pipeline.run(*raw_items)

# 查看跳过数量
filter_report = report.nodes[0]
print(f"过滤掉 {filter_report.items_skipped}/{filter_report.items_in} 条低质量 item")
```

`NodeSkip` 可以在流水线任意节点使用，不影响其他 item 的处理：

```python
@rivus.node
def enrich(ctx: rivus.Context):
    item = ctx.require("input")
    try:
        extra = fetch_extra_info(item["id"])
    except NotFoundError:
        raise rivus.NodeSkip(f"id={item['id']} 无额外信息，跳过")
    return {**item, **extra}
```

### 背压控制（queue_size）

当下游节点处理速度远低于上游时，设置 `queue_size` 防止内存无限增长：

```python
import rivus

@rivus.node(workers=16)     # 快速抓取
def fast_fetch(ctx: rivus.Context):
    return requests.get(ctx.require("input")).content

@rivus.node(workers=2, queue_size=50)   # 慢速处理，队列上限 50
def slow_process(ctx: rivus.Context):
    data = ctx.require("input")
    return heavy_image_processing(data)

pipeline = rivus.Pipeline("pipeline") | fast_fetch | slow_process
```

当 `slow_process` 的输入队列达到 50 时，`fast_fetch` 的输出会阻塞，形成自然的背压。

### 优雅停止

**方式一：`ctx.stop()` — 立即停止当前节点**

```python
@rivus.node
def check_termination(ctx: rivus.Context):
    item = ctx.require("input")
    if item.get("type") == "EOF":
        ctx.stop("received EOF signal")   # 立即终止
    return process(item)
```

**方式二：`ctx.request_stop()` + `ctx.stop_requested` — 协作式退出**

```python
@rivus.node(workers=4)
def process_with_early_exit(ctx: rivus.Context):
    data = ctx.require("input")
    results = []
    for chunk in data.chunks():
        if ctx.stop_requested:     # 轮询停止标志
            print("early exit due to stop request")
            break
        results.append(process_chunk(chunk))
    return results
```

**方式三：外部调用 `pipeline.stop()`**

```python
import threading, rivus

pipeline = rivus.Pipeline("long_run") | heavy_node

def _timeout_watcher():
    time.sleep(30)
    pipeline.stop()

threading.Thread(target=_timeout_watcher, daemon=True).start()
pipeline.run_background(*items)
report = pipeline.wait()
```

### 超时保护

最简单的超时写法：

```python
try:
    report = pipeline.run(*items, timeout=60.0)
except rivus.PipelineTimeoutError:
    print("超时，任务未完成")
```

后台模式超时：

```python
pipeline.run_background(*items, timeout=30.0)
report = pipeline.wait()   # 超时后 report.status == "timeout"
```

---

## 结果处理模式

### 有序结果

```python
pipeline = rivus.Pipeline("ordered", ordered=True) | process_node
report = pipeline.run(*items)
# report.results 保证与 items 顺序一致
for item, result in zip(items, report.results):
    print(f"{item} → {result}")
```

### 访问中间数据

每个 item 的最终 `Context` 保存了整条派生链上所有节点写入的数据：

```python
@rivus.node
def stage1(ctx: rivus.Context):
    result = compute_stage1(ctx.require("input"))
    ctx.set("stage1_result", result)   # 显式保存中间数据
    return result

@rivus.node
def stage2(ctx: rivus.Context):
    result = compute_stage2(ctx.require("input"))
    ctx.set("stage2_result", result)
    return result

pipeline = rivus.Pipeline("multi_stage") | stage1 | stage2
report = pipeline.run(*items)

for final_ctx in report.contexts:
    print(final_ctx.get("stage1_result"))   # 访问中间结果
    print(final_ctx.get("stage2_result"))
    print(final_ctx.get("input"))           # 最终输出
```

> **注意**：`ctx.set()` 写入的是**当前 item 的 Context**，不影响其他 item 的 Context。

### 容错收集（fail_fast=False）

处理部分失败的场景（如爬虫，允许少量失败）：

```python
import rivus

@rivus.node(workers=8)
def fetch_url(ctx: rivus.Context):
    url = ctx.require("input")
    resp = requests.get(url, timeout=5)
    resp.raise_for_status()
    return resp.json()

pipeline = rivus.Pipeline("crawler", fail_fast=False) | fetch_url
report = pipeline.run(*urls)

success_results = [r for r in report.results if r is not None]
failed_count = len(report.errors)
print(f"成功 {len(success_results)}/{len(urls)}，失败 {failed_count}")

for err in report.errors:
    if isinstance(err, rivus.NodeError):
        print(f"  节点 {err.node_name}: {err.cause}")
```

---

## 流式模式

### 持续数据流

```python
import rivus
from kafka import KafkaConsumer

@rivus.node(workers=4)
def process_message(ctx: rivus.Context):
    msg = ctx.require("input")
    return transform(msg.value)

@rivus.node
def write_to_db(ctx: rivus.Context):
    database.insert(ctx.require("input"))

pipeline = rivus.Pipeline("kafka_consumer") | process_message | write_to_db
pipeline.start()

consumer = KafkaConsumer("my_topic")
try:
    for msg in consumer:
        pipeline.submit(msg)
except KeyboardInterrupt:
    pass
finally:
    pipeline.close()
    report = pipeline.wait(timeout=30)
    print(f"处理了 {len(report.results)} 条消息")
```

### 后台运行 + 超时监控

```python
import rivus

pipeline = rivus.Pipeline("background_task", fail_fast=False) | step1 | step2
pipeline.run_background(*large_dataset, timeout=300)

# 主线程做其他工作
while pipeline.is_running:
    time.sleep(5)
    print(f"状态: {pipeline.status}")

report = pipeline.wait()
print(f"完成: {report.status}, 耗时: {report.total_duration_s:.1f}s")
```

---

## 高级模式

### Dynamic context 传递

节点可以直接返回 `Context` 实例，完全控制传递给下游的数据：

```python
@rivus.node
def enrich(ctx: rivus.Context):
    item = ctx.require("input")
    # 手动 derive，可自定义键名
    return ctx.derive(
        input=item["data"],
        metadata=item["meta"],
        timestamp=time.time(),
    )
```

### 节内协作式停止

满足某个外部条件时，从另一个线程发出停止信号：

```python
import threading
import rivus

pipeline = rivus.Pipeline("conditional") | node_a | node_b

stop_flag = threading.Event()

@rivus.node(workers=4)
def process(ctx: rivus.Context):
    if stop_flag.is_set():
        ctx.stop("external stop signal received")
    return ctx.require("input") * 2

# 外部条件触发停止
def external_monitor():
    wait_for_external_condition()
    stop_flag.set()

threading.Thread(target=external_monitor, daemon=True).start()
report = pipeline.run(*items)
```

### 多阶段 RAG 流水线

综合运用 fan-out、gather、并发模式的完整示例：

```python
import rivus

class EmbedNode(rivus.BaseNode):
    workers = 8
    name = "Embed"

    def setup(self, ctx: rivus.Context) -> None:
        self.embed_model = load_embedding_model(ctx.require("embed_model"))

    def process(self, ctx: rivus.Context):
        text = ctx.require("input")
        return {"text": text, "vector": self.embed_model.encode(text)}


@rivus.node
def chunk_document(ctx: rivus.Context):
    """文档 → 多个 chunk（fan-out）"""
    doc = ctx.require("input")
    for chunk in split_into_chunks(doc, max_tokens=512):
        yield chunk


@rivus.node(gather=True)
def build_index(ctx: rivus.Context):
    """所有 chunk 向量化完成后，统一构建索引"""
    embeddings = ctx.require("input")   # list of {"text": ..., "vector": ...}
    index = VectorIndex()
    for item in embeddings:
        index.add(item["vector"], item["text"])
    index.save("index.faiss")
    return len(embeddings)


pipeline = (
    rivus.Pipeline("rag_indexer")
    | chunk_document
    | EmbedNode()
    | build_index
)

report = pipeline.run(
    *documents,
    initial={"embed_model": "text-embedding-ada-002"},
    timeout=600,
)
print(f"索引完成，共 {report.results[0]} 个 chunk")
```

---

## 性能调优建议

| 场景                    | 建议                                              |
| ----------------------- | ------------------------------------------------- |
| I/O 密集型（HTTP/DB）   | `workers=16~64`，`concurrency_type="thread"`      |
| CPU 密集型（图像/数值） | `workers=CPU核数`，`concurrency_type="process"`   |
| 下游明显慢于上游        | 给慢速节点设置 `queue_size`，防止内存溢出         |
| 需要保持顺序            | `Pipeline(ordered=True)`（有轻微开销）            |
| 允许部分失败            | `fail_fast=False`                                 |
| 需要全量结果才能继续    | 下游节点加 `gather=True`                          |
| 长时间任务              | 设置 `timeout`，或节点内轮询 `ctx.stop_requested` |
| 节点间数据量大          | 考虑传递数据引用（如文件路径）而非数据本身        |
