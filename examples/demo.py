import time

import rivus


# ── once 节点：整个 Pipeline 生命周期内只执行一次 ──────────────────────────
@rivus.node(once=True)
def init_resources(ctx: rivus.Context):
    ctx.log.info(">>> 初始化资源（只执行一次）")
    ctx.set("resource", "loaded_model_v1")
    return ctx.get("input")  # pass-through input


# ── filter 节点：使用 NodeSkip 过滤不需要的 item ──────────────────────────
@rivus.node
def step1(ctx: rivus.Context):
    para = ctx.require("input")
    resource = ctx.get("resource", "N/A")
    ctx.log.info(f"Step 1 processing: {para}  resource={resource}")
    return "result from step 1"


@rivus.node
def step2(ctx: rivus.Context):
    step1_ret = ctx.require("input")
    ctx.log.info(f"Step 2 received: {step1_ret}")

    for i in range(20):
        yield {"id": i}


@rivus.node(workers=4)
def step3(ctx: rivus.Context):
    item = ctx.require("input")
    ctx.log.info(f"Step 3 processing item: {item}")
    time.sleep(1)  # 模拟耗时操作
    return f"result for {item['id']}"


@rivus.node
def step3_filter(ctx: rivus.Context):
    """只保留 id 为偶数的结果"""
    result = ctx.require("input")
    # 从结果字符串中提取 id
    item_id = int(result.split("result for ")[-1])
    if item_id % 2 != 0:
        raise rivus.NodeSkip(f"跳过奇数 id={item_id}")
    return result


@rivus.node(gather=True)
def step4(ctx: rivus.Context):
    items = ctx.require("input")  # items 是 step3_filter 所有结果组成的 list
    ctx.log.info(f"Step 4 received {len(items)} items: {items}")
    return f"merged result: {len(items)} items processed"


def main():
    pipeline = rivus.Pipeline(
        name="Test Pipeline",
        log_config=rivus.LogConfig(level="INFO"),
    )

    # ── 生命周期钩子 ───────────────────────────────────────────────────────
    @pipeline.on_init
    def on_init(ctx: rivus.Context):
        ctx.log.info("=== [hook] on_init: Pipeline 首次运行前 ===")

    @pipeline.on_start
    def on_start(ctx: rivus.Context):
        ctx.log.info("=== [hook] on_start: 每次 run 开始 ===")

    @pipeline.on_end
    def on_end(report: rivus.PipelineReport):
        filter_nr = report.nodes[4]  # step3_filter (index 4)
        print(f"=== [hook] on_end: 成功，耗时 {report.total_duration_s:.3f}s，"
              f"{len(report.results)} 条结果，"
              f"过滤掉 {filter_nr.items_skipped} 条 ===")

    @pipeline.on_failure
    def on_failure(report: rivus.PipelineReport):
        print(f"=== [hook] on_failure: 运行失败，错误数 {len(report.errors)} ===")

    pipeline.add_nodes(
        init_resources,
        step1,
        step2,
        step3,
        step3_filter,
        step4,
    )

    print("\n--- 第一次 run（init_resources 会执行）---")
    report = pipeline.run("hello")
    for res in report.results:
        print(res)

    print("\n--- 第二次 run（init_resources 将跳过，直接透传）---")
    report2 = pipeline.run("world")
    for res in report2.results:
        print(res)


if __name__ == "__main__":
    main()
