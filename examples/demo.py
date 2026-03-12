import time

from loguru import logger

import rivus


# ── once 节点：整个 Pipeline 生命周期内只执行一次 ──────────────────────────
@rivus.node(once=True)
def init_resources(ctx: rivus.Context):
    logger.debug(">>> 初始化资源（只执行一次）")
    ctx.set("resource", "loaded_model_v1")
    return ctx.get("input")  # pass-through input


# ── filter 节点：使用 NodeSkip 过滤不需要的 item ──────────────────────────
@rivus.node
def step1(ctx: rivus.Context):
    para = ctx.require("input")
    resource = ctx.get("resource", "N/A")
    logger.debug(f"Step 1 processing: {para}  resource={resource}")
    return "result from step 1"


@rivus.node
def step2(ctx: rivus.Context):
    step1_ret = ctx.require("input")
    logger.debug(f"Step 2 received: {step1_ret}")

    for i in range(20):
        yield {"id": i}


@rivus.node(workers=4)
def step3(ctx: rivus.Context):
    item = ctx.require("input")
    logger.debug(f"Step 3 processing item: {item}")
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
    logger.debug(f"Step 4 received {len(items)} items: {items}")
    return f"merged result: {len(items)} items processed"


def main():
    pipeline = rivus.Pipeline(name="Test Pipeline")

    # ── 生命周期钩子 ───────────────────────────────────────────────────────
    @pipeline.on_init
    def on_init(ctx: rivus.Context):
        logger.debug("=== [hook] on_init: Pipeline 首次运行前 ===")

    @pipeline.on_start
    def on_start(ctx: rivus.Context):
        logger.debug("=== [hook] on_start: 每次 run 开始 ===")

    @pipeline.on_end
    def on_end(report: rivus.PipelineReport):
        filter_nr = report.nodes[4]  # step3_filter (index 4)
        logger.debug(f"=== [hook] on_end: 成功，耗时 {report.total_duration_s:.3f}s，"
                     f"{len(report.results)} 条结果，"
                     f"过滤掉 {filter_nr.items_skipped} 条 ===")

    @pipeline.on_failure
    def on_failure(report: rivus.PipelineReport):
        logger.debug(
            f"=== [hook] on_failure: 运行失败，错误数 {len(report.errors)} ===")

    pipeline.add_nodes(
        init_resources,
        step1,
        step2,
        step3,
        step3_filter,
        step4,
    )

    logger.debug("\n--- 第一次 run（init_resources 会执行）---")
    report = pipeline.run("hello")
    logger.debug(f"run_id: {report.run_id}")
    for res in report.results:
        logger.debug(res)
    logger.debug("\n--- 第二次 run（init_resources 将跳过，直接透传）---")
    report2 = pipeline.run("world")
    logger.debug(f"run_id: {report2.run_id}")
    for res in report2.results:
        logger.debug(res)

    # ── RunStorage 演示 ───────────────────────────────────────────────────
    logger.debug("\n--- storage.list() ---")
    for rid in pipeline.storage.list():
        state = pipeline.storage.get(rid)
        logger.debug(f"  {rid[:8]}...  status={state.report.status}  "
                     f"results={len(state.report.results)}")

    logger.debug("\n--- storage[第一次 run_id] ---")
    state1 = pipeline.storage[report.run_id]
    logger.debug(f"  结果: {state1.report.results}")
    logger.debug(f"  耗时: {state1.report.total_duration_s:.3f}s")

    logger.debug(f"\n--- len(storage)={len(pipeline.storage)} ---")


if __name__ == "__main__":
    main()
