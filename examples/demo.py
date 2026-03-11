import time

import rivus


@rivus.node
def step1(ctx: rivus.Context):
    para = ctx.require("input")
    ctx.log.info(f"Step 1 processing: {para}")
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


@rivus.node(gather=True)
def step4(ctx: rivus.Context):
    items = ctx.require("input")  # items 是 step3 所有结果组成的 list
    ctx.log.info(f"Step 4 received {len(items)} items from step3: {items}")
    return f"merged result: {len(items)} items processed"


def main():
    pipeline = rivus.Pipeline(
        name="Test Pipeline",
        log_config=rivus.LogConfig(level="INFO"),
    )

    pipeline.add_nodes(
        step1,
        step2,
        step3,
        step4,
    )

    report = pipeline.run()

    for res in report.results:
        print(res)


if __name__ == "__main__":
    main()
