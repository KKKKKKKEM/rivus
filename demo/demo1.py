import time

from loguru import logger

import rivus


@rivus.node
def step1(ctx: rivus.Context):
    para = ctx.input
    resource = ctx.get("resource", "N/A")
    logger.debug(f"Step 1 processing: {para}  resource={resource}")
    return "result from step 1"


@rivus.node
def step2(ctx: rivus.Context):
    step1_ret = ctx.input
    logger.debug(f"Step 2 received: {step1_ret}")

    for i in range(10):
        yield {"id": i}


@rivus.node(workers=4)
def step3(ctx: rivus.Context):
    item = ctx.input
    logger.debug(f"Step 3 processing item: {item}")
    time.sleep(1)  # 模拟耗时操作
    return f"result for {item['id']}"




if __name__ == "__main__":
    pipeline = rivus.Pipeline("demo1", initial={"resource": "my_resource"})
    pipeline.add_nodes(step1, step2, step3)

    # 非流式输出模式
    results = pipeline.run("my_input")
    for item in results:
        logger.info(f"Final output: {item}")
    
    
    # 流式输出模式
    # for item in pipeline.stream("my_input"):
    #     logger.info(f"Final output: {item}")
    
    # rpc 模式
    # pipeline.serve(port=9997)
    
    