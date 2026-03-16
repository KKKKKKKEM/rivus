"""Stream output examples.

Demonstrates pipeline.stream() for consuming results one-by-one without
waiting for the entire pipeline to finish.

Scenarios covered
-----------------
1. Basic streaming    — iterate results as they arrive
2. Stream with errors — ctx.error is set when a node raises; non-fatal
3. Early termination  — break out of the stream loop after N items
"""

import time

import rivus
from rivus import Context, Pipeline, node


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

@node
def generate_ids(ctx: Context):
    """Fan-out: yield N integer IDs."""
    n = ctx.input
    for i in range(n):
        yield i


@node(workers=4)
def slow_square(ctx: Context):
    """Simulate async/IO-bound work with a tiny sleep."""
    n = ctx.input
    time.sleep(0.05)
    return n * n


@node(workers=4)
def risky_transform(ctx: Context):
    """Raise for odd numbers to demonstrate error handling in stream mode."""
    n = ctx.input
    if n % 2 != 0:
        raise ValueError(f"odd number not allowed: {n}")
    return n * 10


# ---------------------------------------------------------------------------
# Example 1: Basic streaming
# ---------------------------------------------------------------------------

def example_basic_stream():
    p = Pipeline("basic_stream")
    p.add_nodes(generate_ids, slow_square)

    received = []
    t0 = time.time()
    for ctx in p.stream(5):
        received.append(ctx.input)
    elapsed = time.time() - t0

    received.sort()
    expected = sorted(i * i for i in range(5))
    assert received == expected, received

    # With workers=4 and 0.05s sleep, wall-clock should be much less than 5*0.05
    print(f"[1] basic_stream: {received}  ({elapsed:.2f}s)")


# ---------------------------------------------------------------------------
# Example 2: Stream with errors (non-fatal per-item errors)
# ---------------------------------------------------------------------------

def example_stream_errors():
    p = Pipeline("stream_errors")
    p.add_nodes(generate_ids, risky_transform)

    ok, err = [], []
    for ctx in p.stream(6):
        if ctx.error:
            err.append(ctx.error)
        else:
            ok.append(ctx.input)

    ok.sort()
    # Even inputs 0, 2, 4 → 0, 20, 40
    assert ok == [0, 20, 40], ok
    # Odd inputs 1, 3, 5 → errors
    assert len(err) == 3, err
    print(f"[2] stream_errors: ok={ok}, errors={len(err)}")


# ---------------------------------------------------------------------------
# Example 3: Early termination — stop after receiving 3 results
# ---------------------------------------------------------------------------

def example_early_exit():
    p = Pipeline("early_exit")
    p.add_nodes(generate_ids, slow_square)

    collected = []
    for ctx in p.stream(10):
        collected.append(ctx.input)
        if len(collected) >= 3:
            break  # stop consuming; pipeline threads will drain naturally

    assert len(collected) == 3
    print(f"[3] early_exit: first 3 results = {sorted(collected)}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("example_basic_stream ...", end=" ")
    example_basic_stream()
    print("OK")

    print("example_stream_errors ...", end=" ")
    example_stream_errors()
    print("OK")

    print("example_early_exit ...", end=" ")
    example_early_exit()
    print("OK")

    print("\nAll stream examples passed.")
