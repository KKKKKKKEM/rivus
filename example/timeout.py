"""Timeout control examples.

Demonstrates pipeline.run(item, timeout=N) for capping total run duration.

Scenarios covered
-----------------
1. Completes within timeout  — normal path, no exception
2. Exceeds timeout           — TimeoutError is raised
3. Timeout with fan-out      — timeout applies to the whole batch
"""

import time

import rivus
from rivus import Context, Pipeline, node


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

@node
def fast_node(ctx: Context):
    """Completes instantly."""
    return f"done:{ctx.input}"


@node
def slow_node(ctx: Context):
    """Simulates a slow operation (2 seconds)."""
    time.sleep(2)
    return f"slow:{ctx.input}"


@node
def generate_items(ctx: Context):
    """Fan-out: yield N items."""
    n = ctx.input
    for i in range(n):
        yield i


@node(workers=2)
def slow_worker(ctx: Context):
    """Each item takes 0.5 s."""
    time.sleep(0.5)
    return ctx.input * 2


# ---------------------------------------------------------------------------
# Example 1: Completes within timeout — no exception
# ---------------------------------------------------------------------------

def example_within_timeout():
    p = Pipeline("within_timeout")
    p.add_node(fast_node)

    results = p.run("hello", timeout=5.0)
    assert results == ["done:hello"], results
    print(f"[1] within_timeout: {results}")


# ---------------------------------------------------------------------------
# Example 2: Exceeds timeout — TimeoutError raised
# ---------------------------------------------------------------------------

def example_exceeds_timeout():
    p = Pipeline("exceeds_timeout")
    p.add_node(slow_node)

    try:
        p.run("hello", timeout=0.1)
        raise AssertionError("Expected TimeoutError")
    except TimeoutError as e:
        print(f"[2] exceeds_timeout: caught TimeoutError — {e}")


# ---------------------------------------------------------------------------
# Example 3: Fan-out with timeout
# 4 items × 0.5 s each but workers=2, so wall-clock ≈ 1 s
# timeout=3 s should be sufficient; timeout=0.2 s should fail
# ---------------------------------------------------------------------------

def example_fanout_timeout_ok():
    p = Pipeline("fanout_timeout_ok")
    p.add_nodes(generate_items, slow_worker)

    results = p.run(4, timeout=5.0)
    assert sorted(results) == [0, 2, 4, 6], results
    print(f"[3a] fanout_timeout_ok: {sorted(results)}")


def example_fanout_timeout_fail():
    p = Pipeline("fanout_timeout_fail")
    p.add_nodes(generate_items, slow_worker)

    try:
        p.run(10, timeout=0.05)
        raise AssertionError("Expected TimeoutError")
    except TimeoutError as e:
        print(f"[3b] fanout_timeout_fail: caught TimeoutError — {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("example_within_timeout ...", end=" ")
    example_within_timeout()
    print("OK")

    print("example_exceeds_timeout ...", end=" ")
    example_exceeds_timeout()
    print("OK")

    print("example_fanout_timeout_ok ...", end=" ")
    example_fanout_timeout_ok()
    print("OK")

    print("example_fanout_timeout_fail ...", end=" ")
    example_fanout_timeout_fail()
    print("OK")

    print("\nAll timeout examples passed.")
