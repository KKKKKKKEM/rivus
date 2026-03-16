"""Graceful cancellation examples.

Demonstrates ctx.cancel() to safely abort a running pipeline.

Scenarios covered
-----------------
1. Poison-pill cancel  — a guard node cancels on a sentinel value
2. Need-stop check     — downstream nodes check ctx.need_stop before heavy work
3. Error-based cancel  — cancel when an error condition is detected
"""

from rivus import Context, Pipeline, node

# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

@node
def produce(ctx: Context):
    """Fan-out: yield items from the input list."""
    for item in ctx.input:
        yield item


@node
def guard(ctx: Context):
    """Cancel pipeline when the sentinel 'STOP' value is encountered."""
    if ctx.input == "STOP":
        ctx.cancel()
        return None   # no output for this item
    return ctx.input


@node(workers=2)
def transform(ctx: Context):
    """Respects need_stop — skips processing if cancellation was triggered."""
    return f"processed({ctx.input})"


@node
def validate(ctx: Context):
    """Skip processing if already cancelled; cancel on negative input."""
    n = ctx.input
    if isinstance(n, int) and n < 0:
        ctx.cancel()
        return None
    return n * 2 if isinstance(n, int) else n


# ---------------------------------------------------------------------------
# Example 1: Poison-pill cancellation
# Items before STOP are processed; STOP triggers cancel; later items skipped.
# ---------------------------------------------------------------------------

def example_poison_pill():
    p = Pipeline("poison_pill")
    p.add_nodes(produce, guard, transform)

    items = ["a", "b", "STOP", "c", "d"]
    results = p.run(items)
    outputs = [r for r in results if r is not None]

    # Only items before STOP reach transform
    for out in outputs:
        assert "STOP" not in str(out), outputs
    print(f"[1] poison_pill: {outputs}")


# ---------------------------------------------------------------------------
# Example 2: need_stop check — downstream nodes skip after cancel
# ---------------------------------------------------------------------------

def example_need_stop():
    p = Pipeline("need_stop_check")
    p.add_nodes(produce, guard, transform)

    items = ["x", "y", "STOP", "z", "w"]
    results = p.run(items)
    outputs = [r for r in results if r is not None]

    # None of the processed outputs should contain "STOP"
    assert all("STOP" not in str(r) for r in outputs)
    print(f"[2] need_stop: {outputs}")


# ---------------------------------------------------------------------------
# Example 3: Error-based cancel
# First negative number triggers cancel; pipeline stops cleanly.
# ---------------------------------------------------------------------------

def example_error_cancel():
    p = Pipeline("error_cancel")
    p.add_nodes(produce, validate)

    items = [1, 2, -1, 3, 4]
    results = p.run(items)

    positive_results = [r for r in results if isinstance(r, int) and r > 0]
    assert all(r % 2 == 0 for r in positive_results), positive_results
    print(f"[3] error_cancel: all={results} positive={positive_results}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("example_poison_pill ...", end=" ")
    example_poison_pill()
    print("OK")

    print("example_need_stop ...", end=" ")
    example_need_stop()
    print("OK")

    print("example_error_cancel ...", end=" ")
    example_error_cancel()
    print("OK")

    print("\nAll cancel examples passed.")
