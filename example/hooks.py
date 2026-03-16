"""Lifecycle hooks examples.

Demonstrates on_startup, on_run_start, and on_run_end hooks.

Scenarios covered
-----------------
1. on_startup  — one-time initialisation (runs exactly once across all .run() calls)
2. on_run_start / on_run_end — per-run bookkeeping (timing, logging, counters)
3. Combined    — all three hooks together with a shared pipeline variable
"""

import time
import threading

import rivus
from rivus import Context, Pipeline, node


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

@node
def upper(ctx: Context):
    return str(ctx.input).upper()


@node
def wrap(ctx: Context):
    return f"[{ctx.input}]"


# ---------------------------------------------------------------------------
# Example 1: on_startup — runs exactly once, even with multiple .run() calls
# ---------------------------------------------------------------------------

def example_startup_once():
    startup_log = []

    p = Pipeline("startup_once")
    p.add_nodes(upper, wrap)

    @p.on_startup
    def init(pipeline: Pipeline):
        startup_log.append("startup")
        pipeline.set("initialized", True)

    p.run("first")
    p.run("second")
    p.run("third")

    # startup called only once regardless of how many times run() is called
    assert startup_log == ["startup"], startup_log
    assert p.get("initialized") is True
    print(f"[1] startup_once: startup fired {len(startup_log)} time(s)")


# ---------------------------------------------------------------------------
# Example 2: on_run_start / on_run_end — per-run timing
# ---------------------------------------------------------------------------

def example_run_hooks():
    timings = []

    p = Pipeline("run_hooks")
    p.add_nodes(upper, wrap)

    @p.on_run_start
    def before(ctx: Context):
        ctx.set("_t0", time.time())

    @p.on_run_end
    def after(ctx: Context):
        elapsed = time.time() - ctx.get("_t0", time.time())
        timings.append(elapsed)

    results = []
    for item in ["hello", "world"]:
        results.append(p.run(item))

    assert len(timings) == 2
    assert results == [["[HELLO]"], ["[WORLD]"]]
    print(f"[2] run_hooks: timings={[f'{t*1000:.1f}ms' for t in timings]}")


# ---------------------------------------------------------------------------
# Example 3: All hooks combined with pipeline-level shared counter
# ---------------------------------------------------------------------------

def example_combined_hooks():
    p = Pipeline("combined_hooks")
    p.add_nodes(upper, wrap)

    counter_lock = threading.Lock()

    @p.on_startup
    def init(pipeline: Pipeline):
        pipeline.set("total_runs", 0)
        pipeline.set("total_items_seen", 0)

    @p.on_run_start
    def count_run(ctx: Context):
        pipeline = ctx.get("pipeline")
        with counter_lock:
            pipeline.set("total_runs", pipeline.get("total_runs", 0) + 1)

    @p.on_run_end
    def summarise(ctx: Context):
        pipeline = ctx.get("pipeline")
        with counter_lock:
            pipeline.set(
                "total_items_seen",
                pipeline.get("total_items_seen", 0) + 1,
            )

    for item in ["a", "b", "c", "d", "e"]:
        p.run(item)

    assert p.get("total_runs") == 5
    assert p.get("total_items_seen") == 5
    print(
        f"[3] combined_hooks: "
        f"total_runs={p.get('total_runs')}, "
        f"total_items_seen={p.get('total_items_seen')}"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("example_startup_once ...", end=" ")
    example_startup_once()
    print("OK")

    print("example_run_hooks ...", end=" ")
    example_run_hooks()
    print("OK")

    print("example_combined_hooks ...", end=" ")
    example_combined_hooks()
    print("OK")

    print("\nAll hooks examples passed.")
