"""Context metadata sharing examples.

Demonstrates how to pass shared state through ctx.set() / ctx.get() and via
pipeline.initial so that every node in every run can access global resources.

Scenarios covered
-----------------
1. initial dict       — inject shared resources at Pipeline construction time
2. ctx.set / ctx.get  — write metadata in one node, read in a downstream node
3. pipeline.set/get   — pipeline-level mutable variables (e.g. accumulator)
4. ctx.require        — strict access that raises KeyError when key is absent
"""

import threading

from rivus import Context, Pipeline, node

# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

@node
def attach_config(ctx: Context):
    """Write per-item metadata; downstream nodes can read it."""
    item = ctx.input
    ctx.set("source", f"origin:{item}")
    ctx.set("version", 2)
    return item


@node
def read_config(ctx: Context):
    """Read metadata set by the previous node."""
    source = ctx.get("source", "unknown")
    version = ctx.get("version", 0)
    return f"{ctx.input}|src={source}|v={version}"


@node
def strict_reader(ctx: Context):
    """Use require() — raises KeyError if 'source' is not set."""
    source = ctx.require("source")   # must exist
    return f"{ctx.input}+{source}"


_accumulate_lock = threading.Lock()


@node
def accumulate(ctx: Context):
    """Append current input to a pipeline-level list (shared across runs)."""
    pipeline = ctx.get("pipeline")
    with _accumulate_lock:
        log = pipeline.get("log", [])
        log.append(ctx.input)
        pipeline.set("log", log)
    return ctx.input


# ---------------------------------------------------------------------------
# Example 1: initial dict — global resources visible to all nodes/runs
# ---------------------------------------------------------------------------

def example_initial_dict():
    config = {"env": "production", "batch_size": 100}
    p = Pipeline("initial_dict", initial=config)

    @node
    def read_global(ctx: Context):
        env = ctx.get("env")
        bs = ctx.get("batch_size")
        return f"env={env},bs={bs}"

    p.add_node(read_global)
    results = p.run("ignored_input")
    assert results == ["env=production,bs=100"], results
    print(f"[1] initial_dict: {results}")


# ---------------------------------------------------------------------------
# Example 2: ctx.set / ctx.get — per-item metadata chain
# ---------------------------------------------------------------------------

def example_ctx_chain():
    p = Pipeline("ctx_chain")
    p.add_nodes(attach_config, read_config)

    results = p.run("hello")
    assert results == ["hello|src=origin:hello|v=2"], results
    print(f"[2] ctx_chain: {results}")


# ---------------------------------------------------------------------------
# Example 3: pipeline.set / pipeline.get — mutable pipeline-level state
# ---------------------------------------------------------------------------

def example_pipeline_vars():
    p = Pipeline("pipeline_vars")
    p.add_node(accumulate)
    p.set("log", [])  # pre-initialise

    for item in ["a", "b", "c"]:
        p.run(item)

    log = p.get("log")
    assert sorted(log) == ["a", "b", "c"], log
    print(f"[3] pipeline_vars: log={log}")


# ---------------------------------------------------------------------------
# Example 4: ctx.require — strict access raises on missing keys
# ---------------------------------------------------------------------------

def example_require():
    p = Pipeline("require_example")
    p.add_nodes(attach_config, strict_reader)

    results = p.run("world")
    assert isinstance(results[0], str) and results[0].startswith("world+origin:"), results
    print(f"[4] require: {results}")

    p2 = Pipeline("require_fail")
    p2.add_node(strict_reader)
    results2 = p2.run("bare")
    print(f"[4b] require_fail: KeyError propagated, results={results2}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("example_initial_dict ...", end=" ")
    example_initial_dict()
    print("OK")

    print("example_ctx_chain ...", end=" ")
    example_ctx_chain()
    print("OK")

    print("example_pipeline_vars ...", end=" ")
    example_pipeline_vars()
    print("OK")

    print("example_require ...", end=" ")
    example_require()
    print("OK")

    print("\nAll context_sharing examples passed.")
