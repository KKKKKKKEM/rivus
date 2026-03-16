"""Route branching examples.

Demonstrates branch(mode="route") where a classifier node returns
(branch_key, value) to direct each item to a specific downstream branch.

IMPORTANT: Each branch node's name must match the branch_key returned by the
classifier (set via @node(name="key") or the function name must equal the key).

Scenarios covered
-----------------
1. Two-way route   — classify into two distinct branches
2. Three-way route — classify into three branches (number types)
3. Fan-out + route — expand a list then route each item independently
"""

import rivus
from rivus import Context, Pipeline, node


# ---------------------------------------------------------------------------
# Nodes — classifiers (return (branch_key, value))
# ---------------------------------------------------------------------------

@node
def classify_case(ctx: Context):
    """Route strings: 'upper' branch if already uppercase, 'lower' otherwise."""
    text = str(ctx.input)
    if text == text.upper():
        return ("upper", text)
    return ("lower", text)


@node
def classify_number(ctx: Context):
    """Route integers to 'neg', 'zero', or 'pos' branches."""
    n = int(ctx.input)
    if n < 0:
        return ("neg", n)
    if n == 0:
        return ("zero", n)
    return ("pos", n)


# ---------------------------------------------------------------------------
# Nodes — branch handlers (name= must match the branch_key)
# ---------------------------------------------------------------------------

@node(name="upper")
def handle_upper(ctx: Context):
    return f"UPPER:{ctx.input}"


@node(name="lower")
def handle_lower(ctx: Context):
    return f"lower:{ctx.input}"


@node(name="neg")
def handle_neg(ctx: Context):
    return f"neg({ctx.input})"


@node(name="zero")
def handle_zero(ctx: Context):
    return "zero"


@node(name="pos")
def handle_pos(ctx: Context):
    return f"pos({ctx.input})"


@node(gather=True)
def collect_all(ctx: Context):
    return sorted(str(x) for x in ctx.input)


@node
def produce(ctx: Context):
    for item in ctx.input:
        yield item


# ---------------------------------------------------------------------------
# Example 1: Two-way route (uppercase vs lowercase)
# ---------------------------------------------------------------------------

def example_two_way_route():
    p = Pipeline("two_way_route")
    p.add_node(classify_case).branch(
        handle_upper, handle_lower, mode="route"
    ).join(collect_all, gather=True)

    results = p.run("HELLO")
    assert results == [["UPPER:HELLO"]], results
    print(f"[1] two_way_route (HELLO): {results[0]}")

    results2 = p.run("world")
    assert results2 == [["lower:world"]], results2
    print(f"[1] two_way_route (world): {results2[0]}")


# ---------------------------------------------------------------------------
# Example 2: Three-way route (negative / zero / positive)
# ---------------------------------------------------------------------------

def example_three_way_route():
    p = Pipeline("three_way_route")
    p.add_node(classify_number).branch(
        handle_neg, handle_zero, handle_pos, mode="route"
    ).join(collect_all, gather=True)

    for inp, expected in [(-5, ["neg(-5)"]), (0, ["zero"]), (7, ["pos(7)"])]:
        results = p.run(inp)
        assert results == [expected], (inp, results)
        print(f"[2] three_way_route ({inp}): {results[0]}")


# ---------------------------------------------------------------------------
# Example 3: Fan-out then route — each item is independently classified
# ---------------------------------------------------------------------------

def example_fanout_then_route():
    p = Pipeline("fanout_route")
    p.add_nodes(produce, classify_case).branch(
        handle_upper, handle_lower, mode="route"
    ).join(collect_all, gather=True)

    items = ["ALPHA", "beta", "GAMMA", "delta"]
    results = p.run(items)
    expected = sorted(["UPPER:ALPHA", "lower:beta", "UPPER:GAMMA", "lower:delta"])
    assert results == [expected], results
    print(f"[3] fanout_route: {results[0]}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("example_two_way_route ...", end=" ")
    example_two_way_route()
    print("OK")

    print("example_three_way_route ...", end=" ")
    example_three_way_route()
    print("OK")

    print("example_fanout_then_route ...", end=" ")
    example_fanout_then_route()
    print("OK")

    print("\nAll route examples passed.")
