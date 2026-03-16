"""Tests for branch() with Node | Pipeline arguments."""

from rivus import Context, Pipeline, node

# ---------------------------------------------------------------------------
# Shared nodes
# ---------------------------------------------------------------------------

@node
def parse(ctx: Context):
    return ctx.input.strip().upper()


@node
def enrich_a(ctx: Context):
    return f"A:{ctx.input}"


@node
def enrich_b(ctx: Context):
    return f"B:{ctx.input}"


@node
def clean_a(ctx: Context):
    return f"clean({ctx.input})"


@node
def clean_b(ctx: Context):
    return f"clean({ctx.input})"


@node(gather=True)
def merge(ctx: Context):
    return sorted(ctx.input)


# ---------------------------------------------------------------------------
# Test 1: branch(Node, Node) — existing behaviour unchanged
# ---------------------------------------------------------------------------

def test_branch_plain_nodes():
    p = Pipeline("test_plain_nodes")
    p.add_node(parse).branch(enrich_a, enrich_b).join(merge, gather=True)
    result = p.run("hello")
    assert result == [["A:HELLO", "B:HELLO"]], result


# ---------------------------------------------------------------------------
# Test 2: branch(Pipeline, Pipeline) — sub-pipeline branches
# ---------------------------------------------------------------------------

def test_branch_pipeline_branches():
    branch_a = Pipeline("branch_a").add_nodes(enrich_a, clean_a)
    branch_b = Pipeline("branch_b").add_nodes(enrich_b, clean_b)

    p = Pipeline("test_pipeline_branches")
    p.add_node(parse).branch(branch_a, branch_b).join(merge, gather=True)
    result = p.run("hello")
    assert result == [["clean(A:HELLO)", "clean(B:HELLO)"]], result


# ---------------------------------------------------------------------------
# Test 3: branch(Node, Pipeline) — mixed
# ---------------------------------------------------------------------------

def test_branch_mixed():
    branch_b = Pipeline("branch_b_mixed").add_nodes(enrich_b, clean_b)

    p = Pipeline("test_mixed")
    p.add_node(parse).branch(enrich_a, branch_b).join(merge, gather=True)
    result = p.run("hello")
    assert result == [["A:HELLO", "clean(B:HELLO)"]], result


# ---------------------------------------------------------------------------
# Test 4: nested branches (Pipeline branch containing its own branch/join)
# ---------------------------------------------------------------------------

@node
def tag_x(ctx: Context):
    return f"X:{ctx.input}"


@node
def tag_y(ctx: Context):
    return f"Y:{ctx.input}"


@node(gather=True)
def inner_merge(ctx: Context):
    return f"inner[{','.join(sorted(ctx.input))}]"


@node(gather=True)
def outer_merge(ctx: Context):
    return sorted(ctx.input)


def test_branch_nested():
    inner = (
        Pipeline("inner")
        .add_node(enrich_a)
        .branch(tag_x, tag_y)
        .join(inner_merge, gather=True)
    )

    p = Pipeline("test_nested")
    p.add_node(parse).branch(inner, enrich_b).join(outer_merge, gather=True)
    result = p.run("hello")
    assert result == [["B:HELLO", "inner[X:A:HELLO,Y:A:HELLO]"]], result


# ---------------------------------------------------------------------------
# Test 5: regression — linear pipeline
# ---------------------------------------------------------------------------

@node
def upper(ctx: Context):
    return ctx.input.upper()


@node
def wrap(ctx: Context):
    return f"[{ctx.input}]"


def test_linear_regression():
    p = Pipeline("test_linear")
    p.add_nodes(upper, wrap)
    assert p.run("hello") == ["[HELLO]"]


if __name__ == "__main__":
    print("test_branch_plain_nodes ...", end=" ")
    test_branch_plain_nodes()
    print("OK")

    print("test_branch_pipeline_branches ...", end=" ")
    test_branch_pipeline_branches()
    print("OK")

    print("test_branch_mixed ...", end=" ")
    test_branch_mixed()
    print("OK")

    print("test_branch_nested ...", end=" ")
    test_branch_nested()
    print("OK")

    print("test_linear_regression ...", end=" ")
    test_linear_regression()
    print("OK")

    print("\nAll tests passed.")
