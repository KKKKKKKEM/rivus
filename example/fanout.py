"""Generator / fan-out examples.

Demonstrates how a node can yield multiple values to produce multiple
downstream items from a single upstream input.

Scenarios covered
-----------------
1. Simple fan-out   — one input splits into N independent items
2. Chained fan-out  — two consecutive fan-out nodes (exponential expansion)
3. Fan-out + gather — expand then reduce back to a single aggregated result
"""

import rivus
from rivus import Context, Pipeline, node


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

@node
def split_words(ctx: Context):
    """Split a sentence into individual words."""
    sentence = ctx.input
    for word in sentence.split():
        yield word


@node
def split_chars(ctx: Context):
    """Split a word into individual characters."""
    word = ctx.input
    for ch in word:
        yield ch


@node(workers=4)
def to_upper(ctx: Context):
    return ctx.input.upper()


@node(workers=4)
def add_exclaim(ctx: Context):
    return ctx.input + "!"


@node(gather=True)
def collect(ctx: Context):
    """Gather all upstream results into a sorted list."""
    return sorted(ctx.input)


@node(gather=True)
def join_words(ctx: Context):
    """Gather all upstream results and join as a string."""
    return " ".join(ctx.input)


# ---------------------------------------------------------------------------
# Example 1: Simple fan-out
# One sentence → individual words → uppercased
# ---------------------------------------------------------------------------

def example_simple_fanout():
    p = Pipeline("simple_fanout")
    p.add_nodes(split_words, to_upper, add_exclaim, collect)

    results = p.run("hello world rivus")
    assert results == [["HELLO!", "RIVUS!", "WORLD!"]], results
    print(f"[1] simple_fanout: {results[0]}")


# ---------------------------------------------------------------------------
# Example 2: Chained fan-out
# One sentence → words → individual chars (exponential expansion)
# ---------------------------------------------------------------------------

def example_chained_fanout():
    p = Pipeline("chained_fanout")
    p.add_nodes(split_words, split_chars, collect)

    results = p.run("hi bye")
    # "hi" → h, i    "bye" → b, y, e  (order depends on workers, so sort)
    expected = sorted(list("hibye"))
    assert results == [expected], results
    print(f"[2] chained_fanout: {results[0]}")


# ---------------------------------------------------------------------------
# Example 3: Fan-out + gather (scatter–reduce pattern)
# One sentence → words (fan-out) → upper → gathered back as sentence
# ---------------------------------------------------------------------------

def example_fanout_gather():
    p = Pipeline("fanout_gather")
    p.add_nodes(split_words, to_upper, join_words)

    results = p.run("the quick brown fox")
    # Words arrive in arbitrary order after parallel processing; verify set
    result_words = set(results[0].split())
    assert result_words == {"THE", "QUICK", "BROWN", "FOX"}, results
    print(f"[3] fanout_gather: {results[0]!r}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("example_simple_fanout ...", end=" ")
    example_simple_fanout()
    print("OK")

    print("example_chained_fanout ...", end=" ")
    example_chained_fanout()
    print("OK")

    print("example_fanout_gather ...", end=" ")
    example_fanout_gather()
    print("OK")

    print("\nAll fanout examples passed.")
