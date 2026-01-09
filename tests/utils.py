"""Shared test utilities."""


def sql(query: str) -> str:
    """Convert a multi-line SQL string to a single-line string for comparison.

    Strips each line individually before joining, so varying indentation levels
    don't affect the result. Also normalizes spacing around parentheses.
    """
    lines = [line.strip() for line in query.strip().splitlines()]
    result = " ".join(line for line in lines if line)
    # Normalize spacing around parentheses
    result = result.replace("( ", "(").replace(" )", ")")
    return result
