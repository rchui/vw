"""Shared test utilities."""

import textwrap


def sql(query: str) -> str:
    """Convert a multi-line SQL string to a single-line string for comparison."""
    return " ".join(textwrap.dedent(query).strip().split())
