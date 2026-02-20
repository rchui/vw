"""DuckDB-specific modifier factories for use with .modifiers()."""

from vw.core.base import Factories
from vw.duckdb.base import Expression, RowSet
from vw.duckdb.states import UsingSample


def using_sample(
    *,
    percent: float | None = None,
    rows: int | None = None,
    method: str | None = None,
    seed: int | None = None,
) -> Expression:
    """Create a USING SAMPLE modifier for DuckDB table sampling.

    Exactly one of `percent` or `rows` must be provided.

    Args:
        percent: Sample percentage (0-100).
        rows: Number of rows to sample.
        method: Sampling method ("reservoir", "bernoulli", "system").
        seed: Random seed for reproducibility (REPEATABLE clause).

    Returns:
        An Expression wrapping a UsingSample state, for use with .modifiers().

    Examples:
        >>> ref("big_table").modifiers(using_sample(percent=10))
        >>> ref("big_table").modifiers(using_sample(method="reservoir", rows=1000, seed=42))
    """
    if percent is None and rows is None:
        raise ValueError("using_sample() requires exactly one of: percent or rows")
    if percent is not None and rows is not None:
        raise ValueError("using_sample() accepts only one of: percent or rows (not both)")

    state = UsingSample(percent=percent, rows=rows, method=method, seed=seed)
    return Expression(state=state, factories=Factories(expr=Expression, rowset=RowSet))
