"""SQL functions including window functions.

This module provides SQL functions that can be used in SELECT clauses.
All functions have an .over() method that adds the OVER clause to create
a window function.

Example:
    >>> from vw import col, Source
    >>> from vw.functions import row_number, sum_
    >>>
    >>> # Window function with row_number
    >>> stmt = Source(name="orders").select(
    ...     col("id"),
    ...     row_number().over(order_by=[col("created_at").desc()])
    ... )
    >>>
    >>> # Aggregate as window function
    >>> stmt = Source(name="orders").select(
    ...     col("id"),
    ...     sum_(col("amount")).over(partition_by=[col("customer_id")])
    ... )
"""

from __future__ import annotations

from dataclasses import dataclass, field

from vw.base import Expression
from vw.render import RenderContext


@dataclass(kw_only=True, frozen=True)
class WindowFunction(Expression):
    """A function with an OVER clause for window operations.

    This is the result of calling .over() on a Function. It contains
    the window specification (PARTITION BY, ORDER BY, frame).

    Note: Frame clauses (ROWS/RANGE BETWEEN) are a future enhancement.
    """

    function: Function
    partition_by: list[Expression] = field(default_factory=list)
    order_by: list[Expression] = field(default_factory=list)
    # Future: frame clause support (ROWS BETWEEN, RANGE BETWEEN)

    def __vw_render__(self, context: RenderContext) -> str:
        """Render the window function with OVER clause."""
        func_sql = self.function._render_call(context)

        over_parts: list[str] = []
        if self.partition_by:
            cols = ", ".join(col.__vw_render__(context) for col in self.partition_by)
            over_parts.append(f"PARTITION BY {cols}")
        if self.order_by:
            cols = ", ".join(col.__vw_render__(context) for col in self.order_by)
            over_parts.append(f"ORDER BY {cols}")

        over_clause = " ".join(over_parts)
        return f"{func_sql} OVER ({over_clause})"


@dataclass(kw_only=True, frozen=True)
class Function(Expression):
    """Base class for SQL functions.

    Functions can be used directly as expressions, or converted to
    window functions by calling .over().

    Attributes:
        name: The SQL function name (e.g., "ROW_NUMBER", "SUM").
        args: Arguments passed to the function.
    """

    name: str
    args: list[Expression] = field(default_factory=list)

    def over(
        self,
        *,
        partition_by: list[Expression] | None = None,
        order_by: list[Expression] | None = None,
    ) -> WindowFunction:
        """Convert this function to a window function with OVER clause.

        Args:
            partition_by: Columns to partition by.
            order_by: Columns to order by within each partition.

        Returns:
            A WindowFunction with the specified window specification.

        Example:
            >>> row_number().over(order_by=[col("id").asc()])
            >>> sum_(col("amount")).over(partition_by=[col("customer_id")])
        """
        return WindowFunction(
            function=self,
            partition_by=partition_by or [],
            order_by=order_by or [],
        )

    def _render_call(self, context: RenderContext) -> str:
        """Render just the function call without alias."""
        if self.args:
            args_sql = ", ".join(arg.__vw_render__(context) for arg in self.args)
            return f"{self.name}({args_sql})"
        return f"{self.name}()"

    def __vw_render__(self, context: RenderContext) -> str:
        """Render the function call."""
        return self._render_call(context)


# -----------------------------------------------------------------------------
# Window-only functions (these only make sense with OVER clause)
# -----------------------------------------------------------------------------


def row_number() -> Function:
    """Create a ROW_NUMBER() function.

    Returns sequential row numbers within a partition.

    Example:
        >>> row_number().over(order_by=[col("created_at").desc()])
    """
    return Function(name="ROW_NUMBER")


def rank() -> Function:
    """Create a RANK() function.

    Returns the rank within a partition, with gaps for ties.

    Example:
        >>> rank().over(partition_by=[col("dept")], order_by=[col("salary").desc()])
    """
    return Function(name="RANK")


def dense_rank() -> Function:
    """Create a DENSE_RANK() function.

    Returns the rank within a partition, without gaps for ties.

    Example:
        >>> dense_rank().over(partition_by=[col("dept")], order_by=[col("salary").desc()])
    """
    return Function(name="DENSE_RANK")


def ntile(n: int) -> Function:
    """Create an NTILE(n) function.

    Divides rows into n roughly equal groups.

    Args:
        n: Number of groups to divide into.

    Example:
        >>> ntile(4).over(order_by=[col("score").desc()])  # Quartiles
    """
    from vw.column import col

    return Function(name="NTILE", args=[col(str(n))])


# -----------------------------------------------------------------------------
# Aggregate functions (can be used as aggregates or window functions)
# -----------------------------------------------------------------------------


def sum_(expr: Expression) -> Function:
    """Create a SUM() aggregate function.

    Note: Named sum_ to avoid shadowing Python's built-in sum.

    Args:
        expr: Expression to sum.

    Example:
        >>> sum_(col("amount"))  # As aggregate
        >>> sum_(col("amount")).over(partition_by=[col("customer_id")])  # As window
    """
    return Function(name="SUM", args=[expr])


def count(expr: Expression | None = None) -> Function:
    """Create a COUNT() aggregate function.

    Args:
        expr: Expression to count, or None for COUNT(*).

    Example:
        >>> count()  # COUNT(*)
        >>> count(col("id"))  # COUNT(id)
        >>> count(col("id")).over(partition_by=[col("dept")])  # As window
    """
    if expr is None:
        from vw.column import col

        return Function(name="COUNT", args=[col("*")])
    return Function(name="COUNT", args=[expr])


def avg(expr: Expression) -> Function:
    """Create an AVG() aggregate function.

    Args:
        expr: Expression to average.

    Example:
        >>> avg(col("price"))  # As aggregate
        >>> avg(col("price")).over(partition_by=[col("category")])  # As window
    """
    return Function(name="AVG", args=[expr])


def min_(expr: Expression) -> Function:
    """Create a MIN() aggregate function.

    Note: Named min_ to avoid shadowing Python's built-in min.

    Args:
        expr: Expression to find minimum of.

    Example:
        >>> min_(col("price"))  # As aggregate
        >>> min_(col("price")).over(partition_by=[col("category")])  # As window
    """
    return Function(name="MIN", args=[expr])


def max_(expr: Expression) -> Function:
    """Create a MAX() aggregate function.

    Note: Named max_ to avoid shadowing Python's built-in max.

    Args:
        expr: Expression to find maximum of.

    Example:
        >>> max_(col("price"))  # As aggregate
        >>> max_(col("price")).over(partition_by=[col("category")])  # As window
    """
    return Function(name="MAX", args=[expr])


# -----------------------------------------------------------------------------
# Offset functions (require ORDER BY in OVER clause)
# -----------------------------------------------------------------------------


def lag(expr: Expression, offset: int = 1, default: Expression | None = None) -> Function:
    """Create a LAG() function.

    Access a row at a given offset before the current row.

    Args:
        expr: Expression to retrieve.
        offset: Number of rows back (default 1).
        default: Default value if offset goes out of bounds.

    Example:
        >>> lag(col("price")).over(order_by=[col("date").asc()])
        >>> lag(col("price"), 2).over(order_by=[col("date").asc()])
    """
    from vw.column import col

    args: list[Expression] = [expr, col(str(offset))]
    if default is not None:
        args.append(default)
    return Function(name="LAG", args=args)


def lead(expr: Expression, offset: int = 1, default: Expression | None = None) -> Function:
    """Create a LEAD() function.

    Access a row at a given offset after the current row.

    Args:
        expr: Expression to retrieve.
        offset: Number of rows forward (default 1).
        default: Default value if offset goes out of bounds.

    Example:
        >>> lead(col("price")).over(order_by=[col("date").asc()])
        >>> lead(col("price"), 2).over(order_by=[col("date").asc()])
    """
    from vw.column import col

    args: list[Expression] = [expr, col(str(offset))]
    if default is not None:
        args.append(default)
    return Function(name="LEAD", args=args)


def first_value(expr: Expression) -> Function:
    """Create a FIRST_VALUE() function.

    Returns the first value in the window frame.

    Args:
        expr: Expression to retrieve.

    Example:
        >>> first_value(col("price")).over(
        ...     partition_by=[col("category")],
        ...     order_by=[col("date").asc()]
        ... )
    """
    return Function(name="FIRST_VALUE", args=[expr])


def last_value(expr: Expression) -> Function:
    """Create a LAST_VALUE() function.

    Returns the last value in the window frame.

    Args:
        expr: Expression to retrieve.

    Example:
        >>> last_value(col("price")).over(
        ...     partition_by=[col("category")],
        ...     order_by=[col("date").asc()]
        ... )
    """
    return Function(name="LAST_VALUE", args=[expr])


__all__ = [
    # Classes
    "Function",
    "WindowFunction",
    # Window-only functions
    "row_number",
    "rank",
    "dense_rank",
    "ntile",
    # Aggregate/window functions
    "sum_",
    "count",
    "avg",
    "min_",
    "max_",
    # Offset functions
    "lag",
    "lead",
    "first_value",
    "last_value",
]
