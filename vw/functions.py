"""SQL functions including window functions.

This module provides SQL functions via the F namespace.
All functions have an .over() method that adds the OVER clause to create
a window function.

Example:
    >>> from vw import col, Source
    >>> from vw.functions import F
    >>>
    >>> # Window function with row_number
    >>> stmt = Source(name="orders").select(
    ...     col("id"),
    ...     F.row_number().over(order_by=[col("created_at").desc()])
    ... )
    >>>
    >>> # Aggregate as window function
    >>> stmt = Source(name="orders").select(
    ...     col("id"),
    ...     F.sum(col("amount")).over(partition_by=[col("customer_id")])
    ... )
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

from strenum import StrEnum

from vw.base import Expression
from vw.frame import FrameBoundary
from vw.render import RenderContext


class FrameMode(StrEnum):
    """Window frame mode."""

    ROWS = "ROWS"
    RANGE = "RANGE"


@dataclass(kw_only=True, frozen=True)
class FrameClause:
    """Window frame clause specification."""

    mode: FrameMode
    start: FrameBoundary
    end: FrameBoundary

    def __str__(self) -> str:
        return f"{self.mode} BETWEEN {self.start} AND {self.end}"


@dataclass(kw_only=True, frozen=True)
class WindowFunction(Expression):
    """A function with an OVER clause for window operations.

    This is the result of calling .over() on a Function. It contains
    the window specification (PARTITION BY, ORDER BY, frame).
    """

    function: Function
    partition_by: list[Expression] = field(default_factory=list)
    order_by: list[Expression] = field(default_factory=list)
    _frame: FrameClause | None = None

    def rows_between(self, start: FrameBoundary, end: FrameBoundary) -> WindowFunction:
        """Add ROWS BETWEEN frame clause.

        Args:
            start: The start boundary of the frame.
            end: The end boundary of the frame.

        Returns:
            A new WindowFunction with the frame clause.

        Example:
            >>> F.sum(col("amount")).over(order_by=[col("date")]).rows_between(
            ...     frame.UNBOUNDED_PRECEDING, frame.CURRENT_ROW
            ... )
        """
        return replace(self, _frame=FrameClause(mode=FrameMode.ROWS, start=start, end=end))

    def range_between(self, start: FrameBoundary, end: FrameBoundary) -> WindowFunction:
        """Add RANGE BETWEEN frame clause.

        Args:
            start: The start boundary of the frame.
            end: The end boundary of the frame.

        Returns:
            A new WindowFunction with the frame clause.

        Example:
            >>> F.sum(col("amount")).over(order_by=[col("date")]).range_between(
            ...     frame.UNBOUNDED_PRECEDING, frame.CURRENT_ROW
            ... )
        """
        return replace(self, _frame=FrameClause(mode=FrameMode.RANGE, start=start, end=end))

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
        if self._frame:
            over_parts.append(str(self._frame))

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
            >>> F.row_number().over(order_by=[col("id").asc()])
            >>> F.sum(col("amount")).over(partition_by=[col("customer_id")])
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


class F:
    """SQL functions namespace.

    Provides access to SQL functions via static methods.

    Example:
        >>> from vw.functions import F
        >>> F.sum(col("amount"))
        >>> F.count()
        >>> F.row_number().over(order_by=[col("id").asc()])
    """

    # -------------------------------------------------------------------------
    # Window-only functions (these only make sense with OVER clause)
    # -------------------------------------------------------------------------

    @staticmethod
    def row_number() -> Function:
        """Create a ROW_NUMBER() function.

        Returns sequential row numbers within a partition.

        Example:
            >>> F.row_number().over(order_by=[col("created_at").desc()])
        """
        return Function(name="ROW_NUMBER")

    @staticmethod
    def rank() -> Function:
        """Create a RANK() function.

        Returns the rank within a partition, with gaps for ties.

        Example:
            >>> F.rank().over(partition_by=[col("dept")], order_by=[col("salary").desc()])
        """
        return Function(name="RANK")

    @staticmethod
    def dense_rank() -> Function:
        """Create a DENSE_RANK() function.

        Returns the rank within a partition, without gaps for ties.

        Example:
            >>> F.dense_rank().over(partition_by=[col("dept")], order_by=[col("salary").desc()])
        """
        return Function(name="DENSE_RANK")

    @staticmethod
    def ntile(n: int) -> Function:
        """Create an NTILE(n) function.

        Divides rows into n roughly equal groups.

        Args:
            n: Number of groups to divide into.

        Example:
            >>> F.ntile(4).over(order_by=[col("score").desc()])  # Quartiles
        """
        from vw.column import col

        return Function(name="NTILE", args=[col(str(n))])

    # -------------------------------------------------------------------------
    # Aggregate functions (can be used as aggregates or window functions)
    # -------------------------------------------------------------------------

    @staticmethod
    def sum(expr: Expression) -> Function:
        """Create a SUM() aggregate function.

        Args:
            expr: Expression to sum.

        Example:
            >>> F.sum(col("amount"))  # As aggregate
            >>> F.sum(col("amount")).over(partition_by=[col("customer_id")])  # As window
        """
        return Function(name="SUM", args=[expr])

    @staticmethod
    def count(expr: Expression | None = None) -> Function:
        """Create a COUNT() aggregate function.

        Args:
            expr: Expression to count, or None for COUNT(*).

        Example:
            >>> F.count()  # COUNT(*)
            >>> F.count(col("id"))  # COUNT(id)
            >>> F.count(col("id")).over(partition_by=[col("dept")])  # As window
        """
        if expr is None:
            from vw.column import col

            return Function(name="COUNT", args=[col("*")])
        return Function(name="COUNT", args=[expr])

    @staticmethod
    def avg(expr: Expression) -> Function:
        """Create an AVG() aggregate function.

        Args:
            expr: Expression to average.

        Example:
            >>> F.avg(col("price"))  # As aggregate
            >>> F.avg(col("price")).over(partition_by=[col("category")])  # As window
        """
        return Function(name="AVG", args=[expr])

    @staticmethod
    def min(expr: Expression) -> Function:
        """Create a MIN() aggregate function.

        Args:
            expr: Expression to find minimum of.

        Example:
            >>> F.min(col("price"))  # As aggregate
            >>> F.min(col("price")).over(partition_by=[col("category")])  # As window
        """
        return Function(name="MIN", args=[expr])

    @staticmethod
    def max(expr: Expression) -> Function:
        """Create a MAX() aggregate function.

        Args:
            expr: Expression to find maximum of.

        Example:
            >>> F.max(col("price"))  # As aggregate
            >>> F.max(col("price")).over(partition_by=[col("category")])  # As window
        """
        return Function(name="MAX", args=[expr])

    # -------------------------------------------------------------------------
    # Offset functions (require ORDER BY in OVER clause)
    # -------------------------------------------------------------------------

    @staticmethod
    def lag(expr: Expression, offset: int = 1, default: Expression | None = None) -> Function:
        """Create a LAG() function.

        Access a row at a given offset before the current row.

        Args:
            expr: Expression to retrieve.
            offset: Number of rows back (default 1).
            default: Default value if offset goes out of bounds.

        Example:
            >>> F.lag(col("price")).over(order_by=[col("date").asc()])
            >>> F.lag(col("price"), 2).over(order_by=[col("date").asc()])
        """
        from vw.column import col

        args: list[Expression] = [expr, col(str(offset))]
        if default is not None:
            args.append(default)
        return Function(name="LAG", args=args)

    @staticmethod
    def lead(expr: Expression, offset: int = 1, default: Expression | None = None) -> Function:
        """Create a LEAD() function.

        Access a row at a given offset after the current row.

        Args:
            expr: Expression to retrieve.
            offset: Number of rows forward (default 1).
            default: Default value if offset goes out of bounds.

        Example:
            >>> F.lead(col("price")).over(order_by=[col("date").asc()])
            >>> F.lead(col("price"), 2).over(order_by=[col("date").asc()])
        """
        from vw.column import col

        args: list[Expression] = [expr, col(str(offset))]
        if default is not None:
            args.append(default)
        return Function(name="LEAD", args=args)

    @staticmethod
    def first_value(expr: Expression) -> Function:
        """Create a FIRST_VALUE() function.

        Returns the first value in the window frame.

        Args:
            expr: Expression to retrieve.

        Example:
            >>> F.first_value(col("price")).over(
            ...     partition_by=[col("category")],
            ...     order_by=[col("date").asc()]
            ... )
        """
        return Function(name="FIRST_VALUE", args=[expr])

    @staticmethod
    def last_value(expr: Expression) -> Function:
        """Create a LAST_VALUE() function.

        Returns the last value in the window frame.

        Args:
            expr: Expression to retrieve.

        Example:
            >>> F.last_value(col("price")).over(
            ...     partition_by=[col("category")],
            ...     order_by=[col("date").asc()]
            ... )
        """
        return Function(name="LAST_VALUE", args=[expr])

    # -------------------------------------------------------------------------
    # Null handling functions
    # -------------------------------------------------------------------------

    @staticmethod
    def coalesce(*exprs: Expression) -> Function:
        """Create a COALESCE() function.

        Returns the first non-NULL expression from the arguments.

        Args:
            *exprs: Expressions to evaluate in order.

        Example:
            >>> F.coalesce(col("nickname"), col("name"))
            >>> F.coalesce(col("preferred_email"), col("work_email"), col("personal_email"))
        """
        return Function(name="COALESCE", args=list(exprs))

    @staticmethod
    def nullif(expr1: Expression, expr2: Expression) -> Function:
        """Create a NULLIF() function.

        Returns NULL if expr1 equals expr2, otherwise returns expr1.

        Args:
            expr1: The expression to return if not equal to expr2.
            expr2: The expression to compare against.

        Example:
            >>> F.nullif(col("value"), param("empty", ""))
            >>> F.nullif(col("divisor"), col("0"))
        """
        return Function(name="NULLIF", args=[expr1, expr2])

    # -------------------------------------------------------------------------
    # Comparison functions
    # -------------------------------------------------------------------------

    @staticmethod
    def greatest(*exprs: Expression) -> Function:
        """Create a GREATEST() function.

        Returns the largest value from the arguments.

        Args:
            *exprs: Expressions to compare.

        Example:
            >>> F.greatest(col("price"), col("min_price"))
            >>> F.greatest(col("a"), col("b"), col("c"))
        """
        return Function(name="GREATEST", args=list(exprs))

    @staticmethod
    def least(*exprs: Expression) -> Function:
        """Create a LEAST() function.

        Returns the smallest value from the arguments.

        Args:
            *exprs: Expressions to compare.

        Example:
            >>> F.least(col("price"), col("max_price"))
            >>> F.least(col("a"), col("b"), col("c"))
        """
        return Function(name="LEAST", args=list(exprs))


__all__ = [
    "F",
    "Function",
    "WindowFunction",
]
