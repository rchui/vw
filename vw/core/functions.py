"""ANSI SQL standard function namespace.

This module provides standard SQL functions (SQL-92, SQL:2003) via the Functions class.
Functions are instantiated with factories to create dialect-specific expression wrappers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from vw.core.base import FactoryT

if TYPE_CHECKING:
    from vw.core.base import ExprT, Factories


class Functions(FactoryT):
    """Base function namespace for ANSI SQL standard functions (SQL-92, SQL:2003).

    All aggregate functions can be used as window functions via .over():
        F.sum(col("amount")).over(partition_by=[col("customer_id")])

    Window-only functions must use .over() to add window specification:
        F.row_number().over(order_by=[col("created_at").desc()])
    """

    def __init__(self, factories: Factories) -> None:
        """Initialize with factories for wrapping states.

        Args:
            factories: Factories for creating Expression, RowSet, SetOperation instances.
        """
        self.factories = factories

    # --- Aggregate Functions (SQL-92) -------------------------------------- #

    def count(self, expr: ExprT | None = None, *, distinct: bool = False) -> ExprT:
        """Create a COUNT aggregate function.

        Args:
            expr: Expression to count (None for COUNT(*)).
            distinct: Whether to count distinct values (COUNT(DISTINCT ...)).

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.count()  # COUNT(*)
            >>> F.count(col("id"))  # COUNT(id)
            >>> F.count(col("id"), distinct=True)  # COUNT(DISTINCT id)
        """
        from vw.core.states import Function

        if expr is None:
            # COUNT(*)
            state = Function(name="COUNT(*)" if not distinct else "COUNT(DISTINCT *)", args=())
        elif distinct:
            # COUNT(DISTINCT expr)
            state = Function(name="COUNT(DISTINCT", args=(expr.state,))
        else:
            # COUNT(expr)
            state = Function(name="COUNT", args=(expr.state,))

        return self.factories.expr(state=state, factories=self.factories)

    def sum(self, expr: ExprT) -> ExprT:
        """Create a SUM aggregate function.

        Args:
            expr: Expression to sum.

        Returns:
            An Expression wrapping a Function state.

        Example:
            >>> F.sum(col("amount"))  # SUM(amount)
        """
        from vw.core.states import Function

        state = Function(name="SUM", args=(expr.state,))
        return self.factories.expr(state=state, factories=self.factories)

    def avg(self, expr: ExprT) -> ExprT:
        """Create an AVG aggregate function.

        Args:
            expr: Expression to average.

        Returns:
            An Expression wrapping a Function state.

        Example:
            >>> F.avg(col("price"))  # AVG(price)
        """
        from vw.core.states import Function

        state = Function(name="AVG", args=(expr.state,))
        return self.factories.expr(state=state, factories=self.factories)

    def min(self, expr: ExprT) -> ExprT:
        """Create a MIN aggregate function.

        Args:
            expr: Expression to find minimum of.

        Returns:
            An Expression wrapping a Function state.

        Example:
            >>> F.min(col("price"))  # MIN(price)
        """
        from vw.core.states import Function

        state = Function(name="MIN", args=(expr.state,))
        return self.factories.expr(state=state, factories=self.factories)

    def max(self, expr: ExprT) -> ExprT:
        """Create a MAX aggregate function.

        Args:
            expr: Expression to find maximum of.

        Returns:
            An Expression wrapping a Function state.

        Example:
            >>> F.max(col("price"))  # MAX(price)
        """
        from vw.core.states import Function

        state = Function(name="MAX", args=(expr.state,))
        return self.factories.expr(state=state, factories=self.factories)

    # --- Window-Only Functions (SQL:2003) ---------------------------------- #

    def row_number(self) -> ExprT:
        """Create a ROW_NUMBER window function.

        Must be used with .over() to add window specification.

        Returns:
            An Expression wrapping a Function state.

        Example:
            >>> F.row_number().over(order_by=[col("created_at").desc()])
        """
        from vw.core.states import Function

        state = Function(name="ROW_NUMBER", args=())
        return self.factories.expr(state=state, factories=self.factories)

    def rank(self) -> ExprT:
        """Create a RANK window function.

        Must be used with .over() to add window specification.

        Returns:
            An Expression wrapping a Function state.

        Example:
            >>> F.rank().over(order_by=[col("score").desc()])
        """
        from vw.core.states import Function

        state = Function(name="RANK", args=())
        return self.factories.expr(state=state, factories=self.factories)

    def dense_rank(self) -> ExprT:
        """Create a DENSE_RANK window function.

        Must be used with .over() to add window specification.

        Returns:
            An Expression wrapping a Function state.

        Example:
            >>> F.dense_rank().over(order_by=[col("score").desc()])
        """
        from vw.core.states import Function

        state = Function(name="DENSE_RANK", args=())
        return self.factories.expr(state=state, factories=self.factories)

    def ntile(self, n: int) -> ExprT:
        """Create an NTILE window function.

        Must be used with .over() to add window specification.

        Args:
            n: Number of buckets to divide rows into.

        Returns:
            An Expression wrapping a Function state.

        Example:
            >>> F.ntile(4).over(order_by=[col("salary").desc()])
        """
        from vw.core.states import Function

        # Store n as an integer argument
        state = Function(name="NTILE", args=(n,))
        return self.factories.expr(state=state, factories=self.factories)

    # --- Offset Functions (SQL:2003) --------------------------------------- #

    def lag(self, expr: ExprT, offset: int = 1, default: ExprT | None = None) -> ExprT:
        """Create a LAG window function.

        Must be used with .over() to add window specification (requires ORDER BY).

        Args:
            expr: Expression to get previous value of.
            offset: Number of rows back (default 1).
            default: Default value when there is no previous row.

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.lag(col("price")).over(order_by=[col("date")])
            >>> F.lag(col("price"), 2).over(order_by=[col("date")])
            >>> F.lag(col("price"), 1, param("default", 0)).over(order_by=[col("date")])
        """
        from vw.core.states import Function

        # LAG(expr, offset, default)
        args: list[object] = [expr.state]
        if offset != 1 or default is not None:
            args.append(offset)
        if default is not None:
            args.append(default.state)

        state = Function(name="LAG", args=tuple(args))
        return self.factories.expr(state=state, factories=self.factories)

    def lead(self, expr: ExprT, offset: int = 1, default: ExprT | None = None) -> ExprT:
        """Create a LEAD window function.

        Must be used with .over() to add window specification (requires ORDER BY).

        Args:
            expr: Expression to get next value of.
            offset: Number of rows forward (default 1).
            default: Default value when there is no next row.

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.lead(col("price")).over(order_by=[col("date")])
            >>> F.lead(col("price"), 2).over(order_by=[col("date")])
            >>> F.lead(col("price"), 1, param("default", 0)).over(order_by=[col("date")])
        """
        from vw.core.states import Function

        # LEAD(expr, offset, default)
        args: list[object] = [expr.state]
        if offset != 1 or default is not None:
            args.append(offset)
        if default is not None:
            args.append(default.state)

        state = Function(name="LEAD", args=tuple(args))
        return self.factories.expr(state=state, factories=self.factories)

    def first_value(self, expr: ExprT) -> ExprT:
        """Create a FIRST_VALUE window function.

        Must be used with .over() to add window specification (requires ORDER BY).

        Args:
            expr: Expression to get first value of.

        Returns:
            An Expression wrapping a Function state.

        Example:
            >>> F.first_value(col("price")).over(order_by=[col("date")])
        """
        from vw.core.states import Function

        state = Function(name="FIRST_VALUE", args=(expr.state,))
        return self.factories.expr(state=state, factories=self.factories)

    def last_value(self, expr: ExprT) -> ExprT:
        """Create a LAST_VALUE window function.

        Must be used with .over() to add window specification (requires ORDER BY).

        Args:
            expr: Expression to get last value of.

        Returns:
            An Expression wrapping a Function state.

        Example:
            >>> F.last_value(col("price")).over(order_by=[col("date")])
        """
        from vw.core.states import Function

        state = Function(name="LAST_VALUE", args=(expr.state,))
        return self.factories.expr(state=state, factories=self.factories)

    # --- Date/Time Functions (SQL-92) -------------------------------------- #

    def current_timestamp(self) -> ExprT:
        """CURRENT_TIMESTAMP — returns current date and time."""
        from vw.core.states import CurrentTimestamp

        return self.factories.expr(state=CurrentTimestamp(), factories=self.factories)

    def current_date(self) -> ExprT:
        """CURRENT_DATE — returns current date."""
        from vw.core.states import CurrentDate

        return self.factories.expr(state=CurrentDate(), factories=self.factories)

    def current_time(self) -> ExprT:
        """CURRENT_TIME — returns current time."""
        from vw.core.states import CurrentTime

        return self.factories.expr(state=CurrentTime(), factories=self.factories)

    # --- Grouping Functions (SQL:1999) ------------------------------------- #

    def grouping(self, *exprs: ExprT) -> ExprT:
        """GROUPING() function for identifying grouping level in GROUPING SETS/CUBE/ROLLUP.

        Args:
            *exprs: Column expressions to check grouping status.

        Returns:
            An Expression wrapping a Function state.

        Example:
            >>> F.grouping(col("region"), col("product"))
        """
        from vw.core.states import Function

        state = Function(name="GROUPING", args=tuple(e.state for e in exprs))
        return self.factories.expr(state=state, factories=self.factories)

    # --- Null Handling Functions (SQL-92) ---------------------------------- #

    def coalesce(self, *exprs: ExprT) -> ExprT:
        """Create a COALESCE() function. Returns first non-NULL value.

        Args:
            *exprs: Two or more expressions to evaluate in order.

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.coalesce(col("nickname"), col("name"))
            >>> F.coalesce(col("a"), col("b"), param("default", "unknown"))
        """
        from vw.core.states import Function

        state = Function(name="COALESCE", args=tuple(e.state for e in exprs))
        return self.factories.expr(state=state, factories=self.factories)

    def nullif(self, expr1: ExprT, expr2: ExprT) -> ExprT:
        """Create a NULLIF() function. Returns NULL if expr1 == expr2.

        Args:
            expr1: First expression.
            expr2: Second expression to compare against.

        Returns:
            An Expression wrapping a Function state.

        Example:
            >>> F.nullif(col("value"), param("sentinel", 0))
        """
        from vw.core.states import Function

        state = Function(name="NULLIF", args=(expr1.state, expr2.state))
        return self.factories.expr(state=state, factories=self.factories)

    def greatest(self, *exprs: ExprT) -> ExprT:
        """Create a GREATEST() function. Returns largest non-NULL value.

        Args:
            *exprs: Two or more expressions to compare.

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.greatest(col("a"), col("b"))
            >>> F.greatest(col("a"), col("b"), col("c"))
        """
        from vw.core.states import Function

        state = Function(name="GREATEST", args=tuple(e.state for e in exprs))
        return self.factories.expr(state=state, factories=self.factories)

    def least(self, *exprs: ExprT) -> ExprT:
        """Create a LEAST() function. Returns smallest non-NULL value.

        Args:
            *exprs: Two or more expressions to compare.

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.least(col("a"), col("b"))
            >>> F.least(col("a"), col("b"), col("c"))
        """
        from vw.core.states import Function

        state = Function(name="LEAST", args=tuple(e.state for e in exprs))
        return self.factories.expr(state=state, factories=self.factories)
