"""Shared function mixins for cross-dialect compatibility."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vw.core.base import FactoryT

if TYPE_CHECKING:
    from vw.core.base import ExprT, Factories, RowSetT


class ArrayAggMixin(FactoryT):
    """Mixin for ARRAY_AGG function (PostgreSQL, DuckDB)."""

    factories: Factories[ExprT, RowSetT]

    def array_agg(self, expr: ExprT, *, distinct: bool = False, order_by: list[ExprT] | None = None) -> ExprT:
        """ARRAY_AGG() — aggregate values into an array.

        Args:
            expr: Expression to aggregate.
            distinct: Whether to aggregate distinct values only.
            order_by: Optional ORDER BY expressions (inside function).

        Examples:
            >>> F.array_agg(col("name"))
            >>> F.array_agg(col("name"), distinct=True)
            >>> F.array_agg(col("name"), order_by=[col("name").asc()])
            >>> F.array_agg(col("tag"), distinct=True, order_by=[col("tag")])
        """
        from vw.core.states import Function

        order_by_tuple = tuple(e.state for e in order_by) if order_by else ()
        state = Function(name="ARRAY_AGG", args=(expr.state,), distinct=distinct, order_by=order_by_tuple)
        return self.factories.expr(state=state, factories=self.factories)


class UnnestMixin(FactoryT):
    """Mixin for UNNEST function (PostgreSQL, DuckDB)."""

    factories: Factories[ExprT, RowSetT]

    def unnest(self, array: ExprT) -> ExprT:
        """UNNEST() — expand array/list to set of rows.

        Can be used in SELECT clause. For FROM clause usage, use raw.rowset().

        Example:
            >>> ref("data").select(F.unnest(col("array_col")))
            >>> # For FROM: raw.rowset("unnest({arr}) AS t(elem)", arr=...)
        """
        from vw.core.states import Function

        state = Function(name="UNNEST", args=(array.state,))
        return self.factories.expr(state=state, factories=self.factories)


class StringAggMixin(FactoryT):
    """Mixin for STRING_AGG function (PostgreSQL, DuckDB)."""

    factories: Factories[ExprT, RowSetT]

    def string_agg(self, expr: ExprT, separator: ExprT, *, order_by: list[ExprT] | None = None) -> ExprT:
        """STRING_AGG() — concatenate values with separator.

        Args:
            expr: Expression to aggregate.
            separator: Separator string (typically lit() for constants).
            order_by: Optional ORDER BY expressions (inside function).

        Examples:
            >>> F.string_agg(col("name"), lit(", "))
            >>> F.string_agg(col("name"), lit(", "), order_by=[col("name")])
        """
        from vw.core.states import Function

        order_by_tuple = tuple(e.state for e in order_by) if order_by else ()
        state = Function(name="STRING_AGG", args=(expr.state, separator.state), order_by=order_by_tuple)
        return self.factories.expr(state=state, factories=self.factories)


class JsonAggMixin(FactoryT):
    """Mixin for JSON_AGG function (PostgreSQL, DuckDB)."""

    factories: Factories[ExprT, RowSetT]

    def json_agg(self, expr: ExprT, *, order_by: list[ExprT] | None = None) -> ExprT:
        """JSON_AGG() — aggregate values into a JSON array.

        Args:
            expr: Expression to aggregate.
            order_by: Optional ORDER BY expressions (inside function).

        Examples:
            >>> F.json_agg(col("user_data"))
            >>> F.json_agg(col("name"), order_by=[col("created_at")])
            >>> F.json_agg(col("data"), order_by=[col("created_at")])
        """
        from vw.core.states import Function

        order_by_tuple = tuple(e.state for e in order_by) if order_by else ()
        state = Function(name="JSON_AGG", args=(expr.state,), order_by=order_by_tuple)
        return self.factories.expr(state=state, factories=self.factories)


class BitAndMixin(FactoryT):
    """Mixin for BIT_AND function (PostgreSQL, DuckDB)."""

    factories: Factories[ExprT, RowSetT]

    def bit_and(self, expr: ExprT) -> ExprT:
        """BIT_AND(expr) — Bitwise AND aggregate.

        Computes the bitwise AND of all non-null input values.

        Args:
            expr: Expression to aggregate (typically an integer column).

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.bit_and(col("flags"))  # BIT_AND(flags)
            >>> F.bit_and(col("permissions")).alias("combined_perms")
        """
        from vw.core.states import Function

        state = Function(name="BIT_AND", args=(expr.state,))
        return self.factories.expr(state=state, factories=self.factories)


class BitOrMixin(FactoryT):
    """Mixin for BIT_OR function (PostgreSQL, DuckDB)."""

    factories: Factories[ExprT, RowSetT]

    def bit_or(self, expr: ExprT) -> ExprT:
        """BIT_OR(expr) — Bitwise OR aggregate.

        Computes the bitwise OR of all non-null input values.

        Args:
            expr: Expression to aggregate (typically an integer column).

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.bit_or(col("flags"))  # BIT_OR(flags)
            >>> F.bit_or(col("permissions")).alias("combined_perms")
        """
        from vw.core.states import Function

        state = Function(name="BIT_OR", args=(expr.state,))
        return self.factories.expr(state=state, factories=self.factories)


class BoolAndMixin(FactoryT):
    """Mixin for BOOL_AND function (PostgreSQL, DuckDB)."""

    factories: Factories[ExprT, RowSetT]

    def bool_and(self, expr: ExprT) -> ExprT:
        """BOOL_AND(expr) — Boolean AND aggregate (also known as EVERY).

        Returns true if all input values are true, otherwise false.

        Args:
            expr: Expression to aggregate (boolean expression or column).

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.bool_and(col("is_active"))  # BOOL_AND(is_active)
            >>> F.bool_and(col("verified")).alias("all_verified")
        """
        from vw.core.states import Function

        state = Function(name="BOOL_AND", args=(expr.state,))
        return self.factories.expr(state=state, factories=self.factories)


class BoolOrMixin(FactoryT):
    """Mixin for BOOL_OR function (PostgreSQL, DuckDB)."""

    factories: Factories[ExprT, RowSetT]

    def bool_or(self, expr: ExprT) -> ExprT:
        """BOOL_OR(expr) — Boolean OR aggregate.

        Returns true if at least one input value is true, otherwise false.

        Args:
            expr: Expression to aggregate (boolean expression or column).

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.bool_or(col("needs_review"))  # BOOL_OR(needs_review)
            >>> F.bool_or(col("has_error")).alias("any_errors")
        """
        from vw.core.states import Function

        state = Function(name="BOOL_OR", args=(expr.state,))
        return self.factories.expr(state=state, factories=self.factories)
