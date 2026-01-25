"""Base classes for SQL expression building."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from vw.reference.build import Statement
    from vw.reference.column import Column
    from vw.reference.datetime import DateTimeAccessor
    from vw.reference.joins import Join, JoinAccessor
    from vw.reference.operators import (
        Add,
        Alias,
        And,
        Asc,
        Between,
        Cast,
        Desc,
        Divide,
        Equals,
        GreaterThan,
        GreaterThanOrEqual,
        IsIn,
        IsNotIn,
        IsNotNull,
        IsNull,
        LessThan,
        LessThanOrEqual,
        Like,
        Modulo,
        Multiply,
        Not,
        NotBetween,
        NotEquals,
        NotLike,
        Or,
        Subtract,
    )
    from vw.reference.render import RenderContext
    from vw.reference.star import StarExpression
    from vw.reference.text import TextAccessor

from typing_extensions import Self


@dataclass(kw_only=True, frozen=True)
class RowSet:
    """Base class for things that produce rows (tables, subqueries, CTEs).

    Used in FROM and JOIN clauses.
    """

    _alias: str | None = None
    _joins: list[Join] = field(default_factory=list)

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the row set."""
        raise NotImplementedError

    def alias(self, name: str, /) -> Self:
        """Create an aliased copy of this row set.

        Args:
            name: The alias name.

        Returns:
            A copy of this row set with the alias set.

        Example:
            >>> Source(name="users").alias("u")
            >>> subquery.alias("sq")
        """
        return replace(self, _alias=name)

    def col(self, column_name: str, /) -> Column:
        """Create a column reference qualified with this row set's alias.

        Args:
            column_name: Column name to qualify.

        Returns:
            A Column with the alias as prefix.

        Example:
            >>> Source(name="users").alias("u").col("id")  # Returns Column("u.id")
        """
        from vw.reference.column import Column

        if self._alias:
            return Column(name=f"{self._alias}.{column_name}")
        return Column(name=column_name)

    @property
    def star(self) -> StarExpression:
        """Create a star expression qualified with this row set's alias.

        Returns:
            A StarExpression with the alias as prefix.

        Example:
            >>> Source(name="users").alias("u").star()  # Returns StarExpression(Column("u.*"))
        """
        from vw.reference.column import Column
        from vw.reference.star import StarExpression

        if self._alias:
            return StarExpression(column=Column(name=f"{self._alias}.*"))
        return StarExpression(column=Column(name="*"))

    @property
    def join(self) -> JoinAccessor:
        """Access join operations."""
        from vw.reference.joins import JoinAccessor

        return JoinAccessor(self)

    def select(self, *columns: Expression) -> Statement:
        """
        Select columns from this row set.

        Args:
            *columns: Expression objects to select.

        Returns:
            A Statement object for method chaining.

        Example:
            >>> from vw.reference import col
            >>> Source(name="users").select(col("*"))
            >>> subquery.alias("sq").select(col("*"))
        """
        from vw.reference.build import Statement

        return Statement(source=self, columns=list(columns))


class Expression:
    """Protocol for SQL expressions."""

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the expression."""
        raise NotImplementedError

    def __eq__(self, other: Expression) -> Equals:  # type: ignore[override]
        """Create an equality comparison expression.

        Args:
            other: Expression to compare with.

        Returns:
            An Equals representing the equality comparison.
        """
        from vw.reference.operators import Equals

        return Equals(left=self, right=other)

    def __ne__(self, other: Expression) -> NotEquals:  # type: ignore[override]
        """Create an inequality comparison expression.

        Args:
            other: Expression to compare with.

        Returns:
            A NotEquals representing the inequality comparison.
        """
        from vw.reference.operators import NotEquals

        return NotEquals(left=self, right=other)

    def __lt__(self, other: Expression) -> LessThan:
        """Create a less than comparison expression.

        Args:
            other: Expression to compare with.

        Returns:
            A LessThan representing the less than comparison.
        """
        from vw.reference.operators import LessThan

        return LessThan(left=self, right=other)

    def __le__(self, other: Expression) -> LessThanOrEqual:
        """Create a less than or equal comparison expression.

        Args:
            other: Expression to compare with.

        Returns:
            A LessThanOrEqual representing the less than or equal comparison.
        """
        from vw.reference.operators import LessThanOrEqual

        return LessThanOrEqual(left=self, right=other)

    def __gt__(self, other: Expression) -> GreaterThan:
        """Create a greater than comparison expression.

        Args:
            other: Expression to compare with.

        Returns:
            A GreaterThan representing the greater than comparison.
        """
        from vw.reference.operators import GreaterThan

        return GreaterThan(left=self, right=other)

    def __ge__(self, other: Expression) -> GreaterThanOrEqual:
        """Create a greater than or equal comparison expression.

        Args:
            other: Expression to compare with.

        Returns:
            A GreaterThanOrEqual representing the greater than or equal comparison.
        """
        from vw.reference.operators import GreaterThanOrEqual

        return GreaterThanOrEqual(left=self, right=other)

    def __and__(self, other: Expression) -> And:
        """Create a logical AND expression with another expression

        Args:
            other: The other expression to combine with.

        Returns:
            An And expression representing the logical AND of this and the other expression.

        Example:
            >>> expr1 = col("age") > param("min_age", 18)
            >>> expr2 = col("status") == param("active_status", "active")
            >>> combined_expr = expr1 & expr2
        """
        from vw.reference.operators import And

        return And(left=self, right=other)

    def __or__(self, other: Expression) -> Or:
        """Create a logical OR expression with another expression

        Args:
            other: The other expression to combine with.

        Returns:
            An Or expression representing the logical OR of this and the other expression.

        Example:
            >>> expr1 = col("age") < param("max_age", 65)
            >>> expr2 = col("status") == param("inactive_status", "inactive")
            >>> combined_expr = expr1 | expr2
        """
        from vw.reference.operators import Or

        return Or(left=self, right=other)

    def __invert__(self) -> Not:
        """Create a logical NOT expression

        Returns:
            A Not expression representing the logical NOT of this expression.

        Example:
            >>> expr = col("active") == col("true")
            >>> negated = ~expr
        """
        from vw.reference.operators import Not

        return Not(operand=self)

    def __add__(self, other: Expression) -> Add:
        """Create an addition expression.

        Args:
            other: Expression to add.

        Returns:
            An Add expression representing the addition.

        Example:
            >>> col("price") + col("tax")
        """
        from vw.reference.operators import Add

        return Add(left=self, right=other)

    def __sub__(self, other: Expression) -> Subtract:
        """Create a subtraction expression.

        Args:
            other: Expression to subtract.

        Returns:
            A Subtract expression representing the subtraction.

        Example:
            >>> col("total") - col("discount")
        """
        from vw.reference.operators import Subtract

        return Subtract(left=self, right=other)

    def __mul__(self, other: Expression) -> Multiply:
        """Create a multiplication expression.

        Args:
            other: Expression to multiply by.

        Returns:
            A Multiply expression representing the multiplication.

        Example:
            >>> col("price") * col("quantity")
        """
        from vw.reference.operators import Multiply

        return Multiply(left=self, right=other)

    def __truediv__(self, other: Expression) -> Divide:
        """Create a division expression.

        Args:
            other: Expression to divide by.

        Returns:
            A Divide expression representing the division.

        Example:
            >>> col("total") / col("count")
        """
        from vw.reference.operators import Divide

        return Divide(left=self, right=other)

    def __mod__(self, other: Expression) -> Modulo:
        """Create a modulo expression.

        Args:
            other: Expression to take modulo by.

        Returns:
            A Modulo expression representing the modulo.

        Example:
            >>> col("value") % col("divisor")
        """
        from vw.reference.operators import Modulo

        return Modulo(left=self, right=other)

    def alias(self, name: str, /) -> Alias:
        """Create an aliased expression.

        Args:
            name: The alias name.

        Returns:
            An Alias expression.

        Example:
            >>> col("price").alias("unit_price")
        """
        from vw.reference.operators import Alias

        return Alias(expr=self, name=name)

    def cast(self, data_type: str, /) -> Cast:
        """Cast expression to a SQL type.

        Args:
            data_type: The SQL data type to cast to.

        Returns:
            A Cast expression.

        Example:
            >>> col("price").cast(decimal(10, 2))
        """
        from vw.reference.operators import Cast

        return Cast(expr=self, data_type=data_type)

    def asc(self) -> Asc:
        """Sort expression in ascending order.

        Returns:
            An Asc expression.

        Example:
            >>> col("name").asc()
        """
        from vw.reference.operators import Asc

        return Asc(expr=self)

    def desc(self) -> Desc:
        """Sort expression in descending order.

        Returns:
            A Desc expression.

        Example:
            >>> col("created_at").desc()
        """
        from vw.reference.operators import Desc

        return Desc(expr=self)

    def is_null(self) -> IsNull:
        """Check if expression is NULL.

        Returns:
            An IsNull expression.

        Example:
            >>> col("deleted_at").is_null()
        """
        from vw.reference.operators import IsNull

        return IsNull(expr=self)

    def is_not_null(self) -> IsNotNull:
        """Check if expression is not NULL.

        Returns:
            An IsNotNull expression.

        Example:
            >>> col("name").is_not_null()
        """
        from vw.reference.operators import IsNotNull

        return IsNotNull(expr=self)

    def is_in(self, *values: Expression) -> IsIn:
        """Check if expression is in a list of values or subquery.

        Args:
            *values: Values to check against. Can be expressions or a single subquery.

        Returns:
            An IsIn expression.

        Example:
            >>> col("status").is_in(col("'active'"), col("'pending'"))
            >>> col("id").is_in(subquery)
        """
        from vw.reference.operators import IsIn

        return IsIn(expr=self, values=values)

    def is_not_in(self, *values: Expression) -> IsNotIn:
        """Check if expression is not in a list of values or subquery.

        Args:
            *values: Values to check against. Can be expressions or a single subquery.

        Returns:
            An IsNotIn expression.

        Example:
            >>> col("status").is_not_in(col("'deleted'"), col("'archived'"))
            >>> col("id").is_not_in(subquery)
        """
        from vw.reference.operators import IsNotIn

        return IsNotIn(expr=self, values=values)

    def like(self, pattern: str) -> Like:
        """Create a LIKE pattern match expression.

        Args:
            pattern: Pattern string to match against.

        Returns:
            A Like expression representing the LIKE comparison.

        Example:
            >>> col("name").like("%john%")
        """
        from vw.reference.operators import Like

        return Like(left=self, right=pattern)

    def not_like(self, pattern: str) -> NotLike:
        """Create a NOT LIKE pattern match expression.

        Args:
            pattern: Pattern string to match against.

        Returns:
            A NotLike expression representing the NOT LIKE comparison.

        Example:
            >>> col("name").not_like("%admin%")
        """
        from vw.reference.operators import NotLike

        return NotLike(left=self, right=pattern)

    def between(self, lower_bound: Expression, upper_bound: Expression) -> Between:
        """Create a BETWEEN range check expression.

        Args:
            lower_bound: The lower bound of the range (inclusive).
            upper_bound: The upper bound of the range (inclusive).

        Returns:
            A Between expression representing the range check.

        Example:
            >>> col("age").between(col("18"), col("65"))
        """
        from vw.reference.operators import Between

        return Between(expr=self, lower_bound=lower_bound, upper_bound=upper_bound)

    def not_between(self, lower_bound: Expression, upper_bound: Expression) -> NotBetween:
        """Create a NOT BETWEEN range check expression.

        Args:
            lower_bound: The lower bound of the range (inclusive).
            upper_bound: The upper bound of the range (inclusive).

        Returns:
            A NotBetween expression representing the negated range check.

        Example:
            >>> col("score").not_between(col("0"), col("100"))
        """
        from vw.reference.operators import NotBetween

        return NotBetween(expr=self, lower_bound=lower_bound, upper_bound=upper_bound)

    @property
    def text(self) -> TextAccessor:
        """Access string operations.

        Returns:
            A TextAccessor for string operations.

        Example:
            >>> col("name").text.upper()
            >>> col("name").text.lower()
            >>> col("name").text.trim()
        """
        from vw.reference.text import TextAccessor

        return TextAccessor(self)

    @property
    def dt(self) -> DateTimeAccessor:
        """Access datetime operations.

        Returns:
            A DateTimeAccessor for datetime operations.

        Example:
            >>> col("created_at").dt.year()
            >>> col("created_at").dt.month()
            >>> col("created_at").dt.truncate("month")
        """
        from vw.reference.datetime import DateTimeAccessor

        return DateTimeAccessor(self)
