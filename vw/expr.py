"""Expression classes for SQL query building."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from vw.query import InnerJoin, JoinAccessor, Statement
    from vw.render import RenderContext


@dataclass(kw_only=True)
class RowSet:
    """Base class for things that produce rows (tables, subqueries, CTEs).

    Used in FROM and JOIN clauses.
    """

    _alias: str | None = None
    _joins: list[InnerJoin] = field(default_factory=list)

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
            >>> Source("users").alias("u")
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
            >>> Source("users").alias("u").col("id")  # Returns Column("u.id")
        """
        if self._alias:
            return Column(f"{self._alias}.{column_name}")
        return Column(column_name)

    @property
    def join(self) -> JoinAccessor:
        """Access join operations."""
        from vw.query import JoinAccessor

        return JoinAccessor(self)

    def select(self, *columns: Expression) -> Statement:
        """
        Select columns from this row set.

        Args:
            *columns: Expression objects to select.

        Returns:
            A Statement object for method chaining.

        Example:
            >>> from vw import col
            >>> Source("users").select(col("*"))
            >>> subquery.alias("sq").select(col("*"))
        """
        from vw.query import Statement

        return Statement(source=self, columns=list(columns))


class Expression:
    """Protocol for SQL expressions."""

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the expression."""
        raise NotImplementedError

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

        return Or(left=self, right=other)

    def __invert__(self) -> Not:
        """Create a logical NOT expression

        Returns:
            A Not expression representing the logical NOT of this expression.

        Example:
            >>> expr = col("active") == col("true")
            >>> negated = ~expr
        """

        return Not(operand=self)


@dataclass
class Equals(Expression):
    """Represents an equality comparison (=) between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the equality comparison."""
        return f"{self.left.__vw_render__(context)} = {self.right.__vw_render__(context)}"


@dataclass
class NotEquals(Expression):
    """Represents an inequality comparison (<>) between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the inequality comparison."""
        return f"{self.left.__vw_render__(context)} <> {self.right.__vw_render__(context)}"


@dataclass
class LessThan(Expression):
    """Represents a less than comparison (<) between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the less than comparison."""
        return f"{self.left.__vw_render__(context)} < {self.right.__vw_render__(context)}"


@dataclass
class LessThanOrEqual(Expression):
    """Represents a less than or equal comparison (<=) between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the less than or equal comparison."""
        return f"{self.left.__vw_render__(context)} <= {self.right.__vw_render__(context)}"


@dataclass
class GreaterThan(Expression):
    """Represents a greater than comparison (>) between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the greater than comparison."""
        return f"{self.left.__vw_render__(context)} > {self.right.__vw_render__(context)}"


@dataclass
class GreaterThanOrEqual(Expression):
    """Represents a greater than or equal comparison (>=) between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the greater than or equal comparison."""
        return f"{self.left.__vw_render__(context)} >= {self.right.__vw_render__(context)}"


@dataclass
class Not(Expression):
    """Represents a logical NOT of an expression."""

    operand: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the NOT expression."""
        return f"NOT ({self.operand.__vw_render__(context)})"


@dataclass
class And(Expression):
    """Represents a logical AND between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the AND expression with parentheses."""
        return f"({self.left.__vw_render__(context)}) AND ({self.right.__vw_render__(context)})"


@dataclass
class Or(Expression):
    """Represents a logical OR between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the OR expression with parentheses."""
        return f"({self.left.__vw_render__(context)}) OR ({self.right.__vw_render__(context)})"


@dataclass
class Column(Expression):
    """Represents a column reference in SQL."""

    name: str

    def __eq__(self, other: Expression) -> Equals:
        """
        Create an equality comparison expression.

        Args:
            other: Expression to compare with.

        Returns:
            An Equals representing the equality comparison.

        Example:
            >>> col("users.id") == col("orders.user_id")
        """
        return Equals(left=self, right=other)

    def __ne__(self, other: Expression) -> NotEquals:
        """
        Create an inequality comparison expression.

        Args:
            other: Expression to compare with.

        Returns:
            A NotEquals representing the inequality comparison.

        Example:
            >>> col("status") <> col("'active'")
        """
        return NotEquals(left=self, right=other)

    def __lt__(self, other: Expression) -> LessThan:
        """
        Create a less than comparison expression.

        Args:
            other: Expression to compare with.

        Returns:
            A LessThan representing the less than comparison.

        Example:
            >>> col("age") < col("18")
        """
        return LessThan(left=self, right=other)

    def __le__(self, other: Expression) -> LessThanOrEqual:
        """
        Create a less than or equal comparison expression.

        Args:
            other: Expression to compare with.

        Returns:
            A LessThanOrEqual representing the less than or equal comparison.

        Example:
            >>> col("price") <= col("100.00")
        """
        return LessThanOrEqual(left=self, right=other)

    def __gt__(self, other: Expression) -> GreaterThan:
        """
        Create a greater than comparison expression.

        Args:
            other: Expression to compare with.

        Returns:
            A GreaterThan representing the greater than comparison.

        Example:
            >>> col("score") > col("90")
        """
        return GreaterThan(left=self, right=other)

    def __ge__(self, other: Expression) -> GreaterThanOrEqual:
        """
        Create a greater than or equal comparison expression.

        Args:
            other: Expression to compare with.

        Returns:
            A GreaterThanOrEqual representing the greater than or equal comparison.

        Example:
            >>> col("quantity") >= col("1")
        """
        return GreaterThanOrEqual(left=self, right=other)

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the column."""
        return self.name


@dataclass
class Parameter(Expression):
    """Represents a parameterized value in SQL."""

    name: str
    value: str | int | float | bool

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL placeholder for the parameter and register it in the context."""
        return context.add_param(self.name, self.value)


def col(name: str, /) -> Column:
    """
    Create a column reference.

    Args:
        name: Column name or "*" for all columns.

    Returns:
        A Column object representing the column reference.

    Example:
        >>> col("id")
        >>> col("name")
        >>> col("*")
    """
    return Column(name)


def param(name: str, value: str | int | float | bool, /) -> Parameter:
    """
    Create a parameterized value.

    Args:
        name: Parameter name used in the params dictionary.
        value: Parameter value (string, int, float, or bool).

    Returns:
        A Parameter object representing the parameterized value.

    Raises:
        TypeError: If value is not a supported type.

    Example:
        >>> param("age", 25)
        >>> param("name", "Alice")
        >>> param("active", True)
    """
    if not isinstance(value, (str, int, float, bool)):
        raise TypeError(f"Unsupported parameter type: {type(value).__name__}. Must be str, int, float, or bool.")
    return Parameter(name=name, value=value)
