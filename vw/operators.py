"""Comparison and logical operators for SQL expressions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from vw.base import Expression

if TYPE_CHECKING:
    from vw.render import RenderContext


@dataclass(kw_only=True, frozen=True)
class Equals(Expression):
    """Represents an equality comparison (=) between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the equality comparison."""
        nested = context.recurse()
        return f"{self.left.__vw_render__(nested)} = {self.right.__vw_render__(nested)}"


@dataclass(kw_only=True, frozen=True)
class NotEquals(Expression):
    """Represents an inequality comparison (<>) between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the inequality comparison."""
        nested = context.recurse()
        return f"{self.left.__vw_render__(nested)} <> {self.right.__vw_render__(nested)}"


@dataclass(kw_only=True, frozen=True)
class LessThan(Expression):
    """Represents a less than comparison (<) between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the less than comparison."""
        nested = context.recurse()
        return f"{self.left.__vw_render__(nested)} < {self.right.__vw_render__(nested)}"


@dataclass(kw_only=True, frozen=True)
class LessThanOrEqual(Expression):
    """Represents a less than or equal comparison (<=) between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the less than or equal comparison."""
        nested = context.recurse()
        return f"{self.left.__vw_render__(nested)} <= {self.right.__vw_render__(nested)}"


@dataclass(kw_only=True, frozen=True)
class GreaterThan(Expression):
    """Represents a greater than comparison (>) between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the greater than comparison."""
        nested = context.recurse()
        return f"{self.left.__vw_render__(nested)} > {self.right.__vw_render__(nested)}"


@dataclass(kw_only=True, frozen=True)
class GreaterThanOrEqual(Expression):
    """Represents a greater than or equal comparison (>=) between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the greater than or equal comparison."""
        nested = context.recurse()
        return f"{self.left.__vw_render__(nested)} >= {self.right.__vw_render__(nested)}"


@dataclass(kw_only=True, frozen=True)
class IsNull(Expression):
    """Represents an IS NULL check."""

    expr: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the IS NULL check."""
        return f"{self.expr.__vw_render__(context)} IS NULL"


@dataclass(kw_only=True, frozen=True)
class IsNotNull(Expression):
    """Represents an IS NOT NULL check."""

    expr: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the IS NOT NULL check."""
        return f"{self.expr.__vw_render__(context)} IS NOT NULL"


@dataclass(kw_only=True, frozen=True)
class Not(Expression):
    """Represents a logical NOT of an expression."""

    operand: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the NOT expression."""
        return f"NOT ({self.operand.__vw_render__(context)})"


@dataclass(kw_only=True, frozen=True)
class And(Expression):
    """Represents a logical AND between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the AND expression with parentheses."""
        return f"({self.left.__vw_render__(context)}) AND ({self.right.__vw_render__(context)})"


@dataclass(kw_only=True, frozen=True)
class Or(Expression):
    """Represents a logical OR between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the OR expression with parentheses."""
        return f"({self.left.__vw_render__(context)}) OR ({self.right.__vw_render__(context)})"


@dataclass(kw_only=True, frozen=True)
class Alias(Expression):
    """Represents an aliased expression (expr AS name)."""

    expr: Expression
    name: str

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the aliased expression."""
        return f"{self.expr.__vw_render__(context)} AS {self.name}"


@dataclass(kw_only=True, frozen=True)
class Cast(Expression):
    """Represents a type cast (CAST(expr AS type) or expr::type)."""

    expr: Expression
    data_type: str

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the cast expression."""
        from vw.exceptions import UnsupportedDialectError
        from vw.render import Dialect

        if context.config.dialect == Dialect.POSTGRES:
            return f"{self.expr.__vw_render__(context)}::{self.data_type}"
        elif context.config.dialect in (Dialect.SQLALCHEMY, Dialect.SQLSERVER):
            return f"CAST({self.expr.__vw_render__(context)} AS {self.data_type})"
        raise UnsupportedDialectError(f"Unsupported dialect: {context.config.dialect}")


@dataclass(kw_only=True, frozen=True)
class Asc(Expression):
    """Represents an ascending sort order (expr ASC)."""

    expr: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the ascending expression."""
        return f"{self.expr.__vw_render__(context)} ASC"


@dataclass(kw_only=True, frozen=True)
class Desc(Expression):
    """Represents a descending sort order (expr DESC)."""

    expr: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the descending expression."""
        return f"{self.expr.__vw_render__(context)} DESC"


@dataclass(kw_only=True, frozen=True)
class IsIn(Expression):
    """Represents an IN check against a list of values or subquery."""

    expr: Expression
    values: tuple[Expression, ...]

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the IN check."""
        rendered_values = ", ".join(v.__vw_render__(context.recurse()) for v in self.values)
        return f"{self.expr.__vw_render__(context)} IN ({rendered_values})"


@dataclass(kw_only=True, frozen=True)
class IsNotIn(Expression):
    """Represents a NOT IN check against a list of values or subquery."""

    expr: Expression
    values: tuple[Expression, ...]

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the NOT IN check."""
        rendered_values = ", ".join(v.__vw_render__(context.recurse()) for v in self.values)
        return f"{self.expr.__vw_render__(context)} NOT IN ({rendered_values})"


@dataclass(kw_only=True, frozen=True)
class Exists(Expression):
    """Represents an EXISTS check on a subquery."""

    subquery: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the EXISTS check."""
        return f"EXISTS {self.subquery.__vw_render__(context.recurse())}"


def exists(subquery: Expression, /) -> Exists:
    """Create an EXISTS expression for a subquery.

    Args:
        subquery: The subquery to check for existence.

    Returns:
        An Exists expression.

    Example:
        >>> from vw import Source, col, exists
        >>> orders = Source(name="orders")
        >>> users = Source(name="users")
        >>> users.select(col("*")).where(
        ...     exists(orders.select(col("1")).where(orders.col("user_id") == users.col("id")))
        ... )
    """
    return Exists(subquery=subquery)
