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


# -----------------------------------------------------------------------------
# Mathematical operators
# -----------------------------------------------------------------------------


@dataclass(kw_only=True, frozen=True)
class Add(Expression):
    """Represents addition (+) between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the addition."""
        nested = context.recurse()
        return f"{self.left.__vw_render__(nested)} + {self.right.__vw_render__(nested)}"


@dataclass(kw_only=True, frozen=True)
class Subtract(Expression):
    """Represents subtraction (-) between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the subtraction."""
        nested = context.recurse()
        return f"{self.left.__vw_render__(nested)} - {self.right.__vw_render__(nested)}"


@dataclass(kw_only=True, frozen=True)
class Multiply(Expression):
    """Represents multiplication (*) between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the multiplication."""
        nested = context.recurse()
        return f"{self.left.__vw_render__(nested)} * {self.right.__vw_render__(nested)}"


@dataclass(kw_only=True, frozen=True)
class Divide(Expression):
    """Represents division (/) between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the division."""
        nested = context.recurse()
        return f"{self.left.__vw_render__(nested)} / {self.right.__vw_render__(nested)}"


@dataclass(kw_only=True, frozen=True)
class Modulo(Expression):
    """Represents modulo (%) between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the modulo."""
        nested = context.recurse()
        return f"{self.left.__vw_render__(nested)} % {self.right.__vw_render__(nested)}"


@dataclass(kw_only=True, frozen=True)
class Like(Expression):
    """Represents a LIKE pattern match."""

    left: Expression
    right: str

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the LIKE comparison."""
        return f"{self.left.__vw_render__(context)} LIKE '{self.right}'"


@dataclass(kw_only=True, frozen=True)
class NotLike(Expression):
    """Represents a NOT LIKE pattern match."""

    left: Expression
    right: str

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the NOT LIKE comparison."""
        return f"{self.left.__vw_render__(context)} NOT LIKE '{self.right}'"


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


# -----------------------------------------------------------------------------
# CASE/WHEN expressions
# -----------------------------------------------------------------------------


@dataclass(kw_only=True, frozen=True)
class WhenThen:
    """A single WHEN ... THEN ... branch."""

    when: Expression
    then: Expression


@dataclass(kw_only=True, frozen=True, eq=False)
class CaseExpression(Expression):
    """A CASE expression with branches and optional ELSE.

    Example SQL: CASE WHEN x = 1 THEN 'one' WHEN x = 2 THEN 'two' ELSE 'other' END
    """

    branches: list[WhenThen]
    _otherwise: Expression | None = None

    def when(self, condition: Expression, /) -> When:
        """Add another WHEN branch to this CASE expression.

        Args:
            condition: The condition to check.

        Returns:
            A When object that must be completed with .then().
        """
        return When(condition=condition, branches=self.branches)

    def otherwise(self, result: Expression, /) -> CaseExpression:
        """Complete this CASE expression with an ELSE clause.

        Args:
            result: The result when no WHEN conditions match.

        Returns:
            A CaseExpression with the ELSE clause.
        """
        return CaseExpression(branches=self.branches, _otherwise=result)

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the CASE expression."""
        parts = ["CASE"]
        for branch in self.branches:
            when_sql = branch.when.__vw_render__(context)
            then_sql = branch.then.__vw_render__(context)
            parts.append(f"WHEN {when_sql} THEN {then_sql}")
        if self._otherwise is not None:
            parts.append(f"ELSE {self._otherwise.__vw_render__(context)}")
        parts.append("END")
        return " ".join(parts)


@dataclass(kw_only=True, frozen=True)
class When:
    """Incomplete WHEN clause waiting for .then().

    This is not an Expression - it must be completed with .then().
    """

    condition: Expression
    branches: list[WhenThen] | None = None

    def then(self, result: Expression, /) -> CaseExpression:
        """Complete this WHEN clause with a THEN result.

        Args:
            result: The result when the condition is true.

        Returns:
            A CaseExpression that can be extended with .when() or .otherwise().
        """
        new_branch = WhenThen(when=self.condition, then=result)
        prior = self.branches or []
        return CaseExpression(branches=[*prior, new_branch])


def when(condition: Expression, /) -> When:
    """Start a CASE expression with a WHEN clause.

    Args:
        condition: The condition to check.

    Returns:
        A When object that must be completed with .then().

    Example:
        >>> from vw import when, col
        >>> when(col("status") == col("'active'")).then(col("1"))
        ...     .when(col("status") == col("'pending'")).then(col("2"))
        ...     .otherwise(col("0"))

        # Can also be used without .otherwise() (NULL when no match):
        >>> when(col("status") == col("'active'")).then(col("1"))
    """
    return When(condition=condition)
