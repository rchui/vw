"""Comparison and logical operators for SQL expressions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from vw.base import Expression

if TYPE_CHECKING:
    from vw.render import RenderContext


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
