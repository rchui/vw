"""Column class for SQL column references."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from vw.base import Expression
from vw.operators import (
    Equals,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    NotEquals,
)

if TYPE_CHECKING:
    from vw.render import RenderContext


@dataclass(kw_only=True, frozen=True)
class Column(Expression):
    """Represents a column reference in SQL."""

    name: str

    def __eq__(self, other: Expression) -> Equals:  # ty: ignore
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

    def __ne__(self, other: Expression) -> NotEquals:  # ty: ignore
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
    return Column(name=name)
