"""Expression classes for SQL query building."""

from dataclasses import dataclass
from typing import Protocol


class Expression(Protocol):
    """Protocol for SQL expressions."""

    def __vw_render__(self) -> str:
        """Return the SQL representation of the expression."""
        ...


@dataclass
class Equals:
    """Represents an equality comparison (=) between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self) -> str:
        """Return the SQL representation of the equality comparison."""
        return f"{self.left.__vw_render__()} = {self.right.__vw_render__()}"


@dataclass
class NotEquals:
    """Represents an inequality comparison (!=) between two expressions."""

    left: Expression
    right: Expression

    def __vw_render__(self) -> str:
        """Return the SQL representation of the inequality comparison."""
        return f"{self.left.__vw_render__()} != {self.right.__vw_render__()}"


@dataclass
class Column:
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
            >>> col("status") != col("'active'")
        """
        return NotEquals(left=self, right=other)

    def __vw_render__(self) -> str:
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
    return Column(name)
