"""Expression classes for SQL query building."""

from dataclasses import dataclass
from typing import Protocol


class Expression(Protocol):
    """Protocol for SQL expressions."""

    def __vw_render__(self) -> str:
        """Return the SQL representation of the expression."""
        ...


@dataclass
class Column:
    """Represents a column reference in SQL."""

    name: str

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
