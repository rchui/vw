"""Column class for SQL column references."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from vw.base import Expression

if TYPE_CHECKING:
    from vw.render import RenderContext


@dataclass(kw_only=True, frozen=True, eq=False)
class Column(Expression):
    """Represents a column reference in SQL."""

    name: str

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
