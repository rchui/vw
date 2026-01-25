"""Grouping constructs for advanced GROUP BY operations.

This module provides GROUPING SETS, CUBE, and ROLLUP support for
multi-dimensional aggregation queries.

Example:
    >>> from vw.reference import col, Source, rollup, cube, grouping_sets
    >>>
    >>> # ROLLUP for hierarchical subtotals
    >>> stmt = Source(name="sales").select(
    ...     col("year"), col("quarter"), F.sum(col("amount"))
    ... ).group_by(rollup(col("year"), col("quarter")))
    >>>
    >>> # CUBE for all combinations
    >>> stmt = Source(name="sales").select(
    ...     col("region"), col("product"), F.sum(col("amount"))
    ... ).group_by(cube(col("region"), col("product")))
    >>>
    >>> # GROUPING SETS for specific groupings
    >>> stmt = Source(name="sales").select(
    ...     col("year"), col("region"), F.sum(col("amount"))
    ... ).group_by(grouping_sets(
    ...     (col("year"), col("region")),
    ...     (col("year"),),
    ...     ()
    ... ))
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from vw.reference.base import Expression
from vw.reference.render import RenderContext


@dataclass(kw_only=True, frozen=True, eq=False)
class Rollup(Expression):
    """ROLLUP grouping construct for hierarchical subtotals.

    ROLLUP creates grouping sets that represent a hierarchy. For columns
    (a, b, c), it generates: (a, b, c), (a, b), (a), ().

    This is useful for reports that need subtotals at each level of a
    hierarchy, such as year/quarter/month breakdowns.

    Example:
        >>> rollup(col("year"), col("quarter"), col("month"))
        # Generates: (year, quarter, month), (year, quarter), (year), ()
    """

    columns: list[Expression] = field(default_factory=list)

    def __vw_render__(self, context: RenderContext) -> str:
        cols_str = ", ".join(c.__vw_render__(context) for c in self.columns)
        return f"ROLLUP ({cols_str})"


@dataclass(kw_only=True, frozen=True, eq=False)
class Cube(Expression):
    """CUBE grouping construct for all possible combinations.

    CUBE creates grouping sets for all possible combinations of the
    specified columns (power set). For n columns, generates 2^n grouping sets.

    This is useful for cross-tabulation and OLAP-style analysis where
    you need aggregates across all dimension combinations.

    Example:
        >>> cube(col("region"), col("product"))
        # Generates: (region, product), (region), (product), ()
    """

    columns: list[Expression] = field(default_factory=list)

    def __vw_render__(self, context: RenderContext) -> str:
        cols_str = ", ".join(c.__vw_render__(context) for c in self.columns)
        return f"CUBE ({cols_str})"


@dataclass(kw_only=True, frozen=True, eq=False)
class GroupingSets(Expression):
    """GROUPING SETS construct for explicit grouping combinations.

    GROUPING SETS allows you to specify exactly which grouping combinations
    to compute, giving full control over the aggregation.

    Each set is a tuple of expressions. An empty tuple () represents
    the grand total (aggregation over all rows).

    Example:
        >>> grouping_sets(
        ...     (col("year"), col("region")),  # Group by year and region
        ...     (col("year"),),                 # Subtotal by year
        ...     ()                              # Grand total
        ... )
    """

    sets: list[tuple[Expression, ...]] = field(default_factory=list)

    def __vw_render__(self, context: RenderContext) -> str:
        rendered_sets: list[str] = []
        for group in self.sets:
            if not group:
                # Empty tuple = grand total
                rendered_sets.append("()")
            else:
                cols = ", ".join(e.__vw_render__(context) for e in group)
                rendered_sets.append(f"({cols})")
        return f"GROUPING SETS ({', '.join(rendered_sets)})"


def rollup(*columns: Expression) -> Rollup:
    """Create a ROLLUP grouping construct.

    ROLLUP generates hierarchical grouping sets by progressively removing
    columns from right to left. For n columns, produces n+1 grouping sets.

    Args:
        *columns: Columns to include in the rollup hierarchy.

    Returns:
        A Rollup expression for use in group_by().

    Example:
        >>> Source(name="sales").select(
        ...     col("year"), col("quarter"), F.sum(col("amount"))
        ... ).group_by(rollup(col("year"), col("quarter")))
        # SQL: GROUP BY ROLLUP (year, quarter)
        # Produces groups: (year, quarter), (year), ()
    """
    return Rollup(columns=list(columns))


def cube(*columns: Expression) -> Cube:
    """Create a CUBE grouping construct.

    CUBE generates all possible combinations of the specified columns
    (the power set). For n columns, produces 2^n grouping sets.

    Args:
        *columns: Columns to include in the cube.

    Returns:
        A Cube expression for use in group_by().

    Example:
        >>> Source(name="sales").select(
        ...     col("region"), col("product"), F.sum(col("amount"))
        ... ).group_by(cube(col("region"), col("product")))
        # SQL: GROUP BY CUBE (region, product)
        # Produces groups: (region, product), (region), (product), ()
    """
    return Cube(columns=list(columns))


def grouping_sets(*sets: Sequence[Expression]) -> GroupingSets:
    """Create a GROUPING SETS construct.

    GROUPING SETS allows explicit specification of which grouping
    combinations to compute. Use an empty sequence for the grand total.

    Args:
        *sets: Each argument is a sequence of expressions forming a grouping set.
               Use an empty tuple () for grand total.

    Returns:
        A GroupingSets expression for use in group_by().

    Example:
        >>> Source(name="sales").select(
        ...     col("year"), col("region"), F.sum(col("amount"))
        ... ).group_by(grouping_sets(
        ...     (col("year"), col("region")),
        ...     (col("year"),),
        ...     ()
        ... ))
        # SQL: GROUP BY GROUPING SETS ((year, region), (year), ())
    """
    return GroupingSets(sets=[tuple(s) for s in sets])


__all__ = [
    "Cube",
    "GroupingSets",
    "Rollup",
    "cube",
    "grouping_sets",
    "rollup",
]
