"""DuckDB star extensions (EXCLUDE, REPLACE)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from vw.duckdb.base import Expression, RowSet
    from vw.duckdb.states import StarExclude, StarReplace


@dataclass(eq=False, frozen=True, kw_only=True)
class StarAccessor:
    """Callable accessor for DuckDB star extensions.

    Usage:
        users.star()                                      # Base star: *
        users.star(users.star.exclude(col("password")))   # With EXCLUDE
        users.star(users.star.replace(name=expr))         # With REPLACE
        users.star(                                       # Both
            users.star.exclude(col("a")),
            users.star.replace(name=expr)
        )
    """

    rowset: RowSet

    def exclude(self, *columns: Expression) -> StarExclude:
        """Create an EXCLUDE clause helper.

        Args:
            *columns: Column expressions to exclude.

        Returns:
            StarExclude to pass to star() call.
        """
        from vw.duckdb.states import StarExclude

        return StarExclude(columns=tuple(c.state for c in columns))

    def replace(self, **replacements: Expression) -> StarReplace:
        """Create a REPLACE clause helper.

        Args:
            **replacements: Column name to replacement expression mapping.

        Returns:
            StarReplace to pass to star() call.
        """
        from vw.duckdb.states import StarReplace

        return StarReplace(replacements={k: v.state for k, v in replacements.items()})

    def __call__(self, *modifiers: StarExclude | StarReplace) -> Expression:
        """Build a star expression with optional EXCLUDE/REPLACE modifiers.

        Args:
            *modifiers: Zero or more StarExclude or StarReplace objects in order.

        Returns:
            Expression representing the star with modifiers applied in order.
        """
        from vw.duckdb.base import Expression
        from vw.duckdb.states import Star

        return Expression(
            state=Star(source=self.rowset.state, modifiers=modifiers),
            factories=self.rowset.factories,
        )
