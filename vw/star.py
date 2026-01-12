"""Star expressions for SQL wildcard operations with fluent modifiers."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from vw import Column
from vw.base import Expression

if TYPE_CHECKING:
    from vw.render import RenderContext


@dataclass(kw_only=True, frozen=True)
class StarExpression(Expression):
    """A SQL star expression with fluent modifiers.

    Wraps a Column object representing "*" or "table.*" and allows
    chaining of operations like exclude(), rename(), etc.
    """

    column: Column = field(default_factory=lambda: Column(name="*"))
    excludes: list[Expression] = field(default_factory=list)
    replaces: list[Expression] = field(default_factory=list)
    renames: list[Expression] = field(default_factory=list)

    def exclude(self, *exprs: Expression) -> StarExpression:
        """Add EXCLUDE clause to the star expression.

        Args:
            *exprs: Expression objects to exclude from the star expansion.

        Returns:
            A new StarExpression with the specified expressions excluded.

        Example:
            >>> import vw
            >>> vw.star.exclude(vw.col("password"), vw.col("secret"))
        """
        return replace(self, excludes=self.excludes + list(exprs))

    def replace(self, *exprs: Expression) -> StarExpression:
        """Add REPLACE clause to the star expression.

        Args:
            *exprs: Expression objects to replace in the star expansion.

        Returns:
            A new StarExpression with the specified expressions replaced.

        Example:
            >>> import vw
            >>> vw.star.replace(vw.col("old_name").as_("new_name"))
        """

        return replace(self, replaces=self.replaces + list(exprs))

    def rename(self, *exprs: Expression) -> StarExpression:
        """Add RENAME clause to the star expression.

        Args:
            *exprs: Expression objects to rename in the star expansion.

        Returns:
            A new StarExpression with the specified expressions renamed.

        Example:
            >>> import vw
            >>> vw.star.rename(vw.col("old_name").as_("new_name"))
        """

        return replace(self, renames=self.renames + list(exprs))

    def __vw_render__(self, context: RenderContext) -> str:
        """Render star expression with any excludes.

        Renders EXCLUDE/EXCEPT clauses for supported dialects.
        For unsupported dialects, falls back to basic star.
        """

        star_sql = self.column.__vw_render__(context)

        if self.excludes:
            exclude_columns = ", ".join(col.__vw_render__(context) for col in self.excludes)
            star_sql = f"{star_sql} EXCLUDE ({exclude_columns})"

        if self.replaces:
            replace_columns = ", ".join(col.__vw_render__(context) for col in self.replaces)
            star_sql = f"{star_sql} REPLACE ({replace_columns})"

        if self.renames:
            rename_columns = ", ".join(col.__vw_render__(context) for col in self.renames)
            star_sql = f"{star_sql} RENAME ({rename_columns})"

        return star_sql


star = StarExpression()
