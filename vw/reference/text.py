"""SQL string functions and accessor."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from vw.reference.base import Expression

if TYPE_CHECKING:
    from vw.reference.render import RenderContext


@dataclass(kw_only=True, frozen=True, eq=False)
class Upper(Expression):
    """Represents an UPPER() function call."""

    expr: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        return f"UPPER({self.expr.__vw_render__(context)})"


@dataclass(kw_only=True, frozen=True, eq=False)
class Lower(Expression):
    """Represents a LOWER() function call."""

    expr: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        return f"LOWER({self.expr.__vw_render__(context)})"


@dataclass(kw_only=True, frozen=True, eq=False)
class Trim(Expression):
    """Represents a TRIM() function call."""

    expr: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        return f"TRIM({self.expr.__vw_render__(context)})"


@dataclass(kw_only=True, frozen=True, eq=False)
class LTrim(Expression):
    """Represents an LTRIM() function call."""

    expr: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        return f"LTRIM({self.expr.__vw_render__(context)})"


@dataclass(kw_only=True, frozen=True, eq=False)
class RTrim(Expression):
    """Represents an RTRIM() function call."""

    expr: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        return f"RTRIM({self.expr.__vw_render__(context)})"


@dataclass(kw_only=True, frozen=True, eq=False)
class Length(Expression):
    """Represents a LENGTH() function call."""

    expr: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        return f"LENGTH({self.expr.__vw_render__(context)})"


@dataclass(kw_only=True, frozen=True, eq=False)
class Substring(Expression):
    """Represents a SUBSTRING() function call."""

    expr: Expression
    start: int
    length: int | None = None

    def __vw_render__(self, context: RenderContext) -> str:
        if self.length is not None:
            return f"SUBSTRING({self.expr.__vw_render__(context)}, {self.start}, {self.length})"
        return f"SUBSTRING({self.expr.__vw_render__(context)}, {self.start})"


@dataclass(kw_only=True, frozen=True, eq=False)
class Replace(Expression):
    """Represents a REPLACE() function call."""

    expr: Expression
    old: str
    new: str

    def __vw_render__(self, context: RenderContext) -> str:
        return f"REPLACE({self.expr.__vw_render__(context)}, '{self.old}', '{self.new}')"


@dataclass(kw_only=True, frozen=True, eq=False)
class Concat(Expression):
    """Represents a CONCAT() function call."""

    exprs: tuple[Expression, ...]

    def __vw_render__(self, context: RenderContext) -> str:
        rendered = ", ".join(e.__vw_render__(context) for e in self.exprs)
        return f"CONCAT({rendered})"


class TextAccessor:
    """Accessor for string operations on an Expression."""

    def __init__(self, expr: Expression):
        self._expr = expr

    def upper(self) -> Upper:
        """Convert to uppercase.

        Returns:
            An Upper expression.

        Example:
            >>> col("name").text.upper()
        """
        return Upper(expr=self._expr)

    def lower(self) -> Lower:
        """Convert to lowercase.

        Returns:
            A Lower expression.

        Example:
            >>> col("name").text.lower()
        """
        return Lower(expr=self._expr)

    def trim(self) -> Trim:
        """Remove leading and trailing whitespace.

        Returns:
            A Trim expression.

        Example:
            >>> col("name").text.trim()
        """
        return Trim(expr=self._expr)

    def ltrim(self) -> LTrim:
        """Remove leading whitespace.

        Returns:
            An LTrim expression.

        Example:
            >>> col("name").text.ltrim()
        """
        return LTrim(expr=self._expr)

    def rtrim(self) -> RTrim:
        """Remove trailing whitespace.

        Returns:
            An RTrim expression.

        Example:
            >>> col("name").text.rtrim()
        """
        return RTrim(expr=self._expr)

    def length(self) -> Length:
        """Get the length of the string.

        Returns:
            A Length expression.

        Example:
            >>> col("name").text.length()
        """
        return Length(expr=self._expr)

    def substring(self, start: int, length: int | None = None) -> Substring:
        """Extract a substring.

        Args:
            start: Starting position (1-indexed in SQL).
            length: Optional length of substring.

        Returns:
            A Substring expression.

        Example:
            >>> col("name").text.substring(1, 5)
        """
        return Substring(expr=self._expr, start=start, length=length)

    def replace(self, old: str, new: str) -> Replace:
        """Replace occurrences of a substring.

        Args:
            old: Substring to replace.
            new: Replacement string.

        Returns:
            A Replace expression.

        Example:
            >>> col("text").text.replace("foo", "bar")
        """
        return Replace(expr=self._expr, old=old, new=new)

    def concat(self, *others: Expression) -> Concat:
        """Concatenate with other expressions.

        Args:
            *others: Expressions to concatenate with.

        Returns:
            A Concat expression.

        Example:
            >>> col("first_name").text.concat(col("' '"), col("last_name"))
        """
        return Concat(exprs=(self._expr, *others))
