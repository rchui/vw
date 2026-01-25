"""Parameter class for SQL parameterized values."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from vw.reference.base import Expression

if TYPE_CHECKING:
    from vw.reference.render import RenderContext


@dataclass(kw_only=True, frozen=True)
class Parameter(Expression):
    """Represents a parameterized value in SQL."""

    name: str
    value: str | int | float | bool

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL placeholder for the parameter and register it in the context."""
        return context.add_param(self.name, self.value)


def param(name: str, value: str | int | float | bool, /) -> Parameter:
    """
    Create a parameterized value.

    Args:
        name: Parameter name used in the params dictionary.
        value: Parameter value (string, int, float, or bool).

    Returns:
        A Parameter object representing the parameterized value.

    Raises:
        TypeError: If value is not a supported type.

    Example:
        >>> param("age", 25)
        >>> param("name", "Alice")
        >>> param("active", True)
    """
    if not isinstance(value, (str, int, float, bool)):
        raise TypeError(f"Unsupported parameter type: {type(value).__name__}. Must be str, int, float, or bool.")
    return Parameter(name=name, value=value)
