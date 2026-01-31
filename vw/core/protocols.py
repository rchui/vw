from __future__ import annotations

from typing import Any, Protocol


class Stateful(Protocol):
    """Protocol for objects that wrap state."""

    state: Any  # Can be any state object (Source, Statement, Column, etc.)
