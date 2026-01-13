"""Custom exceptions for the vw library."""


class VWError(Exception):
    """Base exception for all vw errors."""


class CTENameCollisionError(VWError):
    """Raised when multiple CTEs with the same name are registered."""


class UnsupportedParamStyleError(VWError):
    """Raised when an unsupported parameter style is used."""


class UnsupportedDialectError(VWError):
    """Raised when an unsupported dialect is used."""


class JoinError(VWError):
    """Base exception for join-related errors."""


class JoinConditionError(JoinError):
    """Raised when join conditions are invalid (e.g., both ON and USING specified)."""


class WindowError(VWError):
    """Base exception for window function errors."""


class IncompleteFrameError(WindowError):
    """Raised when a frame clause is incomplete (e.g., EXCLUDE without frame boundaries)."""
