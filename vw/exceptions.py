"""Custom exceptions for the vw library."""


class VWError(Exception):
    """Base exception for all vw errors."""


class CTENameCollisionError(VWError):
    """Raised when multiple CTEs with the same name are registered."""
