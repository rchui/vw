"""Custom exceptions for vw core."""


class VWError(Exception):
    """Base exception for all vw errors."""


class CTENameCollisionError(VWError):
    """Raised when multiple CTEs with the same name are registered."""


class RenderError(VWError):
    """Raised when rendering encounters an unsupported or invalid state."""
