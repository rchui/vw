"""Custom exceptions for the vw library."""


class VWError(Exception):
    """Base exception for all vw errors."""


class CTENameCollisionError(VWError):
    """Raised when multiple CTEs with the same name are registered."""


class UnsupportedParamStyleError(VWError):
    """Raised when an unsupported parameter style is used."""


class UnsupportedDialectError(VWError):
    """Raised when an unsupported dialect is used."""
