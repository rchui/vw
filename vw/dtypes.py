"""Type definitions.

Usage:
    >>> from vw.types impo
    >>> vw.col()
"""

from typing_extensions import NewType

dtype = NewType("dtype", str)

# --- Character String ----------------------------------------------------------------------------------------------- #


def char(n: int | None = None) -> dtype:
    """Fixed-length character type with length n.

    Args:
        n: Length of the char.

    Returns:
        dtype: A string representing the char type.

    Example:
        >>> char(10)
        'CHAR(10)'
        >>> char()
        'CHAR'
    """

    if n is None:
        return dtype("CHAR")
    return dtype(f"CHAR({n})")


def varchar(n: int | None = None) -> dtype:
    """Variable-length character type with maximum length n.

    Args:
        n: Maximum length of the varchar.

    Returns:
        dtype: A string representing the varchar type.

    Example:
        >>> varchar(50)
        'VARCHAR(50)'
        >>> varchar()
        'VARCHAR'
    """

    if n is None:
        return dtype("VARCHAR")
    return dtype(f"VARCHAR({n})")


def text() -> dtype:
    """Text type.

    Returns:
        dtype: A string representing the text type.

    Example:
        >>> text()
        'TEXT'
    """
    return dtype("TEXT")


# --- Binary --------------------------------------------------------------------------------------------------------- #


def bytea() -> dtype:
    """Binary data type.

    Returns:
        dtype: A string representing the binary data type.

    Example:
        >>> bytea()
        'BYTEA'
    """
    return dtype("BYTEA")


def blob() -> dtype:
    """Binary Large Object type.

    Returns:
        dtype: A string representing the BLOB type.

    Example:
        >>> blob()
        'BLOB'
    """
    return dtype("BLOB")


def uuid() -> dtype:
    """UUID type.

    Returns:
        dtype: A string representing the UUID type.

    Example:
        >>> uuid()
        'UUID'
    """
    return dtype("UUID")


# --- Numeric -------------------------------------------------------------------------------------------------------- #


def smallint() -> dtype:
    """Small integer type.

    Returns:
        dtype: A string representing the small integer type.

    Example:
        >>> smallint()
        'SMALLINT'
    """
    return dtype("SMALLINT")


def integer() -> dtype:
    """Integer type.

    Returns:
        dtype: A string representing the integer type.

    Example:
        >>> integer()
        'INTEGER'
    """
    return dtype("INTEGER")


def bigint() -> dtype:
    """Big integer type.

    Returns:
        dtype: A string representing the big integer type.

    Example:
        >>> bigint()
        'BIGINT'
    """
    return dtype("BIGINT")


def float() -> dtype:
    """Floating-point number type.

    Returns:
        dtype: A string representing the floating-point number type.

    Example:
        >>> float()
        'FLOAT'
    """
    return dtype("FLOAT")


def float4() -> dtype:
    """4-byte floating-point number type.

    Returns:
        dtype: A string representing the 4-byte floating-point number type.

    Example:
        >>> float4()
        'FLOAT4'
    """
    return dtype("FLOAT4")


def real() -> dtype:
    """Real number type.

    Returns:
        dtype: A string representing the real number type.

    Example:
        >>> real()
        'REAL'
    """
    return dtype("REAL")


def double() -> dtype:
    """Double precision floating-point number type.

    Returns:
        dtype: A string representing the double precision floating-point number type.

    Example:
        >>> double()
        'DOUBLE'
    """
    return dtype("DOUBLE")


def float8() -> dtype:
    """8-byte floating-point number type.

    Returns:
        dtype: A string representing the 8-byte floating-point number type.

    Example:
        >>> float8()
        'FLOAT8'
    """
    return dtype("FLOAT8")


def double_precision() -> dtype:
    """Double precision floating-point number type.

    Returns:
        dtype: A string representing the double precision floating-point number type.

    Example:
        >>> double_precision()
        'DOUBLE PRECISION'
    """
    return dtype("DOUBLE PRECISION")


def numeric(precision: int | None = None, scale: int | None = None) -> dtype:
    """Numeric type with specified precision and scale.

    Args:
        precision: Total number of digits.
        scale: Number of digits to the right of the decimal point.

    Returns:
        dtype: A string representing the numeric type.

    Example:
        >>> numeric(10, 2)
        'NUMERIC(10,2)'
    """
    if precision is None and scale is None:
        return dtype("NUMERIC")
    if scale is None:
        return dtype(f"NUMERIC({precision})")
    return dtype(f"NUMERIC({precision},{scale})")


def decimal(precision: int | None = None, scale: int | None = None) -> dtype:
    """Decimal type with specified precision and scale.

    Args:
        precision: Total number of digits.
        scale: Number of digits to the right of the decimal point.

    Returns:
        dtype: A string representing the decimal type.

    Example:
        >>> decimal(10, 2)
        'DECIMAL(10,2)'
    """
    if precision is None and scale is None:
        return dtype("DECIMAL")
    if scale is None:
        return dtype(f"DECIMAL({precision})")
    return dtype(f"DECIMAL({precision},{scale})")


# --- Logical -------------------------------------------------------------------------------------------------------- #


def boolean() -> dtype:
    """Boolean type.

    Returns:
        dtype: A string representing the boolean type.

    Example:
        >>> boolean()
        'BOOLEAN'
    """
    return dtype("BOOLEAN")


# --- Container ------------------------------------------------------------------------------------------------------ #


def array(element_type: str, n: int | None = None) -> dtype:
    """Array type with specified element type and length.

    Args:
        element_type: The type of elements in the array.
        n: Length of the array.

    Returns:
        dtype: A string representing the array type.

    Example:
        >>> array(integer(), 5)
        'INTEGER[5]'
    """

    if n is None:
        return dtype(f"{element_type}[]")
    return dtype(f"{element_type}[{n}]")


def list(element_type: str) -> dtype:
    """List type with specified element type.

    Args:
        element_type: The type of elements in the list.

    Returns:
        dtype: A string representing the list type.

    Example:
        >>> list(text())
        'TEXT[]'
    """
    return dtype(f"{element_type}[]")


def json() -> dtype:
    """JSON type.

    Returns:
        dtype: A string representing the JSON type.

    Example:
        >>> json()
        'JSON'
    """
    return dtype("JSON")


def jsonb() -> dtype:
    """Binary JSON type.

    Returns:
        dtype: A string representing the binary JSON type.

    Example:
        >>> jsonb()
        'JSONB'
    """
    return dtype("JSONB")


def struct(entries: dict[str, str]) -> dtype:
    """Struct type with specified entries.

    Args:
        entries: A dictionary mapping field names to their types.

    Returns:
        dtype: A string representing the struct type.

    Example:
        >>> struct({'id': integer(), 'name': text()})
        'STRUCT(id INTEGER, name TEXT)'
    """

    fields = ", ".join(f"{name} {dtype}" for name, dtype in entries.items())
    return dtype(f"STRUCT({fields})")


def variant() -> dtype:
    """Variant type.

    Returns:
        dtype: A string representing the variant type.

    Example:
        >>> variant()
        'VARIANT'
    """
    return dtype("VARIANT")


# --- Date and Time -------------------------------------------------------------------------------------------------- #


def date() -> dtype:
    """Date type.

    Returns:
        dtype: A string representing the date type.

    Example:
        >>> date()
        'DATE'
    """
    return dtype("DATE")


def time() -> dtype:
    """Time type.

    Returns:
        dtype: A string representing the time type.

    Example:
        >>> time()
        'TIME'
    """
    return dtype("TIME")


def datetime() -> dtype:
    """Datetime type.

    Returns:
        dtype: A string representing the datetime type.

    Example:
        >>> datetime()
        'DATETIME'
    """
    return dtype("DATETIME")


def timestamp() -> dtype:
    """Timestamp type.

    Returns:
        dtype: A string representing the timestamp type.

    Example:
        >>> timestamp()
        'TIMESTAMP'
    """
    return dtype("TIMESTAMP")


def timestamptz() -> dtype:
    """Timestamp with time zone type.

    Returns:
        dtype: A string representing the timestamp with time zone type.

    Example:
        >>> timestamptz()
        'TIMESTAMPTZ'
    """
    return dtype("TIMESTAMP WITH TIME ZONE")
