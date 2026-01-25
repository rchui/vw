"""Type definitions.

Usage:
    >>> from vw.reference.types impo
    >>> vw.col()
"""


# --- Character String ----------------------------------------------------------------------------------------------- #


def char(n: int | None = None) -> str:
    """Fixed-length character type with length n.

    Args:
        n: Length of the char.

    Returns:
        str: A string representing the char type.

    Example:
        >>> char(10)
        'CHAR(10)'
        >>> char()
        'CHAR'
    """

    if n is None:
        return "CHAR"
    return f"CHAR({n})"


def varchar(n: int | None = None) -> str:
    """Variable-length character type with maximum length n.

    Args:
        n: Maximum length of the varchar.

    Returns:
        str: A string representing the varchar type.

    Example:
        >>> varchar(50)
        'VARCHAR(50)'
        >>> varchar()
        'VARCHAR'
    """

    if n is None:
        return "VARCHAR"
    return f"VARCHAR({n})"


def text() -> str:
    """Text type.

    Returns:
        str: A string representing the text type.

    Example:
        >>> text()
        'TEXT'
    """
    return "TEXT"


# --- Binary --------------------------------------------------------------------------------------------------------- #


def bytea() -> str:
    """Binary data type.

    Returns:
        str: A string representing the binary data type.

    Example:
        >>> bytea()
        'BYTEA'
    """
    return "BYTEA"


def blob() -> str:
    """Binary Large Object type.

    Returns:
        str: A string representing the BLOB type.

    Example:
        >>> blob()
        'BLOB'
    """
    return "BLOB"


def uuid() -> str:
    """UUID type.

    Returns:
        str: A string representing the UUID type.

    Example:
        >>> uuid()
        'UUID'
    """
    return "UUID"


# --- Numeric -------------------------------------------------------------------------------------------------------- #


def smallint() -> str:
    """Small integer type.

    Returns:
        str: A string representing the small integer type.

    Example:
        >>> smallint()
        'SMALLINT'
    """
    return "SMALLINT"


def integer() -> str:
    """Integer type.

    Returns:
        str: A string representing the integer type.

    Example:
        >>> integer()
        'INTEGER'
    """
    return "INTEGER"


def bigint() -> str:
    """Big integer type.

    Returns:
        str: A string representing the big integer type.

    Example:
        >>> bigint()
        'BIGINT'
    """
    return "BIGINT"


def float() -> str:
    """Floating-point number type.

    Returns:
        str: A string representing the floating-point number type.

    Example:
        >>> float()
        'FLOAT'
    """
    return "FLOAT"


def float4() -> str:
    """4-byte floating-point number type.

    Returns:
        str: A string representing the 4-byte floating-point number type.

    Example:
        >>> float4()
        'FLOAT4'
    """
    return "FLOAT4"


def real() -> str:
    """Real number type.

    Returns:
        str: A string representing the real number type.

    Example:
        >>> real()
        'REAL'
    """
    return "REAL"


def double() -> str:
    """Double precision floating-point number type.

    Returns:
        str: A string representing the double precision floating-point number type.

    Example:
        >>> double()
        'DOUBLE'
    """
    return "DOUBLE"


def float8() -> str:
    """8-byte floating-point number type.

    Returns:
        str: A string representing the 8-byte floating-point number type.

    Example:
        >>> float8()
        'FLOAT8'
    """
    return "FLOAT8"


def double_precision() -> str:
    """Double precision floating-point number type.

    Returns:
        str: A string representing the double precision floating-point number type.

    Example:
        >>> double_precision()
        'DOUBLE PRECISION'
    """
    return "DOUBLE PRECISION"


def numeric(precision: int | None = None, scale: int | None = None) -> str:
    """Numeric type with specified precision and scale.

    Args:
        precision: Total number of digits.
        scale: Number of digits to the right of the decimal point.

    Returns:
        str: A string representing the numeric type.

    Example:
        >>> numeric(10, 2)
        'NUMERIC(10,2)'
    """
    if precision is None and scale is None:
        return "NUMERIC"
    if scale is None:
        return f"NUMERIC({precision})"
    return f"NUMERIC({precision},{scale})"


def decimal(precision: int | None = None, scale: int | None = None) -> str:
    """Decimal type with specified precision and scale.

    Args:
        precision: Total number of digits.
        scale: Number of digits to the right of the decimal point.

    Returns:
        str: A string representing the decimal type.

    Example:
        >>> decimal(10, 2)
        'DECIMAL(10,2)'
    """
    if precision is None and scale is None:
        return "DECIMAL"
    if scale is None:
        return f"DECIMAL({precision})"
    return f"DECIMAL({precision},{scale})"


# --- Logical -------------------------------------------------------------------------------------------------------- #


def boolean() -> str:
    """Boolean type.

    Returns:
        str: A string representing the boolean type.

    Example:
        >>> boolean()
        'BOOLEAN'
    """
    return "BOOLEAN"


# --- Container ------------------------------------------------------------------------------------------------------ #


def array(element_type: str, n: int | None = None) -> str:
    """Array type with specified element type and length.

    Args:
        element_type: The type of elements in the array.
        n: Length of the array.

    Returns:
        str: A string representing the array type.

    Example:
        >>> array(integer(), 5)
        'INTEGER[5]'
    """

    if n is None:
        return f"{element_type}[]"
    return f"{element_type}[{n}]"


def list(element_type: str) -> str:
    """List type with specified element type.

    Args:
        element_type: The type of elements in the list.

    Returns:
        str: A string representing the list type.

    Example:
        >>> list(text())
        'TEXT[]'
    """
    return f"{element_type}[]"


def json() -> str:
    """JSON type.

    Returns:
        str: A string representing the JSON type.

    Example:
        >>> json()
        'JSON'
    """
    return "JSON"


def jsonb() -> str:
    """Binary JSON type.

    Returns:
        str: A string representing the binary JSON type.

    Example:
        >>> jsonb()
        'JSONB'
    """
    return "JSONB"


def struct(entries: dict[str, str]) -> str:
    """Struct type with specified entries.

    Args:
        entries: A dictionary mapping field names to their types.

    Returns:
        str: A string representing the struct type.

    Example:
        >>> struct({'id': integer(), 'name': text()})
        'STRUCT(id INTEGER, name TEXT)'
    """

    fields = ", ".join(f"{name} {dtype}" for name, dtype in entries.items())
    return f"STRUCT({fields})"


def variant() -> str:
    """Variant type.

    Returns:
        str: A string representing the variant type.

    Example:
        >>> variant()
        'VARIANT'
    """
    return "VARIANT"


# --- Date and Time -------------------------------------------------------------------------------------------------- #


def date() -> str:
    """Date type.

    Returns:
        str: A string representing the date type.

    Example:
        >>> date()
        'DATE'
    """
    return "DATE"


def time() -> str:
    """Time type.

    Returns:
        str: A string representing the time type.

    Example:
        >>> time()
        'TIME'
    """
    return "TIME"


def datetime() -> str:
    """Datetime type.

    Returns:
        str: A string representing the datetime type.

    Example:
        >>> datetime()
        'DATETIME'
    """
    return "DATETIME"


def timestamp() -> str:
    """Timestamp type.

    Returns:
        str: A string representing the timestamp type.

    Example:
        >>> timestamp()
        'TIMESTAMP'
    """
    return "TIMESTAMP"


def timestamptz() -> str:
    """Timestamp with time zone type.

    Returns:
        str: A string representing the timestamp with time zone type.

    Example:
        >>> timestamptz()
        'TIMESTAMPTZ'
    """
    return "TIMESTAMP WITH TIME ZONE"
