from __future__ import annotations

from dataclasses import dataclass, field

# --- Base Classes ---------------------------------------------------------- #


@dataclass(eq=False, frozen=True, kw_only=True)
class Expr:
    """Base class for all expression nodes."""


@dataclass(eq=False, frozen=True, kw_only=True)
class Source:
    """Base class for table-like sources."""

    alias: str | None = None


@dataclass(eq=False, frozen=True, kw_only=True)
class Reference(Source):
    """Represents a named table/view reference."""

    name: str


@dataclass(eq=False, frozen=True, kw_only=True)
class Values(Source):
    """Represents a VALUES clause as a row source.

    Can be used in FROM clauses, JOINs, and CTEs.
    Requires an alias when used as a source.
    """

    rows: tuple[dict[str, object], ...]


# --- Raw SQL Escape Hatches ------------------------------------------------ #


@dataclass(eq=False, frozen=True, kw_only=True)
class RawExpr(Expr):
    """Raw SQL expression with named parameter substitution.

    WARNING: Bypasses vw's type checking and syntax validation.
    Only use for SQL features vw doesn't support yet.

    The sql field contains a template string with {name} placeholders.
    The params field contains (name, Expr) tuples for substitution.
    Rendering happens at render time to support context-dependent features.

    Example:
        RawExpr(
            sql="{x} @> {y}",
            params=(("x", Column(name="tags")), ("y", Parameter(name="tag", value="python")))
        )
        # Renders to: tags @> $tag
    """

    sql: str
    params: tuple[tuple[str, Expr], ...] = field(default_factory=tuple)


@dataclass(eq=False, frozen=True, kw_only=True)
class RawSource(Source):
    """Raw SQL source/table expression with named parameter substitution.

    WARNING: Bypasses vw's type checking and syntax validation.
    Only use for SQL features vw doesn't support yet.

    The sql field contains a template string with {name} placeholders.
    The params field contains (name, Expr) tuples for substitution.
    Rendering happens at render time to support context-dependent features.

    Example:
        RawSource(
            sql="generate_series(1, {n})",
            params=(("n", Parameter(name="max", value=10)),),
            alias="t"
        )
        # Renders to: generate_series(1, $max) AS t
    """

    sql: str
    params: tuple[tuple[str, Expr], ...] = field(default_factory=tuple)


@dataclass(eq=False, frozen=True, kw_only=True)
class Column(Expr):
    """Represents a column reference."""

    name: str
    alias: str | None = None


@dataclass(eq=False, frozen=True, kw_only=True)
class Parameter(Expr):
    """Represents a query parameter."""

    name: str
    value: object


@dataclass(eq=False, frozen=True, kw_only=True)
class Limit:
    """Represents LIMIT and optional OFFSET for pagination."""

    count: int
    offset: int | None = None


@dataclass(eq=False, frozen=True, kw_only=True)
class Distinct:
    """Represents DISTINCT or DISTINCT ON clause in a SQL statement.

    on is populated only for DISTINCT ON — PostgreSQL-specific.
    """

    on: tuple[Expr, ...] = field(default_factory=tuple)


# --- Joins ----------------------------------------------------------------- #


@dataclass(eq=False, frozen=True, kw_only=True)
class Join:
    """Represents a SQL join clause."""

    jtype: str
    "Join type (e.g., 'INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS')"
    right: Reference | Statement | SetOperation | Values | RawSource
    on: tuple[Expr, ...] = field(default_factory=tuple)
    using: tuple[Expr, ...] = field(default_factory=tuple)
    lateral: bool = False


@dataclass(eq=False, frozen=True, kw_only=True)
class Statement(Source):
    """Represents a SELECT query."""

    source: Reference | Statement | SetOperation | Values | RawSource
    columns: tuple[Expr, ...] = field(default_factory=tuple)
    where_conditions: tuple[Expr, ...] = field(default_factory=tuple)
    group_by_columns: tuple[Expr, ...] = field(default_factory=tuple)
    having_conditions: tuple[Expr, ...] = field(default_factory=tuple)
    order_by_columns: tuple[Expr, ...] = field(default_factory=tuple)
    limit: Limit | None = None
    distinct: Distinct | None = None
    joins: tuple[Join, ...] = field(default_factory=tuple)


# --- Binary Operators ------------------------------------------------------ #


@dataclass(eq=False, frozen=True, kw_only=True)
class Operator(Expr):
    """Represents an infix binary operator (e.g. =, +, AND, ||)."""

    operator: str
    left: Expr
    right: Expr


# --- Logical Operators ----------------------------------------------------- #


@dataclass(eq=False, frozen=True, kw_only=True)
class Not(Expr):
    """Represents a logical NOT expression."""

    operand: Expr


# --- Pattern Matching ------------------------------------------------------ #


@dataclass(eq=False, frozen=True, kw_only=True)
class Like(Expr):
    """Represents a LIKE pattern match."""

    left: Expr
    right: Expr


@dataclass(eq=False, frozen=True, kw_only=True)
class NotLike(Expr):
    """Represents a NOT LIKE pattern match."""

    left: Expr
    right: Expr


@dataclass(eq=False, frozen=True, kw_only=True)
class IsIn(Expr):
    """Represents an IN expression."""

    expr: Expr
    values: tuple[Expr, ...]


@dataclass(eq=False, frozen=True, kw_only=True)
class IsNotIn(Expr):
    """Represents a NOT IN expression."""

    expr: Expr
    values: tuple[Expr, ...]


@dataclass(eq=False, frozen=True, kw_only=True)
class Between(Expr):
    """Represents a BETWEEN expression."""

    expr: Expr
    lower_bound: Expr
    upper_bound: Expr


@dataclass(eq=False, frozen=True, kw_only=True)
class NotBetween(Expr):
    """Represents a NOT BETWEEN expression."""

    expr: Expr
    lower_bound: Expr
    upper_bound: Expr


# --- NULL Checks ----------------------------------------------------------- #


@dataclass(eq=False, frozen=True, kw_only=True)
class IsNull(Expr):
    """Represents IS NULL check."""

    expr: Expr


@dataclass(eq=False, frozen=True, kw_only=True)
class IsNotNull(Expr):
    """Represents IS NOT NULL check."""

    expr: Expr


# --- Subquery Operators ---------------------------------------------------- #


@dataclass(eq=False, frozen=True, kw_only=True)
class Exists(Expr):
    """Represents EXISTS subquery check."""

    subquery: Reference | Statement | SetOperation | Values | RawSource


@dataclass(eq=False, frozen=True, kw_only=True)
class ScalarSubquery(Expr):
    """Represents a subquery used as a scalar expression (e.g., in SELECT or comparisons)."""

    query: Statement | SetOperation


# --- Conditional Expressions ----------------------------------------------- #


@dataclass(eq=False, frozen=True, kw_only=True)
class WhenThen:
    """A single WHEN/THEN pair in a CASE expression."""

    condition: Expr
    result: Expr


@dataclass(eq=False, frozen=True, kw_only=True)
class Case(Expr):
    """Searched CASE expression: CASE WHEN cond THEN val ... [ELSE val] END"""

    whens: tuple[WhenThen, ...]
    else_result: Expr | None = None


# --- Set Operations -------------------------------------------------------- #


@dataclass(eq=False, frozen=True, kw_only=True)
class SetOperation(Source):
    """Represents a set operation (UNION, INTERSECT, EXCEPT)."""

    left: Reference | Statement | SetOperation | Values | RawSource
    operator: str  # "UNION", "UNION ALL", "INTERSECT", "EXCEPT"
    right: Reference | Statement | SetOperation | Values | RawSource


# --- Common Table Expressions ---------------------------------------------- #


@dataclass(eq=False, frozen=True, kw_only=True)
class CTE(Statement):
    """A Common Table Expression (WITH clause).

    Extends Statement to represent a named query that can be
    referenced in subsequent queries like a table.
    """

    name: str
    recursive: bool = False


# --- Expression Modifiers -------------------------------------------------- #


@dataclass(eq=False, frozen=True, kw_only=True)
class Alias(Expr):
    """Represents an aliased expression (expr AS name)."""

    expr: Expr
    name: str


@dataclass(eq=False, frozen=True, kw_only=True)
class Cast(Expr):
    """Represents type cast (CAST or ::)."""

    expr: Expr
    data_type: str


@dataclass(eq=False, frozen=True, kw_only=True)
class Asc(Expr):
    """Represents ascending sort order (ASC)."""

    expr: Expr


@dataclass(eq=False, frozen=True, kw_only=True)
class Desc(Expr):
    """Represents descending sort order (DESC)."""

    expr: Expr


# --- Date/Time Expressions ------------------------------------------------- #


@dataclass(eq=False, frozen=True, kw_only=True)
class Extract(Expr):
    """Represents EXTRACT(field FROM expr) — ANSI SQL."""

    field: str
    expr: Expr


@dataclass(eq=False, frozen=True, kw_only=True)
class CurrentTimestamp(Expr):
    """Represents CURRENT_TIMESTAMP — ANSI SQL."""


@dataclass(eq=False, frozen=True, kw_only=True)
class CurrentDate(Expr):
    """Represents CURRENT_DATE — ANSI SQL."""


@dataclass(eq=False, frozen=True, kw_only=True)
class CurrentTime(Expr):
    """Represents CURRENT_TIME — ANSI SQL."""


# --- Grouping Constructs --------------------------------------------------- #


@dataclass(eq=False, frozen=True, kw_only=True)
class Rollup(Expr):
    """ROLLUP grouping construct for hierarchical subtotals."""

    columns: tuple[Expr, ...]


@dataclass(eq=False, frozen=True, kw_only=True)
class Cube(Expr):
    """CUBE grouping construct for all dimension combinations."""

    columns: tuple[Expr, ...]


@dataclass(eq=False, frozen=True, kw_only=True)
class GroupingSets(Expr):
    """GROUPING SETS construct for explicit grouping combinations."""

    sets: tuple[tuple[Expr, ...], ...]


# --- Functions ------------------------------------------------------------- #


@dataclass(eq=False, frozen=True, kw_only=True)
class Function(Expr):
    """Represents a SQL function (aggregate, window-only, or scalar)."""

    name: str
    args: tuple[object, ...] = field(default_factory=tuple)
    filter: object | None = None


@dataclass(eq=False, frozen=True, kw_only=True)
class WindowFunction(Expr):
    """Represents a window function with OVER clause."""

    function: object
    partition_by: tuple[object, ...] = field(default_factory=tuple)
    order_by: tuple[object, ...] = field(default_factory=tuple)
    frame: object | None = None


# --- Window Frame Clauses -------------------------------------------------- #


@dataclass(eq=False, frozen=True, kw_only=True)
class FrameClause:
    """Represents a window frame clause (ROWS/RANGE BETWEEN)."""

    mode: str  # "ROWS" or "RANGE"
    start: object
    end: object
    exclude: str | None = None  # "CURRENT ROW", "GROUP", "TIES", "NO OTHERS"


# --- Frame Boundaries ------------------------------------------------------ #


@dataclass(eq=False, frozen=True, kw_only=True)
class UnboundedPreceding:
    """Represents UNBOUNDED PRECEDING frame boundary."""


@dataclass(eq=False, frozen=True, kw_only=True)
class UnboundedFollowing:
    """Represents UNBOUNDED FOLLOWING frame boundary."""


@dataclass(eq=False, frozen=True, kw_only=True)
class CurrentRow:
    """Represents CURRENT ROW frame boundary."""


@dataclass(eq=False, frozen=True, kw_only=True)
class Preceding:
    """Represents n PRECEDING frame boundary."""

    count: int


@dataclass(eq=False, frozen=True, kw_only=True)
class Following:
    """Represents n FOLLOWING frame boundary."""

    count: int
