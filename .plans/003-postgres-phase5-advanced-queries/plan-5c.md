# Plan: Phase 5c - CTEs (Common Table Expressions)

## Overview
Implement Common Table Expressions (CTEs) for PostgreSQL dialect by adding CTE support to the core wrapper classes and implementing PostgreSQL-specific rendering.

## Goals
1. Enable basic CTEs via `cte(name, query)`
2. Support recursive CTEs via `cte(name, query, recursive=True)`
3. Allow CTEs to be used as sources (FROM, JOIN)
4. Support multiple CTEs in a single query
5. Detect and error on CTE name collisions
6. Preserve parameters from CTE queries

## Architecture Decision

**CTEs belong in vw/core/** because:
- Standard SQL feature with identical behavior across dialects
- No dialect-specific CTE functionality
- Reduces duplication across PostgreSQL, DuckDB, SQLite, etc.

## Implementation Strategy

### Step 1: Add CommonTableExpression to vw/core/base.py

Add after SetOperation class:

```python
@dataclass(eq=False, frozen=True, kw_only=True)
class CommonTableExpression(RowSet[ExprT, RowSetT, SetOpT]):
    """Wrapper for Common Table Expression (CTE).

    CTEs are temporary named result sets defined with WITH clause.
    Can be used anywhere a table can be used (FROM, JOIN, etc.).

    Extends RowSet, inheriting all query-building methods.
    """

    name: str
    query: RowSetT | SetOpT  # Statement or SetOperation
    recursive: bool = False

    # Override state to return the query's state
    @property
    def state(self) -> Any:
        """Return the underlying query's state."""
        return self.query.state
```

**Key design points:**
- Extends RowSet, so inherits select(), where(), join, alias(), etc.
- Stores name, query, and recursive flag
- Query is typed as RowSetT | SetOpT (can be Statement or SetOperation)
- state property returns the underlying query's state for rendering

### Step 2: Add PostgreSQL Subclass to vw/postgres/base.py

Add after SetOperation:

```python
@dataclass(eq=False, frozen=True, kw_only=True)
class CommonTableExpression(core_base.CommonTableExpression[ExprT, RowSetT, SetOpT]):
    """PostgreSQL Common Table Expression wrapper."""

    pass  # Thin wrapper, no custom behavior
```

### Step 3: Update Factories to Include CTE Type

Update vw/core/base.py Factories class:

```python
@dataclass(eq=False, frozen=True, kw_only=True)
class Factories(FactoryT):
    expr: type[ExprT]
    rowset: type[RowSetT]
    setop: type[SetOpT]
    cte: type[CTET]  # Add CTE type
```

Add CTET type variable:

```python
ExprT = TypeVar("ExprT", bound="Expression")
RowSetT = TypeVar("RowSetT", bound="RowSet")
SetOpT = TypeVar("SetOpT", bound="SetOperation")
CTET = TypeVar("CTET", bound="CommonTableExpression")  # Add CTE type variable
```

Update FactoryT:

```python
FactoryT = Generic[ExprT, RowSetT, SetOpT, CTET]
```

Update all class signatures to include CTET:

```python
class Expression(Stateful, Generic[ExprT, RowSetT, SetOpT, CTET]):
    factories: Factories[ExprT, RowSetT, SetOpT, CTET]

class RowSet(Stateful, Generic[ExprT, RowSetT, SetOpT, CTET]):
    factories: Factories[ExprT, RowSetT, SetOpT, CTET]

class SetOperation(RowSet[ExprT, RowSetT, SetOpT, CTET]):
    pass

class CommonTableExpression(RowSet[ExprT, RowSetT, SetOpT, CTET]):
    pass
```

### Step 4: Add cte() Factory Function to vw/postgres/public.py

```python
def cte(name: str, query: RowSet, /, *, recursive: bool = False) -> CommonTableExpression:
    """Create a Common Table Expression (CTE).

    CTEs define temporary named result sets using the WITH clause.
    They can be used anywhere a table can be used (FROM, JOIN, subqueries).

    Args:
        name: The name for the CTE.
        query: The query that defines the CTE (must have .select() called).
        recursive: If True, creates WITH RECURSIVE for self-referencing CTEs.

    Returns:
        A CommonTableExpression that can be used like a table.

    Example:
        >>> active_users = cte(
        ...     "active_users",
        ...     source("users").select(col("*")).where(col("active") == True)
        ... )
        >>> result = active_users.select(col("id"), col("name"))
        >>> # WITH active_users AS (SELECT * FROM users WHERE active = true)
        >>> # SELECT id, name FROM active_users

    Recursive Example:
        >>> # Anchor: top-level items
        >>> anchor = source("items").select(col("*")).where(col("parent_id").is_null())
        >>>
        >>> # Recursive: find children
        >>> tree = cte("tree", anchor, recursive=True)  # Temporary
        >>> recursive_part = (
        ...     tree.alias("t")
        ...     .join.inner(source("items").alias("i"), on=[col("i.parent_id") == col("t.id")])
        ...     .select(col("i.*"))
        ... )
        >>>
        >>> # Final CTE with UNION ALL
        >>> tree = cte("tree", anchor + recursive_part, recursive=True)
    """
    return CommonTableExpression(
        name=name,
        query=query,
        recursive=recursive,
        factories=Factories,
    )
```

Export from vw/postgres/__init__.py:

```python
from vw.postgres.public import cte, col, param, render, source, F
```

### Step 5: Update RenderContext in vw/postgres/render.py

Add CTE tracking to RenderContext:

```python
@dataclass
class RenderContext:
    config: RenderConfig
    params: dict[str, object] = field(default_factory=dict)
    ctes: list[tuple[str, str, bool]] = field(default_factory=list)  # (name, body_sql, recursive)

    def register_cte(self, name: str, body_sql: str, recursive: bool) -> None:
        """Register a CTE with its rendered body SQL.

        Args:
            name: CTE name
            body_sql: Rendered SQL for CTE body
            recursive: Whether this is a recursive CTE

        Raises:
            ValueError: If CTE name already registered with different definition
        """
        for existing_name, existing_sql, _ in self.ctes:
            if existing_name == name:
                if existing_sql == body_sql:
                    return  # Same CTE, already registered
                raise ValueError(f"CTE name '{name}' defined multiple times with different queries")
        self.ctes.append((name, body_sql, recursive))
```

### Step 6: Add CTE Rendering Functions to vw/postgres/render.py

```python
def render_cte_reference(cte: CommonTableExpression, ctx: RenderContext) -> str:
    """Render CTE reference and register its definition.

    Args:
        cte: CommonTableExpression to reference
        ctx: Rendering context for CTE registration

    Returns:
        The CTE reference name (or aliased name)
    """
    from vw.core.states import Statement, SetOperationState

    # Render the CTE body
    if isinstance(cte.query.state, Statement):
        body_sql = render_statement(cte.query.state, ctx)
    elif isinstance(cte.query.state, SetOperationState):
        body_sql = render_set_operation(cte.query.state, ctx)
    else:
        raise TypeError(f"CTE query must be Statement or SetOperationState, got {type(cte.query.state)}")

    # Register CTE
    ctx.register_cte(cte.name, body_sql, cte.recursive)

    # Return reference
    reference = cte.name
    if cte.state.alias:  # CTEs can be aliased
        reference += f" AS {cte.state.alias}"

    return reference


def render_with_clause(ctx: RenderContext) -> str:
    """Render the WITH clause from registered CTEs.

    Args:
        ctx: Rendering context with registered CTEs

    Returns:
        The WITH clause SQL (e.g., "WITH RECURSIVE cte1 AS (...), cte2 AS (...)")
    """
    if not ctx.ctes:
        return ""

    # Check if any CTE is recursive
    has_recursive = any(recursive for _, _, recursive in ctx.ctes)

    # Build CTE definitions
    cte_definitions = [f"{name} AS ({body_sql})" for name, body_sql, _ in ctx.ctes]

    # Build WITH clause
    with_keyword = "WITH RECURSIVE" if has_recursive else "WITH"
    return f"{with_keyword} {', '.join(cte_definitions)}"
```

### Step 7: Update render_state() to Handle CTEs

Add case in render_state() match statement:

```python
def render_state(state: object, ctx: RenderContext) -> str:
    match state:
        # ... existing cases ...

        case CommonTableExpression():
            return render_cte_reference(state, ctx)

        case _:
            raise TypeError(f"Unknown state type: {type(state)}")
```

Wait, this won't work because CommonTableExpression is a wrapper, not a state. Let me reconsider...

Actually, when rendering queries that USE a CTE, we need to detect it in the FROM clause. The CTE itself doesn't have a separate state - it wraps a query that has a state.

**Revised approach:**

Update render_statement() to check if source is a CTE:

```python
def render_statement(stmt: Statement, ctx: RenderContext) -> str:
    """Render a Statement to SQL."""
    from vw.postgres.base import CommonTableExpression

    parts = []

    # SELECT clause
    if stmt.columns:
        columns_sql = ", ".join(render_state(col, ctx) for col in stmt.columns)
        parts.append(f"SELECT {columns_sql}")

    # FROM clause
    if isinstance(stmt.source, CommonTableExpression):
        # CTE reference
        from_sql = render_cte_reference(stmt.source, ctx)
        parts.append(f"FROM {from_sql}")
    elif isinstance(stmt.source, Source):
        parts.append(f"FROM {render_source(stmt.source, ctx)}")
    elif isinstance(stmt.source, Statement):
        subquery_sql = render_statement(stmt.source, ctx)
        parts.append(f"FROM ({subquery_sql})")
        if stmt.source.alias:
            parts.append(f"AS {stmt.source.alias}")

    # ... rest of rendering ...
```

### Step 8: Update render() to Prepend WITH Clause

```python
def render(obj: RowSet | Expression, *, config: RenderConfig | None = None) -> SQL:
    """Render a RowSet or Expression to PostgreSQL SQL."""
    ctx = RenderContext(config=config or RenderConfig(param_style=ParamStyle.DOLLAR))

    # Render main query
    if isinstance(obj.state, Source):
        query = f"FROM {render_source(obj.state, ctx)}"
    elif isinstance(obj.state, SetOperationState):
        query = render_set_operation(obj.state, ctx)
    else:
        query = render_state(obj.state, ctx)

    # Prepend WITH clause if CTEs were registered
    if ctx.ctes:
        with_clause = render_with_clause(ctx)
        query = f"{with_clause} {query}"

    return SQL(query=query, params=ctx.params)
```

### Step 9: Create Tests

Create tests/postgres/integration/test_ctes.py:

**Basic CTE tests:**
- Simple CTE with SELECT
- CTE with WHERE clause
- CTE with qualified columns
- CTE with parameters

**Multiple CTE tests:**
- Two CTEs in same query
- CTE referencing another CTE

**Recursive CTE tests:**
- Basic recursive CTE (hierarchy traversal)
- Recursive CTE with UNION ALL

**CTE usage tests:**
- CTE in FROM clause
- CTE in JOIN clause
- CTE with alias

## Testing Checklist

- [ ] Basic CTE creation
- [ ] CTE with WHERE, qualified columns, parameters
- [ ] CTE in FROM clause
- [ ] CTE in JOIN clause
- [ ] Multiple CTEs in single query
- [ ] CTE referencing another CTE
- [ ] Recursive CTE with UNION ALL
- [ ] CTE name collision detection
- [ ] All existing tests still pass
- [ ] Type checking passes

## Success Criteria

- ✅ cte() factory function creates CommonTableExpression
- ✅ CTEs can be used in FROM and JOIN clauses
- ✅ Multiple CTEs work in single query
- ✅ Recursive CTEs generate WITH RECURSIVE
- ✅ CTE name collisions are detected and raise error
- ✅ Parameters preserved from CTE queries
- ✅ All tests pass (existing + new)
- ✅ Type checking passes
