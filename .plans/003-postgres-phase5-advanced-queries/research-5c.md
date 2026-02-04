# Research: Phase 5c - CTEs (Common Table Expressions)

## Overview
CTEs (Common Table Expressions) allow defining temporary named result sets that can be referenced in a query. They use the WITH clause and support both non-recursive and recursive queries.

## Reference Implementation Analysis

### Architecture

The reference implementation uses a **registration pattern** during rendering:

1. **CommonTableExpression class** extends RowSet (vw/reference/build.py)
   - Stores: name, query (Statement | SetOperation), _recursive flag
   - Inherits all RowSet methods (select, where, join, alias, etc.)
   - Can be used anywhere a table can be used

2. **Factory function**: `cte(name, query, *, recursive=False)`
   - Creates CommonTableExpression instances
   - Exported from vw/reference/__init__.py

3. **RenderContext tracking** (vw/reference/render.py)
   - `ctes: list[tuple[CommonTableExpression, str]]` - stores (cte_obj, body_sql)
   - `register_cte()` - adds CTE during rendering, checks for name collisions
   - `render_ctes()` - generates WITH clause from registered CTEs

4. **Rendering flow**:
   - CTE's `__vw_render__()` calls `context.register_cte(self, body_sql)`
   - Returns just the CTE name (or aliased name)
   - After main query renders, `render_ctes()` prepends WITH clause

### Key Features

**Non-Recursive CTEs:**
```python
active_users = cte(
    "active_users",
    source("users").select(col("*")).where(col("status") == "active")
)
result = active_users.select(col("id"), col("name"))
# WITH active_users AS (SELECT * FROM users WHERE status = 'active')
# SELECT id, name FROM active_users
```

**Recursive CTEs:**
```python
# Anchor: top-level employees
anchor = source("employees").select(col("*")).where(col("manager_id").is_null())

# Recursive part: join to find subordinates
recursive = (
    source("org_hierarchy").alias("h")
    .join.inner(source("employees").alias("e"), on=[col("e.manager_id") == col("h.id")])
    .select(col("e.*"))
)

# Combine with UNION ALL
org = cte("org_hierarchy", anchor + recursive, recursive=True)
# WITH RECURSIVE org_hierarchy AS (
#   SELECT * FROM employees WHERE manager_id IS NULL
#   UNION ALL
#   SELECT e.* FROM org_hierarchy h INNER JOIN employees e ON e.manager_id = h.id
# )
```

**Multiple CTEs:**
```python
cte1 = cte("cte1", source("table1").select(col("*")))
cte2 = cte("cte2", source("table2").select(col("*")))
result = cte1.join.inner(cte2, on=[cte1.col("id") == cte2.col("id")])
# WITH cte1 AS (SELECT * FROM table1), cte2 AS (SELECT * FROM table2)
# SELECT * FROM cte1 INNER JOIN cte2 ON cte1.id = cte2.id
```

**CTE Name Collision Detection:**
```python
class CTENameCollisionError(VWError):
    """Raised when multiple CTEs with the same name are registered."""
```

### Test Coverage (test_ctes.py)

- ✅ Basic CTE with WHERE, qualified columns, parameters
- ✅ CTE in INNER JOIN
- ✅ Multiple CTEs in single query
- ✅ CTEs referencing other CTEs (dependency chain)
- ✅ CTEs with GROUP BY, HAVING, ORDER BY, LIMIT
- ✅ Recursive CTEs (hierarchy, graph traversal)
- ✅ Recursive with parameters
- ✅ UNION vs UNION ALL in recursive part
- ✅ Mixed recursive and non-recursive CTEs

## PostgreSQL Implementation Strategy

### Option 1: Port CommonTableExpression to vw/postgres/base.py

**Pros:**
- Mirrors reference implementation exactly
- Consistent pattern with Expression, RowSet, SetOperation

**Cons:**
- CommonTableExpression is dialect-agnostic (no PostgreSQL-specific behavior)
- Adds another class to vw/postgres/base.py

### Option 2: Use vw/core/ base class

**Pros:**
- CTEs are standard SQL, no dialect-specific behavior
- Reduces duplication between dialects

**Cons:**
- Would require refactoring reference implementation

### Recommendation: Option 1 (Port to vw/postgres/)

Follow existing pattern:
1. Create `CommonTableExpression` class in `vw/postgres/base.py`
2. Add `cte()` factory function to `vw/postgres/public.py`
3. Extend `RenderContext` in `vw/postgres/render.py` with CTE tracking
4. Add `CTENameCollisionError` to exceptions (if needed)
5. Port tests from `tests/reference/integration/test_ctes.py`

## Differences from Reference

**Reference:**
- Uses `__vw_render__(context)` method on all objects
- RenderContext has `recurse()` method for nested contexts
- Source, Statement, SetOperation all have `__vw_render__()`

**PostgreSQL:**
- Uses `render_state(state, ctx)` function pattern
- No `__vw_render__()` methods on wrapper classes
- Rendering is centralized in render.py

### Adaptation Strategy

Instead of:
```python
def __vw_render__(self, context):
    body_sql = self.query.__vw_render__(context.recurse())
    context.register_cte(self, body_sql)
    return self.name
```

Use:
```python
def render_cte(cte: CommonTableExpression, ctx: RenderContext) -> str:
    """Render CTE reference and register definition."""
    # Render the CTE body (Statement or SetOperationState)
    if isinstance(cte.query.state, Statement):
        body_sql = render_statement(cte.query.state, ctx)
    else:  # SetOperationState
        body_sql = render_set_operation(cte.query.state, ctx)

    # Register CTE with context
    ctx.register_cte(cte, body_sql)

    # Return reference (name or aliased name)
    if cte.alias:
        return f"{cte.name} AS {cte.alias}"
    return cte.name
```

## Implementation Challenges

### Challenge 1: CTE as Source

CTEs can be used in FROM clause, but PostgreSQL currently only accepts `Source | Statement[ExprT]` for RowSet.state.

**Solution:** Update RowSet.state type to include CommonTableExpression:
```python
class RowSet:
    state: Source | Statement[ExprT] | CommonTableExpression
```

### Challenge 2: Rendering WITH Clause

PostgreSQL render() currently returns SQL immediately. Need to check for CTEs and prepend WITH clause.

**Solution:** Update render() function:
```python
def render(obj: RowSet | Expression, *, config: RenderConfig | None = None) -> SQL:
    ctx = RenderContext(config=config or RenderConfig(param_style=ParamStyle.DOLLAR))

    # Render main query
    query = render_state(obj.state, ctx)

    # Prepend WITH clause if CTEs registered
    if ctx.ctes:
        with_clause = render_with_clause(ctx)
        query = f"{with_clause} {query}"

    return SQL(query=query, params=ctx.params)
```

### Challenge 3: CTE Name Collision

Need to track CTE names and detect collisions during rendering.

**Solution:** Add to RenderContext:
```python
@dataclass
class RenderContext:
    params: dict[str, object]
    ctes: list[tuple[CommonTableExpression, str]] = field(default_factory=list)

    def register_cte(self, cte: CommonTableExpression, body_sql: str) -> None:
        for existing_cte, _ in self.ctes:
            if existing_cte is cte:
                return  # Already registered
            if existing_cte.name == cte.name:
                raise CTENameCollisionError(f"CTE name '{cte.name}' defined multiple times")
        self.ctes.append((cte, body_sql))
```

## Files to Modify

1. **vw/postgres/base.py**
   - Add CommonTableExpression class

2. **vw/postgres/public.py**
   - Add cte() factory function
   - Export in __init__.py

3. **vw/postgres/render.py**
   - Add CTEs field to RenderContext
   - Add register_cte() method
   - Add render_with_clause() function
   - Update render() to prepend WITH clause
   - Add render_cte() function
   - Update render_state() to handle CommonTableExpression

4. **vw/postgres/exceptions.py** (create if needed)
   - Add CTENameCollisionError

5. **tests/postgres/integration/test_ctes.py** (create)
   - Port tests from reference implementation
   - Test basic CTEs, multiple CTEs, recursive CTEs

## Success Criteria

- ✅ Basic CTE creation with cte(name, query)
- ✅ CTE used as source in FROM clause
- ✅ CTE used in JOIN clauses
- ✅ Multiple CTEs in single query
- ✅ Recursive CTEs with recursive=True flag
- ✅ CTE name collision detection
- ✅ Parameters preserved from CTE queries
- ✅ All tests pass
- ✅ Type checking passes
