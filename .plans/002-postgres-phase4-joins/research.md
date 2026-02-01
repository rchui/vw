# Research: PostgreSQL Phase 4 - Joins Implementation

## User Requirements

**Implement:** Standard SQL joins (INNER, LEFT, RIGHT, FULL OUTER, CROSS)
**Exclude:**
- SEMI JOIN (not standard PostgreSQL)
- ANTI JOIN (not standard PostgreSQL)

**Support:**
- ON clause for arbitrary join conditions
- USING clause for natural joins on column names

**Validation:**
- No validation on join conditions - let PostgreSQL handle errors

---

## Architecture Exploration

### 1. State Pattern (vw/core/states.py)

All states are immutable dataclasses with pattern:
```python
@dataclass(eq=False, frozen=True, kw_only=True)
class SomeState:
    field1: type
    field2: type = default
```

**Existing State Classes:**
- `Source` - table/view reference (name, alias)
- `Column` - column reference (name, alias)
- `Statement` - complete SELECT query with fields:
  - `source: Source | Statement`
  - `alias: str | None`
  - `columns: tuple[ExpressionState, ...]`
  - `where_conditions: tuple[ExpressionState, ...]`
  - `group_by_columns: tuple[ExpressionState, ...]`
  - `having_conditions: tuple[ExpressionState, ...]`
  - `order_by_columns: tuple[ExpressionState, ...]`
  - `limit: Limit | None`
  - `distinct: Distinct | None`

**Pattern for Joins:**
Need to add `joins: tuple[Join, ...]` field to Statement.

---

### 2. Wrapper Pattern (vw/core/base.py)

**RowSet Wrapper:**
- Wraps `Source | Statement` in `state` field
- Methods return new RowSet with modified state (immutability)
- Source → Statement transformation happens automatically

**Method Patterns:**
- **Accumulating:** `.where()`, `.having()` - append to tuple
- **Replacing:** `.group_by()`, `.order_by()`, `.limit()` - replace previous value
- **Accessor pattern:** Properties that return helper objects

**Example - WHERE (accumulating):**
```python
def where(self, *conditions: ExprT) -> RowSetT:
    stmt = self._ensure_statement()
    return self.factories.rowset(
        replace(stmt, where_conditions=stmt.where_conditions + tuple(conditions))
    )
```

**Example - ORDER BY (replacing):**
```python
def order_by(self, *columns: ExprT) -> RowSetT:
    stmt = self._ensure_statement()
    return self.factories.rowset(
        replace(stmt, order_by_columns=tuple(columns))
    )
```

**Join Implementation Pattern:**
Joins should ACCUMULATE (allow multiple joins), similar to WHERE:
```python
@property
def join(self) -> JoinAccessor[ExprT, RowSetT, SetOpT]:
    return JoinAccessor(self)

# Then JoinAccessor has methods:
class JoinAccessor:
    def inner(self, right: RowSetT, *, on: list[ExprT]) -> RowSetT:
        # Accumulate join to tuple
```

---

### 3. Rendering Pattern (vw/postgres/render.py)

**Structure:**
```python
def render(obj) -> SQL:
    ctx = RenderContext(params={}, config=RenderConfig(...))
    query = render_state(obj.state, ctx)
    return SQL(query=query, params=ctx.params)

def render_state(state, ctx) -> str:
    match state:
        case Statement():
            return render_statement(state, ctx)
        case Source():
            return render_source(state, ctx)
        case Column():
            return render_column(state, ctx)
        # ... etc
```

**render_statement() Structure:**
```python
def render_statement(stmt: Statement, ctx: RenderContext) -> str:
    parts = []

    # SELECT clause
    parts.append("SELECT")
    if stmt.distinct:
        parts.append("DISTINCT")
    parts.append(", ".join([render_state(col, ctx) for col in stmt.columns]))

    # FROM clause
    parts.append("FROM")
    parts.append(render_state(stmt.source, ctx))

    # WHERE clause
    if stmt.where_conditions:
        parts.append("WHERE")
        parts.append(" AND ".join([render_state(cond, ctx) for cond in stmt.where_conditions]))

    # GROUP BY, HAVING, ORDER BY, LIMIT...

    return " ".join(parts)
```

**Pattern for Adding Joins:**
Insert join rendering between FROM and WHERE:
```python
# After FROM clause
for join in stmt.joins:
    join_sql = render_state(join, ctx)
    parts.append(join_sql)

# Then WHERE clause
```

Add join case to render_state:
```python
case Join():
    return render_join(state, ctx)
```

---

### 4. Reference Implementation (vw/reference/)

The reference implementation has complete joins at `vw/reference/joins.py`:

**Key Classes:**
- `JoinType` enum with INNER, LEFT, RIGHT, FULL_OUTER, CROSS, SEMI, ANTI
- `Join` dataclass with `jtype`, `right`, `on`, `using`, `lateral`
- Concrete join classes: `InnerJoin`, `LeftJoin`, `RightJoin`, `FullOuterJoin`, `CrossJoin`
- `JoinAccessor` - provides `.join.inner()`, `.join.left()`, etc.

**Key Validation:**
```python
def __post_init__(self):
    if self.on and self.using:
        raise ValueError("Cannot specify both ON and USING clauses")
```

**Rendering:**
```python
def __vw_render__(self, context):
    sql = f"{self.jtype.value} {render(self.right, context)}"
    if self.on:
        on_sql = " AND ".join([render(expr, context) for expr in self.on])
        sql += f" ON ({on_sql})"
    if self.using:
        using_sql = ", ".join([render(expr, context) for expr in self.using])
        sql += f" USING ({using_sql})"
    return sql
```

Note: Both ON and USING can be rendered if user specifies both. No validation is performed.

**Tests at tests/reference/integration/test_joins.py:**
- Comprehensive test coverage for all join types
- Tests single joins, multiple joins, complex conditions
- Tests parameters in join conditions

---

### 5. PostgreSQL Join Support

PostgreSQL natively supports:
- ✅ INNER JOIN
- ✅ LEFT JOIN (LEFT OUTER JOIN)
- ✅ RIGHT JOIN (RIGHT OUTER JOIN)
- ✅ FULL JOIN (FULL OUTER JOIN)
- ✅ CROSS JOIN
- ❌ SEMI JOIN (not a standard SQL keyword in PostgreSQL)
- ❌ ANTI JOIN (not a standard SQL keyword in PostgreSQL)

**PostgreSQL Alternatives:**
- Semi join: Use `EXISTS` or `IN` subquery
- Anti join: Use `NOT EXISTS` or `NOT IN` subquery

**ON vs USING:**
- ON: Arbitrary join condition (expressions)
- USING: Natural join on column names

Both ON and USING are supported. No validation is performed - users can specify both, neither, or just one. PostgreSQL will handle any errors.

---

### 6. Data Structures Needed

**States (vw/core/states.py):**
```python
class JoinType(str, Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"

@dataclass(eq=False, frozen=True, kw_only=True)
class Join:
    jtype: JoinType
    right: object  # RowSet (typed properly in implementation)
    on: tuple[object, ...] = ()  # Expression conditions
    using: tuple[object, ...] = ()  # Column expressions for USING clause
```

**Statement modification:**
Add `joins: tuple[Join, ...] = ()` field.

**Wrapper (vw/core/base.py):**
```python
class JoinAccessor(Generic[ExprT, RowSetT, SetOpT]):
    def __init__(self, rowset: RowSet[ExprT, RowSetT, SetOpT]):
        self._rowset = rowset

    def inner(self, right: RowSetT, *, on: list[ExprT] = [], using: list[ExprT] = []) -> RowSetT:
        ...

    def left(self, right: RowSetT, *, on: list[ExprT] = [], using: list[ExprT] = []) -> RowSetT:
        ...

    def right(self, right: RowSetT, *, on: list[ExprT] = [], using: list[ExprT] = []) -> RowSetT:
        ...

    def full_outer(self, right: RowSetT, *, on: list[ExprT] = [], using: list[ExprT] = []) -> RowSetT:
        ...

    def cross(self, right: RowSetT) -> RowSetT:
        # No on/using parameters - CROSS JOIN doesn't support them
        ...
```

**RowSet modification:**
Add property:
```python
@property
def join(self) -> JoinAccessor[ExprT, RowSetT, SetOpT]:
    return JoinAccessor(self)
```

---

### 7. SQL Generation Examples

**INNER JOIN:**
```sql
SELECT u.id, o.total
FROM users AS u
INNER JOIN orders AS o ON (u.id = o.user_id)
```

**LEFT JOIN:**
```sql
SELECT u.id, o.total
FROM users AS u
LEFT JOIN orders AS o ON (u.id = o.user_id)
```

**Multiple Joins:**
```sql
SELECT u.id, o.total, p.name
FROM users AS u
INNER JOIN orders AS o ON (u.id = o.user_id)
LEFT JOIN products AS p ON (o.product_id = p.id)
```

**CROSS JOIN:**
```sql
SELECT u.id, t.tag
FROM users AS u
CROSS JOIN tags AS t
```

**Join with USING:**
```sql
SELECT u.id, o.total
FROM users AS u
INNER JOIN orders AS o USING (user_id)
```

**Join with WHERE:**
```sql
SELECT u.id, o.total
FROM users AS u
INNER JOIN orders AS o ON (u.id = o.user_id)
WHERE u.active = TRUE
```

---

### 8. API Design

**Basic Usage:**
```python
from vw.postgres import source, col, render

users = source("users").alias("u")
orders = source("orders").alias("o")

query = (
    users
    .join.inner(orders, on=[users.col("id") == orders.col("user_id")])
    .select(users.col("id"), orders.col("total"))
)

result = render(query)
# SQL: SELECT u.id, o.total FROM users AS u INNER JOIN orders AS o ON (u.id = o.user_id)
```

**Multiple Conditions:**
```python
query = (
    users
    .join.inner(
        orders,
        on=[
            users.col("id") == orders.col("user_id"),
            orders.col("status") == param("status", "active")
        ]
    )
    .select(users.col("id"), orders.col("total"))
)

result = render(query)
# SQL: SELECT u.id, o.total FROM users AS u
#      INNER JOIN orders AS o ON (u.id = o.user_id AND o.status = $status)
```

**Multiple Joins:**
```python
products = source("products").alias("p")

query = (
    users
    .join.inner(orders, on=[users.col("id") == orders.col("user_id")])
    .join.left(products, on=[orders.col("product_id") == products.col("id")])
    .select(users.col("id"), orders.col("total"), products.col("name"))
)

result = render(query)
# SQL: SELECT u.id, o.total, p.name FROM users AS u
#      INNER JOIN orders AS o ON (u.id = o.user_id)
#      LEFT JOIN products AS p ON (o.product_id = p.id)
```

**CROSS JOIN:**
```python
tags = source("tags").alias("t")

query = (
    users
    .join.cross(tags)
    .select(users.col("id"), tags.col("tag"))
)

result = render(query)
# SQL: SELECT u.id, t.tag FROM users AS u CROSS JOIN tags AS t
```

**USING Clause:**
```python
query = (
    users
    .join.inner(orders, using=[col("user_id")])
    .select(users.col("id"), orders.col("total"))
)

result = render(query)
# SQL: SELECT u.id, o.total FROM users AS u INNER JOIN orders AS o USING (user_id)
```

---

### 9. Test Strategy

**Test Files:**
- Unit tests: `tests/postgres/test_joins.py` (state construction)
- Integration tests: `tests/postgres/integration/test_joins.py` (SQL rendering)

**Test Categories:**
1. **Basic join types** - INNER, LEFT, RIGHT, FULL, CROSS
2. **Single condition** - Simple equality join
3. **Multiple conditions** - AND-combined conditions
4. **Multiple joins** - Chain multiple joins together
5. **Joins with other clauses** - WHERE, GROUP BY, ORDER BY, LIMIT
6. **Parameters in joins** - Use param() in ON conditions
7. **Subqueries as join targets** - Join to a subquery
8. **Star selection with joins** - SELECT * with qualified stars

**Test Pattern:**
```python
def it_builds_inner_join():
    """
    SELECT u.id, o.total
    FROM users AS u
    INNER JOIN orders AS o ON (u.id = o.user_id)
    """
    users = source("users").alias("u")
    orders = source("orders").alias("o")

    query = (
        users
        .join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        .select(users.col("id"), orders.col("total"))
    )

    result = render(query)
    assert result.query == sql("""
        SELECT u.id, o.total
        FROM users AS u
        INNER JOIN orders AS o ON (u.id = o.user_id)
    """)
    assert result.params == {}
```

---

### 10. Edge Cases to Handle

1. **Empty ON clause:** Should error for INNER/LEFT/RIGHT/FULL (CROSS JOIN doesn't use ON)
2. **Joining to subqueries:** Right side can be a Statement with alias
3. **Self joins:** Joining table to itself with different aliases
4. **Complex expressions in ON:** Support any expression (arithmetic, functions, etc.)
5. **NULL handling:** ON conditions should work with NULL checks (IS NULL, IS NOT NULL)
6. **Type checking:** Ensure `right` parameter is a RowSet, `on` is list of Expression

---

## Key Implementation Decisions

1. ✅ **Exclude SEMI/ANTI joins** - Not standard PostgreSQL syntax
2. ✅ **Support both ON and USING** - Allow arbitrary expressions in both
3. ✅ **No SQL validation** - Let PostgreSQL handle all validation errors
4. ✅ **Accumulate joins** - Allow multiple joins to be chained
5. ✅ **Preserve join order** - Render joins in the order they were added
6. ✅ **Support all standard types** - INNER, LEFT, RIGHT, FULL, CROSS
7. ✅ **Follow existing patterns** - Immutable states, accessor pattern, match/case rendering

---

## Files to Modify

1. **vw/core/states.py**
   - Add `JoinType` enum
   - Add `Join` dataclass
   - Add `joins` field to `Statement`

2. **vw/core/base.py**
   - Add `JoinAccessor` class
   - Add `.join` property to `RowSet`

3. **vw/postgres/render.py**
   - Add `render_join()` function
   - Add `Join` case to `render_state()`
   - Update `render_statement()` to render joins

4. **tests/postgres/test_joins.py** (new)
   - Unit tests for join state construction

5. **tests/postgres/integration/test_joins.py** (new)
   - Integration tests for join SQL rendering

6. **docs/api/core.md**
   - Document `.join` property
   - Document `JoinAccessor` methods

7. **docs/api/postgres.md**
   - Add join examples
   - Update feature status

8. **docs/development/postgres-parity.md**
   - Mark Phase 4 as completed
   - Check off implemented features

9. **CLAUDE.md**
   - Add join documentation references to index

---

## Questions & Clarifications

**Q1:** Should we validate that ON and USING aren't both specified?
**A1:** No. No validation - let PostgreSQL handle it.

**Q2:** Should we validate that `on` is not empty for INNER/LEFT/RIGHT/FULL joins?
**A2:** No. No validation - let PostgreSQL handle it.

**Q3:** Should CROSS JOIN accept `on` and `using` parameters?
**A3:** Yes, for consistency. No validation - let PostgreSQL handle errors.

**Q4:** Should joins work with Source or only Statement?
**A4:** Joins should work with any RowSet (Source gets auto-converted to Statement).

**Q5:** What type should `using` accept?
**A5:** `list[ExprT]` - column expressions, same as other expression lists.

---

## Implementation Order

1. Add state classes (JoinType, Join, update Statement)
2. Add JoinAccessor and RowSet.join property
3. Add rendering (render_join, update render_statement)
4. Add unit tests (state construction)
5. Add integration tests (SQL rendering)
6. Update documentation
7. Run full test suite
8. Update CLAUDE.md index

---

## Success Criteria

- ✅ All 5 join types work: INNER, LEFT, RIGHT, FULL, CROSS
- ✅ Multiple conditions supported (AND-combined)
- ✅ Multiple joins can be chained
- ✅ Joins work with WHERE, GROUP BY, ORDER BY, LIMIT
- ✅ Parameters work in join conditions
- ✅ Subqueries can be join targets
- ✅ All tests pass (existing + new)
- ✅ Linters pass (ruff check, ruff format)
- ✅ Type checker passes (pyright)
- ✅ Documentation updated
