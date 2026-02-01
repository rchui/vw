# Research: PostgreSQL Phase 5 - Advanced Query Features

## Overview

Phase 5 is large and will be broken into sub-phases:
- **Phase 5a:** EXISTS and Subquery Support
- **Phase 5b:** Set Operations (UNION, INTERSECT, EXCEPT)
- **Phase 5c:** CTEs (Common Table Expressions)
- **Phase 5d:** VALUES and CASE Expressions

---

## Current Implementation Status

### What's Already Working ✅

From previous phases:
- Subqueries in FROM clause (Statement as source) - Phase 1
- Statement can be aliased and used as subquery
- Rendering of nested Statement in FROM clause

### Reference Implementation Status

The `vw/reference/` module has COMPLETE implementations of all Phase 5 features:

1. **Subqueries** ✅
   - `Exists` class in `vw/reference/operators.py`
   - `exists(subquery)` factory function
   - Subqueries work naturally through Statement class

2. **Set Operations** ✅
   - `SetOperation` class in `vw/reference/build.py` (lines 410-487)
   - Operator overloads: `|` (UNION), `+` (UNION ALL), `&` (INTERSECT), `-` (EXCEPT)
   - Proper parenthesization for nested operations
   - Tests: `tests/reference/integration/test_set_operations.py` (7 tests)

3. **CTEs** ✅
   - `CommonTableExpression` class in `vw/reference/build.py` (lines 34-92)
   - `cte(name, query, recursive=False)` factory
   - Multiple CTEs via nesting
   - Tests: `tests/reference/integration/test_ctes.py` (13 tests)

4. **VALUES** ✅
   - `Values` class in `vw/reference/values.py`
   - `values(*rows)` factory function
   - Support for dicts, mixed types, parameterization
   - Tests: `tests/reference/integration/test_values.py` (8 tests)

5. **CASE Expressions** ✅
   - `CaseExpression`, `When`, `WhenThen` classes in `vw/reference/operators.py` (lines 392-488)
   - `when(condition).then(value).otherwise(default)` API
   - Multiple WHEN branches via chaining
   - Tests: `tests/reference/integration/test_case_expressions.py` (9 tests)

---

## Architecture Pattern

### State/Wrapper/Render Pattern (Used by vw/postgres)

Each feature requires:

1. **State Classes** (`vw/core/states.py`)
   - Immutable frozen dataclasses
   - Pattern: `@dataclass(eq=False, frozen=True, kw_only=True)`
   - Store structure, not behavior

2. **Wrapper Methods** (various files in `vw/core/`)
   - Expression/RowSet wrapper methods
   - Fluent API for building queries
   - Return new instances (immutability)

3. **Rendering Functions** (`vw/postgres/render.py`)
   - Match on state type
   - Generate SQL strings
   - Collect parameters via RenderContext

4. **Factory Functions** (`vw/postgres/public.py`)
   - User-facing API
   - Create wrapped states
   - Type-safe construction

5. **Tests** (`tests/postgres/integration/`)
   - Integration tests verify SQL rendering
   - Unit tests verify state construction

---

## Phase 5a: EXISTS and Subquery Support

### Features to Implement

1. **EXISTS Operator**
   - `exists(subquery)` - returns Expression
   - Use in WHERE: `where(exists(subquery))`
   - Use with NOT: `where(~exists(subquery))`

2. **Subqueries in IN**
   - Extend existing `IsIn` to accept Statement
   - `col("id").is_in(subquery)`
   - `col("id").is_not_in(subquery)`

3. **Scalar Subqueries**
   - Subqueries in SELECT: `select(subquery.alias("count"))`
   - Subqueries in comparisons: `col("price") > subquery`
   - Return single value (user's responsibility to ensure)

4. **Correlated Subqueries**
   - Reference outer query columns in subquery
   - Already works naturally through column references
   - Need to test and document

### Reference Implementation Analysis

**Exists Class** (`vw/reference/operators.py:357-384`):
```python
@dataclass(frozen=True)
class Exists:
    """EXISTS subquery check."""
    subquery: Statement

    def __vw_render__(self, context):
        subquery_sql = render(self.subquery, context)
        return f"EXISTS ({subquery_sql})"
```

**Usage Pattern:**
```python
# Reference API
def exists(subquery: Statement) -> Exists:
    return Exists(subquery=subquery)

# Usage
users.where(
    exists(
        orders.where(orders.col("user_id") == users.col("id"))
    )
)
# SQL: WHERE EXISTS (SELECT * FROM orders WHERE orders.user_id = users.id)
```

**IsIn with Subquery** (`vw/reference/operators.py:181-221`):
```python
@dataclass(frozen=True)
class IsIn:
    expr: Expression
    values: tuple[Expression | Statement, ...]  # Can include Statement

    def __vw_render__(self, context):
        expr_sql = render(self.expr, context)

        # Check if any values are subqueries (Statement)
        if any(isinstance(v, Statement) for v in self.values):
            # Single subquery
            subquery = self.values[0]
            subquery_sql = render(subquery, context)
            return f"{expr_sql} IN ({subquery_sql})"
        else:
            # Regular values
            values_sql = ", ".join(render(v, context) for v in self.values)
            return f"{expr_sql} IN ({values_sql})"
```

### Data Structures Needed

**State Classes** (add to `vw/core/states.py`):
```python
@dataclass(eq=False, frozen=True, kw_only=True)
class Exists:
    """EXISTS subquery check state."""
    subquery: object  # Statement
```

**Modifications to Existing States:**
- `IsIn` state already exists, just needs to handle Statement in values tuple
- `IsNotIn` state already exists, same modification needed

### Implementation Steps

1. **Add Exists State** (`vw/core/states.py`)
   - Add Exists dataclass
   - Export in __all__

2. **Add exists() Factory** (`vw/postgres/public.py`)
   - Create exists(subquery) function
   - Returns Expression wrapping Exists state

3. **Update IsIn/IsNotIn** (`vw/core/states.py`)
   - Already support tuple of values
   - No state changes needed, just rendering logic

4. **Add Rendering** (`vw/postgres/render.py`)
   - Add `render_exists(exists, ctx)` function
   - Update `render_state()` match to handle Exists
   - Update `render_is_in()` and `render_is_not_in()` to handle Statement in values

5. **Add Tests** (`tests/postgres/integration/test_subqueries.py`)
   - EXISTS basic usage
   - EXISTS with NOT
   - EXISTS with correlated subquery
   - IN with subquery
   - NOT IN with subquery
   - Scalar subqueries in SELECT
   - Scalar subqueries in comparisons
   - Correlated subqueries

### SQL Examples

**EXISTS:**
```sql
-- Basic EXISTS
SELECT * FROM users
WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)

-- NOT EXISTS
SELECT * FROM users
WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
```

**IN with Subquery:**
```sql
-- IN with subquery
SELECT * FROM users
WHERE id IN (SELECT user_id FROM orders WHERE status = 'active')

-- NOT IN with subquery
SELECT * FROM users
WHERE id NOT IN (SELECT user_id FROM banned_users)
```

**Scalar Subqueries:**
```sql
-- Scalar subquery in SELECT
SELECT
    u.name,
    (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count
FROM users u

-- Scalar subquery in comparison
SELECT * FROM products
WHERE price > (SELECT AVG(price) FROM products)
```

**Correlated Subquery:**
```sql
-- Correlated subquery (references outer query)
SELECT * FROM users u
WHERE 10 < (
    SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id
)
```

### Testing Strategy

1. **Unit Tests** (`tests/postgres/test_subqueries.py`)
   - Exists state construction
   - IsIn state with Statement values

2. **Integration Tests** (`tests/postgres/integration/test_subqueries.py`)
   - EXISTS rendering
   - NOT EXISTS rendering
   - IN with subquery
   - NOT IN with subquery
   - Scalar subquery in SELECT
   - Scalar subquery in WHERE
   - Correlated subquery
   - Nested subqueries
   - Subquery with parameters

### Key Design Decisions

1. **No Subquery Validation**
   - Don't validate that scalar subqueries return single column
   - Don't validate that EXISTS subqueries are used correctly
   - Let PostgreSQL handle all validation errors
   - Follows existing philosophy from Phase 4 (joins)

2. **IsIn/IsNotIn Accepts Mixed Values**
   - Can accept both literals and Statement
   - If Statement present, render as subquery
   - Otherwise render as value list

3. **Scalar Subqueries as Expressions**
   - Statement can be used anywhere Expression is accepted
   - No special "scalar subquery" wrapper needed
   - User ensures it returns single value

4. **Correlated Subqueries Work Naturally**
   - Column references in subquery can refer to outer query
   - No special handling needed
   - Just works through column qualification

---

## Files to Create/Modify for Phase 5a

### Create:
- `tests/postgres/test_subqueries.py` - Unit tests
- `tests/postgres/integration/test_subqueries.py` - Integration tests

### Modify:
- `vw/core/states.py` - Add Exists state
- `vw/postgres/render.py` - Add render_exists(), update render_is_in/is_not_in
- `vw/postgres/public.py` - Add exists() factory
- `vw/postgres/__init__.py` - Export exists

---

## Questions & Clarifications

**Q1:** Should EXISTS subqueries be automatically wrapped in SELECT if not already?
**A1:** No. User provides Statement, we render it as-is. Let PostgreSQL handle errors.

**Q2:** Should we optimize EXISTS subqueries (e.g., force SELECT 1)?
**A2:** No. Render what user provides. No optimization or transformation.

**Q3:** How to handle IN with both literals and subquery?
**A3:** If any value is Statement, treat as subquery (must be only value). Otherwise literals.

**Q4:** Should scalar subqueries require explicit marking?
**A4:** No. Statement can be used as Expression anywhere. User ensures it returns single value.

---

## Success Criteria for Phase 5a

- ✅ EXISTS operator works in WHERE clauses
- ✅ NOT EXISTS works via ~ operator
- ✅ IN with subquery works
- ✅ NOT IN with subquery works
- ✅ Scalar subqueries work in SELECT
- ✅ Scalar subqueries work in comparisons
- ✅ Correlated subqueries work (tested)
- ✅ All tests pass (existing + new)
- ✅ Documentation updated
- ✅ CLAUDE.md index updated

---

## Implementation Timeline

Estimated: 2-3 hours for Phase 5a

- Add states and factories: 30 minutes
- Add rendering: 30 minutes
- Add tests: 60 minutes
- Documentation: 30 minutes
- Testing and fixes: 30 minutes
