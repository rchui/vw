# Implementation Plan: PostgreSQL Phase 5a - EXISTS and Subquery Support

## Overview

Implement EXISTS operator and subquery support for PostgreSQL dialect, including:
- EXISTS and NOT EXISTS in WHERE clauses
- Subqueries in IN/NOT IN operators
- Scalar subqueries in SELECT and expressions
- Correlated subqueries

**Approach:** Port from `vw/reference/` to `vw/postgres/` using state/wrapper/render pattern.

---

## Implementation Phases

### Phase 1: Core State Classes

Add EXISTS state to `vw/core/states.py`:

1. **Exists Dataclass**
   - Field: `subquery: object` (Statement)
   - Pattern: `@dataclass(eq=False, frozen=True, kw_only=True)`
   - Immutable, keyword-only

**Pattern:**
```python
@dataclass(eq=False, frozen=True, kw_only=True)
class Exists:
    """EXISTS subquery check."""
    subquery: object  # Statement
```

**No changes needed for IsIn/IsNotIn:**
- Already support tuple of values
- Can already include Statement objects
- Just needs rendering logic update

---

### Phase 2: Factory Functions

Add factory functions to `vw/postgres/public.py`:

1. **exists() Function**
   - Signature: `def exists(subquery: RowSet) -> Expression`
   - Extract subquery.state
   - Create Exists state
   - Wrap in Expression

**Pattern:**
```python
def exists(subquery: RowSet) -> Expression:
    """Create EXISTS subquery check.

    Args:
        subquery: Subquery to check for existence

    Returns:
        Expression wrapping Exists state

    Example:
        users.where(exists(orders.where(orders.col("user_id") == users.col("id"))))
    """
    from vw.core.states import Exists
    return Expression(state=Exists(subquery=subquery.state))
```

2. **Update Exports**
   - Add exists to `vw/postgres/__init__.py`
   - Add to __all__ list

---

### Phase 3: PostgreSQL Rendering

Add rendering functions to `vw/postgres/render.py`:

1. **render_exists() Function**
   - Input: `exists: Exists`, `ctx: RenderContext`
   - Output: `str` (e.g., "EXISTS (SELECT ...)")
   - Logic:
     - Render subquery: `render_statement(exists.subquery, ctx)`
     - Format: `"EXISTS ({subquery_sql})"`

2. **Update render_state()**
   - Add case: `case Exists(): return render_exists(state, ctx)`

3. **Update render_is_in() and render_is_not_in()**
   - Check if any value in `state.values` is Statement
   - If yes: render as subquery (should be only value)
   - If no: render as value list (current behavior)

**Pattern:**
```python
def render_exists(exists: Exists, ctx: RenderContext) -> str:
    """Render EXISTS subquery check."""
    subquery_sql = render_statement(exists.subquery, ctx)
    return f"EXISTS ({subquery_sql})"

def render_is_in(state: IsIn, ctx: RenderContext) -> str:
    """Render IN expression."""
    expr_sql = render_state(state.expr, ctx)

    # Check if subquery (Statement) in values
    has_statement = any(isinstance(v, Statement) for v in state.values)

    if has_statement:
        # Subquery IN
        subquery = state.values[0]  # Should be only value
        subquery_sql = render_statement(subquery, ctx)
        return f"{expr_sql} IN ({subquery_sql})"
    else:
        # Value list IN (existing logic)
        values = ", ".join(render_state(v, ctx) for v in state.values)
        return f"{expr_sql} IN ({values})"
```

---

### Phase 4: Unit Tests

Create `tests/postgres/test_subqueries.py`:

1. **Test Exists State Construction**
   - Test Exists dataclass creation
   - Test immutability (frozen)

2. **Test exists() Factory**
   - Test exists() creates correct state
   - Test wraps in Expression

3. **Test IsIn with Statement**
   - Test IsIn with Statement in values tuple
   - Verify state structure

**Pattern:**
```python
def describe_exists():
    def it_creates_exists_state():
        """Test EXISTS state construction."""
        orders = source("orders")
        subquery = orders.where(col("status") == "active")

        expr = exists(subquery)

        assert isinstance(expr.state, Exists)
        assert isinstance(expr.state.subquery, Statement)
```

---

### Phase 5: Integration Tests

Create `tests/postgres/integration/test_subqueries.py`:

1. **describe_exists()**
   - Basic EXISTS in WHERE
   - NOT EXISTS via ~ operator
   - EXISTS with correlated subquery
   - EXISTS with complex subquery

2. **describe_in_subquery()**
   - Basic IN with subquery
   - NOT IN with subquery
   - IN with parameterized subquery

3. **describe_scalar_subqueries()**
   - Scalar subquery in SELECT
   - Scalar subquery in WHERE comparison
   - Scalar subquery with aggregation

4. **describe_correlated_subqueries()**
   - Correlated subquery with EXISTS
   - Correlated subquery with IN
   - Correlated subquery with scalar comparison

**Pattern:**
```python
def describe_exists():
    def it_builds_basic_exists():
        """
        SELECT * FROM users
        WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
        """
        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = users.where(
            exists(
                orders.select(lit(1)).where(
                    orders.col("user_id") == users.col("id")
                )
            )
        )

        result = render(query)
        assert result.query == sql("""
            SELECT * FROM users AS u
            WHERE EXISTS (SELECT 1 FROM orders AS o WHERE o.user_id = u.id)
        """)

    def it_builds_not_exists():
        """
        SELECT * FROM users
        WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
        """
        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = users.where(
            ~exists(
                orders.select(lit(1)).where(
                    orders.col("user_id") == users.col("id")
                )
            )
        )

        result = render(query)
        assert result.query == sql("""
            SELECT * FROM users AS u
            WHERE NOT (EXISTS (SELECT 1 FROM orders AS o WHERE o.user_id = u.id))
        """)

def describe_in_subquery():
    def it_builds_in_with_subquery():
        """
        SELECT * FROM users
        WHERE id IN (SELECT user_id FROM orders WHERE status = 'active')
        """
        users = source("users")
        orders = source("orders")

        query = users.where(
            col("id").is_in(
                orders.select(col("user_id")).where(col("status") == "active")
            )
        )

        result = render(query)
        assert result.query == sql("""
            SELECT * FROM users
            WHERE id IN (SELECT user_id FROM orders WHERE status = 'active')
        """)
```

---

### Phase 6: Documentation Updates

1. **docs/api/core.md**
   - Add "Subquery Expressions" section
   - Document exists() function
   - Document subqueries in is_in()
   - Document scalar subqueries
   - Add examples

2. **docs/api/postgres.md**
   - Add "Subqueries" section with examples
   - Show EXISTS, IN with subquery, scalar subquery examples
   - Update Feature Status

3. **docs/quickstart.md**
   - Add "Subqueries" section (expand existing)
   - Add EXISTS example
   - Add IN with subquery example
   - Add scalar subquery example

4. **docs/development/postgres-parity.md**
   - Mark Phase 5a tasks as completed
   - Update progress percentage

5. **CLAUDE.md**
   - Add subquery documentation references

---

## Testing Strategy

### After Each Phase:
1. Run tests: `uv run pytest`
2. Run linter: `uv run ruff check`
3. Run formatter: `uv run ruff format`
4. Run type checker: `uv run ty check`

### Complete Test Coverage:
- Unit tests for Exists state construction
- Integration tests for EXISTS rendering
- Integration tests for IN with subquery
- Integration tests for scalar subqueries
- Integration tests for correlated subqueries
- Edge cases (nested subqueries, parameters)

---

## Incremental Implementation

Follow CLAUDE.md principle #10 (Incrementality):

1. **Atomic changes** - Each phase is complete and testable
2. **Sequential** - Phases build on each other
3. **Testable** - Test after each phase
4. **Rollback-friendly** - Each commit is self-contained

**Commit Strategy:**
- Phase 1-2: "feat(core): add EXISTS state and factory"
- Phase 3: "feat(postgres): add subquery rendering (EXISTS, IN)"
- Phases 4-5: "test(postgres): add subquery tests"
- Phase 6: "docs: add subquery documentation"

Or combine into:
- "feat(postgres): implement Phase 5a subqueries (EXISTS, IN, scalar)"

---

## Success Criteria

**Implementation:**
- ✅ Exists state in vw/core/states.py
- ✅ exists() factory in vw/postgres/public.py
- ✅ render_exists() function
- ✅ Updated render_is_in() and render_is_not_in()
- ✅ Updated render_state() match statement

**Testing:**
- ✅ Unit tests for Exists state
- ✅ Integration tests for EXISTS
- ✅ Integration tests for IN with subquery
- ✅ Integration tests for scalar subqueries
- ✅ Integration tests for correlated subqueries
- ✅ All existing tests still pass

**Documentation:**
- ✅ API documentation updated
- ✅ Examples added to quickstart
- ✅ Roadmap marked complete
- ✅ CLAUDE.md index updated

**Quality:**
- ✅ All tests pass
- ✅ Linters pass
- ✅ Type checker passes
- ✅ No regressions in existing features

---

## Risk Mitigation

**Risk 1: Breaking existing IsIn behavior**
- Mitigation: Check for Statement type before subquery logic
- Mitigation: Maintain backward compatibility with value lists

**Risk 2: Scalar subquery validation**
- Mitigation: No validation - let PostgreSQL handle errors
- Mitigation: Document user responsibility clearly

**Risk 3: Correlated subquery complexity**
- Mitigation: Already works through column references
- Mitigation: Just test and document

---

## Notes

- No SQL validation - let PostgreSQL handle all errors
- Scalar subqueries use Statement as Expression (no wrapper needed)
- Correlated subqueries work naturally through column qualification
- EXISTS is just a state wrapper around Statement
- Follow existing patterns from Phases 1-4
