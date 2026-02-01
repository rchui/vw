# Implementation Plan: PostgreSQL Phase 4 - Joins

## Overview

Implement standard SQL joins (INNER, LEFT, RIGHT, FULL OUTER, CROSS) for vw/postgres, following the existing state/wrapper/renderer pattern.

**Exclusions:**
- SEMI JOIN (not standard PostgreSQL)
- ANTI JOIN (not standard PostgreSQL)

**Features:**
- ON clause support (arbitrary expressions)
- USING clause support (column expressions)
- No SQL validation (let PostgreSQL handle errors)

---

## Implementation Approach

### Phase 1: Core State Classes

Add join-related state classes to `vw/core/states.py`:

1. **JoinType Enum**
   - Values: INNER, LEFT, RIGHT, FULL, CROSS
   - Enum base: `str, Enum` for easy rendering

2. **Join Dataclass**
   - Fields: `jtype: JoinType`, `right: object`, `on: tuple[object, ...]`
   - Pattern: `@dataclass(eq=False, frozen=True, kw_only=True)`
   - Immutable, keyword-only arguments

3. **Update Statement**
   - Add field: `joins: tuple[Join, ...] = ()`
   - Maintains tuple of joins in order

**Pattern:**
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
    right: object  # RowSet
    on: tuple[object, ...] = ()  # Expression conditions for ON clause
    using: tuple[object, ...] = ()  # Column expressions for USING clause
```

---

### Phase 2: Wrapper API

Add join accessor pattern to `vw/core/base.py`:

1. **JoinAccessor Class**
   - Generic: `JoinAccessor[ExprT, RowSetT, SetOpT]`
   - Methods: `.inner()`, `.left()`, `.right()`, `.full_outer()`, `.cross()`
   - Each method returns new RowSet with accumulated join
   - Store reference to parent RowSet

2. **RowSet.join Property**
   - Add `@property def join(self) -> JoinAccessor`
   - Returns accessor instance bound to self
   - Enables fluent `.join.inner()` syntax

**Key Implementation Details:**
- INNER/LEFT/RIGHT/FULL accept both `on` and `using` parameters (keyword-only, optional)
- CROSS does not accept `on` or `using` parameters
- No validation performed - PostgreSQL handles errors
- All methods ensure Statement (auto-convert Source)
- Accumulate joins: `replace(stmt, joins=stmt.joins + (new_join,))`
- Use `self.factories.rowset()` to create new RowSet

**Pattern:**
```python
class JoinAccessor(Generic[ExprT, RowSetT, SetOpT]):
    def __init__(self, rowset: RowSet[ExprT, RowSetT, SetOpT]):
        self._rowset = rowset

    def inner(self, right: RowSetT, *, on: list[ExprT] = [], using: list[ExprT] = []) -> RowSetT:
        stmt = self._rowset._ensure_statement()
        join = Join(jtype=JoinType.INNER, right=right.state, on=tuple(on), using=tuple(using))
        return self._rowset.factories.rowset(
            replace(stmt, joins=stmt.joins + (join,))
        )

    # Similar for left, right, full_outer

    def cross(self, right: RowSetT) -> RowSetT:
        stmt = self._rowset._ensure_statement()
        join = Join(jtype=JoinType.CROSS, right=right.state, on=(), using=())
        return self._rowset.factories.rowset(
            replace(stmt, joins=stmt.joins + (join,))
        )
```

---

### Phase 3: PostgreSQL Rendering

Add join rendering to `vw/postgres/render.py`:

1. **render_join() Function**
   - Input: `join: Join`, `ctx: RenderContext`
   - Output: `str` (e.g., "INNER JOIN orders AS o ON (u.id = o.user_id)")
   - Logic:
     - Render join type: `join.jtype.value`
     - Render right side: `render_state(join.right, ctx)`
     - Render ON clause: join conditions with " AND ", wrap in parentheses
     - Format: `"{TYPE} JOIN {right} ON ({conditions})"`
     - CROSS JOIN: omit ON clause entirely

2. **Update render_state()**
   - Add case: `case Join(): return render_join(state, ctx)`

3. **Update render_statement()**
   - After rendering FROM clause, before WHERE
   - Loop through `stmt.joins` and render each
   - Append to parts list

**Pattern:**
```python
def render_join(join: Join, ctx: RenderContext) -> str:
    # Render right side
    right_sql = render_state(join.right, ctx)

    # Build base join clause
    parts = [f"{join.jtype.value} JOIN {right_sql}"]

    # Add ON clause if present
    if join.on:
        on_conditions = [render_state(cond, ctx) for cond in join.on]
        on_sql = " AND ".join(on_conditions)
        parts.append(f"ON ({on_sql})")

    # Add USING clause if present
    if join.using:
        using_columns = [render_state(col, ctx) for col in join.using]
        using_sql = ", ".join(using_columns)
        parts.append(f"USING ({using_sql})")

    return " ".join(parts)

def render_statement(stmt: Statement, ctx: RenderContext) -> str:
    parts = []

    # SELECT clause
    # ...

    # FROM clause
    parts.append("FROM")
    parts.append(render_state(stmt.source, ctx))

    # JOIN clauses (NEW)
    for join in stmt.joins:
        parts.append(render_state(join, ctx))

    # WHERE clause
    # ...
```

---

### Phase 4: Unit Tests

Create `tests/postgres/test_joins.py` for state construction tests:

1. **Test join state creation**
   - Test JoinType enum values
   - Test Join dataclass construction
   - Test immutability (frozen)

2. **Test JoinAccessor**
   - Test `.join.inner()` creates correct state
   - Test `.join.left()` creates correct state
   - Test `.join.right()` creates correct state
   - Test `.join.full_outer()` creates correct state
   - Test `.join.cross()` creates correct state
   - Test accumulation (multiple joins)

3. **Test Statement joins field**
   - Test default empty tuple
   - Test accumulating joins

**Pattern:**
```python
def describe_join_accessor():
    def it_creates_inner_join():
        users = source("users")
        orders = source("orders")
        query = users.join.inner(orders, on=[col("id") == col("user_id")])

        # Check state
        assert isinstance(query.state, Statement)
        assert len(query.state.joins) == 1
        assert query.state.joins[0].jtype == JoinType.INNER
```

---

### Phase 5: Integration Tests

Create `tests/postgres/integration/test_joins.py` for SQL rendering tests:

1. **describe_inner_joins()**
   - Basic inner join with single condition
   - Inner join with multiple conditions
   - Multiple inner joins

2. **describe_left_joins()**
   - Basic left join
   - Left join with WHERE clause
   - Left join with aggregation

3. **describe_right_joins()**
   - Basic right join
   - Right join with complex condition

4. **describe_full_outer_joins()**
   - Basic full outer join
   - Full outer join with filtering

5. **describe_cross_joins()**
   - Basic cross join
   - Cross join with WHERE clause

6. **describe_multiple_joins()**
   - Mix of join types
   - Three-way joins
   - Joins with subqueries

7. **describe_joins_with_clauses()**
   - Joins + WHERE
   - Joins + GROUP BY + HAVING
   - Joins + ORDER BY + LIMIT
   - Joins + DISTINCT

8. **describe_parameters_in_joins()**
   - Parameters in ON conditions
   - Multiple parameters in single join
   - Parameters across multiple joins

**Pattern:**
```python
def describe_inner_joins():
    def it_builds_basic_inner_join():
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

    def it_builds_inner_join_with_multiple_conditions():
        """
        SELECT u.id, o.total
        FROM users AS u
        INNER JOIN orders AS o ON (u.id = o.user_id AND o.status = $status)
        """
        users = source("users").alias("u")
        orders = source("orders").alias("o")

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
        assert result.query == sql("""
            SELECT u.id, o.total
            FROM users AS u
            INNER JOIN orders AS o ON (u.id = o.user_id AND o.status = $status)
        """)
        assert result.params == {"status": "active"}
```

---

### Phase 6: Documentation Updates

1. **docs/api/core.md**
   - Add section: "RowSet > .join"
   - Document JoinAccessor class
   - Document all join methods (.inner, .left, .right, .full_outer, .cross)
   - Add examples

2. **docs/api/postgres.md**
   - Add "Complete Examples > Joins" section
   - Show basic join, multiple joins, joins with other clauses
   - Update Feature Status to mark Joins as completed

3. **docs/quickstart.md**
   - Add "Joins" section after "Subqueries"
   - Show practical join examples

4. **docs/development/postgres-parity.md**
   - Mark Phase 4 tasks as completed
   - Update progress percentage

5. **CLAUDE.md**
   - Add to Documentation Index:
     ```markdown
     ### API Reference - Joins
     - .join.inner() → `docs/api/core.md` (RowSet > .join > inner)
     - .join.left() → `docs/api/core.md` (RowSet > .join > left)
     - .join.right() → `docs/api/core.md` (RowSet > .join > right)
     - .join.full_outer() → `docs/api/core.md` (RowSet > .join > full_outer)
     - .join.cross() → `docs/api/core.md` (RowSet > .join > cross)
     - Join examples → `docs/quickstart.md` (Joins)
     ```

---

## Testing Strategy

### After Each Phase:
1. Run tests: `uv run pytest`
2. Run linter: `uv run ruff check`
3. Run formatter: `uv run ruff format`
4. Run type checker: `uv run pyright` (if available)

### Complete Test Coverage:
- Unit tests for state construction
- Integration tests for SQL rendering
- Edge cases (empty ON, complex expressions, subqueries)
- Compatibility with existing features (WHERE, GROUP BY, etc.)

---

## Incremental Implementation

Follow CLAUDE.md principle #10 (Incrementality):

1. **Atomic changes** - Each phase is a complete, testable change
2. **Sequential** - Phases build on each other
3. **Testable** - Test after each phase
4. **Rollback-friendly** - Each commit is self-contained

**Commit Strategy:**
- Phase 1: "feat(core): add join state classes"
- Phase 2: "feat(core): add join accessor API"
- Phase 3: "feat(postgres): add join rendering"
- Phases 4-5: "test(postgres): add join tests"
- Phase 6: "docs: add join documentation"

Or combine into fewer commits if phases are small.

---

## Success Criteria

**Implementation:**
- ✅ JoinType enum with 5 types
- ✅ Join dataclass with jtype, right, on
- ✅ Statement.joins field
- ✅ JoinAccessor class with 5 methods
- ✅ RowSet.join property
- ✅ render_join() function
- ✅ Updated render_statement()

**Testing:**
- ✅ Unit tests for state construction
- ✅ Integration tests for all 5 join types
- ✅ Tests for multiple joins
- ✅ Tests for joins + other clauses
- ✅ Tests for parameters in joins
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

**Risk 1: Breaking existing tests**
- Mitigation: Run full test suite after each phase
- Mitigation: Statement.joins defaults to empty tuple (backward compatible)

**Risk 2: Type safety issues**
- Mitigation: Use Generic types correctly (ExprT, RowSetT)
- Mitigation: Run type checker frequently

**Risk 3: Rendering edge cases**
- Mitigation: Comprehensive integration tests
- Mitigation: Reference implementation as guide

**Risk 4: Documentation drift**
- Mitigation: Update docs as part of same PR
- Mitigation: Update CLAUDE.md index immediately

---

## Validation Checklist

Before considering complete:

- [ ] All 5 join types work correctly
- [ ] Multiple conditions in ON clause work (AND-combined)
- [ ] Multiple joins can be chained
- [ ] Joins work with WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
- [ ] Joins work with DISTINCT
- [ ] Parameters work in join conditions
- [ ] Subqueries can be joined (right side is Statement)
- [ ] Self-joins work (same table, different aliases)
- [ ] CROSS JOIN doesn't accept ON parameter
- [ ] All tests pass (existing + new)
- [ ] Linters pass
- [ ] Type checker passes
- [ ] Documentation updated and accurate
- [ ] CLAUDE.md index updated

---

## Timeline

Estimated implementation: Single work session (2-4 hours)

- Phase 1: 15 minutes (state classes)
- Phase 2: 30 minutes (wrapper API)
- Phase 3: 30 minutes (rendering)
- Phase 4: 30 minutes (unit tests)
- Phase 5: 60 minutes (integration tests)
- Phase 6: 30 minutes (documentation)
- Testing/fixes: 30 minutes (buffer)

Total: ~3.5 hours

---

## Follow-up Work (Future)

Not part of this phase, but related:

- **Phase 5:** Advanced query features (CTEs, subqueries in FROM)
- **Lateral joins:** LATERAL keyword support
- **Natural joins:** NATURAL JOIN support
- **Join optimizations:** Query plan hints
- **Join validation:** Warn about Cartesian products

These are explicitly out of scope for Phase 4.
