# TODO: PostgreSQL Phase 4 - Joins Implementation

## Phase 1: Core State Classes

- [ ] 1.1 Add `JoinType` enum to `vw/core/states.py`
  - Values: INNER, LEFT, RIGHT, FULL, CROSS
  - Base: `str, Enum`

- [ ] 1.2 Add `Join` dataclass to `vw/core/states.py`
  - Fields: `jtype: JoinType`, `right: object`, `on: tuple[object, ...]`, `using: tuple[object, ...]`
  - Pattern: `@dataclass(eq=False, frozen=True, kw_only=True)`

- [ ] 1.3 Update `Statement` dataclass in `vw/core/states.py`
  - Add field: `joins: tuple[Join, ...] = ()`

- [ ] 1.4 Run tests to ensure no regressions
  - `uv run pytest`

---

## Phase 2: Wrapper API

- [ ] 2.1 Add `JoinAccessor` class to `vw/core/base.py`
  - Generic: `JoinAccessor[ExprT, RowSetT, SetOpT]`
  - Store reference to parent RowSet in `__init__`

- [ ] 2.2 Implement `JoinAccessor.inner()` method
  - Signature: `def inner(self, right: RowSetT, *, on: list[ExprT] = [], using: list[ExprT] = []) -> RowSetT`
  - Ensure Statement, create Join with both on and using, accumulate to tuple

- [ ] 2.3 Implement `JoinAccessor.left()` method
  - Similar to inner, use `JoinType.LEFT`

- [ ] 2.4 Implement `JoinAccessor.right()` method
  - Similar to inner, use `JoinType.RIGHT`

- [ ] 2.5 Implement `JoinAccessor.full_outer()` method
  - Similar to inner, use `JoinType.FULL`

- [ ] 2.6 Implement `JoinAccessor.cross()` method
  - Signature: `def cross(self, right: RowSetT) -> RowSetT` (no on/using parameters)
  - Use `JoinType.CROSS`, set `on=()` and `using=()` (empty tuples)

- [ ] 2.7 Add `join` property to `RowSet` class in `vw/core/base.py`
  - Return `JoinAccessor` instance bound to self

- [ ] 2.8 Run tests to ensure no regressions
  - `uv run pytest`

---

## Phase 3: PostgreSQL Rendering

- [ ] 3.1 Add `render_join()` function to `vw/postgres/render.py`
  - Signature: `def render_join(join: Join, ctx: RenderContext) -> str`
  - Render right side: `render_state(join.right, ctx)`
  - Build base: `"{TYPE} JOIN {right}"`
  - If `join.on`: append `"ON ({conditions})"` with AND joining
  - If `join.using`: append `"USING ({columns})"` with comma joining
  - Both can be present (no validation)

- [ ] 3.2 Add `Join` case to `render_state()` in `vw/postgres/render.py`
  - `case Join(): return render_join(state, ctx)`

- [ ] 3.3 Update `render_statement()` in `vw/postgres/render.py`
  - After FROM clause, before WHERE clause
  - Loop through `stmt.joins` and append rendered joins

- [ ] 3.4 Run tests to ensure no regressions
  - `uv run pytest`

---

## Phase 4: Unit Tests

- [ ] 4.1 Create `tests/postgres/test_joins.py`

- [ ] 4.2 Add test group: `describe_join_type_enum()`
  - Test enum values (INNER, LEFT, RIGHT, FULL, CROSS)

- [ ] 4.3 Add test group: `describe_join_dataclass()`
  - Test construction
  - Test immutability (frozen)

- [ ] 4.4 Add test group: `describe_join_accessor()`
  - Test `.join.inner()` creates correct state
  - Test `.join.left()` creates correct state
  - Test `.join.right()` creates correct state
  - Test `.join.full_outer()` creates correct state
  - Test `.join.cross()` creates correct state
  - Test multiple joins accumulate correctly

- [ ] 4.5 Run unit tests
  - `uv run pytest tests/postgres/test_joins.py`

---

## Phase 5: Integration Tests

- [ ] 5.1 Create `tests/postgres/integration/test_joins.py`

- [ ] 5.2 Add test group: `describe_inner_joins()`
  - Basic inner join with single condition
  - Inner join with multiple conditions (AND)
  - Multiple inner joins chained

- [ ] 5.3 Add test group: `describe_left_joins()`
  - Basic left join
  - Left join with WHERE clause
  - Left join with NULL handling

- [ ] 5.4 Add test group: `describe_right_joins()`
  - Basic right join
  - Right join with complex expression

- [ ] 5.5 Add test group: `describe_full_outer_joins()`
  - Basic full outer join
  - Full outer join with filtering

- [ ] 5.6 Add test group: `describe_cross_joins()`
  - Basic cross join
  - Cross join with WHERE clause
  - Cross join (cartesian product)

- [ ] 5.7 Add test group: `describe_multiple_joins()`
  - Mix of join types
  - Three-way joins
  - Four-way joins

- [ ] 5.8 Add test group: `describe_joins_with_subqueries()`
  - Join to subquery as right side
  - Subquery with alias
  - Multiple subquery joins

- [ ] 5.9 Add test group: `describe_joins_with_clauses()`
  - Joins + WHERE
  - Joins + GROUP BY
  - Joins + HAVING
  - Joins + ORDER BY
  - Joins + LIMIT/OFFSET
  - Joins + DISTINCT

- [ ] 5.10 Add test group: `describe_parameters_in_joins()`
  - Single parameter in ON clause
  - Multiple parameters in single join
  - Parameters across multiple joins

- [ ] 5.11 Add test group: `describe_self_joins()`
  - Join table to itself with different aliases
  - Self join with complex condition

- [ ] 5.12 Add test group: `describe_qualified_columns_in_joins()`
  - Use `.col()` for qualified columns
  - Use `.star` for qualified stars
  - SELECT * with multiple joined tables

- [ ] 5.13 Add test group: `describe_using_clause()`
  - Basic USING with single column
  - USING with multiple columns
  - USING with qualified columns

- [ ] 5.14 Run integration tests
  - `uv run pytest tests/postgres/integration/test_joins.py`

- [ ] 5.15 Run full test suite
  - `uv run pytest`

---

## Phase 6: Code Quality

- [ ] 6.1 Run linter
  - `uv run ruff check`

- [ ] 6.2 Run formatter
  - `uv run ruff format`

- [ ] 6.3 Run type checker (if available)
  - `uv run pyright` or `uv run mypy`

- [ ] 6.4 Fix any issues found

---

## Phase 7: Documentation

- [ ] 7.1 Update `docs/api/core.md`
  - Add "RowSet > .join" section
  - Document JoinAccessor class
  - Document `.join.inner()` method with example
  - Document `.join.left()` method with example
  - Document `.join.right()` method with example
  - Document `.join.full_outer()` method with example
  - Document `.join.cross()` method with example

- [ ] 7.2 Update `docs/api/postgres.md`
  - Add "Complete Examples > Joins" section
  - Show basic join example
  - Show multiple joins example
  - Show joins with other clauses
  - Update "Feature Status" section to mark joins as completed

- [ ] 7.3 Update `docs/quickstart.md`
  - Add "Joins" section after "Subqueries" section
  - Show practical join examples:
    - Basic INNER JOIN
    - LEFT JOIN
    - Multiple joins
    - Joins with WHERE

- [ ] 7.4 Update `docs/development/postgres-parity.md`
  - Mark Phase 4 tasks as completed (✅)
  - Update "Current Status Summary"
  - Update progress percentage

- [ ] 7.5 Update `CLAUDE.md`
  - Add to "API Reference - RowSet Class" section:
    - `.join` property reference
  - Add new section "API Reference - Joins":
    - `.join.inner()` → location
    - `.join.left()` → location
    - `.join.right()` → location
    - `.join.full_outer()` → location
    - `.join.cross()` → location
  - Add to "Query Building Examples":
    - Join examples → location

---

## Phase 8: Final Validation

- [ ] 8.1 Run complete test suite
  - `uv run pytest`
  - Verify all tests pass

- [ ] 8.2 Run all quality checks
  - `uv run ruff check`
  - `uv run ruff format`
  - `uv run pyright` (if available)

- [ ] 8.3 Manual testing
  - Test basic join in Python REPL
  - Verify SQL rendering looks correct
  - Test with actual PostgreSQL database (optional)

- [ ] 8.4 Review documentation
  - Ensure all examples are accurate
  - Ensure documentation index is up to date

- [ ] 8.5 Check success criteria
  - ✅ All 5 join types work
  - ✅ Multiple conditions work (AND-combined)
  - ✅ Multiple joins can be chained
  - ✅ Joins work with other clauses
  - ✅ Parameters work in joins
  - ✅ Subqueries can be joined
  - ✅ All tests pass
  - ✅ Documentation updated
  - ✅ CLAUDE.md index updated

---

## Phase 9: Commit and Cleanup

- [ ] 9.1 Stage all changes
  - `git add .`

- [ ] 9.2 Review changes
  - `git diff --staged`

- [ ] 9.3 Commit with descriptive message
  - `git commit -m "feat(postgres): implement Phase 4 joins (INNER, LEFT, RIGHT, FULL, CROSS)"`
  - Or separate commits per phase if preferred

- [ ] 9.4 Verify git status clean
  - `git status`

- [ ] 9.5 Remove plan directory (per CLAUDE.md guidelines)
  - `rm -rf .plans/002-postgres-phase4-joins/`

---

## Notes

- Follow CLAUDE.md principle #11: Test continuously after each phase
- Follow CLAUDE.md principle #10: Work in digestible, atomic pieces
- Follow CLAUDE.md principle #13: Update docs when learning new information
- Use subagents for discrete work where appropriate
- Each checkbox is an atomic, complete change
- Test after every 2-3 tasks to catch issues early

---

## Exclusions (Per User Requirements)

- ❌ SEMI JOIN - Not standard PostgreSQL
- ❌ ANTI JOIN - Not standard PostgreSQL
- ❌ NATURAL JOIN - Not in scope for this phase
- ❌ LATERAL JOIN - Not in scope for this phase (future Phase 5)

## Implementation Philosophy

- ✅ No SQL validation - Let PostgreSQL handle all errors
- ✅ Support both ON and USING clauses (for INNER/LEFT/RIGHT/FULL)
- ✅ Allow users to specify both ON and USING (PostgreSQL will error)
- ✅ CROSS JOIN has different signature - no ON/USING parameters

---

## Total Tasks: 52

**Breakdown:**
- Phase 1 (States): 4 tasks
- Phase 2 (Wrapper): 8 tasks
- Phase 3 (Rendering): 4 tasks
- Phase 4 (Unit Tests): 5 tasks
- Phase 5 (Integration Tests): 15 tasks (added USING clause tests)
- Phase 6 (Quality): 4 tasks
- Phase 7 (Docs): 5 tasks
- Phase 8 (Validation): 5 tasks
- Phase 9 (Commit): 5 tasks
