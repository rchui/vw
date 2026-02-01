# TODO: PostgreSQL Phase 5a - EXISTS and Subquery Support

## Phase 1: Core State Classes

- [ ] 1.1 Add `Exists` dataclass to `vw/core/states.py`
  - Field: `subquery: object`
  - Pattern: `@dataclass(eq=False, frozen=True, kw_only=True)`

- [ ] 1.2 Add Exists to imports in `vw/postgres/render.py`
  - Import Exists from vw.core.states

- [ ] 1.3 Run tests to ensure no regressions
  - `uv run pytest`

---

## Phase 2: Factory Functions

- [ ] 2.1 Add `exists()` function to `vw/postgres/public.py`
  - Signature: `def exists(subquery: RowSet) -> Expression`
  - Extract subquery.state
  - Create Exists state, wrap in Expression

- [ ] 2.2 Export exists from `vw/postgres/__init__.py`
  - Add to imports
  - Add to __all__ list

- [ ] 2.3 Run tests to ensure no regressions
  - `uv run pytest`

---

## Phase 3: PostgreSQL Rendering

- [ ] 3.1 Add `render_exists()` function to `vw/postgres/render.py`
  - Signature: `def render_exists(exists: Exists, ctx: RenderContext) -> str`
  - Render subquery with render_statement()
  - Format: `"EXISTS ({subquery_sql})"`

- [ ] 3.2 Add `Exists` case to `render_state()` in `vw/postgres/render.py`
  - `case Exists(): return render_exists(state, ctx)`

- [ ] 3.3 Update `render_is_in()` in `vw/postgres/render.py`
  - Check if any value is Statement instance
  - If yes: render as subquery `IN (SELECT ...)`
  - If no: use existing value list logic

- [ ] 3.4 Update `render_is_not_in()` in `vw/postgres/render.py`
  - Same logic as render_is_in() but for NOT IN

- [ ] 3.5 Run tests to ensure no regressions
  - `uv run pytest`

---

## Phase 4: Unit Tests

- [ ] 4.1 Create `tests/postgres/test_subqueries.py`

- [ ] 4.2 Add test group: `describe_exists_state()`
  - Test Exists construction
  - Test immutability (frozen)

- [ ] 4.3 Add test group: `describe_exists_factory()`
  - Test exists() creates correct state
  - Test wraps in Expression

- [ ] 4.4 Add test group: `describe_is_in_with_statement()`
  - Test IsIn with Statement in values
  - Test state structure

- [ ] 4.5 Run unit tests
  - `uv run pytest tests/postgres/test_subqueries.py`

---

## Phase 5: Integration Tests

- [ ] 5.1 Create `tests/postgres/integration/test_subqueries.py`

- [ ] 5.2 Add test group: `describe_exists()`
  - Basic EXISTS in WHERE
  - NOT EXISTS via ~ operator
  - EXISTS with correlated subquery
  - EXISTS with complex conditions

- [ ] 5.3 Add test group: `describe_in_subquery()`
  - Basic IN with subquery
  - NOT IN with subquery
  - IN with parameterized subquery
  - IN with aliased subquery

- [ ] 5.4 Add test group: `describe_scalar_subqueries()`
  - Scalar subquery in SELECT
  - Scalar subquery in WHERE comparison
  - Scalar subquery with aggregation
  - Scalar subquery with alias

- [ ] 5.5 Add test group: `describe_correlated_subqueries()`
  - Correlated EXISTS
  - Correlated IN
  - Correlated scalar comparison
  - Multiple correlated references

- [ ] 5.6 Add test group: `describe_nested_subqueries()`
  - Subquery in subquery
  - EXISTS in subquery
  - Multiple nesting levels

- [ ] 5.7 Add test group: `describe_subqueries_with_parameters()`
  - EXISTS with parameters
  - IN subquery with parameters
  - Scalar subquery with parameters

- [ ] 5.8 Run integration tests
  - `uv run pytest tests/postgres/integration/test_subqueries.py`

- [ ] 5.9 Run full test suite
  - `uv run pytest`

---

## Phase 6: Code Quality

- [ ] 6.1 Run linter
  - `uv run ruff check`

- [ ] 6.2 Run formatter
  - `uv run ruff format`

- [ ] 6.3 Run type checker
  - `uv run ty check`

- [ ] 6.4 Fix any issues found

---

## Phase 7: Documentation

- [ ] 7.1 Update `docs/api/core.md`
  - Add "Subquery Expressions" section
  - Document EXISTS operator
  - Document subqueries in IN/NOT IN
  - Document scalar subqueries usage
  - Add examples for each

- [ ] 7.2 Update `docs/api/postgres.md`
  - Add "Subqueries" section under "Complete Examples"
  - Show EXISTS example
  - Show IN with subquery example
  - Show scalar subquery example
  - Show correlated subquery example
  - Update Feature Status section

- [ ] 7.3 Update `docs/quickstart.md`
  - Expand existing "Subqueries" section
  - Add EXISTS example
  - Add IN with subquery example
  - Add scalar subquery example

- [ ] 7.4 Update `docs/development/postgres-parity.md`
  - Mark Phase 5a subquery tasks as completed (✅)
  - Update "Current Status Summary"
  - Update progress if applicable

- [ ] 7.5 Update `CLAUDE.md`
  - Add to "Query Building Examples":
    - EXISTS examples → location
    - Subquery examples → location
  - Add to "API Reference - Expression Class":
    - exists() → location
  - Update as needed

---

## Phase 8: Final Validation

- [ ] 8.1 Run complete test suite
  - `uv run pytest`
  - Verify all tests pass

- [ ] 8.2 Run all quality checks
  - `uv run ruff check`
  - `uv run ruff format`
  - `uv run ty check`

- [ ] 8.3 Manual testing
  - Test EXISTS in Python REPL
  - Test IN with subquery
  - Test scalar subquery
  - Verify SQL rendering looks correct

- [ ] 8.4 Review documentation
  - Ensure all examples are accurate
  - Ensure documentation index is up to date

- [ ] 8.5 Check success criteria
  - ✅ EXISTS operator works
  - ✅ NOT EXISTS works
  - ✅ IN with subquery works
  - ✅ NOT IN with subquery works
  - ✅ Scalar subqueries work
  - ✅ Correlated subqueries work
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
  - `git commit -m "feat(postgres): implement Phase 5a subqueries (EXISTS, IN, scalar)"`

- [ ] 9.4 Verify git status clean
  - `git status`

- [ ] 9.5 Keep plan directory for reference
  - Plan directory remains for other Phase 5 sub-phases

---

## Notes

- Follow CLAUDE.md principle #11: Test continuously after each phase
- Follow CLAUDE.md principle #10: Work in digestible, atomic pieces
- Follow CLAUDE.md principle #13: Update docs when learning new information
- Use subagents for discrete work where appropriate
- Each checkbox is an atomic, complete change
- Test after every 2-3 tasks to catch issues early

---

## Exclusions (Per Design)

- ❌ No SQL validation - Let PostgreSQL handle all errors
- ❌ No scalar subquery validation - User ensures single value
- ❌ No optimization of EXISTS subqueries
- ❌ No transformation of user-provided queries

---

## Implementation Philosophy

- ✅ No SQL validation - Let PostgreSQL handle all errors
- ✅ Scalar subqueries are just Statement used as Expression
- ✅ Correlated subqueries work naturally through column references
- ✅ EXISTS is a simple wrapper around Statement
- ✅ Follow existing patterns from Phases 1-4

---

## Total Tasks: 46

**Breakdown:**
- Phase 1 (States): 3 tasks
- Phase 2 (Factories): 3 tasks
- Phase 3 (Rendering): 5 tasks
- Phase 4 (Unit Tests): 5 tasks
- Phase 5 (Integration Tests): 9 tasks
- Phase 6 (Quality): 4 tasks
- Phase 7 (Docs): 5 tasks
- Phase 8 (Validation): 5 tasks
- Phase 9 (Commit): 5 tasks

---

## Next Sub-Phases

After Phase 5a is complete:
- **Phase 5b:** Set Operations (UNION, INTERSECT, EXCEPT)
- **Phase 5c:** CTEs (Common Table Expressions)
- **Phase 5d:** VALUES and CASE Expressions
