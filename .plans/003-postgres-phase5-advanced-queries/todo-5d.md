# TODO: PostgreSQL Phase 5d - CASE/WHEN

## Step 1: State Classes

- [ ] 1.1 Add `WhenThen` dataclass to `vw/core/states.py`
- [ ] 1.2 Add `Case` dataclass to `vw/core/states.py`
- [ ] 1.3 Run `uv run pytest` to verify no regressions

## Step 2: Builders

- [ ] 2.1 Create `vw/core/case.py` with `When` and `CaseExpression` builder classes
- [ ] 2.2 Run `uv run pytest` to verify no regressions

## Step 3: Public API

- [ ] 3.1 Add `when()` factory function to `vw/postgres/public.py`
- [ ] 3.2 Export `when` from `vw/postgres/__init__.py`
- [ ] 3.3 Run `uv run pytest` to verify no regressions

## Step 4: Rendering

- [ ] 4.1 Add `Case`, `WhenThen` to imports in `vw/postgres/render.py`
- [ ] 4.2 Add `case Case()` branch to `render_state()` in `vw/postgres/render.py`
- [ ] 4.3 Run `uv run pytest` to verify no regressions

## Step 5: Integration Tests

- [ ] 5.1 Create `tests/postgres/integration/test_case.py`
- [ ] 5.2 Add `describe_basic_case()` - single WHEN with/without ELSE
- [ ] 5.3 Add `describe_multiple_when()` - chained WHEN clauses
- [ ] 5.4 Add `describe_case_in_select()` - CASE as a SELECT column
- [ ] 5.5 Add `describe_case_in_where()` - CASE in WHERE clause
- [ ] 5.6 Add `describe_nested_case()` - CASE result is another CASE
- [ ] 5.7 Add `describe_case_with_parameters()` - params in conditions/results
- [ ] 5.8 Run `uv run pytest` - all tests pass

## Step 6: Code Quality

- [ ] 6.1 Run `uv run ruff check`
- [ ] 6.2 Run `uv run ruff format`
- [ ] 6.3 Run `uv run ty check`
- [ ] 6.4 Fix any issues

## Step 7: Documentation

- [ ] 7.1 Update `docs/development/postgres-parity.md` - mark CASE/WHEN items ✅
