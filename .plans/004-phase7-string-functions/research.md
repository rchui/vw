# Research: Phase 7 String Functions

## Pattern Reference

String functions follow the Phase 7 null handling functions pattern (commit de91c3e):
- Functions are added to `vw/core/functions.py` as methods on the `Functions` class
- Each method creates a `Function` state from `vw/core/states.py`
- `render_function()` in `vw/postgres/render.py` handles rendering automatically

## New Pattern: `.text` Accessor

The roadmap requires `col("x").text.upper()` syntax, not `F.upper(col("x"))`.
This requires a `TextAccessor` class similar to pandas' `.str` accessor:
- `TextAccessor` holds a reference to the parent Expression and its factories
- Each method creates a new Expression with appropriate Function state
- `.text` property on `Expression` returns a `TextAccessor`

## State Choices

- UPPER, LOWER, TRIM, LTRIM, RTRIM, LENGTH, SUBSTRING, REPLACE:
  Use `Function` state (existing) — render_function handles automatically
- CONCAT / `||`:
  Add new `Concat` state (left, right) → renders as `left || right`
  The SQL `||` operator is different from CONCAT() function (NULL handling differs)
  Use `||` as it's standard ANSI SQL; CONCAT is PostgreSQL-specific

## SQL Reference

- UPPER(str), LOWER(str), TRIM(str), LTRIM(str), RTRIM(str)
- LENGTH(str)
- SUBSTRING(str FROM start FOR length) — but simpler as SUBSTRING(str, start, length)
- REPLACE(str, old, new)
- str1 || str2 — concatenation operator

## Files to Modify

1. `vw/core/states.py` — add `Concat` dataclass
2. `vw/core/base.py` — add `TextAccessor` class and `.text` property on `Expression`
3. `vw/postgres/render.py` — import and render `Concat` state
4. `tests/postgres/integration/test_string_functions.py` — new test file
5. `docs/development/postgres-parity.md` — mark items complete
