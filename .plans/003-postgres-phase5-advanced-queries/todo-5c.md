# TODO: Phase 5c - CTEs (Common Table Expressions)

## Planning
- [x] Research reference implementation
- [x] Decide on architecture (CTE metadata approach)
- [x] Create research-5c.md
- [x] Create plan-5c.md
- [x] Create todo-5c.md

## Core Implementation

### vw/core/states.py
- [ ] Add CTE dataclass
  - [ ] Add name field (str)
  - [ ] Add recursive field (bool, default False)
  - [ ] Set frozen=True, kw_only=True, eq=False

- [ ] Update Statement dataclass
  - [ ] Add cte field (CTE | None = None)

### vw/core/base.py
- [ ] Update RowSet.select() method
  - [ ] Add elif for Statement with cte metadata
  - [ ] Create new Statement with CTE as source

- [ ] Update RowSet.col() method
  - [ ] Check for cte.name when qualifying columns
  - [ ] Preference order: alias > cte.name > unqualified

- [ ] Update RowSet.star property
  - [ ] Check for cte.name when qualifying star
  - [ ] Preference order: alias > cte.name > unqualified

### vw/postgres/public.py
- [ ] Add cte() factory function
  - [ ] Parameters: name (str), query (RowSet), recursive (bool = False)
  - [ ] Use replace() to add CTE to query.state
  - [ ] Return RowSet with updated state
  - [ ] Add comprehensive docstring with examples

- [ ] Export cte from vw/postgres/__init__.py

### vw/postgres/render.py
- [ ] Update RenderContext
  - [ ] Add ctes field: list[tuple[str, str, bool]]
  - [ ] Format: (name, body_sql, recursive)

- [ ] Add register_cte() method to RenderContext
  - [ ] Check for duplicate CTE names with different definitions
  - [ ] Append to ctes list

- [ ] Add render_with_clause() function
  - [ ] Check if any CTEs are recursive
  - [ ] Build "WITH" or "WITH RECURSIVE"
  - [ ] Join CTE definitions with commas
  - [ ] Format: "name AS (body_sql)"

- [ ] Update render_statement() function
  - [ ] Check if source is Statement with cte metadata
  - [ ] If yes, register CTE and render as CTE reference
  - [ ] Handle alias: "cte_name" or "cte_name AS alias"

- [ ] Update render() top-level function
  - [ ] After rendering main query, check for registered CTEs
  - [ ] Prepend WITH clause if CTEs exist

## Testing

### tests/postgres/test_ctes.py (Unit Tests)
- [ ] Create file with module docstring
- [ ] Add imports

- [ ] describe_cte_metadata group
  - [ ] it_creates_cte_metadata test
  - [ ] it_adds_metadata_to_statement test
  - [ ] it_preserves_existing_statement_fields test

### tests/postgres/integration/test_ctes.py (Integration Tests)
- [ ] Create file with module docstring
- [ ] Add imports

- [ ] describe_basic_ctes group
  - [ ] it_renders_simple_cte test
  - [ ] it_renders_cte_with_where test
  - [ ] it_renders_cte_with_qualified_columns test
  - [ ] it_renders_cte_with_parameters test

- [ ] describe_cte_references group
  - [ ] it_uses_cte_in_from_clause test
  - [ ] it_uses_cte_in_join test
  - [ ] it_uses_aliased_cte test
  - [ ] it_qualifies_columns_with_cte_name test
  - [ ] it_qualifies_columns_with_alias test

- [ ] describe_multiple_ctes group
  - [ ] it_renders_two_ctes test
  - [ ] it_renders_cte_referencing_cte test
  - [ ] it_handles_cte_name_collision test

- [ ] describe_recursive_ctes group
  - [ ] it_renders_recursive_cte test
  - [ ] it_renders_recursive_with_union_all test
  - [ ] it_renders_mixed_recursive_and_non_recursive test

- [ ] describe_complex_scenarios group
  - [ ] it_combines_cte_with_group_by test
  - [ ] it_combines_cte_with_order_by test
  - [ ] it_combines_cte_with_limit test
  - [ ] it_preserves_parameters_from_cte test

## Quality Assurance

### Run Tests
- [ ] Run pytest for new unit tests
- [ ] Run pytest for new integration tests
- [ ] Run full test suite (pytest)
- [ ] Verify all tests pass

### Code Quality
- [ ] Run ruff check
- [ ] Run ruff format
- [ ] Run ty check (type checking)
- [ ] Fix any issues found

## Documentation

### API Documentation
- [ ] Update docs/api/postgres.md
  - [ ] Add "Common Table Expressions (CTEs)" section
  - [ ] Document cte() function
  - [ ] Add basic CTE example
  - [ ] Add recursive CTE example
  - [ ] Add multiple CTEs example
  - [ ] Add CTE with JOIN example

### Quickstart Guide
- [ ] Update docs/quickstart.md
  - [ ] Add "Common Table Expressions" section
  - [ ] Add basic CTE example
  - [ ] Add recursive CTE example

### CLAUDE.md Index
- [ ] Update Documentation Index
  - [ ] Add CTE overview entry
  - [ ] Add cte() function entry
  - [ ] Add basic CTE example entry
  - [ ] Add recursive CTE example entry

## Git Commit

- [ ] Review all changes
- [ ] Stage all changes
- [ ] Create commit with message:
  ```
  feat(postgres): implement Phase 5c CTEs (Common Table Expressions)

  - Add CTE dataclass to vw/core/states.py
  - Add cte field to Statement for CTE metadata
  - Update RowSet.select() to handle CTE-annotated Statements
  - Update RowSet.col() and .star to use cte.name for qualification
  - Add cte() factory function to vw/postgres/public.py
  - Implement CTE registration and rendering in vw/postgres/render.py
  - Add render_with_clause() for WITH clause generation
  - Support both basic and recursive CTEs
  - Support multiple CTEs in single query
  - Detect CTE name collisions
  - Create comprehensive test suite (XX tests)
  - Update documentation

  All tests passing (XXX total)
  All quality checks pass (ruff, ty check)
  ```

## Verification

- [ ] All new functionality works correctly
- [ ] All existing tests still pass
- [ ] CTEs work in FROM and JOIN clauses
- [ ] Recursive CTEs generate WITH RECURSIVE
- [ ] Multiple CTEs work in single query
- [ ] CTE name collisions detected
- [ ] Parameters preserved from CTE queries
- [ ] Qualified columns use cte.name or alias
- [ ] No linting errors
- [ ] No type errors
- [ ] Code properly formatted
- [ ] Documentation complete and accurate

## Notes

- CTE metadata approach avoids creating new wrapper class
- CTEs are Statement objects with metadata, simplifying architecture
- Qualified columns prefer alias over cte.name
- Registration happens during rendering when CTE is used as source
