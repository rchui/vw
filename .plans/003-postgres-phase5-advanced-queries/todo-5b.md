# Phase 5b TODO: Set Operations

## Planning
- [x] Research set operations in reference implementation
- [x] Analyze PostgreSQL dialect current state
- [x] Create research-5b.md
- [x] Create plan-5b.md
- [x] Create todo-5b.md

## Core Implementation

### vw/core/states.py
- [ ] Add "Set Operations" section comment
- [ ] Add SetOperationState dataclass
  - [ ] Add left field (Statement | SetOperationState)
  - [ ] Add operator field (str)
  - [ ] Add right field (Statement | SetOperationState)
  - [ ] Make Generic[ExprT]
  - [ ] Set frozen=True, kw_only=True, eq=False

### vw/core/base.py
- [ ] Add SetOperation wrapper class
  - [ ] Extend RowSet[ExprT]
  - [ ] Make Generic[ExprT]
  - [ ] Add state field (SetOperationState[ExprT])
  - [ ] Set frozen=True, kw_only=True, eq=False

- [ ] Add operator overloads to RowSet class
  - [ ] Implement __or__(self, other) for UNION
  - [ ] Implement __add__(self, other) for UNION ALL
  - [ ] Implement __and__(self, other) for INTERSECT
  - [ ] Implement __sub__(self, other) for EXCEPT
  - [ ] Return SetOperation instance from each

### vw/postgres/render.py
- [ ] Import SetOperationState from vw.core.states
- [ ] Add render_set_operation() function
  - [ ] Handle Statement on left side
  - [ ] Handle SetOperationState on left side (recursive)
  - [ ] Handle Statement on right side
  - [ ] Handle SetOperationState on right side (recursive)
  - [ ] Wrap both sides in parentheses
  - [ ] Combine with operator string
  - [ ] Add docstring

- [ ] Update render_state() match block
  - [ ] Add case for SetOperationState
  - [ ] Call render_set_operation()

- [ ] Update render() top-level function
  - [ ] Add elif for SetOperationState
  - [ ] Call render_set_operation() without FROM prefix

## Testing

### tests/postgres/test_set_operations.py (Unit Tests)
- [ ] Create file with module docstring
- [ ] Add imports (SetOperationState, Statement, col, source)

- [ ] describe_set_operation_state group
  - [ ] it_creates_union_state test
  - [ ] it_creates_union_all_state test
  - [ ] it_creates_intersect_state test
  - [ ] it_creates_except_state test
  - [ ] it_handles_nested_set_operations test

### tests/postgres/integration/test_set_operations.py (Integration Tests)
- [ ] Create file with module docstring
- [ ] Add imports (sql, col, param, render, source)

- [ ] describe_union group
  - [ ] it_builds_basic_union test
  - [ ] it_builds_union_all test
  - [ ] it_builds_union_with_where test

- [ ] describe_intersect group
  - [ ] it_builds_basic_intersect test
  - [ ] it_builds_intersect_with_conditions test

- [ ] describe_except group
  - [ ] it_builds_basic_except test
  - [ ] it_builds_except_with_conditions test

- [ ] describe_chaining group
  - [ ] it_chains_multiple_unions test
  - [ ] it_chains_mixed_operations test
  - [ ] it_handles_complex_nesting test

- [ ] describe_parameters group
  - [ ] it_preserves_parameters_across_union test
  - [ ] it_merges_parameters_from_both_sides test

- [ ] describe_as_subquery group
  - [ ] it_uses_set_operation_as_subquery test
  - [ ] it_aliases_set_operation test

- [ ] describe_complex_scenarios group
  - [ ] it_combines_with_aggregations test
  - [ ] it_combines_with_order_by test
  - [ ] it_combines_with_limit test

## Quality Assurance

### Run Tests
- [ ] Run pytest for new unit tests
- [ ] Run pytest for new integration tests
- [ ] Run full test suite (pytest)
- [ ] Verify all 837+ tests pass

### Code Quality
- [ ] Run ruff check
- [ ] Run ruff format
- [ ] Run ty check (type checking)
- [ ] Fix any issues found

## Documentation

### API Documentation
- [ ] Update docs/api/postgres.md
  - [ ] Add "Set Operations" section
  - [ ] Document | operator (UNION)
  - [ ] Document + operator (UNION ALL)
  - [ ] Document & operator (INTERSECT)
  - [ ] Document - operator (EXCEPT)
  - [ ] Add examples for each operator
  - [ ] Add chaining example
  - [ ] Add parameter preservation example

### Quickstart Guide
- [ ] Update docs/quickstart.md
  - [ ] Add "Set Operations" section
  - [ ] Add basic UNION example
  - [ ] Add UNION ALL example
  - [ ] Add INTERSECT example
  - [ ] Add EXCEPT example
  - [ ] Add chaining example

### CLAUDE.md Index
- [ ] Update Documentation Index
  - [ ] Add set operations overview entry
  - [ ] Add UNION operator entry
  - [ ] Add UNION ALL operator entry
  - [ ] Add INTERSECT operator entry
  - [ ] Add EXCEPT operator entry
  - [ ] Add chaining example entry

## Git Commit

- [ ] Stage all changes (git add -A)
- [ ] Create commit with message:
  ```
  feat(postgres): implement Phase 5b set operations (UNION, INTERSECT, EXCEPT)

  - Add SetOperationState to vw/core/states.py
  - Add SetOperation wrapper class to vw/core/base.py
  - Add operator overloads to RowSet (__or__, __add__, __and__, __sub__)
  - Implement render_set_operation() in vw/postgres/render.py
  - Update render() to handle SetOperation at top level
  - Create unit tests for state construction
  - Create integration tests for SQL rendering
  - Test all operators, chaining, nesting, parameters
  - Update API documentation and quickstart guide
  - All tests passing (XXX total)
  ```

## Verification

- [ ] All new tests pass
- [ ] All existing tests still pass
- [ ] No linting errors
- [ ] No type errors
- [ ] Code properly formatted
- [ ] Documentation updated
- [ ] Changes committed to git

## Notes

- SetOperation is both a RowSet and Expression (can be used in either context)
- No factory functions needed (operators create SetOperation instances)
- Parenthesization always applied for clarity
- Parameters automatically collected from both sides via RenderContext
- Nested operations work recursively
