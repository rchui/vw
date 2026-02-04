# Phase 5b Research: Set Operations (UNION, INTERSECT, EXCEPT)

## Current State

### Reference Implementation (COMPLETE)
Set operations are fully implemented in `vw/reference/`:

- **SetOperation Class** (`vw/reference/build.py:410-487`)
  - Wraps both Statement and SetOperation objects
  - Supports chaining of set operations
  - Extends RowSet and Expression for flexibility

- **Operator Overloads:**
  - `query1 | query2` → UNION (deduplicates)
  - `query1 + query2` → UNION ALL (keeps duplicates)
  - `query1 & query2` → INTERSECT
  - `query1 - query2` → EXCEPT

- **Features:**
  - Proper parenthesization of nested operations
  - Parameter preservation across set operations
  - Can be used as subqueries
  - Supports aliasing

- **Tests:** 7 comprehensive tests in `tests/reference/integration/test_set_operations.py`

### PostgreSQL Implementation (PARTIAL)
Set operations infrastructure exists but rendering is NOT implemented:

- ✅ **SetOperation class defined** in `vw/postgres/base.py` (inherits from core)
- ✅ **SetOperation imported in factories** (`vw/postgres/public.py:19`)
- ❌ **No rendering logic** in `vw/postgres/render.py`
- ❌ **No operator overloads** on RowSet/SetOperation for `|`, `+`, `&`, `-`

## Implementation Requirements

### States/Classes
**NO NEW STATE CLASSES NEEDED** - set operations are handled through composition:

- SetOperation is a wrapper class, not a state
- Left and right operands can be Statement or SetOperation objects
- Operator string indicates operation type: "UNION", "UNION ALL", "INTERSECT", "EXCEPT"

### Architecture Pattern
```python
SetOperation(
    left=Statement(...),      # First query
    operator="UNION",         # Operation type
    right=Statement(...)      # Second query
)
```

## PostgreSQL SQL Syntax

```sql
-- UNION (removes duplicates)
(SELECT id FROM users) UNION (SELECT id FROM admins)

-- UNION ALL (keeps duplicates)
(SELECT id FROM users) UNION ALL (SELECT id FROM admins)

-- INTERSECT
(SELECT id FROM users) INTERSECT (SELECT id FROM banned_users)

-- EXCEPT
(SELECT id FROM users) EXCEPT (SELECT id FROM banned_users)

-- Chaining (left-associative)
(SELECT id FROM users) UNION (SELECT id FROM admins) UNION (SELECT id FROM guests)
```

## Key Considerations

### Parenthesization Rules
- Each operand (Statement or SetOperation) gets wrapped in parentheses
- Prevents ambiguity and improves readability
- Matches reference implementation approach

### Parameter Handling
- Parameters from left query + parameters from right query = merged dict
- Parameter names must be unique across both queries
- RenderContext accumulates parameters from both sides

### Operator Precedence
- All set operators have same precedence in PostgreSQL
- Processed left-to-right
- Explicit parenthesization required for clarity

## Reference Implementation Details

From `vw/reference/build.py`:

```python
@dataclass(kw_only=True, frozen=True)
class SetOperation(RowSet, Expression):
    """Represents combined queries with UNION/INTERSECT/EXCEPT."""

    left: Combinable           # Statement or SetOperation
    operator: str              # "UNION", "UNION ALL", "INTERSECT", "EXCEPT"
    right: Combinable          # Statement or SetOperation

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the set operation."""
        nested = context.recurse()
        left_sql = self.left.__vw_render__(nested)
        right_sql = self.right.__vw_render__(nested)
        sql = f"{left_sql} {self.operator} {right_sql}"

        # Parenthesize if nested
        if context.depth > 0:
            sql = f"({sql})"
            if self._alias:
                sql += f" AS {self._alias}"
        return sql
```

## Testing Strategy

### Test Files
- **Unit tests:** `tests/postgres/test_set_operations.py`
- **Integration tests:** `tests/postgres/integration/test_set_operations.py`

### Test Categories
1. **Operator Overloads** - Test all four operators (|, +, &, -)
2. **Chaining/Nesting** - Multiple unions, mixed operations
3. **Parameters** - Parameterized queries, merged parameter dicts
4. **Subqueries** - Set operations as subquery sources
5. **Complex Scenarios** - With WHERE, ORDER BY, aggregations

## Success Criteria

**Implementation:**
- ✅ Operator overloads (__or__, __add__, __and__, __sub__) on RowSet
- ✅ render_set_operation() function implemented
- ✅ Top-level render() handles SetOperation
- ✅ SetOperation can be nested
- ✅ Parameters collected from both sides

**Testing:**
- ✅ All four operators render correctly
- ✅ Chaining operations works
- ✅ Nested operations have proper parentheses
- ✅ Parameters preserved across operations
- ✅ Set operations can be used as subqueries
- ✅ All existing tests still pass
