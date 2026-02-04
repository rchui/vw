# Phase 5b: Set Operations (UNION, INTERSECT, EXCEPT)

## Overview
Implement PostgreSQL set operations (UNION, UNION ALL, INTERSECT, EXCEPT) with operator overloading for intuitive query composition.

## Goals
- Enable combining multiple queries with set operations
- Support operator overloading: `|` (UNION), `+` (UNION ALL), `&` (INTERSECT), `-` (EXCEPT)
- Handle nested set operations with proper parenthesization
- Preserve parameters across combined queries
- Support set operations as subqueries

## Architecture

### Core Components

#### 1. Operator Overloads (vw/core/base.py)
Add magic methods to RowSet class:
- `__or__(self, other)` → UNION
- `__add__(self, other)` → UNION ALL
- `__and__(self, other)` → INTERSECT
- `__sub__(self, other)` → EXCEPT

Each creates a SetOperation wrapper with appropriate operator string.

#### 2. Rendering (vw/postgres/render.py)
Add `render_set_operation()` function:
- Recursively render left and right operands
- Handle Statement or SetOperation on both sides
- Wrap operands in parentheses
- Preserve parameters from both queries

Update `render()` function:
- Handle SetOperation at top level (like Source)
- Return rendered SQL without FROM prefix

## Implementation Steps

### Step 1: Add Operator Overloads to RowSet
**File:** `vw/core/base.py`

Add four methods to RowSet class:

```python
def __or__(self, other: RowSet) -> SetOperation[Self]:
    """UNION operator (deduplicates)."""
    return SetOperation(
        state=SetOperationState(left=self.state, operator="UNION", right=other.state),
        factories=self.factories
    )

def __add__(self, other: RowSet) -> SetOperation[Self]:
    """UNION ALL operator (keeps duplicates)."""
    return SetOperation(
        state=SetOperationState(left=self.state, operator="UNION ALL", right=other.state),
        factories=self.factories
    )

def __and__(self, other: RowSet) -> SetOperation[Self]:
    """INTERSECT operator."""
    return SetOperation(
        state=SetOperationState(left=self.state, operator="INTERSECT", right=other.state),
        factories=self.factories
    )

def __sub__(self, other: RowSet) -> SetOperation[Self]:
    """EXCEPT operator."""
    return SetOperation(
        state=SetOperationState(left=self.state, operator="EXCEPT", right=other.state),
        factories=self.factories
    )
```

**Note:** These return SetOperation (wrapper), not RowSet.

### Step 2: Add SetOperation State
**File:** `vw/core/states.py`

Add in new "Set Operations" section (after Subquery Operators):

```python
# --- Set Operations -------------------------------------------------------- #

@dataclass(eq=False, frozen=True, kw_only=True)
class SetOperationState(Generic[ExprT]):
    """Represents a set operation (UNION, INTERSECT, EXCEPT)."""

    left: Statement[ExprT] | SetOperationState[ExprT]
    operator: str  # "UNION", "UNION ALL", "INTERSECT", "EXCEPT"
    right: Statement[ExprT] | SetOperationState[ExprT]
```

**Import into vw/postgres/render.py:**
```python
from vw.core.states import (
    ...
    SetOperationState,
)
```

### Step 3: Add SetOperation Wrapper Class
**File:** `vw/core/base.py`

Add after RowSet class definition:

```python
@dataclass(eq=False, frozen=True, kw_only=True)
class SetOperation(RowSet[ExprT], Generic[ExprT]):
    """Wrapper for set operation states (UNION, INTERSECT, EXCEPT)."""

    state: SetOperationState[ExprT]

    # Inherit all RowSet methods
    # SetOperation can be used as subquery, aliased, etc.
```

### Step 4: Implement render_set_operation()
**File:** `vw/postgres/render.py`

Add new function (after render_exists):

```python
def render_set_operation(setop: SetOperationState, ctx: RenderContext) -> str:
    """Render a set operation to SQL.

    Args:
        setop: A SetOperationState to render.
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL string (e.g., "(SELECT ...) UNION (SELECT ...)").
    """
    # Render left side (Statement or SetOperationState)
    if isinstance(setop.left, Statement):
        left_sql = render_statement(setop.left, ctx)
    else:  # SetOperationState (nested)
        left_sql = render_set_operation(setop.left, ctx)

    # Render right side (Statement or SetOperationState)
    if isinstance(setop.right, Statement):
        right_sql = render_statement(setop.right, ctx)
    else:  # SetOperationState (nested)
        right_sql = render_set_operation(setop.right, ctx)

    # Wrap each side in parentheses and combine
    return f"({left_sql}) {setop.operator} ({right_sql})"
```

Add case to render_state():
```python
case SetOperationState():
    return render_set_operation(state, ctx)
```

### Step 5: Update render() Top-Level Function
**File:** `vw/postgres/render.py`

Update render() function to handle SetOperation:

```python
def render(obj: RowSet | Expression, *, config: RenderConfig | None = None) -> SQL:
    """Render a RowSet or Expression to PostgreSQL SQL."""
    ctx = RenderContext(config=config or RenderConfig(param_style=ParamStyle.DOLLAR))

    # Handle different top-level types
    if isinstance(obj.state, Source):
        query = f"FROM {render_source(obj.state, ctx)}"
    elif isinstance(obj.state, SetOperationState):
        query = render_set_operation(obj.state, ctx)
    else:
        query = render_state(obj.state, ctx)

    return SQL(query=query, params=ctx.params)
```

### Step 6: Create Unit Tests
**File:** `tests/postgres/test_set_operations.py`

Test state construction:

```python
"""Unit tests for set operation state construction."""

from vw.core.states import SetOperationState, Statement
from vw.postgres import col, source

def describe_set_operation_state():
    def it_creates_union_state():
        """Test UNION state construction."""
        query1 = source("users").select(col("id"))
        query2 = source("admins").select(col("id"))

        setop = query1 | query2

        assert isinstance(setop.state, SetOperationState)
        assert setop.state.operator == "UNION"
        assert isinstance(setop.state.left, Statement)
        assert isinstance(setop.state.right, Statement)

    def it_creates_union_all_state():
        """Test UNION ALL state construction."""
        query1 = source("users").select(col("id"))
        query2 = source("admins").select(col("id"))

        setop = query1 + query2

        assert isinstance(setop.state, SetOperationState)
        assert setop.state.operator == "UNION ALL"

    def it_creates_intersect_state():
        """Test INTERSECT state construction."""
        query1 = source("users").select(col("id"))
        query2 = source("banned").select(col("user_id"))

        setop = query1 & query2

        assert isinstance(setop.state, SetOperationState)
        assert setop.state.operator == "INTERSECT"

    def it_creates_except_state():
        """Test EXCEPT state construction."""
        query1 = source("users").select(col("id"))
        query2 = source("banned").select(col("user_id"))

        setop = query1 - query2

        assert isinstance(setop.state, SetOperationState)
        assert setop.state.operator == "EXCEPT"

    def it_handles_nested_set_operations():
        """Test nested set operations."""
        query1 = source("users").select(col("id"))
        query2 = source("admins").select(col("id"))
        query3 = source("guests").select(col("id"))

        # (query1 UNION query2) UNION query3
        setop = (query1 | query2) | query3

        assert isinstance(setop.state, SetOperationState)
        assert isinstance(setop.state.left, SetOperationState)
        assert isinstance(setop.state.right, Statement)
```

### Step 7: Create Integration Tests
**File:** `tests/postgres/integration/test_set_operations.py`

Test SQL rendering for all operators and scenarios:

```python
"""Integration tests for set operation SQL rendering."""

from tests.utils import sql
from vw.postgres import col, param, render, source

def describe_union():
    def it_builds_basic_union():
        """
        (SELECT id FROM users) UNION (SELECT id FROM admins)
        """
        query1 = source("users").select(col("id"))
        query2 = source("admins").select(col("id"))

        result = render(query1 | query2)

        assert result.query == sql("""
            (SELECT id FROM users) UNION (SELECT id FROM admins)
        """)
        assert result.params == {}

    def it_builds_union_all():
        """
        (SELECT id FROM users) UNION ALL (SELECT id FROM admins)
        """
        query1 = source("users").select(col("id"))
        query2 = source("admins").select(col("id"))

        result = render(query1 + query2)

        assert result.query == sql("""
            (SELECT id FROM users) UNION ALL (SELECT id FROM admins)
        """)
        assert result.params == {}

def describe_intersect():
    def it_builds_basic_intersect():
        """
        (SELECT id FROM users) INTERSECT (SELECT user_id FROM banned)
        """
        query1 = source("users").select(col("id"))
        query2 = source("banned").select(col("user_id"))

        result = render(query1 & query2)

        assert result.query == sql("""
            (SELECT id FROM users) INTERSECT (SELECT user_id FROM banned)
        """)
        assert result.params == {}

def describe_except():
    def it_builds_basic_except():
        """
        (SELECT id FROM users) EXCEPT (SELECT user_id FROM banned)
        """
        query1 = source("users").select(col("id"))
        query2 = source("banned").select(col("user_id"))

        result = render(query1 - query2)

        assert result.query == sql("""
            (SELECT id FROM users) EXCEPT (SELECT user_id FROM banned)
        """)
        assert result.params == {}

def describe_chaining():
    def it_chains_multiple_unions():
        """
        ((SELECT id FROM users) UNION (SELECT id FROM admins)) UNION (SELECT id FROM guests)
        """
        users = source("users").select(col("id"))
        admins = source("admins").select(col("id"))
        guests = source("guests").select(col("id"))

        result = render((users | admins) | guests)

        assert result.query == sql("""
            ((SELECT id FROM users) UNION (SELECT id FROM admins)) UNION (SELECT id FROM guests)
        """)

def describe_parameters():
    def it_preserves_parameters_across_union():
        """
        (SELECT id FROM users WHERE active = $active)
        UNION
        (SELECT id FROM admins WHERE role = $role)
        """
        query1 = source("users").select(col("id")).where(col("active") == param("active", True))
        query2 = source("admins").select(col("id")).where(col("role") == param("role", "admin"))

        result = render(query1 | query2)

        assert result.query == sql("""
            (SELECT id FROM users WHERE active = $active)
            UNION
            (SELECT id FROM admins WHERE role = $role)
        """)
        assert result.params == {"active": True, "role": "admin"}
```

## Testing Checklist

- [ ] Unit tests for state construction (5 tests)
- [ ] UNION operator rendering
- [ ] UNION ALL operator rendering
- [ ] INTERSECT operator rendering
- [ ] EXCEPT operator rendering
- [ ] Chained set operations (3+ queries)
- [ ] Nested set operations with mixed operators
- [ ] Parameters preserved across set operations
- [ ] Set operations with WHERE clauses
- [ ] Set operations with ORDER BY
- [ ] Set operations as subqueries
- [ ] All existing tests still pass

## Quality Checks

```bash
uv run pytest          # All tests pass
uv run ruff check      # No linting errors
uv run ruff format     # Code formatted
uv run ty check        # Type checking passes
```

## Success Criteria

1. All four set operation operators work via overloading
2. Nested/chained operations render correctly with parentheses
3. Parameters collected from all queries in set operation
4. Set operations can be used anywhere a RowSet is expected
5. All tests pass (existing + new)
6. Code quality checks pass

## Notes

- SetOperation extends both RowSet and Expression (like reference implementation)
- No explicit factory functions needed (operators handle creation)
- SetOperationState is NOT an ExpressionState (it's its own thing)
- Parenthesization always applied for clarity and correctness
