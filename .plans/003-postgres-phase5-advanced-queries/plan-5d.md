# Implementation Plan: PostgreSQL Phase 5d - CASE/WHEN

## Overview

Implement searched CASE/WHEN expressions following the reference implementation pattern.

**API:**
```python
when(col("status") == param("a", "active")).then(param("one", 1))
    .when(col("status") == param("i", "inactive")).then(param("zero", 0))
    .otherwise(param("default", -1))
```

**Produces:**
```sql
CASE WHEN status = $a THEN $one WHEN status = $i THEN $zero ELSE $default END
```

---

## Step 1: Add State Classes to `vw/core/states.py`

Add two new dataclasses after the existing subquery operators section:

```python
# --- Conditional Expressions ----------------------------------------------- #

@dataclass(eq=False, frozen=True, kw_only=True)
class WhenThen:
    """A single WHEN/THEN pair in a CASE expression."""
    condition: object  # ExpressionState
    result: object     # ExpressionState


@dataclass(eq=False, frozen=True, kw_only=True)
class Case(ExpressionState):
    """Searched CASE expression: CASE WHEN cond THEN val ... [ELSE val] END"""
    whens: tuple[WhenThen, ...]
    else_result: object | None = None
```

**Note:** `WhenThen` is not an `ExpressionState` itself - it's a helper struct embedded in `Case`.

---

## Step 2: Create `vw/core/case.py`

New file following the `vw/core/joins.py` pattern:

```python
"""CASE/WHEN expression builders."""

from dataclasses import dataclass, replace
from typing import Generic

from vw.core.base import Factories
from vw.core.protocols import ExprT, RowSetT
from vw.core.states import Case, WhenThen


@dataclass(eq=False, frozen=True, kw_only=True)
class When(Generic[ExprT, RowSetT]):
    """Incomplete WHEN clause - must be completed with .then()."""
    condition: object  # ExpressionState
    prior_whens: tuple[WhenThen, ...]
    factories: Factories[ExprT, RowSetT]

    def then(self, result: ExprT, /) -> CaseExpression[ExprT, RowSetT]:
        """Complete this WHEN clause with a THEN result."""
        new_when = WhenThen(condition=self.condition, result=result.state)
        return CaseExpression(
            whens=(*self.prior_whens, new_when),
            factories=self.factories,
        )


@dataclass(eq=False, frozen=True, kw_only=True)
class CaseExpression(Generic[ExprT, RowSetT]):
    """Accumulated CASE branches - can add more WHEN clauses or finalize."""
    whens: tuple[WhenThen, ...]
    factories: Factories[ExprT, RowSetT]

    def when(self, condition: ExprT, /) -> When[ExprT, RowSetT]:
        """Add another WHEN clause."""
        return When(
            condition=condition.state,
            prior_whens=self.whens,
            factories=self.factories,
        )

    def otherwise(self, result: ExprT, /) -> ExprT:
        """Finalize with an ELSE clause."""
        state = Case(whens=self.whens, else_result=result.state)
        return self.factories.expr(state=state, factories=self.factories)

    def end(self) -> ExprT:
        """Finalize without an ELSE clause (NULL when no branch matches)."""
        state = Case(whens=self.whens)
        return self.factories.expr(state=state, factories=self.factories)
```

---

## Step 3: Add `when()` Factory to `vw/postgres/public.py`

```python
def when(condition: Expression, /) -> When:
    """Start a CASE WHEN expression.

    Args:
        condition: The boolean condition to check.

    Returns:
        A When builder that must be completed with .then().

    Example:
        >>> when(col("status") == param("a", "active")).then(param("one", 1))
        ...     .when(col("status") == param("i", "inactive")).then(param("zero", 0))
        ...     .otherwise(param("default", -1))
    """
    from vw.core.case import When as WhenBuilder
    return WhenBuilder(
        condition=condition.state,
        prior_whens=(),
        factories=Factories(expr=Expression, rowset=RowSet),
    )
```

---

## Step 4: Add Rendering to `vw/postgres/render.py`

Add `Case` and `WhenThen` to imports, then add to `render_state()`:

```python
# --- Conditional Expressions ------------------------------------------- #
case Case():
    whens = " ".join(
        f"WHEN {render_state(w.condition, ctx)} THEN {render_state(w.result, ctx)}"
        for w in state.whens
    )
    else_sql = f" ELSE {render_state(state.else_result, ctx)}" if state.else_result is not None else ""
    return f"CASE {whens}{else_sql} END"
```

---

## Step 5: Export `when` from `vw/postgres/__init__.py`

---

## Step 6: Add Integration Tests (`tests/postgres/integration/test_case.py`)

Test groups:
- `describe_basic_case()` - single WHEN, with and without ELSE
- `describe_multiple_when()` - chained WHEN clauses
- `describe_case_in_select()` - CASE as a column expression
- `describe_case_in_where()` - CASE in WHERE clause
- `describe_nested_case()` - CASE as result of another CASE
- `describe_case_with_parameters()` - parameters in conditions and results

---

## Step 7: Update `docs/development/postgres-parity.md`

Mark CASE/WHEN items as completed (✅).
