# Research: PostgreSQL Phase 5d - CASE/WHEN

## Reference Implementation (`vw/reference/operators.py`)

The reference uses a two-step builder pattern:

```python
when(condition)         # -> When (not an Expression, incomplete)
    .then(result)       # -> CaseExpression (has branches, chainable)
    .when(condition)    # -> When (carries accumulated branches)
    .then(result)       # -> CaseExpression
    .otherwise(result)  # -> final CaseExpression with ELSE
```

**Key classes:**
- `WhenThen` - frozen dataclass, stores `when: Expression` and `then: Expression`
- `When` - intermediate builder, holds `condition` + `branches: list[WhenThen] | None`
- `CaseExpression(Expression)` - accumulates `branches: list[WhenThen]`, optional `_otherwise`

**Notes:**
- Only searched CASE (no simple `CASE expr WHEN value` variant)
- Rendering is inline: `CASE WHEN ... THEN ... [ELSE ...] END`
- Reference uses mutable `list`; new implementation uses immutable `tuple` (frozen pattern)

## Architecture Patterns (from existing codebase)

- All state: frozen dataclasses in `vw/core/states.py`
- Builders: separate files (e.g., `vw/core/joins.py` for `JoinAccessor`)
- Public API factories: `vw/postgres/public.py`
- Rendering: `render_state()` pattern match in `vw/postgres/render.py`
- Operators take `Expression` objects; `.state` is extracted

## Proposed Design

### State (`vw/core/states.py`)

```python
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

### Builders (`vw/core/case.py`, new file)

```python
@dataclass(eq=False, frozen=True, kw_only=True)
class When(Generic[ExprT, RowSetT]):
    """Incomplete WHEN clause - must be completed with .then()."""
    condition: object  # ExpressionState
    prior_whens: tuple[WhenThen, ...]
    factories: Factories[ExprT, RowSetT]

    def then(self, result: ExprT) -> CaseExpression[ExprT, RowSetT]: ...

@dataclass(eq=False, frozen=True, kw_only=True)
class CaseExpression(Generic[ExprT, RowSetT]):
    """Accumulated CASE branches - can add more or finalize."""
    whens: tuple[WhenThen, ...]
    factories: Factories[ExprT, RowSetT]

    def when(self, condition: ExprT) -> When[ExprT, RowSetT]: ...
    def otherwise(self, result: ExprT) -> ExprT: ...
    def end(self) -> ExprT: ...  # Finalize without ELSE
```

### Public API (`vw/postgres/public.py`)

```python
def when(condition: Expression, /) -> When:
    """Start a searched CASE expression."""
```

### Rendering (`vw/postgres/render.py`)

```python
case Case():
    whens = " ".join(
        f"WHEN {render_state(w.condition, ctx)} THEN {render_state(w.result, ctx)}"
        for w in state.whens
    )
    else_sql = f" ELSE {render_state(state.else_result, ctx)}" if state.else_result is not None else ""
    return f"CASE {whens}{else_sql} END"
```

## Scope

### In scope:
- Searched CASE: `when(cond).then(val).when(cond).then(val).otherwise(val)`
- Nested CASE (works naturally - result can be any Expression)
- CASE in SELECT, WHERE, ORDER BY, HAVING (works naturally - it's an Expression)
- Parameters in conditions and results

### Out of scope:
- Simple CASE (`CASE expr WHEN value THEN result`)
- Validation of branch count
