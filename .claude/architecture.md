# Architecture and Design Decisions

## Core Design Principles

### 1. Expression Protocol
All SQL expressions implement the `__vw_render__(context: RenderContext) -> str` protocol method.

```python
class Expression(Protocol):
    def __vw_render__(self, context: RenderContext) -> str: ...
```

**Why Protocol instead of base class?**
- Better type checking without inheritance
- More flexible - any class can be an Expression
- Follows Python's structural typing philosophy

### 2. Rendering System

The rendering system uses a context pattern to collect parameters during tree traversal:

```python
# Create context
config = RenderConfig(parameter_style=ParameterStyle.COLON)
context = RenderContext(config=config)

# Render expression tree
sql = expression.__vw_render__(context)

# Context now contains collected parameters
params = context.params
```

**Key components:**
- `RenderConfig` - Configuration for rendering (parameter style, etc.)
- `RenderContext` - Stateful context that collects parameters during rendering
- `RenderResult` - Final result containing SQL string and params dictionary

**Flow:**
1. `Statement.render(config)` creates a `RenderContext`
2. Walks the expression tree, calling `__vw_render__(context)` on each node
3. Parameters register themselves in the context
4. Returns `RenderResult` with SQL and collected params

### 3. Parameter Handling

Parameters are explicitly declared and reusable:

```python
age_param = vw.param("age", 25)

# Can be used multiple times in the same query
query = users.join.inner(
    orders,
    on=[users.col("age") >= age_param, orders.col("min_age") <= age_param]
)
# Both usages share the same parameter name and value
```

**Design choice**: No auto-wrapping of primitives
- Users must explicitly call `param()` for parameterized values
- Clearer intent - distinguishes parameters from raw SQL
- Prevents accidental parameter creation

### 4. Source-First API

Queries start with `Source()` then chain operations (polars-inspired):

```python
vw.Source("users").join.inner(...).select(...)
```

Not:
```python
vw.select(...).from_("users")  # SQL-style
```

**Why?** 
- More natural for method chaining
- Source is the foundation of the query
- Matches polars/pandas mental model

### 5. Accessor Pattern for Joins

Joins use a property accessor `.join.inner()` instead of direct methods:

```python
source.join.inner(...)  # Accessor pattern
source.inner_join(...)  # NOT this
```

**Why?**
- Namespaces join operations
- Extensible - can add `.join.left()`, `.join.right()`, etc.
- Clear intent - `.join` signals join operations

## Design Decisions

### 1. Separate Operator Classes
Each comparison operator is its own class (Equals, NotEquals) instead of a generic BinaryOp.

**Rationale**: More explicit, easier to extend with operator-specific behavior

### 2. Sequence for ON Conditions
Join ON conditions use `Sequence[Expression]` instead of tuples.

**Rationale**: More flexible - accepts lists, tuples, or any sequence type

### 3. Qualified Columns via Source.col()
`Source.col("id")` creates qualified column references like "table.id".

**Rationale**: Explicit qualification, prevents ambiguity in joins

### 4. Column Comparisons Return Expression Classes
`col("a") == col("b")` returns an `Equals` instance, not a boolean.

**Rationale**: Enables SQL expression building while using Python operators

### 5. Positional-Only param()
`param(name, value, /)` - both parameters are positional-only.

**Rationale**: Clear, unambiguous parameter creation

### 6. Type Validation in param()
`param()` rejects unsupported types (anything other than str, int, float, bool).

**Rationale**: Catch errors early, prevent invalid parameter types

### 7. __vw_render__() Convention
Custom render method instead of `__str__()`.

**Rationale**: 
- `__str__()` is for human-readable output
- `__vw_render__()` is for SQL generation with context
- Clear separation of concerns

### 8. Escape Hatch via Strings
Columns accept raw SQL strings for unsupported features.

**Rationale**: 
- Unblocks users when library doesn't support a feature
- Gradual feature adoption - can add proper support later
- Example: `col("* REPLACE (foo AS bar)")`

## Module Organization

### vw/expr.py
Expression classes that represent SQL components:
- `Column` - Column references
- `Parameter` - Parameterized values
- `Equals`, `NotEquals` - Comparison operators
- Helper functions: `col()`, `param()`

### vw/query.py
Query builder classes:
- `Source` - Table/view sources
- `Statement` - Complete SQL statements
- `InnerJoin` - Join operations
- `JoinAccessor` - Join accessor for method chaining

### vw/render.py
Rendering infrastructure:
- `ParameterStyle` - Enum for parameter styles (:name, $name, @name)
- `RenderConfig` - Rendering configuration
- `RenderContext` - Stateful rendering context
- `RenderResult` - Final rendering result (SQL + params)

## Future Architectural Considerations

### Additional Operators
When adding operators like `<`, `>`, `<=`, `>=`, `LIKE`, `IN`:
- Follow the same pattern as `Equals` and `NotEquals`
- Create separate classes for each operator
- Add operator overloading methods to `Column` class

### WHERE Clause
Will need to handle AND/OR logic:
- Consider `WhereClause` class
- May need `And`, `Or` expression classes
- Design for composability

### Subqueries
Subqueries can appear in multiple places (SELECT, FROM, WHERE):
- `Source` might need to accept another `Statement`
- Consider `Subquery` wrapper class
- Ensure proper parenthesization in rendering
