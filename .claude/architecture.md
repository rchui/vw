# Architecture and Design Decisions

## Core Design Principles

### 1. Rendering System

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

### 2. Parameter Handling

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

### 3. Source-First API

Queries start with `Source()` then chain operations (polars-inspired):

```python
vw.Source(name="users").join.inner(...).select(...)
```

Not:
```python
vw.select(...).from_("users")  # SQL-style
```

**Why?**
- More natural for method chaining
- Source is the foundation of the query
- Matches polars/pandas mental model

### 4. Accessor Pattern for Joins

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

### vw/base.py
Base classes for the expression system:
- `Expression` - Base class for SQL expressions
- `RowSet` - Base class for row-producing objects (tables, subqueries) with:
  - `_alias` field for aliasing
  - `_joins` field for join accumulation
  - `.alias()` method returns aliased copy
  - `.col()` method for qualified column references
  - `.join` property accessor for join operations
  - `.select()` method for creating statements

### vw/column.py
Column reference class:
- `Column` - Column references with optional prefix
- `col()` - Helper function to create columns

### vw/parameter.py
Parameterized value class:
- `Parameter` - Parameterized values for safe query building
- `param()` - Helper function to create parameters

### vw/operators.py
Comparison and logical operators:
- `Equals`, `NotEquals`, `LessThan`, `LessThanOrEqual`, `GreaterThan`, `GreaterThanOrEqual` - Comparison operators
- `And`, `Or`, `Not` - Logical operators

### vw/build.py
Query builder classes:
- `Source` - Table/view sources (extends RowSet), overrides `.col()` to use table name as fallback
- `Statement` - Complete SQL statements (extends Expression and RowSet)
- `InnerJoin` - Join operations
- `JoinAccessor` - Join accessor for method chaining, works with any RowSet
- `CommonTableExpression` - CTE for WITH clauses (extends RowSet)
- `cte()` - Helper function to create CTEs

### vw/render.py
Rendering infrastructure:
- `ParameterStyle` - Enum for parameter styles (:name, $name, @name)
- `RenderConfig` - Rendering configuration
- `RenderContext` - Stateful rendering context with depth tracking, CTE collection
- `RenderResult` - Final rendering result (SQL + params)

### vw/exceptions.py
Custom exceptions (import from `vw.exceptions`, not exported from main package):
- `VWError` - Base exception for all vw errors
- `CTENameCollisionError` - Raised when multiple CTEs with the same name are registered

## Type Hierarchy

The type system separates row-producing objects from expressions:

```
RowSet (things in FROM/JOIN)
├── _alias: str | None (aliasing)
├── _joins: list[InnerJoin] (join accumulation)
├── .alias() -> Self (create aliased copy)
├── .col() -> Column (qualified column reference)
├── .join -> JoinAccessor (join operations)
├── .select() -> Statement (create statement)
│
├── Source (table name)
│   └── .col() uses _alias or table name as prefix
│
├── Statement (subquery)
│   └── .col() uses _alias as prefix
│
└── CommonTableExpression (CTE)
    └── .col() uses _alias or CTE name as prefix

Expression (things in SELECT/WHERE/ON conditions)
├── Column
├── Parameter
├── Equals, NotEquals, LessThan, ...
├── And, Or, Not
└── Statement (subquery as expression)
```

`Statement` inherits from both, allowing it to be used as a subquery in either context.

All RowSets share `.alias()`, `.join`, and `.select()` methods through inheritance. The `.col()` method behavior differs: `Source` uses the alias or table name, while base `RowSet` uses only the alias.

### Depth Tracking for Subqueries

`RenderContext` tracks rendering depth to handle parenthesization:
- Depth 0 = top-level query (no parentheses)
- Depth > 0 = nested subquery (parenthesized)

The `.recurse()` method creates a child context with incremented depth.

## Future Architectural Considerations

### Additional Operators
When adding operators like `LIKE`, `IN`:
- Follow the same pattern as `Equals` and `NotEquals`
- Create separate classes for each operator
- Add operator overloading methods to `Column` class

### CTEs (Common Table Expressions)
CTEs are implemented as a `RowSet` subclass (`CommonTableExpression`):
- Named query that can be referenced like a table
- Registers in `RenderContext.ctes` during tree traversal (similar to Parameters)
- Pre-renders body SQL during registration to discover dependencies
- CTE body SQL is cached in context to avoid double rendering
- Dependencies are automatically ordered (CTEs registered after their dependencies)
- Name collision detection raises `CTENameCollisionError`
- Could support recursive CTEs in future

### Subqueries in WHERE
For `IN (SELECT ...)` and `EXISTS (SELECT ...)`:
- Statement is already an Expression
- Need `In` and `Exists` operator classes that accept Statement
