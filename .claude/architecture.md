# Architecture and Design Decisions

## Core Design Principles

### 1. Rendering System

The rendering system uses a context pattern to collect parameters during tree traversal:

```python
# Create context
config = RenderConfig(dialect=Dialect.POSTGRES)
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

### 9. Accessor Pattern for Domain-Specific Operations
String and datetime operations use accessors instead of direct methods:
- `.text` accessor for string operations
- `.dt` accessor for datetime operations

**Rationale**:
- Separates domain-specific operations from core expression logic
- More discoverable API with clear namespacing
- Easy to extend with new operations without cluttering Expression interface

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

### vw/dtypes.py
SQL type constructors for type-safe casting:
- `dtype` - NewType for SQL data types (essentially typed string)
- Character types: `char()`, `varchar()`, `text()`
- Numeric types: `smallint()`, `integer()`, `bigint()`, `decimal()`, `numeric()`, `float()`, `real()`, `double()`, `double_precision()`
- Date/Time types: `date()`, `time()`, `datetime()`, `timestamp()`, `timestamptz()`
- Boolean types: `boolean()`
- Binary types: `bytea()`, `blob()`, `uuid()`
- Container types: `array()`, `list()`, `json()`, `jsonb()`, `struct()`, `variant()`

### vw/operators.py
Comparison and logical operators:
- `Equals`, `NotEquals`, `LessThan`, `LessThanOrEqual`, `GreaterThan`, `GreaterThanOrEqual` - Comparison operators
- `And`, `Or`, `Not` - Logical operators
- `Alias` - Expression aliasing (expr AS name)
- `Cast` - Type casting (dialect-aware: CAST() or ::)

### vw/build.py
Query builder classes:
- `Limit` - LIMIT/OFFSET for pagination
- `Source` - Table/view sources (extends RowSet), overrides `.col()` to use table name as fallback
- `Statement` - Complete SQL statements (extends Expression and RowSet)
- `CommonTableExpression` - CTE for WITH clauses (extends RowSet)
- `cte()` - Helper function to create CTEs

### vw/joins.py
Join implementation classes:
- `JoinAccessor` - Join accessor for method chaining, works with any RowSet
- `InnerJoin` - INNER JOIN implementation
- `LeftJoin` - LEFT JOIN implementation  
- `RightJoin` - RIGHT JOIN implementation
- `FullOuterJoin` - FULL OUTER JOIN implementation
- `CrossJoin` - CROSS JOIN implementation
- `SemiJoin` - SEMI JOIN implementation
- `AntiJoin` - ANTI JOIN implementation

### vw/render.py
Rendering infrastructure:
- `Dialect` - Enum for SQL dialects (controls parameter style and cast syntax)
  - `POSTGRES` - $param, expr::type
  - `SQLSERVER` - @param, CAST(expr AS type)
- `RenderConfig` - Rendering configuration (dialect selection, parameter style override)
- `RenderContext` - Stateful rendering context with depth tracking, CTE collection
- `RenderResult` - Final rendering result (SQL + params)
- `ParamStyle` - Enum for parameter placeholder styles (:name, $name, @name, %(name)s)

### vw/text.py
String operations and accessors:
- String function classes: `Upper`, `Lower`, `Trim`, `LTrim`, `RTrim`, `Length`, `Substring`, `Replace`, `Concat`
- `TextAccessor` - Provides `.text` accessor on Expression for string operations

### vw/datetime.py
DateTime operations and standalone functions:
- DateTime function classes: `Extract`, `DateTrunc`, `ToDate`, `ToTime`, `CurrentTimestamp`, `CurrentDate`, `CurrentTime`, `Now`, `Interval`, `AddInterval`, `SubtractInterval`
- `DateTimeAccessor` - Provides `.dt` accessor on Expression for datetime operations
- Standalone functions: `current_timestamp()`, `current_date()`, `current_time()`, `now()`, `interval()`

### vw/frame.py
Window frame boundaries:
- `FrameBoundary` base class
- Boundary classes: `_UnboundedPreceding`, `_UnboundedFollowing`, `_CurrentRow`, `preceding`, `following`
- Constants: `UNBOUNDED_PRECEDING`, `UNBOUNDED_FOLLOWING`, `CURRENT_ROW`

### vw/exceptions.py
Custom exceptions (import from `vw.exceptions`, not exported from main package):
- `VWError` - Base exception for all vw errors
- `CTENameCollisionError` - Raised when multiple CTEs with the same name are registered
- `UnsupportedParamStyleError` - Raised when an unsupported parameter style is used
- `UnsupportedDialectError` - Raised when a feature is not supported for the selected dialect

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
├── Alias (expression AS name)
├── Cast (type casting)
└── Statement (subquery as expression)
```

`Statement` inherits from both, allowing it to be used as a subquery in either context.

All RowSets share `.alias()`, `.join`, and `.select()` methods through inheritance. The `.col()` method behavior differs: `Source` uses the alias or table name, while base `RowSet` uses only the alias.

### Depth Tracking for Subqueries

`RenderContext` tracks rendering depth to handle parenthesization:
- Depth 0 = top-level query (no parentheses)
- Depth > 0 = nested subquery (parenthesized)

The `.recurse()` method creates a child context with incremented depth.


