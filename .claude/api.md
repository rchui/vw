# API Reference

## Public API

All public exports from `vw/__init__.py`:

### Functions
- `col(name)` - Create a column reference
- `param(name, value)` - Create a parameterized value

### Classes
- `Column` - Column reference class
- `Expression` - Expression base class
- `RowSet` - Base class for row-producing objects (tables, subqueries) with `.select()`, `.join`, `.alias()`, `.col()` methods
- `Parameter` - Parameterized value class
- `Equals` - Equality comparison operator (=)
- `NotEquals` - Inequality comparison operator (<>)
- `LessThan` - Less than comparison operator (<)
- `LessThanOrEqual` - Less than or equal comparison operator (<=)
- `GreaterThan` - Greater than comparison operator (>)
- `GreaterThanOrEqual` - Greater than or equal comparison operator (>=)
- `Source` - Table/view source (extends RowSet)
- `Statement` - SQL statement (extends Expression and RowSet)
- `InnerJoin` - Inner join operation
- `RenderResult` - Rendering result with SQL and params
- `RenderConfig` - Rendering configuration
- `RenderContext` - Rendering context (for advanced use)
- `ParameterStyle` - Parameter style enum

### Operators (via Expression methods)
- `&` - Logical AND (`expr1 & expr2`)
- `|` - Logical OR (`expr1 | expr2`)
- `~` - Logical NOT (`~expr`)

## Usage Examples

For comprehensive examples, see the integration tests in `tests/test_sql.py`:
- `describe_basic_select` - Basic SELECT statements
- `describe_star_extensions` - Star expression extensions (REPLACE, EXCLUDE)
- `describe_method_chaining` - Method chaining patterns
- `describe_complex_expressions` - Complex SQL expressions via escape hatch
- `describe_joins` - INNER JOIN operations
- `describe_where` - WHERE clause support
- `describe_subqueries` - Subqueries and aliasing
- `describe_parameters` - Parameterized queries

### Basic SELECT

See `tests/test_sql.py::describe_basic_select` for more examples.

```python
import vw

# SELECT * FROM users
result = vw.Source(name="users").select(vw.col("*")).render()
# result.sql: "SELECT * FROM users"
# result.params: {}
```

### SELECT with Qualified Columns

```python
users = vw.Source(name="users")
result = users.select(
    users.col("id"),
    users.col("name")
).render()
# result.sql: "SELECT users.id, users.name FROM users"
```

### INNER JOIN

See `tests/test_sql.py::describe_joins` for comprehensive examples including multiple conditions and chained joins.

```python
users = vw.Source(name="users")
orders = vw.Source(name="orders")

result = (
    users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
    .select(vw.col("*"))
    .render()
)
# result.sql: "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
```

### WHERE Clause

The `where()` method adds WHERE conditions to a statement. Multiple expressions are combined with AND.

See `tests/test_sql.py::describe_where` for comprehensive examples including:
- Single and multiple conditions
- WHERE with parameters
- Chaining multiple where() calls
- WHERE with JOIN
- All comparison operators
- Logical operators (AND, OR, NOT)

Basic example:
```python
result = vw.Source(name="users").select(vw.col("*")).where(
    vw.col("age") >= vw.param("min_age", 18)
).render()
# result.sql: "SELECT * FROM users WHERE age >= :min_age"
# result.params: {"min_age": 18}
```

### Logical Operators

Expressions can be combined using Python operators:

```python
# AND: use &
expr = (vw.col("age") >= vw.col("18")) & (vw.col("status") == vw.col("'active'"))
# Renders: (age >= 18) AND (status = 'active')

# OR: use |
expr = (vw.col("role") == vw.col("'admin'")) | (vw.col("role") == vw.col("'superuser'"))
# Renders: (role = 'admin') OR (role = 'superuser')

# NOT: use ~
expr = ~(vw.col("deleted") == vw.col("true"))
# Renders: NOT (deleted = true)

# Combined
expr = ~(vw.col("deleted") == vw.col("true")) & (vw.col("status") == vw.col("'active'"))
# Renders: (NOT (deleted = true)) AND (status = 'active')
```

### Parameterized Queries

See `tests/test_sql.py::describe_parameters` for comprehensive examples including parameter reuse and different types.

```python
users = vw.Source(name="users")
orders = vw.Source(name="orders")

# Create parameters
user_id = vw.param("user_id", 123)
status = vw.param("status", "active")

result = (
    users.join.inner(
        orders,
        on=[
            users.col("id") == user_id,
            orders.col("status") == status
        ]
    )
    .select(users.col("name"), orders.col("total"))
    .render()
)
# result.sql: "SELECT users.name, orders.total FROM users INNER JOIN orders ON users.id = :user_id AND orders.status = :status"
# result.params: {"user_id": 123, "status": "active"}
```

### Custom Parameter Styles

```python
# Default is colon style (:name)
config = vw.RenderConfig(parameter_style=vw.ParameterStyle.COLON)
result = query.render(config)  # Uses :name

# Dollar style ($name)
config = vw.RenderConfig(parameter_style=vw.ParameterStyle.DOLLAR)
result = query.render(config)  # Uses $name

# At style (@name) - SQL Server
config = vw.RenderConfig(parameter_style=vw.ParameterStyle.AT)
result = query.render(config)  # Uses @name
```

### Subqueries and Aliasing

See `tests/test_sql.py::describe_subqueries` for comprehensive examples.

Statements can be used as subqueries in FROM/JOIN clauses. Use `.alias()` to give them a name:

```python
# Subquery in JOIN
users = vw.Source(name="users")
order_totals = (
    vw.Source(name="orders")
    .select(vw.col("user_id"), vw.col("total"))
    .alias("ot")
)

result = (
    users.join.inner(order_totals, on=[users.col("id") == order_totals.col("user_id")])
    .select(users.col("name"), order_totals.col("total"))
    .render()
)
# result.sql: "SELECT users.name, ot.total FROM users INNER JOIN (SELECT user_id, total FROM orders) AS ot ON (users.id = ot.user_id)"
```

Table aliasing also works:

```python
o = vw.Source(name="orders").alias("o")
result = (
    vw.Source(name="users")
    .join.inner(o, on=[vw.col("users.id") == o.col("user_id")])
    .select(vw.col("*"))
    .render()
)
# result.sql: "SELECT * FROM users INNER JOIN orders AS o ON (users.id = o.user_id)"
```

### Escape Hatch for Raw SQL

See `tests/test_sql.py::describe_star_extensions` and `tests/test_sql.py::describe_complex_expressions` for more examples.

```python
# Use raw SQL strings for unsupported features
vw.col("* REPLACE (name AS full_name)")
vw.col("* EXCLUDE (password, ssn)")
vw.col("CAST(price AS DECIMAL(10,2))")
vw.col("ROUND(total, 2) AS rounded_total")
```

## Method Chaining Pattern

The API follows a fluent, polars-inspired method chaining pattern:

```python
result = (
    vw.Source(name="users")
    .join.inner(vw.Source(name="orders"), on=[...])
    .select(vw.col("*"))
    .render()
)
```

Each method returns an object that can be further chained, enabling readable query construction.
