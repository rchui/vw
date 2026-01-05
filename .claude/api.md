# API Reference

## Public API

All public exports from `vw/__init__.py`:

### Functions
- `col(name)` - Create a column reference
- `param(name, value)` - Create a parameterized value

### Classes
- `And` - Logical combination operator (AND)
- `Column` - Column reference class
- `Expression` - Expression base classes
- `Parameter` - Parameterized value class
- `Equals` - Equality comparison operator (=)
- `NotEquals` - Inequality comparison operator (!=)
- `LessThan` - Less than comparison operator (<)
- `LessThanOrEqual` - Less than or equal comparison operator (<=)
- `GreaterThan` - Greater than comparison operator (>)
- `GreaterThanOrEqual` - Greater than or equal comparison operator (>=)
- `Or` - Logical combination operator (OR)
- `Source` - Table/view source
- `Statement` - SQL statement
- `InnerJoin` - Inner join operation
- `RenderResult` - Rendering result with SQL and params
- `RenderConfig` - Rendering configuration
- `RenderContext` - Rendering context (for advanced use)
- `ParameterStyle` - Parameter style enum

## Usage Examples

For comprehensive examples, see the integration tests in `tests/test_sql.py`:
- `describe_basic_select` - Basic SELECT statements
- `describe_star_extensions` - Star expression extensions (REPLACE, EXCLUDE)
- `describe_method_chaining` - Method chaining patterns
- `describe_complex_expressions` - Complex SQL expressions via escape hatch
- `describe_joins` - INNER JOIN operations
- `describe_where` - WHERE clause support
- `describe_parameters` - Parameterized queries

### Basic SELECT

See `tests/test_sql.py::describe_basic_select` for more examples.

```python
import vw

# SELECT * FROM users
result = vw.Source("users").select(vw.col("*")).render()
# result.sql: "SELECT * FROM users"
# result.params: {}
```

### SELECT with Qualified Columns

```python
users = vw.Source("users")
result = users.select(
    users.col("id"),
    users.col("name")
).render()
# result.sql: "SELECT users.id, users.name FROM users"
```

### INNER JOIN

See `tests/test_sql.py::describe_joins` for comprehensive examples including multiple conditions and chained joins.

```python
users = vw.Source("users")
orders = vw.Source("orders")

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

Basic example:
```python
result = vw.Source("users").select(vw.col("*")).where(
    vw.col("age") >= vw.param("min_age", 18)
).render()
# result.sql: "SELECT * FROM users WHERE age >= :min_age"
# result.params: {"min_age": 18}
```

### Parameterized Queries

See `tests/test_sql.py::describe_parameters` for comprehensive examples including parameter reuse and different types.

```python
users = vw.Source("users")
orders = vw.Source("orders")

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
    vw.Source("users")
    .join.inner(vw.Source("orders"), on=[...])
    .select(vw.col("*"))
    .render()
)
```

Each method returns an object that can be further chained, enabling readable query construction.
