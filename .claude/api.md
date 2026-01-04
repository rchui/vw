# API Reference

## Public API

All public exports from `vw/__init__.py`:

### Functions
- `col(name)` - Create a column reference
- `param(name, value)` - Create a parameterized value

### Classes
- `Column` - Column reference class
- `Parameter` - Parameterized value class
- `Equals` - Equality comparison operator (=)
- `NotEquals` - Inequality comparison operator (!=)
- `LessThan` - Less than comparison operator (<)
- `LessThanOrEqual` - Less than or equal comparison operator (<=)
- `GreaterThan` - Greater than comparison operator (>)
- `GreaterThanOrEqual` - Greater than or equal comparison operator (>=)
- `Source` - Table/view source
- `Statement` - SQL statement
- `InnerJoin` - Inner join operation
- `RenderResult` - Rendering result with SQL and params
- `RenderConfig` - Rendering configuration
- `RenderContext` - Rendering context (for advanced use)
- `ParameterStyle` - Parameter style enum

### Protocols
- `Expression` - Protocol for SQL expressions

## Usage Examples

### Basic SELECT

```python
import vw

# SELECT * FROM users
result = vw.Source("users").select(vw.col("*")).render()
# result.sql: "SELECT * FROM users"
# result.params: {}
```

### SELECT with Multiple Columns

```python
result = vw.Source("users").select(
    vw.col("id"),
    vw.col("name"),
    vw.col("email")
).render()
# result.sql: "SELECT id, name, email FROM users"
```

### Qualified Columns

```python
users = vw.Source("users")
result = users.select(
    users.col("id"),
    users.col("name")
).render()
# result.sql: "SELECT users.id, users.name FROM users"
```

### INNER JOIN

```python
users = vw.Source("users")
orders = vw.Source("orders")

joined = users.join.inner(
    orders,
    on=[users.col("id") == orders.col("user_id")]
)

result = joined.select(vw.col("*")).render()
# result.sql: "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
```

### Multiple JOIN Conditions

```python
users = vw.Source("users")
orders = vw.Source("orders")

joined = users.join.inner(
    orders,
    on=[
        users.col("id") == orders.col("user_id"),
        users.col("status") == vw.col("'active'")
    ]
)

result = joined.select(vw.col("*")).render()
# result.sql: "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id AND users.status = 'active'"
```

### Chaining Multiple JOINs

```python
users = vw.Source("users")
orders = vw.Source("orders")
products = vw.Source("products")

joined = (
    users
    .join.inner(orders, on=[users.col("id") == orders.col("user_id")])
    .join.inner(products, on=[orders.col("product_id") == products.col("id")])
)

result = joined.select(vw.col("*")).render()
# result.sql: "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id INNER JOIN products ON orders.product_id = products.id"
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

```python
users = vw.Source("users")
orders = vw.Source("orders")

# Create parameters
user_id = vw.param("user_id", 123)
status = vw.param("status", "active")

joined = users.join.inner(
    orders,
    on=[
        users.col("id") == user_id,
        orders.col("status") == status
    ]
)

result = joined.select(users.col("name"), orders.col("total")).render()
# result.sql: "SELECT users.name, orders.total FROM users INNER JOIN orders ON users.id = :user_id AND orders.status = :status"
# result.params: {"user_id": 123, "status": "active"}
```

### Reusing Parameters

```python
# Same parameter can be used multiple times
threshold = vw.param("threshold", 100)

joined = users.join.inner(
    orders,
    on=[
        users.col("age") == threshold,
        orders.col("quantity") == threshold
    ]
)

result = joined.select(vw.col("*")).render()
# result.sql: "SELECT * FROM users INNER JOIN orders ON users.age = :threshold AND orders.quantity = :threshold"
# result.params: {"threshold": 100}  # Only appears once in params
```

### Different Parameter Types

```python
# Supports str, int, float, bool
name = vw.param("name", "Alice")         # str
age = vw.param("age", 25)                # int
price = vw.param("price", 19.99)         # float
active = vw.param("active", True)        # bool
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
