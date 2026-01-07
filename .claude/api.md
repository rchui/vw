# API Reference

## Public API

All public exports from `vw/__init__.py`:

### Functions
- `col(name)` - Create a column reference
- `param(name, value)` - Create a parameterized value
- `cte(name, query)` - Create a Common Table Expression

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
- `Dialect` - SQL dialect enum (controls parameter style and cast syntax)

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

### SQL Dialects

The `Dialect` enum controls both parameter style and cast syntax:

```python
# Default is SQLAlchemy dialect (:param, CAST())
config = vw.RenderConfig(dialect=vw.Dialect.SQLALCHEMY)
result = query.render(config=config)  # Uses :name, CAST(x AS type)

# PostgreSQL dialect ($param, ::type)
config = vw.RenderConfig(dialect=vw.Dialect.POSTGRES)
result = query.render(config=config)  # Uses $name, x::type

# SQL Server dialect (@param, CAST())
config = vw.RenderConfig(dialect=vw.Dialect.SQLSERVER)
result = query.render(config=config)  # Uses @name, CAST(x AS type)
```

### ORDER BY

Use `.order_by()` with `.asc()` or `.desc()` for sorting:

```python
# Basic ORDER BY (ascending)
result = (
    vw.Source(name="users")
    .select(vw.col("*"))
    .order_by(vw.col("name").asc())
    .render()
)
# SELECT * FROM users ORDER BY name ASC

# Descending order
result = (
    vw.Source(name="users")
    .select(vw.col("*"))
    .order_by(vw.col("created_at").desc())
    .render()
)
# SELECT * FROM users ORDER BY created_at DESC

# Multiple columns with mixed directions
result = (
    vw.Source(name="users")
    .select(vw.col("*"))
    .order_by(vw.col("last_name").asc(), vw.col("first_name").asc(), vw.col("created_at").desc())
    .render()
)
# SELECT * FROM users ORDER BY last_name ASC, first_name ASC, created_at DESC

# Without explicit direction (defaults to database default, typically ASC)
result = (
    vw.Source(name="users")
    .select(vw.col("*"))
    .order_by(vw.col("name"))
    .render()
)
# SELECT * FROM users ORDER BY name
```

### GROUP BY and HAVING

Use `.group_by()` and `.having()` for aggregation queries:

```python
# Basic GROUP BY
result = (
    vw.Source(name="orders")
    .select(vw.col("customer_id"), vw.col("SUM(total)"))
    .group_by(vw.col("customer_id"))
    .render()
)
# SELECT customer_id, SUM(total) FROM orders GROUP BY customer_id

# GROUP BY with HAVING
result = (
    vw.Source(name="orders")
    .select(vw.col("customer_id"), vw.col("COUNT(*)"))
    .group_by(vw.col("customer_id"))
    .having(vw.col("COUNT(*)") > vw.param("min_orders", 5))
    .render()
)
# SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id HAVING (COUNT(*) > :min_orders)

# Full chain: WHERE -> GROUP BY -> HAVING
result = (
    vw.Source(name="orders")
    .select(vw.col("customer_id"), vw.col("COUNT(*)"))
    .where(vw.col("status") == vw.param("status", "completed"))
    .group_by(vw.col("customer_id"))
    .having(vw.col("COUNT(*)") >= vw.param("min_orders", 3))
    .render()
)
```

### Type Casting

Use `.cast()` to cast expressions to SQL types. The syntax varies by dialect:

```python
# SQLAlchemy/SQL Server: CAST(expr AS type)
result = vw.Source(name="orders").select(
    vw.col("price").cast("DECIMAL(10,2)")
).render()
# SELECT CAST(price AS DECIMAL(10,2)) FROM orders

# PostgreSQL: expr::type
config = vw.RenderConfig(dialect=vw.Dialect.POSTGRES)
result = vw.Source(name="orders").select(
    vw.col("price").cast("numeric")
).render(config=config)
# SELECT price::numeric FROM orders

# Cast can be chained with alias
result = vw.Source(name="orders").select(
    vw.col("price").cast("DECIMAL(10,2)").alias("formatted_price")
).render()
# SELECT CAST(price AS DECIMAL(10,2)) AS formatted_price FROM orders
```

### LIMIT / OFFSET

Use `.limit()` to limit the number of rows returned:

```python
# Basic LIMIT
result = (
    vw.Source(name="users")
    .select(vw.col("*"))
    .limit(10)
    .render()
)
# SELECT * FROM users LIMIT 10

# LIMIT with OFFSET
result = (
    vw.Source(name="users")
    .select(vw.col("*"))
    .order_by(vw.col("id").asc())
    .limit(10, offset=20)
    .render()
)
# SELECT * FROM users ORDER BY id ASC LIMIT 10 OFFSET 20

# SQL Server uses OFFSET/FETCH syntax
config = vw.RenderConfig(dialect=vw.Dialect.SQLSERVER)
result = (
    vw.Source(name="users")
    .select(vw.col("*"))
    .order_by(vw.col("id").asc())
    .limit(10, offset=20)
    .render(config=config)
)
# SELECT * FROM users ORDER BY id ASC OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY
```

### Expression Aliasing

Use `.alias()` to give expressions an alias in SELECT clauses:

```python
# Alias a column
result = vw.Source(name="orders").select(
    vw.col("id"),
    vw.col("price").alias("unit_price")
).render()
# SELECT id, price AS unit_price FROM orders

# Alias a parameter
result = vw.Source(name="orders").select(
    vw.col("id"),
    vw.param("tax_rate", 0.08).alias("tax")
).render()
# SELECT id, :tax_rate AS tax FROM orders
```

Note: For subquery aliasing in FROM/JOIN clauses, use `Statement.alias()` which returns an aliased statement (see Subqueries section).

### Common Table Expressions (CTEs)

CTEs allow you to define named temporary result sets that can be referenced like tables:

```python
# Define a CTE
active_users = vw.cte(
    "active_users",
    vw.Source(name="users").select(vw.col("*")).where(vw.col("status") == vw.param("status", "active"))
)

# Use it like any RowSet
result = active_users.select(vw.col("*")).render()
# result.sql: "WITH active_users AS (SELECT * FROM users WHERE (status = :status)) SELECT * FROM active_users"
# result.params: {"status": "active"}

# Use in joins
orders = vw.Source(name="orders")
result = (
    orders.join.inner(active_users, on=[orders.col("user_id") == active_users.col("id")])
    .select(vw.col("*"))
    .render()
)
# result.sql: "WITH active_users AS (...) SELECT * FROM orders INNER JOIN active_users ON (...)"

# Multiple CTEs - dependencies are automatically ordered
cte_a = vw.cte("a", vw.Source(name="table_a").select(vw.col("*")))
cte_b = vw.cte("b", cte_a.select(vw.col("*")))  # References cte_a
result = cte_b.select(vw.col("*")).render()
# WITH a AS (...), b AS (SELECT * FROM a) SELECT * FROM b
```

CTEs register themselves in the RenderContext during tree traversal, similar to Parameters. Dependencies are automatically discovered and ordered correctly.

**Note**: CTE names must be unique. Using multiple CTEs with the same name raises `CTENameCollisionError` (import from `vw.exceptions`).

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
