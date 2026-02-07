# PostgreSQL API Reference

The `vw.postgres` module provides PostgreSQL-specific implementations and rendering.

## Module Import

```python
from vw.postgres as vw, col, param, render, F
```

## Factory Functions

### `source(name)`

Create a PostgreSQL table or view source.

```python
users = source("users")
# Represents: users table

orders = source("orders").alias("o")
# Represents: orders table with alias 'o'
```

**Parameters:**
- `name` (str) - Table or view name

**Returns:** RowSet wrapping Source state

**Example:**
```python
from vw.postgres as vw, col, render

users = source("users")
query = users.select(col("*"))
result = render(query)

print(result.query)
# SELECT * FROM users
```

### `col(name)`

Create a PostgreSQL column reference.

```python
# Unqualified column
age = col("age")

# Use in query
query = users.select(col("id"), col("name"))
```

**Parameters:**
- `name` (str) - Column name (unqualified)

**Returns:** Expression wrapping Column state

**Note:** For qualified columns, use `rowset.col(name)` instead:
```python
users = source("users").alias("u")
users.col("id")  # u.id
```

### `param(name, value)`

Create a PostgreSQL query parameter.

```python
min_age = param("min_age", 18)
status = param("status", "active")

query = users.where(col("age") >= min_age)
```

**Parameters:**
- `name` (str) - Parameter name
- `value` (Any) - Parameter value (str, int, float, bool, None)

**Returns:** Expression wrapping Parameter state

**Example:**
```python
from vw.postgres as vw, col, param, render

users = source("users")
query = users.where(col("age") >= param("min_age", 18))
result = render(query)

print(result.query)
# SELECT * FROM users WHERE age >= $min_age

print(result.params)
# {'min_age': 18}
```

**Supported Types:**
- `str` - String values
- `int` - Integer values
- `float` - Float values
- `bool` - Boolean values (True/False)
- `None` - NULL values

### `render(rowset)`

Render a RowSet to PostgreSQL SQL.

```python
result = render(query)
print(result.query)   # SQL string
print(result.params)  # Parameters dict
```

**Parameters:**
- `rowset` (RowSet) - Query to render

**Returns:** SQL dataclass with `query` (str) and `params` (dict)

**Example:**
```python
from vw.postgres as vw, col, param, render, F

users = source("users")
query = (
    users
    .select(
        col("id"),
        col("name"),
        col("email")
    )
    .where(col("age") >= param("min_age", 18))
    .where(col("status") == param("status", "active"))
    .order_by(col("name").asc())
    .limit(10)
)

result = render(query)

print(result.query)
# SELECT id, name, email FROM users
# WHERE age >= $min_age AND status = $status
# ORDER BY name ASC
# LIMIT 10

print(result.params)
# {'min_age': 18, 'status': 'active'}
```

## Functions

Import the Functions instance as `F`:

```python
from vw.postgres import F
```

All functions from `vw.core.functions` are available. See [Core API - Functions](core.md#functions) for full reference.

**Quick Reference:**

```python
# Aggregates
F.count()                    # COUNT(*)
F.count(col("id"))           # COUNT(id)
F.sum(col("amount"))         # SUM(amount)
F.avg(col("price"))          # AVG(price)
F.min(col("price"))          # MIN(price)
F.max(col("price"))          # MAX(price)

# Window functions
F.row_number().over(...)     # ROW_NUMBER() OVER (...)
F.rank().over(...)           # RANK() OVER (...)
F.dense_rank().over(...)     # DENSE_RANK() OVER (...)
F.ntile(4).over(...)         # NTILE(4) OVER (...)
F.lag(col("x")).over(...)    # LAG(x) OVER (...)
F.lead(col("x")).over(...)   # LEAD(x) OVER (...)
```

## Set Operations

PostgreSQL supports set operations for combining multiple queries. vw provides operator overloading for intuitive composition.

### Operators

#### `|` - UNION (deduplicates)

Combines results from two queries and removes duplicates.

```python
from vw.postgres as vw, col, render

users = source("users").select(col("id"))
admins = source("admins").select(col("id"))

result = render(users | admins)
print(result.query)
# (SELECT id FROM users) UNION (SELECT id FROM admins)
```

#### `+` - UNION ALL (keeps duplicates)

Combines results from two queries and keeps all rows including duplicates.

```python
users = source("users").select(col("id"))
admins = source("admins").select(col("id"))

result = render(users + admins)
print(result.query)
# (SELECT id FROM users) UNION ALL (SELECT id FROM admins)
```

#### `&` - INTERSECT

Returns only rows that appear in both queries.

```python
users = source("users").select(col("id"))
banned = source("banned").select(col("user_id"))

result = render(users & banned)
print(result.query)
# (SELECT id FROM users) INTERSECT (SELECT user_id FROM banned)
```

#### `-` - EXCEPT

Returns rows from the left query that don't appear in the right query.

```python
users = source("users").select(col("id"))
banned = source("banned").select(col("user_id"))

result = render(users - banned)
print(result.query)
# (SELECT id FROM users) EXCEPT (SELECT user_id FROM banned)
```

### Chaining

Set operations can be chained. Parentheses control precedence.

```python
users = source("users").select(col("id"))
admins = source("admins").select(col("id"))
banned = source("banned").select(col("user_id"))

# (users UNION admins) EXCEPT banned
result = render((users | admins) - banned)
print(result.query)
# ((SELECT id FROM users) UNION (SELECT id FROM admins)) EXCEPT (SELECT user_id FROM banned)

# users UNION (admins EXCEPT banned)
result = render(users | (admins - banned))
print(result.query)
# (SELECT id FROM users) UNION ((SELECT id FROM admins) EXCEPT (SELECT user_id FROM banned))
```

### Parameter Preservation

Parameters are collected from all queries in a set operation.

```python
from vw.postgres import param

active_users = source("users").select(col("id")).where(col("active") == param("active", True))
admin_users = source("admins").select(col("id")).where(col("role") == param("role", "admin"))

result = render(active_users | admin_users)
print(result.query)
# (SELECT id FROM users WHERE active = $active) UNION (SELECT id FROM admins WHERE role = $role)

print(result.params)
# {'active': True, 'role': 'admin'}
```

### Notes

- Both sides of a set operation must have the same number of columns
- Column names come from the left query
- Set operations can be used anywhere a query is expected (as subquery, with ORDER BY/LIMIT, etc.)
- Bare table references (Source) are automatically wrapped as `SELECT * FROM table`

## Parameter Style

PostgreSQL rendering uses **dollar-style** parameters by default:

```python
param("user_id", 123)
# Renders as: $user_id
# Params: {'user_id': 123}
```

This is compatible with:
- SQLAlchemy's `text()` with PostgreSQL dialect
- asyncpg
- psycopg3 (with dollar parameter support)

For psycopg2, you may need to convert parameter style or use SQLAlchemy.

## Complete Examples

### Basic Query

```python
from vw.postgres as vw, col, param, render

users = source("users")
query = (
    users
    .select(col("id"), col("name"), col("email"))
    .where(col("active") == param("active", True))
    .order_by(col("name").asc())
    .limit(10)
)

result = render(query)
```

### Aggregation

```python
from vw.postgres as vw, col, param, render, F

orders = source("orders")
query = (
    orders
    .select(
        col("customer_id"),
        F.count().alias("order_count"),
        F.sum(col("amount")).alias("total_spent"),
        F.avg(col("amount")).alias("avg_order")
    )
    .where(col("created_at") >= param("start_date", "2024-01-01"))
    .group_by(col("customer_id"))
    .having(F.sum(col("amount")) > param("min_total", 1000))
    .order_by(F.sum(col("amount")).desc())
)

result = render(query)
```

### Window Functions

```python
from vw.postgres as vw, col, render, F

sales = source("sales")
query = sales.select(
    col("product_id"),
    col("sale_date"),
    col("amount"),
    F.row_number().over(
        partition_by=[col("product_id")],
        order_by=[col("sale_date").desc()]
    ).alias("sale_rank"),
    F.sum(col("amount")).over(
        partition_by=[col("product_id")],
        order_by=[col("sale_date").asc()]
    ).alias("running_total")
)

result = render(query)
```

### Subquery

```python
from vw.postgres as vw, col, param, render, F

users = source("users")
active_users = (
    users
    .select(col("id"), col("name"))
    .where(col("status") == param("status", "active"))
    .alias("active_users")
)

query = active_users.select(
    col("id"),
    col("name")
).limit(10)

result = render(query)
# SELECT id, name FROM (
#   SELECT id, name FROM users WHERE status = $status
# ) AS active_users
# LIMIT 10
```

### Joins

```python
from vw.postgres as vw, col, param, render, F

# Basic INNER JOIN
users = source("users").alias("u")
orders = source("orders").alias("o")

query = (
    users
    .join.inner(orders, on=[users.col("id") == orders.col("user_id")])
    .select(
        users.col("id"),
        users.col("name"),
        F.count().alias("order_count")
    )
    .group_by(users.col("id"), users.col("name"))
)

result = render(query)
# SELECT u.id, u.name, COUNT(*) AS order_count
# FROM users AS u
# INNER JOIN orders AS o ON (u.id = o.user_id)
# GROUP BY u.id, u.name

# Multiple joins
users = source("users").alias("u")
orders = source("orders").alias("o")
products = source("products").alias("p")

query = (
    users
    .join.inner(orders, on=[users.col("id") == orders.col("user_id")])
    .join.left(products, on=[orders.col("product_id") == products.col("id")])
    .select(
        users.col("name"),
        orders.col("total"),
        products.col("name").alias("product_name")
    )
    .where(orders.col("status") == param("status", "completed"))
)

result = render(query)
# SELECT u.name, o.total, p.name AS product_name
# FROM users AS u
# INNER JOIN orders AS o ON (u.id = o.user_id)
# LEFT JOIN products AS p ON (o.product_id = p.id)
# WHERE o.status = $status

# JOIN with USING clause
users = source("users").alias("u")
orders = source("orders").alias("o")

query = (
    users
    .join.inner(orders, using=[col("user_id")])
    .select(col("user_id"), users.col("name"), F.count().alias("order_count"))
    .group_by(col("user_id"), users.col("name"))
)

result = render(query)
# SELECT user_id, u.name, COUNT(*) AS order_count
# FROM users AS u
# INNER JOIN orders AS o USING (user_id)
# GROUP BY user_id, u.name
```

### Complex Expressions

```python
from vw.postgres as vw, col, param, render, F

orders = source("orders")
query = orders.select(
    col("id"),
    col("subtotal"),
    col("tax"),
    (col("subtotal") + col("tax")).alias("total"),
    ((col("discount") / col("subtotal")) * 100).alias("discount_pct"),
    F.count().filter(col("status") == "completed").alias("completed_count")
)

result = render(query)
```

## Using with SQLAlchemy

```python
from sqlalchemy import create_engine, text
from vw.postgres as vw, col, param, render, F

# Setup
engine = create_engine("postgresql://user:pass@localhost/mydb")

# Build query
users = source("users")
query = users.select(col("id"), col("name")).where(
    col("age") >= param("min_age", 18)
)

# Render
result = render(query)

# Execute
with engine.connect() as conn:
    rows = conn.execute(text(result.query), result.params)
    for row in rows:
        print(row)
```

## Using with asyncpg

```python
import asyncpg
from vw.postgres as vw, col, param, render

async def fetch_users():
    # Connect
    conn = await asyncpg.connect('postgresql://user:pass@localhost/mydb')

    # Build query
    users = source("users")
    query = users.select(col("id"), col("name")).where(
        col("age") >= param("min_age", 18)
    )
    result = render(query)

    # Execute (asyncpg uses $1, $2 style - may need adjustment)
    rows = await conn.fetch(result.query, *result.params.values())

    await conn.close()
    return rows
```

## Feature Status

See [PostgreSQL Parity](../development/postgres-parity.md) for detailed feature roadmap.

**Completed (✅):**
- Core query building (SELECT, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET, DISTINCT)
- Column references (qualified/unqualified, star, aliasing)
- Operators (comparison, arithmetic, logical, pattern matching, NULL checks)
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- Window functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE)
- Window frames (ROWS/RANGE BETWEEN, frame exclusion)
- FILTER clause
- Joins (INNER, LEFT, RIGHT, FULL OUTER, CROSS)
- Parameters and rendering

**In Progress (🚧):**
- None

**Planned (📋):**
- Subqueries and CTEs
- Set operations (UNION, INTERSECT, EXCEPT)
- Scalar functions (string, datetime, null handling)
- DML statements (INSERT, UPDATE, DELETE)
- DDL statements (CREATE TABLE, CREATE VIEW)
- PostgreSQL-specific features (DISTINCT ON, JSONB, arrays)

## Next Steps

- **[Core API](core.md)** - Detailed core API reference
- **[PostgreSQL Parity](../development/postgres-parity.md)** - Feature roadmap
- **[Quickstart](../quickstart.md)** - More examples
