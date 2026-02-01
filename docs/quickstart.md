# Quickstart

This guide shows practical examples of building SQL queries with vw.

## Basic SELECT Query

```python
from vw.postgres import source, col, render

users = source("users")
query = users.select(col("id"), col("name"), col("email"))

result = render(query)
print(result.query)
# SELECT id, name, email FROM users
```

## SELECT with WHERE

```python
from vw.postgres import source, col, param, render

users = source("users")
query = (
    users
    .select(col("id"), col("name"))
    .where(col("age") >= param("min_age", 18))
    .where(col("status") == param("status", "active"))
)

result = render(query)
print(result.query)
# SELECT id, name FROM users WHERE age >= $min_age AND status = $status

print(result.params)
# {'min_age': 18, 'status': 'active'}
```

## Complex WHERE Conditions

```python
from vw.postgres import source, col, param, render

users = source("users")

# AND conditions (using &)
query = users.select(col("*")).where(
    (col("age") >= 18) & (col("verified") == True)
)

# OR conditions (using |)
query = users.select(col("*")).where(
    (col("status") == "active") | (col("status") == "pending")
)

# NOT conditions (using ~)
query = users.select(col("*")).where(
    ~col("deleted").is_null()
)

# Complex combinations
query = users.select(col("*")).where(
    ((col("age") >= 18) & (col("verified") == True)) |
    (col("admin") == True)
)
```

## Pattern Matching

```python
from vw.postgres import source, col, render

users = source("users")

# LIKE
query = users.select(col("email")).where(
    col("email").like("%@example.com")
)

# IN
query = users.select(col("name")).where(
    col("id").is_in(1, 2, 3, 4, 5)
)

# BETWEEN
query = users.select(col("name")).where(
    col("age").between(18, 65)
)

# NULL checks
query = users.select(col("*")).where(
    col("deleted_at").is_null()
)
```

## Arithmetic Operations

```python
from vw.postgres import source, col, render, F

orders = source("orders")

# Calculate total with tax
query = orders.select(
    col("id"),
    (col("subtotal") + col("tax")).alias("total")
)

# Percentage calculation
query = orders.select(
    col("id"),
    ((col("discount") / col("subtotal")) * 100).alias("discount_pct")
)
```

## Aggregation

```python
from vw.postgres import source, col, render, F

orders = source("orders")

# Simple aggregation
query = orders.select(
    F.count().alias("total_orders"),
    F.sum(col("amount")).alias("total_revenue"),
    F.avg(col("amount")).alias("avg_order_value")
)

# Aggregation with GROUP BY
query = (
    orders
    .select(
        col("customer_id"),
        F.count().alias("order_count"),
        F.sum(col("amount")).alias("total_spent")
    )
    .group_by(col("customer_id"))
)

# Aggregation with HAVING
query = (
    orders
    .select(
        col("customer_id"),
        F.sum(col("amount")).alias("total_spent")
    )
    .group_by(col("customer_id"))
    .having(F.sum(col("amount")) > param("min_spent", 1000))
)
```

## Window Functions

```python
from vw.postgres import source, col, render, F

sales = source("sales")

# Row number within partition
query = sales.select(
    col("product_id"),
    col("sale_date"),
    col("amount"),
    F.row_number().over(
        partition_by=[col("product_id")],
        order_by=[col("sale_date").desc()]
    ).alias("sale_rank")
)

# Running total
query = sales.select(
    col("sale_date"),
    col("amount"),
    F.sum(col("amount")).over(
        order_by=[col("sale_date").asc()]
    ).alias("running_total")
)

# Moving average
from vw.core.frame import UNBOUNDED_PRECEDING, CURRENT_ROW

query = sales.select(
    col("sale_date"),
    col("amount"),
    F.avg(col("amount")).over(
        order_by=[col("sale_date").asc()]
    ).rows_between(
        UNBOUNDED_PRECEDING,
        CURRENT_ROW
    ).alias("cumulative_avg")
)
```

## Sorting and Limiting

```python
from vw.postgres import source, col, render

users = source("users")

# ORDER BY
query = (
    users
    .select(col("name"), col("created_at"))
    .order_by(col("created_at").desc())
)

# LIMIT and OFFSET
query = (
    users
    .select(col("*"))
    .order_by(col("name").asc())
    .limit(10)  # First 10 results
)

query = (
    users
    .select(col("*"))
    .order_by(col("name").asc())
    .limit(10, offset=20)  # Results 21-30
)
```

## Distinct

```python
from vw.postgres import source, col, render

orders = source("orders")

# DISTINCT
query = (
    orders
    .select(col("customer_id"))
    .distinct()
)

# DISTINCT with multiple columns
query = (
    orders
    .select(col("customer_id"), col("product_id"))
    .distinct()
)
```

## Aliasing

```python
from vw.postgres import source, col, render, F

# Column aliases
users = source("users")
query = users.select(
    col("id").alias("user_id"),
    col("name").alias("full_name"),
    col("email")
)

# Table aliases
users_alias = source("users").alias("u")
query = users_alias.select(
    users_alias.col("id"),  # Renders as u.id
    users_alias.col("name") # Renders as u.name
)

# Expression aliases
orders = source("orders")
query = orders.select(
    col("id"),
    (col("quantity") * col("price")).alias("total")
)
```

## Subqueries

```python
from vw.postgres import source, col, render, F

# Subquery in FROM
users = source("users")
active_users = (
    users
    .select(col("id"), col("name"))
    .where(col("status") == "active")
    .alias("active_users")
)

# Use subquery as source
query = active_users.select(col("name"))

result = render(query)
# SELECT name FROM (SELECT id, name FROM users WHERE status = 'active') AS active_users
```

## FILTER Clause

```python
from vw.postgres import source, col, param, render, F

orders = source("orders")

# Count with FILTER
query = orders.select(
    F.count().filter(col("status") == "completed").alias("completed_count"),
    F.count().filter(col("status") == "pending").alias("pending_count"),
    F.sum(col("amount")).filter(col("status") == "completed").alias("completed_revenue")
)

result = render(query)
# SELECT
#   COUNT(*) FILTER (WHERE status = 'completed') AS completed_count,
#   COUNT(*) FILTER (WHERE status = 'pending') AS pending_count,
#   SUM(amount) FILTER (WHERE status = 'completed') AS completed_revenue
# FROM orders
```

## Complete Example with SQLAlchemy

```python
from sqlalchemy import create_engine, text
from vw.postgres import source, col, param, render, F

# Setup
engine = create_engine("postgresql://user:pass@localhost/mydb")

# Build query
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
    .having(F.count() >= param("min_orders", 5))
    .order_by(F.sum(col("amount")).desc())
    .limit(10)
)

# Render
result = render(query)

# Execute
with engine.connect() as conn:
    rows = conn.execute(text(result.query), result.params)
    for row in rows:
        print(f"Customer {row.customer_id}: {row.order_count} orders, ${row.total_spent}")
```

## Next Steps

- **[Architecture](architecture.md)** - Understand how vw works
- **[API Reference](api/index.md)** - Detailed API documentation
- **[PostgreSQL API](api/postgres.md)** - PostgreSQL-specific features
