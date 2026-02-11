# Quickstart

Practical examples of building SQL queries with vw.

## Basic SELECT

```python
from vw.postgres import ref, col, render

result = render(ref("users").select(col("id"), col("name"), col("email")))
# SELECT id, name, email FROM users
```

## WHERE Conditions

```python
from vw.postgres import ref, col, param, render

result = render(
    ref("users")
    .select(col("id"), col("name"))
    .where(col("age") >= param("min_age", 18))
    .where(col("status") == param("status", "active"))
)
# SELECT id, name FROM users WHERE age >= $min_age AND status = $status
# params: {'min_age': 18, 'status': 'active'}
```

## Complex WHERE

```python
from vw.postgres import ref, col, param, render

users = ref("users")

# AND / OR / NOT
users.select(col("*")).where(
    (col("age") >= param("a", 18)) & (col("verified") == param("v", True))
)
users.select(col("*")).where(
    (col("status") == param("a", "active")) | (col("status") == param("p", "pending"))
)
users.select(col("*")).where(~col("deleted_at").is_null())
```

## Pattern Matching

```python
from vw.postgres import ref, col, param, render

users = ref("users")

users.select(col("email")).where(col("email").like(param("p", "%@example.com")))
users.select(col("name")).where(col("id").is_in(param("a", 1), param("b", 2), param("c", 3)))
users.select(col("name")).where(col("age").between(param("lo", 18), param("hi", 65)))
users.select(col("*")).where(col("deleted_at").is_null())
```

## Arithmetic

```python
from vw.postgres import ref, col, render

result = render(
    ref("orders").select(
        col("id"),
        (col("subtotal") + col("tax")).alias("total"),
        ((col("discount") / col("subtotal")) * col("hundred")).alias("discount_pct"),
    )
)
```

## Aggregation

```python
from vw.postgres import ref, col, param, render, F

result = render(
    ref("orders")
    .select(
        col("customer_id"),
        F.count().alias("order_count"),
        F.sum(col("amount")).alias("total_spent"),
    )
    .group_by(col("customer_id"))
    .having(F.sum(col("amount")) > param("min", 1000))
)
```

## Window Functions

```python
from vw.postgres import ref, col, render, F, UNBOUNDED_PRECEDING, CURRENT_ROW

result = render(
    ref("sales").select(
        col("sale_date"),
        col("amount"),
        F.row_number().over(
            partition_by=[col("product_id")],
            order_by=[col("sale_date").desc()]
        ).alias("rank"),
        F.sum(col("amount")).over(
            order_by=[col("sale_date").asc()]
        ).rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW).alias("running_total"),
    )
)
```

## Sorting and Limiting

```python
from vw.postgres import ref, col, render

result = render(
    ref("users")
    .select(col("*"))
    .order_by(col("name").asc(), col("id").desc())
    .offset(20)
    .limit(10)
)
# SELECT * FROM users ORDER BY name ASC, id DESC LIMIT 10 OFFSET 20
```

## Conditional Expressions (CASE)

```python
from vw.postgres import ref, col, param, render, when

# Searched CASE with ELSE
result = render(
    ref("users").select(
        col("id"),
        when(col("age") >= param("adult", 18)).then(param("a", "adult"))
        .when(col("age") >= param("teen", 13)).then(param("t", "teen"))
        .otherwise(param("c", "child"))
        .alias("age_group"),
    )
)
# SELECT id, CASE WHEN age >= $adult THEN $a WHEN age >= $teen THEN $t ELSE $c END AS age_group
# FROM users

# CASE without ELSE (NULL when no branch matches)
when(col("vip") == param("t", True)).then(param("label", "VIP")).end()
```

## Subqueries

```python
from vw.postgres import ref, col, param, render

active_users = (
    ref("users")
    .select(col("id"), col("name"))
    .where(col("status") == param("s", "active"))
    .alias("active_users")
)
result = render(active_users.select(col("name")).limit(10))
# SELECT name FROM (SELECT id, name FROM users WHERE status = $s) AS active_users LIMIT 10
```

## VALUES Clause

```python
from vw.postgres import values, col, render

# Inline row data as a named source
result = render(
    values("t", {"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"})
    .select(col("id"), col("name"))
)
# SELECT id, name FROM (VALUES ($1, $2), ($3, $4)) AS t(id, name)

# VALUES in a JOIN
users = ref("users")
allowed_ids = values("allowed", {"id": 1}, {"id": 2}, {"id": 3})
result = render(
    users.join.inner(allowed_ids, on=[col("users.id") == col("allowed.id")])
    .select(col("users.name"))
)
# SELECT users.name FROM users
# INNER JOIN (VALUES ($1), ($2), ($3)) AS allowed(id) ON (users.id = allowed.id)
```

## Set Operations

```python
from vw.postgres import ref, col, render

users = ref("users").select(col("id"))
admins = ref("admins").select(col("id"))

render(users | admins)   # UNION (deduplicates)
render(users + admins)   # UNION ALL (keep duplicates)
render(users & admins)   # INTERSECT
render(users - admins)   # EXCEPT
```

## Joins

```python
from vw.postgres import ref, col, param, render, F

u = ref("users").alias("u")
o = ref("orders").alias("o")

result = render(
    u.join.inner(o, on=[u.col("id") == o.col("user_id")])
    .select(u.col("name"), F.count().alias("order_count"))
    .where(o.col("status") == param("s", "completed"))
    .group_by(u.col("id"), u.col("name"))
)
# SELECT u.name, COUNT(*) AS order_count
# FROM users AS u
# INNER JOIN orders AS o ON (u.id = o.user_id)
# WHERE o.status = $s
# GROUP BY u.id, u.name
```

## LATERAL Joins

LATERAL joins allow correlated subqueries in the FROM clause. The right side can reference columns from the left side.

```python
from vw.postgres import ref, col, render

users = ref("users").alias("u")
orders = ref("orders")

# Get top 3 most recent orders per user
recent_orders = (
    orders
    .select(orders.col("total"), orders.col("created_at"))
    .where(orders.col("user_id") == users.col("id"))  # References users from outer query
    .order_by(orders.col("created_at").desc())
    .limit(3)
    .alias("recent")
)

result = render(
    users
    .join.left(recent_orders, on=[col("TRUE")], lateral=True)
    .select(users.col("name"), recent_orders.col("total"))
)
# SELECT u.name, recent.total
# FROM users AS u
# LEFT JOIN LATERAL (
#     SELECT total, created_at
#     FROM orders
#     WHERE user_id = u.id
#     ORDER BY created_at DESC
#     LIMIT 3
# ) AS recent ON (TRUE)
```

## CTEs

```python
from vw.postgres import ref, col, param, render, cte

active = cte("active", ref("users").select(col("*")).where(col("active") == param("t", True)))
result = render(active.select(col("id"), col("name")))
# WITH active AS (SELECT * FROM users WHERE active = $t)
# SELECT id, name FROM active
```

## Grouping Sets

Use `ROLLUP`, `CUBE`, and `GROUPING SETS` for multi-dimensional aggregation.

```python
from vw.postgres import ref, col, render, rollup, cube, grouping_sets, F

# ROLLUP — hierarchical subtotals
render(
    ref("sales")
    .select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
    .group_by(rollup(col("region"), col("product")))
)
# GROUP BY ROLLUP (region, product)

# CUBE — all combinations
render(
    ref("sales")
    .select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
    .group_by(cube(col("region"), col("product")))
)
# GROUP BY CUBE (region, product)

# GROUPING SETS — explicit combinations (use () for grand total)
render(
    ref("sales")
    .select(
        col("region"), col("product"),
        F.sum(col("amount")).alias("total"),
        F.grouping(col("region")).alias("is_region_total"),
    )
    .group_by(grouping_sets(
        (col("region"), col("product")),
        (col("region"),),
        (),
    ))
)
```

## FILTER Clause

```python
from vw.postgres import ref, col, param, render, F

result = render(
    ref("orders").select(
        F.count().filter(col("status") == param("c", "completed")).alias("completed"),
        F.count().filter(col("status") == param("p", "pending")).alias("pending"),
    )
)
```

## Using with SQLAlchemy

```python
from sqlalchemy import create_engine, text
from vw.postgres import ref, col, param, render

engine = create_engine("postgresql://user:pass@localhost/mydb")
result = render(ref("users").select(col("id")).where(col("age") >= param("min", 18)))

with engine.connect() as conn:
    rows = conn.execute(text(result.query), result.params)
    for row in rows:
        print(row)
```

## Unsupported SQL Features

If you need a SQL feature that vw doesn't support yet, use the `raw` namespace as an escape hatch:

```python
from vw.postgres import raw, col, param, ref, render

# Custom PostgreSQL function
query = ref("sales").select(
    raw.expr("percentile_cont({p}) WITHIN GROUP (ORDER BY {amt})",
             p=param("pct", 0.95),
             amt=col("amount")).alias("p95")
)

# Table function
series = raw.rowset("generate_series(1, {n}) AS t(num)", n=param("max", 10))
result = render(series.select(col("num")))
```

⚠️  **Warning**: Raw SQL bypasses vw's safety guarantees. Always use `param()` for user input, never f-strings. See [Raw SQL Escape Hatches](api/postgres.md#raw-sql-escape-hatches) for details.

## Next Steps

- **[Architecture](architecture.md)** - How vw works
- **[API Reference](api/core.md)** - Full API documentation
- **[PostgreSQL API](api/postgres.md)** - PostgreSQL-specific features
