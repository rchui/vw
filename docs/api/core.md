# Core API Reference

The `vw/core` module provides dialect-agnostic abstractions for building SQL queries.

## Overview

The core API consists of:
- **Expression** - Wrapper for all expression states (columns, operators, functions)
- **RowSet** - Wrapper for sources and statements (tables, queries)
- **Functions** - Factory for aggregate and window functions
- **Frame** - Window frame boundaries for window functions

## Expression

The `Expression` class wraps expression states and provides operators and methods for building SQL expressions.

### Comparison Operators

| Operator | Method | SQL | Example |
|----------|--------|-----|---------|
| `==` | `__eq__` | `=` | `col("age") == 18` |
| `!=` | `__ne__` | `<>` | `col("status") != "deleted"` |
| `<` | `__lt__` | `<` | `col("age") < 65` |
| `<=` | `__le__` | `<=` | `col("age") <= 65` |
| `>` | `__gt__` | `>` | `col("price") > 100` |
| `>=` | `__ge__` | `>=` | `col("price") >= 100` |

### Arithmetic Operators

| Operator | Method | SQL | Example |
|----------|--------|-----|---------|
| `+` | `__add__` | `+` | `col("a") + col("b")` |
| `-` | `__sub__` | `-` | `col("total") - col("discount")` |
| `*` | `__mul__` | `*` | `col("price") * col("quantity")` |
| `/` | `__truediv__` | `/` | `col("total") / col("count")` |
| `%` | `__mod__` | `%` | `col("n") % 2` |

### Logical Operators

| Operator | Method | SQL | Example |
|----------|--------|-----|---------|
| `&` | `__and__` | `AND` | `(col("age") >= 18) & (col("verified") == True)` |
| `\|` | `__or__` | `OR` | `(col("status") == "active") \| (col("status") == "pending")` |
| `~` | `__invert__` | `NOT` | `~col("deleted").is_null()` |

**Note:** Always use parentheses with logical operators due to Python operator precedence:
```python
# Good
(col("age") >= 18) & (col("verified") == True)

# Bad (won't work as expected)
col("age") >= 18 & col("verified") == True
```

### Pattern Matching

#### `.like(pattern)`
Create a LIKE pattern match expression.

```python
col("email").like("%@example.com")
# SQL: email LIKE '%@example.com'
```

#### `.not_like(pattern)`
Create a NOT LIKE pattern match expression.

```python
col("email").not_like("%@spam.com")
# SQL: email NOT LIKE '%@spam.com'
```

#### `.is_in(*values)`
Create an IN expression.

```python
col("status").is_in("active", "pending", "approved")
# SQL: status IN ('active', 'pending', 'approved')
```

#### `.is_not_in(*values)`
Create a NOT IN expression.

```python
col("status").is_not_in("deleted", "banned")
# SQL: status NOT IN ('deleted', 'banned')
```

#### `.between(lower, upper)`
Create a BETWEEN expression.

```python
col("age").between(18, 65)
# SQL: age BETWEEN 18 AND 65
```

#### `.not_between(lower, upper)`
Create a NOT BETWEEN expression.

```python
col("age").not_between(0, 17)
# SQL: age NOT BETWEEN 0 AND 17
```

### NULL Checks

#### `.is_null()`
Create an IS NULL check.

```python
col("deleted_at").is_null()
# SQL: deleted_at IS NULL
```

#### `.is_not_null()`
Create an IS NOT NULL check.

```python
col("email").is_not_null()
# SQL: email IS NOT NULL
```

### Expression Modifiers

#### `.alias(name)`
Alias an expression.

```python
col("first_name").alias("name")
# SQL: first_name AS name

(col("price") * col("quantity")).alias("total")
# SQL: (price * quantity) AS total
```

#### `.cast(data_type)`
Cast expression to a data type.

```python
col("age").cast("INTEGER")
# SQL: CAST(age AS INTEGER)
```

#### `.asc()`
Mark expression for ascending sort order.

```python
col("name").asc()
# SQL: name ASC
```

#### `.desc()`
Mark expression for descending sort order.

```python
col("created_at").desc()
# SQL: created_at DESC
```

### Window Function Methods

#### `.over(*, partition_by=None, order_by=None)`
Convert function to window function with OVER clause.

```python
F.row_number().over(
    partition_by=[col("category")],
    order_by=[col("price").desc()]
)
# SQL: ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC)
```

**Parameters:**
- `partition_by` (list[Expression] | None) - Expressions to partition by
- `order_by` (list[Expression] | None) - Expressions to order by

#### `.filter(condition)`
Add FILTER (WHERE ...) clause to aggregate function.

```python
F.count().filter(col("status") == "completed")
# SQL: COUNT(*) FILTER (WHERE status = 'completed')
```

**Parameters:**
- `condition` (Expression) - Filter condition

#### `.rows_between(start, end)`
Add ROWS BETWEEN frame clause to window function.

```python
from vw.core.frame import UNBOUNDED_PRECEDING, CURRENT_ROW

F.sum(col("amount")).over(
    order_by=[col("date").asc()]
).rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
# SQL: SUM(amount) OVER (ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
```

**Parameters:**
- `start` (FrameBoundary) - Start frame boundary
- `end` (FrameBoundary) - End frame boundary

#### `.range_between(start, end)`
Add RANGE BETWEEN frame clause to window function.

```python
from vw.core.frame import UNBOUNDED_PRECEDING, CURRENT_ROW

F.avg(col("price")).over(
    order_by=[col("date").asc()]
).range_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
# SQL: AVG(price) OVER (ORDER BY date ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
```

**Parameters:**
- `start` (FrameBoundary) - Start frame boundary
- `end` (FrameBoundary) - End frame boundary

#### `.exclude(mode)`
Add EXCLUDE clause to window frame.

```python
F.avg(col("amount")).over(
    order_by=[col("id").asc()]
).rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW).exclude("CURRENT ROW")
# SQL: AVG(amount) OVER (ORDER BY id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW)
```

**Parameters:**
- `mode` (str) - Exclude mode: "CURRENT ROW", "GROUP", "TIES", or "NO OTHERS"

---

## RowSet

The `RowSet` class wraps Source or Statement states and provides query building methods.

### `.select(*columns)`
Add columns to SELECT clause.

```python
users.select(col("id"), col("name"), col("email"))
# SQL: SELECT id, name, email FROM users
```

**Parameters:**
- `*columns` (Expression) - Column expressions to select

**Returns:** New RowSet with columns added

**Note:** Transforms Source to Statement if needed. Multiple calls accumulate columns.

### `.where(*conditions)`
Add WHERE clause conditions.

```python
users.where(col("age") >= 18)
# SQL: SELECT * FROM users WHERE age >= 18

users.where(col("age") >= 18).where(col("verified") == True)
# SQL: SELECT * FROM users WHERE age >= 18 AND verified = TRUE
```

**Parameters:**
- `*conditions` (Expression) - Filter conditions

**Returns:** New RowSet with WHERE conditions added

**Note:** Multiple calls accumulate conditions (combined with AND).

### `.group_by(*columns)`
Add GROUP BY clause.

```python
orders.select(
    col("customer_id"),
    F.count().alias("order_count")
).group_by(col("customer_id"))
# SQL: SELECT customer_id, COUNT(*) AS order_count FROM orders GROUP BY customer_id
```

**Parameters:**
- `*columns` (Expression) - Grouping expressions

**Returns:** New RowSet with GROUP BY set

**Note:** Multiple calls replace previous GROUP BY (last wins).

### `.having(*conditions)`
Add HAVING clause conditions.

```python
orders.select(
    col("customer_id"),
    F.count().alias("cnt")
).group_by(col("customer_id")).having(F.count() > 10)
# SQL: SELECT customer_id, COUNT(*) AS cnt FROM orders GROUP BY customer_id HAVING COUNT(*) > 10
```

**Parameters:**
- `*conditions` (Expression) - Filter conditions for groups

**Returns:** New RowSet with HAVING conditions added

**Note:** Multiple calls accumulate conditions (combined with AND).

### `.order_by(*columns)`
Add ORDER BY clause.

```python
users.select(col("*")).order_by(col("name").asc())
# SQL: SELECT * FROM users ORDER BY name ASC

users.select(col("*")).order_by(col("last_name").asc(), col("first_name").asc())
# SQL: SELECT * FROM users ORDER BY last_name ASC, first_name ASC
```

**Parameters:**
- `*columns` (Expression) - Sort expressions (use `.asc()` or `.desc()`)

**Returns:** New RowSet with ORDER BY set

**Note:** Multiple calls replace previous ORDER BY (last wins).

### `.limit(count, *, offset=None)`
Add LIMIT and optional OFFSET clause.

```python
users.select(col("*")).limit(10)
# SQL: SELECT * FROM users LIMIT 10

users.select(col("*")).limit(10, offset=20)
# SQL: SELECT * FROM users LIMIT 10 OFFSET 20
```

**Parameters:**
- `count` (int) - Maximum number of rows
- `offset` (int | None) - Number of rows to skip (optional)

**Returns:** New RowSet with LIMIT set

**Note:** Multiple calls replace previous LIMIT (last wins).

### `.distinct()`
Add DISTINCT clause to remove duplicate rows.

```python
orders.select(col("customer_id")).distinct()
# SQL: SELECT DISTINCT customer_id FROM orders
```

**Returns:** New RowSet with DISTINCT set

### `.alias(name)`
Alias this rowset.

```python
# Table alias
users = source("users").alias("u")
users.select(users.col("id"))
# SQL: SELECT u.id FROM users AS u

# Subquery alias
active_users = (
    source("users")
    .select(col("id"), col("name"))
    .where(col("status") == "active")
    .alias("active_users")
)
```

**Parameters:**
- `name` (str) - Alias name

**Returns:** New RowSet with alias set

### `.col(name)`
Create a column reference qualified with this rowset's alias.

```python
users = source("users").alias("u")
users.col("id")
# SQL: u.id
```

**Parameters:**
- `name` (str) - Column name

**Returns:** Expression with qualified column

### `.star` (property)
Create a star expression qualified with this rowset's alias.

```python
users = source("users").alias("u")
users.star
# SQL: u.*
```

**Returns:** Expression with qualified star

### `.join` (property)
Access join methods through the JoinAccessor.

```python
users = source("users").alias("u")
orders = source("orders").alias("o")

query = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
# SQL: SELECT * FROM users AS u INNER JOIN orders AS o ON (u.id = o.user_id)
```

**Returns:** JoinAccessor instance

### `.join.inner(right, *, on=None, using=None)`
Create an INNER JOIN.

```python
users = source("users").alias("u")
orders = source("orders").alias("o")

# Single condition
query = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
# SQL: INNER JOIN orders AS o ON (u.id = o.user_id)

# Multiple conditions (AND-combined)
query = users.join.inner(
    orders,
    on=[
        users.col("id") == orders.col("user_id"),
        orders.col("status") == "active"
    ]
)
# SQL: INNER JOIN orders AS o ON (u.id = o.user_id AND o.status = 'active')

# USING clause
query = users.join.inner(orders, using=[col("user_id")])
# SQL: INNER JOIN orders AS o USING (user_id)
```

**Parameters:**
- `right` (RowSet) - Right side of the join
- `on` (list[Expression] | None) - ON clause conditions (optional)
- `using` (list[Expression] | None) - USING clause columns (optional)

**Returns:** New RowSet with join added

**Note:** Multiple calls accumulate joins in order.

### `.join.left(right, *, on=None, using=None)`
Create a LEFT JOIN (LEFT OUTER JOIN).

```python
users = source("users").alias("u")
orders = source("orders").alias("o")

query = users.join.left(orders, on=[users.col("id") == orders.col("user_id")])
# SQL: LEFT JOIN orders AS o ON (u.id = o.user_id)
```

**Parameters:**
- `right` (RowSet) - Right side of the join
- `on` (list[Expression] | None) - ON clause conditions (optional)
- `using` (list[Expression] | None) - USING clause columns (optional)

**Returns:** New RowSet with join added

### `.join.right(right, *, on=None, using=None)`
Create a RIGHT JOIN (RIGHT OUTER JOIN).

```python
users = source("users").alias("u")
orders = source("orders").alias("o")

query = orders.join.right(users, on=[orders.col("user_id") == users.col("id")])
# SQL: RIGHT JOIN users AS u ON (o.user_id = u.id)
```

**Parameters:**
- `right` (RowSet) - Right side of the join
- `on` (list[Expression] | None) - ON clause conditions (optional)
- `using` (list[Expression] | None) - USING clause columns (optional)

**Returns:** New RowSet with join added

### `.join.full_outer(right, *, on=None, using=None)`
Create a FULL OUTER JOIN.

```python
users = source("users").alias("u")
orders = source("orders").alias("o")

query = users.join.full_outer(orders, on=[users.col("id") == orders.col("user_id")])
# SQL: FULL JOIN orders AS o ON (u.id = o.user_id)
```

**Parameters:**
- `right` (RowSet) - Right side of the join
- `on` (list[Expression] | None) - ON clause conditions (optional)
- `using` (list[Expression] | None) - USING clause columns (optional)

**Returns:** New RowSet with join added

### `.join.cross(right)`
Create a CROSS JOIN (Cartesian product).

```python
users = source("users").alias("u")
tags = source("tags").alias("t")

query = users.join.cross(tags)
# SQL: CROSS JOIN tags AS t
```

**Parameters:**
- `right` (RowSet) - Right side of the join

**Returns:** New RowSet with join added

**Note:** CROSS JOIN does not support ON or USING clauses.

---

## Functions

The `Functions` class provides factory methods for aggregate and window functions.

Import as `F`:
```python
from vw.postgres import F
```

### Aggregate Functions

#### `F.count(expr=None, *, distinct=False)`
Create a COUNT aggregate function.

```python
F.count()
# SQL: COUNT(*)

F.count(col("id"))
# SQL: COUNT(id)

F.count(col("email"), distinct=True)
# SQL: COUNT(DISTINCT email)
```

**Parameters:**
- `expr` (Expression | None) - Expression to count (None for COUNT(*))
- `distinct` (bool) - Whether to count distinct values

**Returns:** Expression wrapping Function state

#### `F.sum(expr)`
Create a SUM aggregate function.

```python
F.sum(col("amount"))
# SQL: SUM(amount)
```

**Parameters:**
- `expr` (Expression) - Expression to sum

**Returns:** Expression wrapping Function state

#### `F.avg(expr)`
Create an AVG aggregate function.

```python
F.avg(col("price"))
# SQL: AVG(price)
```

**Parameters:**
- `expr` (Expression) - Expression to average

**Returns:** Expression wrapping Function state

#### `F.min(expr)`
Create a MIN aggregate function.

```python
F.min(col("price"))
# SQL: MIN(price)
```

**Parameters:**
- `expr` (Expression) - Expression to find minimum

**Returns:** Expression wrapping Function state

#### `F.max(expr)`
Create a MAX aggregate function.

```python
F.max(col("price"))
# SQL: MAX(price)
```

**Parameters:**
- `expr` (Expression) - Expression to find maximum

**Returns:** Expression wrapping Function state

### Window Functions

All window functions must be used with `.over()` to add a window specification.

#### `F.row_number()`
Create a ROW_NUMBER window function.

```python
F.row_number().over(order_by=[col("created_at").desc()])
# SQL: ROW_NUMBER() OVER (ORDER BY created_at DESC)
```

**Returns:** Expression wrapping Function state

#### `F.rank()`
Create a RANK window function.

```python
F.rank().over(
    partition_by=[col("category")],
    order_by=[col("price").desc()]
)
# SQL: RANK() OVER (PARTITION BY category ORDER BY price DESC)
```

**Returns:** Expression wrapping Function state

#### `F.dense_rank()`
Create a DENSE_RANK window function.

```python
F.dense_rank().over(order_by=[col("score").desc()])
# SQL: DENSE_RANK() OVER (ORDER BY score DESC)
```

**Returns:** Expression wrapping Function state

#### `F.ntile(n)`
Create an NTILE window function.

```python
F.ntile(4).over(order_by=[col("salary").desc()])
# SQL: NTILE(4) OVER (ORDER BY salary DESC)
```

**Parameters:**
- `n` (int) - Number of buckets

**Returns:** Expression wrapping Function state

#### `F.lag(expr, offset=1, default=None)`
Create a LAG window function (requires ORDER BY).

```python
F.lag(col("price")).over(order_by=[col("date").asc()])
# SQL: LAG(price) OVER (ORDER BY date ASC)

F.lag(col("price"), offset=3).over(order_by=[col("date").asc()])
# SQL: LAG(price, 3) OVER (ORDER BY date ASC)
```

**Parameters:**
- `expr` (Expression) - Expression to get previous value of
- `offset` (int) - Number of rows back (default 1)
- `default` (Expression | None) - Default value when no previous row

**Returns:** Expression wrapping Function state

#### `F.lead(expr, offset=1, default=None)`
Create a LEAD window function (requires ORDER BY).

```python
F.lead(col("price")).over(order_by=[col("date").asc()])
# SQL: LEAD(price) OVER (ORDER BY date ASC)
```

**Parameters:**
- `expr` (Expression) - Expression to get next value of
- `offset` (int) - Number of rows forward (default 1)
- `default` (Expression | None) - Default value when no next row

**Returns:** Expression wrapping Function state

#### `F.first_value(expr)`
Create a FIRST_VALUE window function (requires ORDER BY).

```python
F.first_value(col("price")).over(
    partition_by=[col("product_id")],
    order_by=[col("date").asc()]
)
# SQL: FIRST_VALUE(price) OVER (PARTITION BY product_id ORDER BY date ASC)
```

**Parameters:**
- `expr` (Expression) - Expression to get first value of

**Returns:** Expression wrapping Function state

#### `F.last_value(expr)`
Create a LAST_VALUE window function (requires ORDER BY).

```python
F.last_value(col("price")).over(
    partition_by=[col("product_id")],
    order_by=[col("date").asc()]
)
# SQL: LAST_VALUE(price) OVER (PARTITION BY product_id ORDER BY date ASC)
```

**Parameters:**
- `expr` (Expression) - Expression to get last value of

**Returns:** Expression wrapping Function state

---

## Frame Boundaries

Frame boundaries for window function frame clauses.

### Constants

#### `UNBOUNDED_PRECEDING`
The start of the partition.

```python
from vw.core.frame import UNBOUNDED_PRECEDING

F.sum(col("amount")).over(
    order_by=[col("date").asc()]
).rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
```

#### `UNBOUNDED_FOLLOWING`
The end of the partition.

```python
from vw.core.frame import UNBOUNDED_FOLLOWING

F.sum(col("amount")).over(
    order_by=[col("date").asc()]
).rows_between(CURRENT_ROW, UNBOUNDED_FOLLOWING)
```

#### `CURRENT_ROW`
The current row.

```python
from vw.core.frame import CURRENT_ROW

F.avg(col("price")).over(
    order_by=[col("date").asc()]
).rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
```

### Helper Functions

#### `preceding(n)`
Create an n PRECEDING frame boundary.

```python
from vw.core.frame import preceding, CURRENT_ROW

F.avg(col("price")).over(
    order_by=[col("date").asc()]
).rows_between(preceding(3), CURRENT_ROW)
# SQL: AVG(price) OVER (ORDER BY date ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
```

**Parameters:**
- `n` (int) - Number of rows preceding

**Returns:** Preceding boundary

#### `following(n)`
Create an n FOLLOWING frame boundary.

```python
from vw.core.frame import CURRENT_ROW, following

F.avg(col("price")).over(
    order_by=[col("date").asc()]
).rows_between(CURRENT_ROW, following(3))
# SQL: AVG(price) OVER (ORDER BY date ASC ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING)
```

**Parameters:**
- `n` (int) - Number of rows following

**Returns:** Following boundary

---

## Next Steps

- **[PostgreSQL API](postgres.md)** - PostgreSQL-specific API
- **[DuckDB API](duckdb.md)** - DuckDB-specific API
