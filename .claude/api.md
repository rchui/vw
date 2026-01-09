# API Reference

## Public API

All public exports from `vw/__init__.py`:

### Functions
- `col(name)` - Create a column reference
- `param(name, value)` - Create a parameterized value
- `cte(name, query)` - Create a Common Table Expression
- `exists(subquery)` - Create an EXISTS expression for a subquery
- `when(condition)` - Start a CASE expression (returns `When` object)

### Functions Module (`vw.functions`)
- `row_number()` - ROW_NUMBER() window function
- `rank()` - RANK() window function
- `dense_rank()` - DENSE_RANK() window function
- `ntile(n)` - NTILE(n) window function
- `sum_(expr)` - SUM() aggregate/window function
- `count(expr=None)` - COUNT() aggregate/window function (None for COUNT(*))
- `avg(expr)` - AVG() aggregate/window function
- `min_(expr)` - MIN() aggregate/window function
- `max_(expr)` - MAX() aggregate/window function
- `lag(expr, offset=1, default=None)` - LAG() window function
- `lead(expr, offset=1, default=None)` - LEAD() window function
- `first_value(expr)` - FIRST_VALUE() window function
- `last_value(expr)` - LAST_VALUE() window function

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
- `SetOperation` - Combined queries via UNION/INTERSECT/EXCEPT (extends Expression and RowSet)
- `Function` - SQL function (from `vw.functions`)
- `WindowFunction` - Window function with OVER clause (from `vw.functions`)
- `When` - Incomplete WHEN clause, call `.then()` to complete (from `vw.operators`)
- `CaseExpression` - Complete CASE expression (from `vw.operators`)
- `InnerJoin` - Inner join operation
- `RenderResult` - Rendering result with SQL and params
- `RenderConfig` - Rendering configuration
- `RenderContext` - Rendering context (for advanced use)
- `Dialect` - SQL dialect enum (controls parameter style and cast syntax)

### Operators (via Expression methods)
- `&` - Logical AND (`expr1 & expr2`)
- `|` - Logical OR (`expr1 | expr2`)
- `~` - Logical NOT (`~expr`)

### Set Operators (via Statement/SetOperation)
- `|` - UNION (`query1 | query2`)
- `+` - UNION ALL (`query1 + query2`)
- `&` - INTERSECT (`query1 & query2`)
- `-` - EXCEPT (`query1 - query2`)

### Expression Methods
- `.is_null()` - IS NULL check
- `.is_not_null()` - IS NOT NULL check
- `.is_in(*values)` - IN check (values or subquery)
- `.is_not_in(*values)` - NOT IN check (values or subquery)
- `.alias(name)` - Alias expression (AS name)
- `.cast(type)` - Type cast
- `.asc()` - Ascending sort order
- `.desc()` - Descending sort order

## Usage Examples

For comprehensive examples, see the integration tests in `tests/integration/`:
- `test_queries.py` - SELECT, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
- `test_joins.py` - JOIN operations and subqueries
- `test_expressions.py` - Cast, alias, parameters, null handling, IN/EXISTS
- `test_ctes.py` - Common Table Expressions
- `test_set_operations.py` - UNION, INTERSECT, EXCEPT
- `test_window_functions.py` - Window functions and aggregates
- `test_case_expressions.py` - CASE/WHEN expressions

### Basic SELECT

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

### NULL Handling

Use `.is_null()` and `.is_not_null()` methods:

```python
# IS NULL
result = (
    vw.Source(name="users")
    .select(vw.col("*"))
    .where(vw.col("deleted_at").is_null())
    .render()
)
# SELECT * FROM users WHERE (deleted_at IS NULL)

# IS NOT NULL
result = (
    vw.Source(name="users")
    .select(vw.col("*"))
    .where(vw.col("email").is_not_null())
    .render()
)
# SELECT * FROM users WHERE (email IS NOT NULL)
```

### IN / NOT IN

Use `.is_in()` and `.is_not_in()` with values or subqueries:

```python
# IN with literal values
result = (
    vw.Source(name="users")
    .select(vw.col("*"))
    .where(vw.col("status").is_in(vw.col("'active'"), vw.col("'pending'")))
    .render()
)
# SELECT * FROM users WHERE (status IN ('active', 'pending'))

# IN with subquery
orders_subquery = vw.Source(name="orders").select(vw.col("user_id"))
result = (
    vw.Source(name="users")
    .select(vw.col("*"))
    .where(vw.col("id").is_in(orders_subquery))
    .render()
)
# SELECT * FROM users WHERE (id IN (SELECT user_id FROM orders))

# NOT IN
result = (
    vw.Source(name="users")
    .select(vw.col("*"))
    .where(vw.col("status").is_not_in(vw.col("'deleted'"), vw.col("'archived'")))
    .render()
)
# SELECT * FROM users WHERE (status NOT IN ('deleted', 'archived'))
```

### EXISTS

Use `vw.exists()` for EXISTS subqueries:

```python
users = vw.Source(name="users")
orders = vw.Source(name="orders")

# EXISTS
exists_subquery = orders.select(vw.col("1")).where(orders.col("user_id") == users.col("id"))
result = (
    users.select(vw.col("*"))
    .where(vw.exists(exists_subquery))
    .render()
)
# SELECT * FROM users WHERE (EXISTS (SELECT 1 FROM orders WHERE (orders.user_id = users.id)))

# NOT EXISTS (use ~ operator)
result = (
    users.select(vw.col("*"))
    .where(~vw.exists(exists_subquery))
    .render()
)
# SELECT * FROM users WHERE (NOT (EXISTS (SELECT 1 FROM orders WHERE ...)))
```

### Set Operations (UNION / INTERSECT / EXCEPT)

Use Python operators to combine queries:

```python
query1 = vw.Source(name="users").select(vw.col("id"))
query2 = vw.Source(name="admins").select(vw.col("id"))

# UNION (deduplicated)
result = (query1 | query2).render()
# (SELECT id FROM users) UNION (SELECT id FROM admins)

# UNION ALL (keeps duplicates)
result = (query1 + query2).render()
# (SELECT id FROM users) UNION ALL (SELECT id FROM admins)

# INTERSECT
result = (query1 & query2).render()
# (SELECT id FROM users) INTERSECT (SELECT id FROM admins)

# EXCEPT
result = (query1 - query2).render()
# (SELECT id FROM users) EXCEPT (SELECT id FROM admins)

# Chaining
query3 = vw.Source(name="guests").select(vw.col("id"))
result = (query1 | query2 | query3).render()
# ((SELECT id FROM users) UNION (SELECT id FROM admins)) UNION (SELECT id FROM guests)

# Use as subquery
combined = (query1 | query2).alias("all_ids")
result = combined.select(vw.col("*")).render()
# SELECT * FROM ((SELECT id FROM users) UNION (SELECT id FROM admins)) AS all_ids
```

### CASE Expressions

Use `vw.when()` to build CASE expressions with polars-style chaining:

```python
# Simple CASE with ELSE
result = vw.Source(name="users").select(
    vw.when(vw.col("status") == vw.col("'active'"))
    .then(vw.col("1"))
    .otherwise(vw.col("0"))
    .alias("is_active")
).render()
# SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END AS is_active FROM users

# Multiple WHEN branches
result = vw.Source(name="users").select(
    vw.when(vw.col("age") >= vw.col("18"))
    .then(vw.col("'adult'"))
    .when(vw.col("age") >= vw.col("13"))
    .then(vw.col("'teen'"))
    .otherwise(vw.col("'child'"))
    .alias("age_group")
).render()
# SELECT CASE WHEN age >= 18 THEN 'adult' WHEN age >= 13 THEN 'teen' ELSE 'child' END AS age_group FROM users

# CASE without ELSE (returns NULL when no match)
result = vw.Source(name="users").select(
    vw.when(vw.col("status") == vw.col("'active'"))
    .then(vw.col("1"))
).render()
# SELECT CASE WHEN status = 'active' THEN 1 END FROM users

# CASE in WHERE clause
result = (
    vw.Source(name="orders")
    .select(vw.col("*"))
    .where(
        vw.when(vw.col("priority") == vw.col("'high'"))
        .then(vw.col("1"))
        .otherwise(vw.col("0"))
        == vw.col("1")
    )
).render()
# SELECT * FROM orders WHERE (CASE WHEN priority = 'high' THEN 1 ELSE 0 END = 1)
```

### Window Functions

Use `vw.functions` for window functions and aggregates:

```python
from vw.functions import row_number, sum_, count, avg, lag, lead, rank

# ROW_NUMBER with ORDER BY
result = (
    vw.Source(name="orders")
    .select(
        vw.col("id"),
        row_number().over(order_by=[vw.col("created_at").desc()]).alias("row_num")
    )
    .render()
)
# SELECT id, ROW_NUMBER() OVER (ORDER BY created_at DESC) AS row_num FROM orders

# Window function with PARTITION BY
result = (
    vw.Source(name="orders")
    .select(
        vw.col("id"),
        sum_(vw.col("amount")).over(partition_by=[vw.col("customer_id")]).alias("customer_total")
    )
    .render()
)
# SELECT id, SUM(amount) OVER (PARTITION BY customer_id) AS customer_total FROM orders

# Both PARTITION BY and ORDER BY
result = (
    vw.Source(name="orders")
    .select(
        vw.col("id"),
        row_number().over(
            partition_by=[vw.col("customer_id")],
            order_by=[vw.col("order_date").asc()]
        ).alias("order_num")
    )
    .render()
)
# SELECT id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date ASC) AS order_num FROM orders

# LAG/LEAD for accessing previous/next rows
result = (
    vw.Source(name="prices")
    .select(
        vw.col("date"),
        vw.col("price"),
        lag(vw.col("price")).over(order_by=[vw.col("date").asc()]).alias("prev_price")
    )
    .render()
)
# SELECT date, price, LAG(price, 1) OVER (ORDER BY date ASC) AS prev_price FROM prices

# Aggregate functions without OVER (regular aggregates)
result = (
    vw.Source(name="orders")
    .select(count(), sum_(vw.col("amount")).alias("total"))
    .render()
)
# SELECT COUNT(*), SUM(amount) AS total FROM orders

# Empty OVER() for window over entire result set
result = (
    vw.Source(name="orders")
    .select(
        vw.col("id"),
        sum_(vw.col("amount")).over().alias("grand_total")
    )
    .render()
)
# SELECT id, SUM(amount) OVER () AS grand_total FROM orders
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
    vw.Source(name="users")
    .join.inner(vw.Source(name="orders"), on=[...])
    .select(vw.col("*"))
    .render()
)
```

Each method returns an object that can be further chained, enabling readable query construction.
