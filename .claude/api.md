# API Reference

## Public API

All public exports from `vw/__init__.py`:

### Functions
- `col(name)` - Create a column reference
- `param(name, value)` - Create a parameterized value
- `cte(name, query)` - Create a Common Table Expression
- `exists(subquery)` - Create an EXISTS expression for a subquery
- `when(condition)` - Start a CASE expression (returns `When` object)

### Date/Time Functions
- `current_timestamp()` - Get current timestamp
- `current_date()` - Get current date
- `current_time()` - Get current time
- `now()` - Get current timestamp (NOW() function)
- `interval(amount, unit)` - Create an INTERVAL value for arithmetic
- `date(value)` - Create a DATE literal (e.g., `date("2023-01-01")`)

### Frame Module (Window Functions)
Import as `import vw.frame`:
- `frame.UNBOUNDED_PRECEDING` - Window frame start boundary
- `frame.UNBOUNDED_FOLLOWING` - Window frame end boundary
- `frame.CURRENT_ROW` - Current row boundary
- `frame.preceding(n)` - n rows preceding boundary
- `frame.following(n)` - n rows following boundary

### Functions Namespace (`F` from `vw.functions` or `vw.F`)
All SQL functions are accessed via the `F` class:

**Window-only functions:**
- `F.row_number()` - ROW_NUMBER() window function
- `F.rank()` - RANK() window function
- `F.dense_rank()` - DENSE_RANK() window function
- `F.ntile(n)` - NTILE(n) window function

**Aggregate/window functions:**
- `F.sum(expr)` - SUM() aggregate/window function
- `F.count(expr=None)` - COUNT() aggregate/window function (None for COUNT(*))
- `F.avg(expr)` - AVG() aggregate/window function
- `F.min(expr)` - MIN() aggregate/window function
- `F.max(expr)` - MAX() aggregate/window function

**Offset functions:**
- `F.lag(expr, offset=1, default=None)` - LAG() window function
- `F.lead(expr, offset=1, default=None)` - LEAD() window function
- `F.first_value(expr)` - FIRST_VALUE() window function
- `F.last_value(expr)` - LAST_VALUE() window function

**Null handling functions:**
- `F.coalesce(*exprs)` - COALESCE() - first non-NULL value
- `F.nullif(expr1, expr2)` - NULLIF() - NULL if expr1 equals expr2

**Comparison functions:**
- `F.greatest(*exprs)` - GREATEST() - largest value
- `F.least(*exprs)` - LEAST() - smallest value

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
- `Between` - BETWEEN range comparison operator
- `NotBetween` - NOT BETWEEN range comparison operator
- `Source` - Table/view source (extends RowSet)
- `Statement` - SQL statement (extends Expression and RowSet)
- `SetOperation` - Combined queries via UNION/INTERSECT/EXCEPT (extends Expression and RowSet)
- `Function` - SQL function (from `vw.functions`)
- `WindowFunction` - Window function with OVER clause (from `vw.functions`)
- `When` - Incomplete WHEN clause, call `.then()` to complete (from `vw.operators`)
- `CaseExpression` - Complete CASE expression (from `vw.operators`)
- `InnerJoin` - Inner join operation
- `RenderResult` - Rendering result with SQL and params
- `RenderConfig` - Rendering configuration, including dialect and optional parameter style override
- `RenderContext` - Rendering context (for advanced use)
- `Dialect` - SQL dialect enum (controls cast syntax and default parameter style)
- `ParamStyle` - Enum for specifying parameter placeholder style (e.g., `:name`, `$name`, `@name`, `%(name)s`)

### Exceptions (from `vw.exceptions`)
- `VWError` - Base exception for all vw errors
- `CTENameCollisionError` - Raised when multiple CTEs with the same name are registered
- `UnsupportedParamStyleError` - Raised when an unsupported parameter style is used
- `UnsupportedDialectError` - Raised when a feature is not supported for the selected dialect

### Accessors

#### Text Accessor (`.text`)
Access string operations on any expression via `.text`:
- `.text.upper()` - Convert to uppercase
- `.text.lower()` - Convert to lowercase
- `.text.trim()` - Remove leading/trailing whitespace
- `.text.ltrim()` - Remove leading whitespace
- `.text.rstrip()` - Remove trailing whitespace
- `.text.length()` - Get string length
- `.text.substring(start, length=None)` - Extract substring
- `.text.replace(old, new)` - Replace occurrences
- `.text.concat(*others)` - Concatenate with other expressions

#### DateTime Accessor (`.dt`)
Access datetime operations on any expression via `.dt`:
- `.dt.year()`, `.dt.month()`, `.dt.day()` - Extract date parts
- `.dt.hour()`, `.dt.minute()`, `.dt.second()` - Extract time parts
- `.dt.quarter()`, `.dt.week()`, `.dt.weekday()` - Extract temporal units
- `.dt.truncate(unit)` - Truncate to specified unit ("year", "month", etc.)
- `.dt.date()` - Extract date part
- `.dt.time()` - Extract time part
- `.dt.date_add(amount, unit)` - Add interval
- `.dt.date_sub(amount, unit)` - Subtract interval


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
- `.between(start, end)` - BETWEEN range check (inclusive)
- `.not_between(start, end)` - NOT BETWEEN range check (inclusive)
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

### SELECT * Extensions

```python
users = vw.Source(name="users")
result = users.star
# result.sql: "SELECT * FROM users"

result = users.star.exclude(vw.col("id"))
# result.sql: "SELECT * EXCLUDE (id) FROM users"

result = users.star.replace(vw.col("id").alias("internal_id"))
# result.sql: "SELECT * REPLACE (id as internal_id)"

result = users.star.rename(vw.col("id").alias("identifier"))
# result.sql: "SELECT * RENAME (id as identifier)"
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

### BETWEEN Range Checks

Use `.between()` and `.not_between()` for range filtering:

```python
# Basic BETWEEN with parameters
result = vw.Source(name="users").select(vw.col("*")).where(
    vw.col("age").between(vw.param("min_age", 18), vw.param("max_age", 65))
).render()
# result.sql: "SELECT * FROM users WHERE (age BETWEEN :min_age AND :max_age)"
# result.params: {"min_age": 18, "max_age": 65}

# NOT BETWEEN with column literals
result = vw.Source(name="products").select(vw.col("name"), vw.col("price")).where(
    vw.col("price").not_between(vw.col("10"), vw.col("100"))
).render()
# result.sql: "SELECT name, price FROM products WHERE (price NOT BETWEEN 10 AND 100)"

# BETWEEN with expressions and logical operators
result = vw.Source(name="orders").select(vw.col("*")).where(
    (vw.col("amount").between(vw.col("100"), vw.col("1000"))) &
    (vw.col("status") == vw.col("'complete'"))
).render()
# result.sql: "SELECT * FROM orders WHERE ((amount BETWEEN 100 AND 1000) AND (status = 'complete'))"

# Using NOT operator with BETWEEN
result = vw.Source(name="users").select(vw.col("*")).where(
    ~vw.col("age").between(vw.param("min", 18), vw.param("max", 65))
).render()
# result.sql: "SELECT * FROM users WHERE NOT (age BETWEEN :min AND :max)"
# result.params: {"min": 18, "max": 65}
```


### Parameter Style Override

The default parameter style is determined by the selected `Dialect`. However, you can explicitly override this using the `param_style` attribute in `RenderConfig`.

```python
from vw.render import RenderConfig, ParamStyle, Dialect

# Override parameter style for any dialect
config = RenderConfig(dialect=Dialect.POSTGRES, param_style=ParamStyle.COLON)
result = vw.Source(name="users").select(vw.col("*")).where(
    vw.col("age") >= vw.param("min_age", 18)
).render(config=config)
# result.sql: "SELECT * FROM users WHERE (age >= :min_age)"
```

### ORDER BY

Use `.order_by()` with `.asc()` or `.desc()` for sorting:

```python
# Basic ORDER BY
result = (
    vw.Source(name="users")
    .select(vw.col("*"))
    .order_by(vw.col("name").asc(), vw.col("created_at").desc())
    .render()
)
# SELECT * FROM users ORDER BY name ASC, created_at DESC

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

### SQL Type Constructors

The library provides comprehensive SQL type constructors via the `dtypes` module to prevent typos and ensure type safety:

```python
from vw import col, param
from vw import dtypes  # Import the module
# or specific imports
from vw.dtypes import decimal, varchar, integer, boolean
```

#### Character String Types
```python
dtypes.char(50)        # CHAR(50)
dtypes.char()          # CHAR
dtypes.varchar(255)    # VARCHAR(255)
dtypes.varchar()       # VARCHAR
dtypes.text()          # TEXT
```

#### Numeric Types
```python
dtypes.smallint()      # SMALLINT
dtypes.integer()       # INTEGER
dtypes.bigint()        # BIGINT
dtypes.decimal(10, 2)  # DECIMAL(10,2)
dtypes.decimal()       # DECIMAL
dtypes.numeric(10, 2)  # NUMERIC(10,2)
dtypes.numeric()       # NUMERIC
dtypes.float()         # FLOAT
dtypes.float4()        # FLOAT4 (4-byte floating point)
dtypes.real()          # REAL
dtypes.double()        # DOUBLE
dtypes.float8()        # FLOAT8 (8-byte floating point)
dtypes.double_precision()  # DOUBLE PRECISION
```

#### Date and Time Types
```python
dtypes.date()          # DATE
dtypes.time()          # TIME
dtypes.datetime()      # DATETIME
dtypes.timestamp()     # TIMESTAMP
dtypes.timestamptz()   # TIMESTAMP WITH TIME ZONE
```

#### Boolean and Binary Types
```python
dtypes.boolean()       # BOOLEAN
dtypes.bytea()         # BYTEA
dtypes.blob()          # BLOB
dtypes.uuid()          # UUID
```

#### Container Types
```python
dtypes.array(dtypes.integer(), 5)     # INTEGER[5]
dtypes.array(dtypes.text())           # TEXT[]
dtypes.list(dtypes.varchar(100))      # VARCHAR(100)[]
dtypes.json()          # JSON
dtypes.jsonb()         # JSONB
dtypes.variant()       # VARIANT

# Struct type
dtypes.struct({
    "id": dtypes.integer(),
    "name": dtypes.varchar(100),
    "active": dtypes.boolean()
})  # STRUCT(id INTEGER, name VARCHAR(100), active BOOLEAN)
```

### Type Casting

Use `.cast()` with SQL type constructors to cast expressions to SQL types. The syntax varies by dialect:

```python
# SQL Server: CAST(expr AS type)
result = vw.Source(name="orders").select(
    vw.col("price").cast(dtypes.decimal(10, 2))
).render()
# SELECT CAST(price AS DECIMAL(10,2)) FROM orders

# PostgreSQL: expr::type
config = vw.RenderConfig(dialect=vw.Dialect.POSTGRES)
result = vw.Source(name="orders").select(
    vw.col("price").cast(dtypes.numeric())
).render(config=config)
# SELECT price::numeric FROM orders

# Cast can be chained with alias
result = vw.Source(name="orders").select(
    vw.col("price").cast(dtypes.decimal(10, 2)).alias("formatted_price")
).render()
# SELECT price::DECIMAL(10,2) AS formatted_price FROM orders

# Cast parameters too
result = vw.Source(name="users").select(
    vw.param("age", 25).cast(dtypes.smallint())
).render()
# SELECT $age::SMALLINT FROM users
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

### String Operations

Use the `.text` accessor for string operations:

```python
# Basic string operations
result = (
    vw.Source(name="users")
    .select(
        vw.col("name").text.upper().alias("upper_name"),
        vw.col("email").text.trim().alias("clean_email"),
        vw.col("name").text.length().alias("name_length")
    )
    .render()
)
# SELECT UPPER(name) AS upper_name, TRIM(email) AS clean_email, LENGTH(name) AS name_length FROM users

# Substring and replace
result = (
    vw.Source(name="products")
    .select(
        vw.col("description").text.substring(1, 50).alias("short_desc"),
        vw.col("content").text.replace("old", "new").alias("updated_content")
    )
    .render()
)
# SELECT SUBSTRING(description, 1, 50) AS short_desc, REPLACE(content, 'old', 'new') AS updated_content FROM products

# Concatenation
result = (
    vw.Source(name="users")
    .select(
        vw.col("first_name").text.concat(vw.col("' '"), vw.col("last_name")).alias("full_name")
    )
    .render()
)
# SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM users
```

### DateTime Operations

Use the `.dt` accessor for datetime operations:

```python
# Extract date/time parts
result = (
    vw.Source(name="orders")
    .select(
        vw.col("created_at").dt.year().alias("order_year"),
        vw.col("created_at").dt.month().alias("order_month"),
        vw.col("created_at").dt.day().alias("order_day")
    )
    .render()
)
# SELECT EXTRACT(YEAR FROM created_at) AS order_year, EXTRACT(MONTH FROM created_at) AS order_month, EXTRACT(DAY FROM created_at) AS order_day FROM orders

# Date truncation
result = (
    vw.Source(name="logs")
    .select(
        vw.col("timestamp").dt.truncate("month").alias("month_start")
    )
    .render()
)
# SELECT DATE_TRUNC('month', timestamp) AS month_start FROM logs

# Interval arithmetic
result = (
    vw.Source(name="events")
    .select(
        vw.col("start_time").dt.date_add(7, "days").alias("week_later"),
        vw.col("end_time").dt.date_sub(1, "hours").alias("one_hour_before")
    )
    .render()
)
# SELECT DATE_ADD(start_time, INTERVAL '7 days') AS week_later, DATE_SUB(end_time, INTERVAL '1 hours') AS one_hour_before FROM events

# Standalone datetime functions
result = (
    vw.Source(name="events")
    .select(
        vw.current_timestamp().alias("now"),
        vw.interval(30, "minutes").alias("thirty_minutes")
    )
    .render()
)
# SELECT CURRENT_TIMESTAMP AS now, INTERVAL '30 minutes' AS thirty_minutes FROM events
```

### Window Functions

Use the `F` namespace for window functions and aggregates:

```python
from vw.functions import F

# Window functions with PARTITION BY and ORDER BY
result = (
    vw.Source(name="orders")
    .select(
        vw.col("id"),
        F.row_number().over(
            partition_by=[vw.col("customer_id")],
            order_by=[vw.col("order_date").asc()]
        ).alias("order_num"),
        F.sum(vw.col("amount")).over(partition_by=[vw.col("customer_id")]).alias("customer_total")
    )
    .render()
)
# SELECT id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date ASC) AS order_num,
#        SUM(amount) OVER (PARTITION BY customer_id) AS customer_total FROM orders

# LAG/LEAD for accessing previous/next rows
result = (
    vw.Source(name="prices")
    .select(
        vw.col("date"),
        vw.col("price"),
        F.lag(vw.col("price")).over(order_by=[vw.col("date").asc()]).alias("prev_price")
    )
    .render()
)
# SELECT date, price, LAG(price, 1) OVER (ORDER BY date ASC) AS prev_price FROM prices

# Regular aggregates (without OVER)
result = vw.Source(name="orders").select(F.count(), F.sum(vw.col("amount")).alias("total")).render()
# SELECT COUNT(*), SUM(amount) AS total FROM orders

# Empty OVER() for window over entire result set
result = (
    vw.Source(name="orders")
    .select(vw.col("id"), F.sum(vw.col("amount")).over().alias("grand_total"))
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
