# DuckDB API Reference

The `vw.duckdb` module provides DuckDB-specific implementations and rendering.

**Status:** ✅ **Core Features Complete** - Basic DuckDB support is now available with Star Extensions (EXCLUDE, REPLACE) and File Reading (CSV, Parquet, JSON, JSONL).

## Module Import

```python
from vw.duckdb import ref, col, param, lit, render, F
from vw.duckdb import file, CSV, Parquet, JSON, JSONL  # File reading
from vw.duckdb import cte  # CTEs
```

## Factory Functions

### `ref(name)`

Create a reference to a table or view.

```python
users = ref("users")
# Represents: users table
```

**Parameters:**
- `name` (str) - Table or view name

**Returns:** RowSet wrapping Reference state

**Status:** ✅ Available

### `col(name)`

Create a column reference.

```python
col("name")
col("users.id")
col("*")  # SELECT * syntax
```

**Parameters:**
- `name` (str) - Column name (can include table qualifier)

**Returns:** Expression wrapping Column state

**Status:** ✅ Available

### `param(name, value)`

Create a parameter for parameterized queries.

```python
param("min_age", 18)
ref("users").where(col("age") >= param("min_age", 18))
```

**Parameters:**
- `name` (str) - Parameter name (will be rendered as $name in DuckDB)
- `value` (Any) - Parameter value

**Returns:** Expression wrapping Parameter state

**Status:** ✅ Available

### `lit(value)`

Create a literal value (rendered directly in SQL).

```python
lit(42)
lit("hello")
lit(True)
lit(None)  # NULL
```

**Parameters:**
- `value` (Any) - The literal value (int, float, str, bool, None)

**Returns:** Expression wrapping Literal state

**Status:** ✅ Available

### `render(rowset)`

Render a RowSet or Expression to DuckDB SQL.

```python
query = ref("users").select(col("id"), col("name"))
result = render(query)
# result.query: "SELECT id, name FROM users"
# result.params: {}
```

**Parameters:**
- `rowset` (RowSet | Expression) - Query to render
- `config` (RenderConfig | None) - Optional rendering configuration

**Returns:** SQL result with query string and parameters dict

**Status:** ✅ Available

## Core Features

### Rendering
- ✅ `vw/duckdb/render.py` - DuckDB SQL rendering logic
- ✅ Parameter style configuration (DOLLAR: `$1`, `$2`)
- ✅ DuckDB-specific syntax handling
- ✅ Identifier quoting (double quotes)

### Functions
- ✅ `F` - Functions instance with all ANSI SQL standard functions
- ✅ Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- ✅ Window functions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, etc.)
- ✅ Date/time functions (CURRENT_TIMESTAMP, CURRENT_DATE, EXTRACT)
- ✅ String functions (UPPER, LOWER, TRIM, LENGTH, SUBSTRING, etc.)
- ✅ Null handling (COALESCE, NULLIF, GREATEST, LEAST)

### Query Building
- ✅ SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY
- ✅ LIMIT, OFFSET, FETCH, DISTINCT
- ✅ Joins (INNER, LEFT, RIGHT, FULL, CROSS)
- ✅ Subqueries, CTEs, VALUES clause
- ✅ Set operations (UNION, INTERSECT, EXCEPT)
- ✅ CASE WHEN expressions
- ✅ Window functions with PARTITION BY, ORDER BY, frames

## DuckDB-Specific Features

### Star Extensions

**Status:** ✅ Available

DuckDB's star extensions allow you to modify `SELECT *` queries with EXCLUDE and REPLACE clauses.

#### EXCLUDE - Remove columns from star

```python
users = ref("users")

# Exclude a single column
query = users.select(users.star(users.star.exclude(col("password"))))
# Renders: SELECT users.* EXCLUDE (password) FROM users

# Exclude multiple columns
query = users.select(users.star(users.star.exclude(col("password"), col("ssn"))))
# Renders: SELECT users.* EXCLUDE (password, ssn) FROM users

# With alias
u = ref("users").alias("u")
query = u.select(u.star(u.star.exclude(col("password"))))
# Renders: SELECT u.* EXCLUDE (password) FROM users AS u
```

#### REPLACE - Replace column expressions in star

```python
users = ref("users")

# Replace a single column
query = users.select(users.star(users.star.replace(name=col("first_name"))))
# Renders: SELECT users.* REPLACE (first_name AS name) FROM users

# Replace multiple columns
from vw.duckdb import F

query = users.select(users.star(users.star.replace(
    name=F.upper(col("first_name")),
    age=col("age") + 1
)))
# Renders: SELECT users.* REPLACE (UPPER(first_name) AS name, age + 1 AS age) FROM users
```

#### Combined EXCLUDE and REPLACE

```python
users = ref("users").alias("u")

# Use both modifiers together
query = u.select(
    u.star(
        u.star.exclude(col("password"), col("secret")),
        u.star.replace(name=col("full_name"))
    )
)
# Renders: SELECT u.* EXCLUDE (password, secret) REPLACE (full_name AS name) FROM users AS u

# Modifiers are applied in the order specified
query = users.select(
    users.star(
        users.star.replace(name=col("full_name")),
        users.star.exclude(col("password"))
    )
)
# Renders: SELECT users.* REPLACE (full_name AS name) EXCLUDE (password) FROM users
```

#### Star with other columns

```python
users = ref("users")

# Mix star modifiers with regular columns
query = users.select(
    users.star(users.star.exclude(col("password"))),
    col("created_at")
)
# Renders: SELECT users.* EXCLUDE (password), created_at FROM users
```

#### In JOIN queries

```python
users = ref("users").alias("u")
orders = ref("orders").alias("o")

query = (
    users
    .select(
        users.star(users.star.exclude(col("password"))),
        col("o.total")
    )
    .join.inner(orders, on=[(col("u.id") == col("o.user_id"))])
)
# Renders: SELECT u.* EXCLUDE (password), o.total
#          FROM users AS u INNER JOIN orders AS o ON (u.id = o.user_id)
```

### File Reading

**Status:** ✅ Available

DuckDB can read data directly from files (CSV, Parquet, JSON, JSONL) without loading into tables first. The `file()` function creates a row source from file(s) that can be queried like a table.

#### CSV Files

```python
from vw.duckdb import file, CSV, col, render

# Basic CSV reading
query = file("users.csv", format=CSV(header=True)).select(col("name"), col("email"))
result = render(query)
# Renders: SELECT name, email FROM read_csv('users.csv', header = TRUE)

# With multiple options
query = file("data.csv", format=CSV(
    header=True,
    delim="|",
    skip=10,
    all_varchar=True,
    ignore_errors=True
))
# Renders: FROM read_csv('data.csv', header = TRUE, delim = '|', skip = 10, all_varchar = TRUE, ignore_errors = TRUE)

# Multiple files
query = file("file1.csv", "file2.csv", format=CSV(header=True))
# Renders: FROM read_csv(['file1.csv', 'file2.csv'], header = TRUE)

# Wildcards
query = file("data/*.csv", format=CSV(header=True, union_by_name=True))
# Renders: FROM read_csv('data/*.csv', header = TRUE, union_by_name = TRUE)
```

**CSV Options (22 total):**
- `header` (bool) - Whether the file has a header row
- `delim` (str) - Delimiter character (default: auto-detect)
- `quote` (str) - Quote character (default: '"')
- `escape` (str) - Escape character (default: '"')
- `compression` (str) - Compression format ('none', 'gzip', 'zstd')
- `all_varchar` (bool) - Treat all columns as VARCHAR
- `dateformat` (str) - Date format string
- `timestampformat` (str) - Timestamp format string
- `skip` (int) - Number of rows to skip
- `null_padding` (bool) - Add NULL values for missing columns
- `sample_size` (int) - Number of rows to sample for auto-detection
- `ignore_errors` (bool) - Ignore rows with parsing errors
- `parallel` (bool) - Enable parallel CSV reading
- `filename` (bool) - Add filename column to output
- `hive_partitioning` (bool) - Enable Hive partitioning
- `union_by_name` (bool) - Union by column name instead of position
- `max_line_size` (int) - Maximum line size in bytes
- `columns` (dict) - Dictionary mapping column names to types
- `auto_type_candidates` (list[str]) - List of types to consider during auto-detection
- `names` (list[str]) - List of column names (overrides header row)
- `types` (list[str]) - List of column types
- `decimal_separator` (str) - Decimal separator character

#### Parquet Files

```python
from vw.duckdb import file, Parquet, col, render

# Basic Parquet reading
query = file("data.parquet", format=Parquet()).select(col("id"), col("name"))
result = render(query)
# Renders: SELECT id, name FROM read_parquet('data.parquet')

# With options
query = file("data.parquet", format=Parquet(
    binary_as_string=True,
    filename=True,
    hive_partitioning=True
))
# Renders: FROM read_parquet('data.parquet', binary_as_string = TRUE, filename = TRUE, hive_partitioning = TRUE)
```

**Parquet Options (6 total):**
- `binary_as_string` (bool) - Read BINARY/VARBINARY columns as VARCHAR
- `filename` (bool) - Add filename column to output
- `file_row_number` (bool) - Add file_row_number column to output
- `hive_partitioning` (bool) - Enable Hive partitioning
- `union_by_name` (bool) - Union by column name instead of position
- `compression` (str) - Compression codec ('snappy', 'gzip', 'zstd')

#### JSON Files

```python
from vw.duckdb import file, JSON, col, render

# Standard JSON format
query = file("data.json", format=JSON()).select(col("id"), col("name"))
result = render(query)
# Renders: SELECT id, name FROM read_json('data.json')

# With options
query = file("data.json", format=JSON(
    ignore_errors=True,
    compression="gzip",
    columns={"id": "INTEGER", "name": "VARCHAR"}
))
# Renders: FROM read_json('data.json', ignore_errors = TRUE, compression = 'gzip', columns = {'id': 'INTEGER', 'name': 'VARCHAR'})
```

**JSON Options (7 total):**
- `columns` (dict) - Dict mapping column names to SQL types
- `maximum_object_size` (int) - Maximum size of JSON objects
- `ignore_errors` (bool) - Ignore parse errors
- `compression` (str) - Compression type ('none', 'gzip', 'zstd')
- `filename` (bool) - Add filename column to output
- `hive_partitioning` (bool) - Enable Hive partitioning
- `union_by_name` (bool) - Union by column name instead of position

#### JSONL Files (Newline-Delimited JSON)

```python
from vw.duckdb import file, JSONL, col, render

# JSONL format (one JSON object per line)
query = file("events.jsonl", format=JSONL()).select(col("event_type"), col("timestamp"))
result = render(query)
# Renders: SELECT event_type, timestamp FROM read_json('events.jsonl', format = 'newline_delimited')

# With options
query = file("events.jsonl", format=JSONL(
    ignore_errors=True,
    columns={"event_type": "VARCHAR", "data": "JSON"}
))
# Renders: FROM read_json('events.jsonl', format = 'newline_delimited', ignore_errors = TRUE, columns = {'event_type': 'VARCHAR', 'data': 'JSON'})
```

**JSONL Options (7 total):** Same as JSON (automatically adds `format = 'newline_delimited'`)

#### File Reading with Queries

File sources work seamlessly with all query operations:

```python
from vw.duckdb import file, CSV, col, lit, F, render

# Filtering
query = (
    file("users.csv", format=CSV(header=True))
    .select(col("name"), col("city"))
    .where(col("age") > lit(18))
)
# Renders: SELECT name, city FROM read_csv('users.csv', header = TRUE) WHERE age > 18

# Aggregation
query = (
    file("sales.csv", format=CSV(header=True))
    .select(col("category"), F.sum(col("amount")).alias("total"))
    .group_by(col("category"))
    .order_by(col("category").asc())
)
# Renders: SELECT category, SUM(amount) AS total FROM read_csv('sales.csv', header = TRUE) GROUP BY category ORDER BY category ASC

# With alias
f = file("data.csv", format=CSV(header=True)).alias("d")
query = f.select(f.col("name"))
# Renders: SELECT d.name FROM read_csv('data.csv', header = TRUE) AS d
```

#### File Reading with Joins

```python
from vw.duckdb import file, CSV, ref, col, render

# Join CSV file with table
users = ref("users").alias("u")
scores = file("scores.csv", format=CSV(header=True)).alias("s")

query = users.select(users.col("name"), scores.col("score")).join.inner(
    scores, on=[users.col("id") == scores.col("user_id")]
)
# Renders: SELECT u.name, s.score FROM users AS u INNER JOIN read_csv('scores.csv', header = TRUE) AS s ON (u.id = s.user_id)
```

#### File Reading with CTEs

```python
from vw.duckdb import file, CSV, cte, col, lit, render

# Use file in CTE
high_scores = cte(
    "user_scores",
    file("scores.csv", format=CSV(header=True))
    .select(col("name"), col("score"))
    .where(col("score") > lit(90))
)

query = high_scores.select(col("name"), col("score")).order_by(col("score").desc())
# Renders: WITH user_scores AS (SELECT name, score FROM read_csv('scores.csv', header = TRUE) WHERE score > 90)
#          SELECT name, score FROM user_scores ORDER BY score DESC
```

## Pending DuckDB-Specific Features

The following features are not yet implemented for DuckDB:

### DuckDB-Specific Features (Planned)

#### COPY Statements (Deferred)
- ⏸️ `COPY FROM` - Copy from file to table (can use raw SQL until needed)
- ⏸️ `COPY TO` - Copy from table/query to file (can use raw SQL until needed)

#### DuckDB Functions (Next Priority)
- ❌ List/array functions (list_extract, list_slice, list_contains, list_agg, etc.)
- ❌ Struct functions (struct_pack, struct_extract, etc.)
- ❌ DuckDB-specific aggregates
- ❌ DuckDB-specific statistical functions

#### DuckDB Types
- ❌ LIST type constructors
- ❌ STRUCT type constructors
- ❌ MAP type constructors
- ❌ UNION type constructors

#### Other Features
- ❌ Sampling (USING SAMPLE clause)
- ❌ DuckDB operators (list indexing, struct field access)
- ❌ Advanced features (CTAS, extensions, PIVOT/UNPIVOT)

## Development Status

DuckDB support is actively developed, inheriting core functionality from PostgreSQL. See [DuckDB Parity Roadmap](../development/duckdb-parity.md) for full details.

**Completed:**
- ✅ Phase 1: Core DuckDB Implementation (infrastructure)
- ✅ Phase 2: Star Extensions (EXCLUDE, REPLACE) 🌟
- ✅ Phase 3a: File Reading (CSV, Parquet, JSON, JSONL) 🌟

**Inherited from PostgreSQL:**
- ✅ Core Query Building (SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY)
- ✅ Operators & Expressions (comparison, arithmetic, logical, pattern matching)
- ✅ Aggregate & Window Functions (COUNT, SUM, AVG, ROW_NUMBER, RANK, etc.)
- ✅ Joins (INNER, LEFT, RIGHT, FULL, CROSS)
- ✅ Advanced Features (Subqueries, VALUES, CASE, Set Operations, CTEs)
- ✅ Parameters & Rendering

**Next Steps:**
- Phase 5: List/Array Functions (list operations, list_agg) 🌟
- Phase 4: DuckDB-Specific Types (LIST, STRUCT, MAP)
- Phase 7: Sampling (USING SAMPLE clause)

**Progress:** 30% complete (3 of 10 DuckDB-specific phases complete)

## Example (Future API)

Once implemented, the DuckDB API will look like this:

```python
# This is illustrative - not yet implemented
from vw.duckdb import source, col, param, render, read_csv, F

# Read CSV as source
users = read_csv("users.csv")

# Build query
query = (
    users
    .select(col("*").exclude("password", "ssn"))  # Star with EXCLUDE
    .where(col("age") >= param("min_age", 18))
    .order_by(col("name").asc())
)

# Render
result = render(query)

# Execute with DuckDB
import duckdb
conn = duckdb.connect('mydb.duckdb')
rows = conn.execute(result.query, result.params).fetchall()
```

## Contributing

If you'd like to help implement DuckDB support:

1. Check the [DuckDB Parity Roadmap](../development/duckdb-parity.md)
2. Start with Phase 1: Core DuckDB Implementation
3. Follow the existing patterns from `vw/postgres`
4. Add tests following the PostgreSQL test patterns

## Next Steps

- **[PostgreSQL API](postgres.md)** - See the complete PostgreSQL API for reference
- **[DuckDB Parity](../development/duckdb-parity.md)** - Full feature roadmap
- **[Core API](core.md)** - Shared core abstractions
