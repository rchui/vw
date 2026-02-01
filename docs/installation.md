# Installation

## Requirements

- Python 3.9 or higher
- No required dependencies for core functionality
- Optional: SQLAlchemy, psycopg2, or other database drivers for execution

## Install with uv (Recommended)

```bash
uv pip install .
```

## Install with pip

```bash
pip install .
```

## Development Installation

For contributing to vw, install in editable mode with dev dependencies:

```bash
uv pip install -e ".[dev]"
```

This installs:
- `ruff` - Linting and formatting
- `pytest` - Testing framework
- `mypy` - Type checking (if configured)

## Verification

Verify the installation:

```python
import vw.postgres
from vw.postgres import source, col, render

users = source("users")
query = users.select(col("id"))
result = render(query)

print(result.query)
# Output: SELECT id FROM users
```

## Database Drivers

vw is a query builder only - it doesn't execute queries. You'll need a database driver:

### PostgreSQL

```bash
# SQLAlchemy + psycopg2
uv pip install sqlalchemy psycopg2-binary

# Or asyncpg for async
uv pip install sqlalchemy asyncpg
```

### DuckDB

```bash
uv pip install duckdb
```

## Example with SQLAlchemy

```python
from sqlalchemy import create_engine, text
from vw.postgres import source, col, param, render

# Create engine
engine = create_engine("postgresql://user:pass@localhost/mydb")

# Build query
users = source("users")
query = users.select(col("id"), col("name")).where(col("active") == param("active", True))
result = render(query)

# Execute
with engine.connect() as conn:
    rows = conn.execute(text(result.query), result.params)
    for row in rows:
        print(row)
```

## Example with psycopg2

```python
import psycopg2
from vw.postgres import source, col, param, render

# Connect
conn = psycopg2.connect("dbname=mydb user=postgres")
cursor = conn.cursor()

# Build query
users = source("users")
query = users.select(col("id"), col("name")).where(col("active") == param("active", True))
result = render(query)

# Execute (convert $name to %(name)s for psycopg2)
# Note: vw uses $name by default for PostgreSQL
# You may need to adjust parameter style or use SQLAlchemy
cursor.execute(result.query, result.params)
rows = cursor.fetchall()
```

## Example with DuckDB

```python
import duckdb
from vw.duckdb import source, render

# Note: DuckDB support is incomplete - this is illustrative
conn = duckdb.connect('mydb.duckdb')

users = source("users")
query = users.select(col("id"), col("name"))
result = render(query)

rows = conn.execute(result.query, result.params).fetchall()
```

## Next Steps

- **[Getting Started](getting-started.md)** - Learn the basics
- **[Quickstart](quickstart.md)** - See examples
- **[API Reference](api/index.md)** - Explore the API
