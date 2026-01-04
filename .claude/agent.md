# VW Project Agent Knowledge

## Project Overview

**vw** is a SQL builder library that uses polars-inspired method chaining syntax for constructing SQL queries. The project is set up with modern tooling including uv (package manager), click (CLI framework), and ruff (linter/formatter).

### Purpose
- Build SQL queries using method chaining (e.g., `query.select("*").from_("users")`)
- Inspired by polars syntax for a fluent, readable API
- Support for basic SQL operations starting with SELECT statements

## Project Structure

```
vw/
├── .claude/
│   └── agent.md          # This file - project knowledge base
├── .gitignore            # Python project ignores
├── .venv/                # Virtual environment (uv)
├── README.md             # Setup and usage instructions
├── pyproject.toml        # Project configuration
├── vw/                   # Main package directory
│   ├── __init__.py       # Package init, exports public API
│   ├── cli.py            # CLI entry point
│   ├── expr.py           # Expression classes (Column, Expression protocol)
│   └── query.py          # Query builder classes (Source, Statement)
└── tests/                # Test directory
    ├── __init__.py
    ├── test_expr.py      # Unit tests for expr.py
    ├── test_query.py     # Unit tests for query.py
    └── test_sql.py       # Integration tests for SQL generation
```

## Configuration Details

### Version
- Current version: `0.0.1`
- Location: `vw/__init__.py` and `pyproject.toml`

### Dependencies
- Source of truth: `pyproject.toml` (see `[project.dependencies]` and `[project.optional-dependencies]`)
- To view: `cat pyproject.toml`
- Production dependencies: click (CLI framework)
- Dev dependencies: pytest, pytest-describe (testing), ruff (linting/formatting)

### Python Version
- Requires: Python >=3.9

### Ruff Configuration
- Line length: 120 characters
- Target version: Python 3.9
- Selected linters: pycodestyle (E/W), pyflakes (F), isort (I), flake8-bugbear (B), flake8-comprehensions (C4), pyupgrade (UP)
- Format: double quotes, space indentation

### CLI Entry Point
- Command: `vw`
- Entry point: `vw.cli:main`
- Framework: Click (not typer or ty)

## Development Conventions

### Commit Format
- Uses **Conventional Commits** format
- Example: `feat: add Python library scaffold with uv, click, and ruff`

### Code Style
- Line length: 120 characters
- Managed by ruff for both linting and formatting

## Installation Commands

### Standard Installation
```bash
uv pip install .
```

### Development Installation
```bash
uv pip install -e ".[dev]"
```

### Linting/Formatting
```bash
ruff check .           # Check code
ruff format .          # Format code
ruff check --fix .     # Fix auto-fixable issues
```

## API Design

### Core Concepts

**Expression Protocol**: All SQL expressions implement the `__vw_render__()` protocol method that returns SQL strings.

**Method Chaining**: Polars-inspired fluent API pattern:
```python
import vw

sql = vw.Source("users").select(vw.col("*")).render()
# Result: "SELECT * FROM users"
```

**Key Classes**:
- `Expression`: Protocol defining `__vw_render__() -> str`
- `Column`: Represents a column reference, implements Expression protocol
- `Source`: Represents a table/view (FROM clause)
- `Statement`: Represents a SQL statement (combines Source + columns)

**Escape Hatch**: Columns accept raw SQL strings for unsupported features:
```python
vw.col("* REPLACE (foo AS bar)")  # Star extensions
vw.col("CAST(price AS DECIMAL(10,2))")  # Complex expressions
```

### Public API (exported in `vw/__init__.py`)
- `col(name)` - Create a column reference
- `Column` - Column class
- `Expression` - Expression protocol
- `Source` - Table/view source
- `Statement` - SQL statement

## Design Decisions

1. **Source-first API**: Start with `Source()` then chain `.select()` (polars-inspired)
2. **Expression Protocol**: Use Protocol instead of base class for type checking
3. **Positional-only col()**: `col(name, /)` ensures name is positional
4. **Escape hatch via strings**: Allow raw SQL in Column for unsupported features
5. **__vw_render__() convention**: Custom render method instead of `__str__` for SQL generation
6. **Click over ty/typer**: User preferred click for the CLI framework
7. **Line length 120**: User specified 120 character limit instead of default 100
8. **Version 0.0.1**: Starting version set to 0.0.1 instead of 0.1.0
9. **Minimal version pins**: Using `>=` for dependencies to maximize compatibility
10. **pytest-describe**: BDD-style test organization for better readability

## Repository Information

- Git repository: github.com:rchui/vw.git
- Default branch: main
- Latest commit: 3df55bb (feat: add Python library scaffold)

## Workflow

### Agent File Maintenance
**IMPORTANT**: This agent.md file should be continually updated throughout the project lifecycle:

- Update after adding new features or modules
- Update after making architectural decisions
- Update after adding new dependencies or changing configuration
- Update after establishing new conventions or patterns
- Update after resolving significant issues or bugs
- Update when project structure changes

The agent file should always reflect the current state of the project to make resuming work easier.

## Testing

### Test Structure
- **test_expr.py**: Unit tests for expression classes (Column, col())
- **test_query.py**: Unit tests for query builder (Source, Statement)
- **test_sql.py**: Integration tests for complete SQL generation

### Test Style
- Uses pytest-describe for BDD-style organization
- Test naming: `describe_*` for groups, `it_*` for individual tests
- Focus on behavior, not field assignments (dataclasses auto-assign)

### Running Tests
```bash
source .venv/bin/activate
pytest -v                    # Run all tests with verbose output
pytest tests/test_expr.py    # Run specific test file
```

## Current Features

### Implemented
- ✅ Basic SELECT statements
- ✅ SELECT * FROM table
- ✅ SELECT col1, col2 FROM table
- ✅ Method chaining (Source → select → render)
- ✅ Expression Protocol with __vw_render__()
- ✅ Escape hatch for raw SQL expressions
- ✅ Star extensions via escape hatch (REPLACE, EXCLUDE)

### Test Coverage
- 24 tests passing
- Unit tests for expr.py and query.py
- Integration tests for SQL generation
- All linters passing (ruff)

## Future Considerations

### Planned Features
- Fluent API for star extensions: `col("*").replace(foo="bar").exclude("baz")`
- WHERE clause support
- JOIN operations
- GROUP BY / HAVING
- ORDER BY / LIMIT
- Subqueries
- CTEs (Common Table Expressions)
- Type checking (mypy or pyright)

### Infrastructure
- CI/CD configuration
- Expand CLI functionality beyond basic --version flag
- Documentation site
