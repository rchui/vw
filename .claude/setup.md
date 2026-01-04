# Setup and Configuration

## Installation

```bash
# Development installation
uv pip install -e ".[dev]"

# Standard installation
uv pip install .
```

## Configuration

**Source of truth**: `pyproject.toml`

Key configurations:
- Python version requirement
- Dependencies (production and dev)
- Ruff linting/formatting rules
- CLI entry points

To view: `cat pyproject.toml`

## Development Commands

### Testing
```bash
uv run pytest -v                    # Run all tests with verbose output
uv run pytest tests/test_expr.py    # Run specific test file
uv run pytest -xvs                  # Stop on first failure, verbose, no capture
```

### Linting and Formatting
```bash
uv run ruff check .           # Check code
uv run ruff format .          # Format code
uv run ruff check --fix .     # Fix auto-fixable issues
```

## Development Conventions

### Commit Format
Uses **Conventional Commits** format:
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `test:` - Test changes
- `refactor:` - Code refactoring
