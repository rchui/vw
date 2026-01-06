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

### Workflow: Completing Work

When a piece of work is completed, always run the following before committing:

1. **Run tests** - Ensure all tests pass
   ```bash
   uv run pytest -v
   ```

2. **Run linters** - Check and fix code style
   ```bash
   uv run ruff check .
   uv run ruff format .
   ```

3. **Commit** - Use conventional commit format
   ```bash
   git add -A && git commit -m "type: description"
   ```
