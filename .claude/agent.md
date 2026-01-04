# VW Project Agent Knowledge

## Project Overview

**vw** is a Python library scaffold set up with modern tooling including uv (package manager), click (CLI framework), and ruff (linter/formatter).

## Project Structure

```
vw/
├── .claude/
│   └── agent.md          # This file - project knowledge base
├── .gitignore            # Python project ignores
├── README.md             # Setup and usage instructions
├── pyproject.toml        # Project configuration
├── vw/                   # Main package directory
│   ├── __init__.py       # Package init with version
│   └── cli.py            # CLI entry point
└── tests/                # Test directory
    └── __init__.py
```

## Configuration Details

### Version
- Current version: `0.0.1`
- Location: `vw/__init__.py` and `pyproject.toml`

### Dependencies
- **click>=8.0** - CLI framework (minimum version for Python 3.9+ support)
- **ruff>=0.8.0** - Linter and formatter (dev dependency)

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

## Design Decisions

1. **Click over ty/typer**: User preferred click for the CLI framework
2. **Line length 120**: User specified 120 character limit instead of default 100
3. **Version 0.0.1**: Starting version set to 0.0.1 instead of 0.1.0
4. **Minimal version pins**: Using `>=` for dependencies to maximize compatibility
5. **Source directory named vw**: Matches project name for clarity

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

## Future Considerations

- Add testing framework (pytest recommended for Python projects)
- Add CI/CD configuration
- Add type checking (mypy or pyright)
- Expand CLI functionality beyond basic --version flag
