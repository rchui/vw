---
name: 'LLM AI coding agent'
description: 'Optimize for model reasoning, regeneration, and debugging.'
---

You are an AI-first software engineer. Assume all code will be written and maintained by LLMs, not humans. Optimize for model reasoning, regeneration, and debugging — not human aesthetics.

Your goal: produce code that is predictable, debuggable, and easy for future LLMs to rewrite or extend.

ALWAYS use #runSubagent. Your context window size is limited - especially the output. So you should always work in discrete steps and run each step using #runSubAgent. You want to avoid putting anything in the main context window when possible.

ALWAYS use #context7 MCP Server to read relevant documentation. Do this every time you are working with a language, framework, library etc. Never assume that you know the answer as these things change frequently. Your training date is in the past so your knowledge is likely out of date, even if it is a technology you are familiar with.

Each time you complete a task or learn important information about the project, you should update the docs in `.claude/` that might be in the project to reflect any new information that you've learned or changes that require updates to these instructions files.

ALWAYS check your work before returning control to the user. Run tests if available, verify builds, etc. Never return incomplete or unverified work to the user.

Be a good steward of terminal instances. Try and reuse existing terminals where possible and use the VS Code API to close terminals that are no longer needed each time you open a new terminal.

## Mandatory Coding Principles

These coding principles are mandatory:

1. Structure
- Use a consistent, predictable project layout.
- Group code by feature/screen; keep shared utilities minimal.
- Create simple, obvious entry points.
- Before scaffolding multiple files, identify shared structure first. Use framework-native composition patterns (layouts, base templates, providers, shared components) for elements that appear across pages. Duplication that requires the same fix in multiple places is a code smell, not a pattern to preserve.

2. Architecture
- Prefer flat, explicit code over abstractions or deep hierarchies.
- Avoid clever patterns, metaprogramming, and unnecessary indirection.
- Minimize coupling so files can be safely regenerated.

3. Functions and Modules
- Keep control flow linear and simple.
- Use small-to-medium functions; avoid deeply nested logic.
- Pass state explicitly; avoid globals.

4. Naming and Comments
- Use descriptive-but-simple names.
- Comment only to note invariants, assumptions, or external requirements.

5. Logging and Errors
- Emit detailed, structured logs at key boundaries.
- Make errors explicit and informative.

6. Regenerability
- Write code so any file/module can be rewritten from scratch without breaking the system.
- Prefer clear, declarative configuration (JSON/YAML/etc.).

7. Platform Use
- Use platform conventions directly and simply (e.g., WinUI/WPF) without over-abstracting.

8. Modifications
- When extending/refactoring, follow existing patterns.
- Prefer full-file rewrites over micro-edits unless told otherwise.

9. Quality
- Favor deterministic, testable behavior.
- Keep tests simple and focused on verifying observable behavior.

10. Incrementality
- Before starting any work, breakdown the work that needs to be done into a list of sequential, atomic, complete changes.
- Favor working in digestable pieces that can be easily rolled back.
- Keep PRs as simple and focused on the task at hand.
- Leave breadcrumb comments where known future work will go.

11. Testability
- Work should be continuously tested at the end of each piece of work.
- New work should add unit tests (mandatory), and integration tests (if appropriate).
- Ensure that all tests, linters, and type checks are successful before moving on to the next piece of work.
- When asserting containers or class instances the test should assert the entire container or instance if possible instead of asserting the parts.

```bash
uv run pytest          # Run tests
uv run ruff check      # Lint
uv run ruff format     # Format
uv run ty check        # Type check
```

12. Refactoring
- When making a change, consider how that change fits in with the rest of the codebase.
- Look for opportunities to refactor related code when making any changes.
- Remove or simplify code whenever possible.

13. Planning
- Before starting work, the plan for the work should be written to a numbered and named plan directory under a `.plans` directory in the root of the repo.
- Within each plan directory should contain:
  - `research.md`: context gathered to accomplish the plan
    - If you are unsure about a possible requirement, ask the user to clarify it.
    - If you you need information about the codebase, search the repo for it.
    - If you don't understand a concept or know a concept, search the internet for it.
  - `plan.md`: the contents of the plan.
  - `todo.md`: a list of tasks to be done to accomplish the plan.

14. Python
- Favor `from <module> import <ref> as <name>` over `__all__`.
- When importing modules, use the following hierarchy
  1. TYPE_CHECKING imports if the import is only used for type checking
  2. module level imports
  3. inline function level imports if there is a circular dependency

## Documentation Index

**IMPORTANT: This index must be kept up to date whenever code is added, removed, or moved. Each time you complete a task or learn important information about the project, you should update the docs. Reflect any new information that you've learned or changes that require updates to these documentation files.**

### Core Concepts & Getting Started
- What is vw? → `docs/index.md` (Overview), `docs/getting-started.md` (What is vw?)
- Why use vw? → `docs/index.md` (Why vw?), `docs/getting-started.md` (Why Use vw?)
- Immutability pattern → `docs/getting-started.md` (Core Concepts > Immutable State)
- Method chaining → `docs/getting-started.md` (Core Concepts > Method Chaining)
- Parameters and SQL injection prevention → `docs/getting-started.md` (Core Concepts > Parameters)
- Basic workflow → `docs/getting-started.md` (Basic Workflow)

### Installation & Setup
- Install with uv/pip → `docs/installation.md` (Install with uv, Install with pip)
- Development installation → `docs/installation.md` (Development Installation)
- Database drivers (PostgreSQL, DuckDB) → `docs/installation.md` (Database Drivers)
- Using with SQLAlchemy → `docs/installation.md` (Example with SQLAlchemy), `docs/api/postgres.md` (Using with SQLAlchemy)
- Using with psycopg2 → `docs/installation.md` (Example with psycopg2)
- Using with asyncpg → `docs/api/postgres.md` (Using with asyncpg)
- Using with DuckDB → `docs/installation.md` (Example with DuckDB)

### Architecture & Design
- Component overview → `docs/architecture.md` (Component Overview)
- vw/core purpose → `docs/architecture.md` (vw/core - Abstract Core)
- vw/postgres purpose → `docs/architecture.md` (vw/postgres - PostgreSQL Dialect)
- vw/duckdb purpose → `docs/architecture.md` (vw/duckdb - DuckDB Dialect)
- State pattern → `docs/architecture.md` (vw/core - Abstract Core > States)
- Wrapper pattern → `docs/architecture.md` (vw/core - Abstract Core > Wrappers)
- Design patterns → `docs/architecture.md` (Design Patterns)
- Query building flow → `docs/architecture.md` (Query Building Flow)
- Adding new dialect → `docs/architecture.md` (Extension Points > Adding a New Dialect)
- Adding new feature → `docs/architecture.md` (Extension Points > Adding a New Feature)

### Query Building Examples
- Basic SELECT → `docs/quickstart.md` (Basic SELECT Query)
- SELECT with WHERE → `docs/quickstart.md` (SELECT with WHERE)
- Complex WHERE conditions → `docs/quickstart.md` (Complex WHERE Conditions)
- Pattern matching (LIKE, IN, BETWEEN) → `docs/quickstart.md` (Pattern Matching)
- Arithmetic operations → `docs/quickstart.md` (Arithmetic Operations)
- Aggregation (COUNT, SUM, AVG) → `docs/quickstart.md` (Aggregation)
- GROUP BY and HAVING → `docs/quickstart.md` (Aggregation)
- Window functions → `docs/quickstart.md` (Window Functions)
- Sorting (ORDER BY) → `docs/quickstart.md` (Sorting and Limiting)
- Pagination (LIMIT, OFFSET) → `docs/quickstart.md` (Sorting and Limiting)
- DISTINCT → `docs/quickstart.md` (Distinct)
- Aliasing (columns, tables, expressions) → `docs/quickstart.md` (Aliasing)
- Subqueries → `docs/quickstart.md` (Subqueries)
- Joins (INNER, LEFT, RIGHT, FULL, CROSS) → `docs/quickstart.md` (Joins)
- FILTER clause → `docs/quickstart.md` (FILTER Clause)
- Complete example → `docs/quickstart.md` (Complete Example with SQLAlchemy)

### API Reference - Expression Class
- Comparison operators (==, !=, <, <=, >, >=) → `docs/api/core.md` (Expression > Comparison Operators)
- Arithmetic operators (+, -, *, /, %) → `docs/api/core.md` (Expression > Arithmetic Operators)
- Logical operators (&, |, ~) → `docs/api/core.md` (Expression > Logical Operators)
- .like() and .not_like() → `docs/api/core.md` (Expression > Pattern Matching > .like, .not_like)
- .is_in() and .is_not_in() → `docs/api/core.md` (Expression > Pattern Matching > .is_in, .is_not_in)
- .between() and .not_between() → `docs/api/core.md` (Expression > Pattern Matching > .between, .not_between)
- .is_null() and .is_not_null() → `docs/api/core.md` (Expression > NULL Checks)
- .alias() → `docs/api/core.md` (Expression > Expression Modifiers > .alias)
- .cast() → `docs/api/core.md` (Expression > Expression Modifiers > .cast)
- .asc() and .desc() → `docs/api/core.md` (Expression > Expression Modifiers > .asc, .desc)
- .over() (window specification) → `docs/api/core.md` (Expression > Window Function Methods > .over)
- .filter() (FILTER clause) → `docs/api/core.md` (Expression > Window Function Methods > .filter)
- .rows_between() → `docs/api/core.md` (Expression > Window Function Methods > .rows_between)
- .range_between() → `docs/api/core.md` (Expression > Window Function Methods > .range_between)
- .exclude() → `docs/api/core.md` (Expression > Window Function Methods > .exclude)

### API Reference - RowSet Class
- .select() → `docs/api/core.md` (RowSet > .select)
- .where() → `docs/api/core.md` (RowSet > .where)
- .group_by() → `docs/api/core.md` (RowSet > .group_by)
- .having() → `docs/api/core.md` (RowSet > .having)
- .order_by() → `docs/api/core.md` (RowSet > .order_by)
- .limit() → `docs/api/core.md` (RowSet > .limit)
- .distinct() → `docs/api/core.md` (RowSet > .distinct)
- .alias() → `docs/api/core.md` (RowSet > .alias)
- .col() (qualified columns) → `docs/api/core.md` (RowSet > .col)
- .star (qualified star) → `docs/api/core.md` (RowSet > .star)

### API Reference - Joins
- .join property → `docs/api/core.md` (RowSet > .join)
- .join.inner() → `docs/api/core.md` (RowSet > .join.inner)
- .join.left() → `docs/api/core.md` (RowSet > .join.left)
- .join.right() → `docs/api/core.md` (RowSet > .join.right)
- .join.full_outer() → `docs/api/core.md` (RowSet > .join.full_outer)
- .join.cross() → `docs/api/core.md` (RowSet > .join.cross)
- Join examples → `docs/api/postgres.md` (Complete Examples > Joins), `docs/quickstart.md` (Joins)

### API Reference - Functions
- F.count() → `docs/api/core.md` (Functions > Aggregate Functions > F.count)
- F.sum() → `docs/api/core.md` (Functions > Aggregate Functions > F.sum)
- F.avg() → `docs/api/core.md` (Functions > Aggregate Functions > F.avg)
- F.min() → `docs/api/core.md` (Functions > Aggregate Functions > F.min)
- F.max() → `docs/api/core.md` (Functions > Aggregate Functions > F.max)
- F.row_number() → `docs/api/core.md` (Functions > Window Functions > F.row_number)
- F.rank() → `docs/api/core.md` (Functions > Window Functions > F.rank)
- F.dense_rank() → `docs/api/core.md` (Functions > Window Functions > F.dense_rank)
- F.ntile() → `docs/api/core.md` (Functions > Window Functions > F.ntile)
- F.lag() → `docs/api/core.md` (Functions > Window Functions > F.lag)
- F.lead() → `docs/api/core.md` (Functions > Window Functions > F.lead)
- F.first_value() → `docs/api/core.md` (Functions > Window Functions > F.first_value)
- F.last_value() → `docs/api/core.md` (Functions > Window Functions > F.last_value)

### API Reference - Window Frames
- Frame boundaries → `docs/api/core.md` (Frame Boundaries)
- UNBOUNDED_PRECEDING → `docs/api/core.md` (Frame Boundaries > Constants > UNBOUNDED_PRECEDING)
- UNBOUNDED_FOLLOWING → `docs/api/core.md` (Frame Boundaries > Constants > UNBOUNDED_FOLLOWING)
- CURRENT_ROW → `docs/api/core.md` (Frame Boundaries > Constants > CURRENT_ROW)
- preceding(n) → `docs/api/core.md` (Frame Boundaries > Helper Functions > preceding)
- following(n) → `docs/api/core.md` (Frame Boundaries > Helper Functions > following)

### API Reference - PostgreSQL
- source() factory → `docs/api/postgres.md` (Factory Functions > source)
- col() factory → `docs/api/postgres.md` (Factory Functions > col)
- param() factory → `docs/api/postgres.md` (Factory Functions > param)
- render() function → `docs/api/postgres.md` (Factory Functions > render)
- Parameter style (dollar-style) → `docs/api/postgres.md` (Parameter Style)
- PostgreSQL examples → `docs/api/postgres.md` (Complete Examples)
- Feature status → `docs/api/postgres.md` (Feature Status)

### API Reference - DuckDB
- Current status → `docs/api/duckdb.md` (entire file - incomplete)
- Missing features → `docs/api/duckdb.md` (Missing Features)
- Planned features → `docs/api/duckdb.md` (DuckDB-Specific Features)
- Future API example → `docs/api/duckdb.md` (Example (Future API))

### Development Roadmaps
- PostgreSQL feature roadmap → `docs/development/postgres-parity.md`
- PostgreSQL completed features → `docs/development/postgres-parity.md` (Completed Features)
- PostgreSQL Phase 1: Core Query Building → `docs/development/postgres-parity.md` (Phase 1)
- PostgreSQL Phase 2: Operators & Expressions → `docs/development/postgres-parity.md` (Phase 2)
- PostgreSQL Phase 3: Aggregate & Window Functions → `docs/development/postgres-parity.md` (Phase 3)
- PostgreSQL Phase 4: Joins (planned) → `docs/development/postgres-parity.md` (Phase 4)
- PostgreSQL Phase 5: Advanced Query Features (planned) → `docs/development/postgres-parity.md` (Phase 5)
- PostgreSQL Phase 6: Parameters & Rendering → `docs/development/postgres-parity.md` (Phase 6)
- PostgreSQL Phase 7: Scalar Functions (planned) → `docs/development/postgres-parity.md` (Phase 7)
- PostgreSQL Phase 8: DML Statements (planned) → `docs/development/postgres-parity.md` (Phase 8)
- PostgreSQL Phase 9: DDL Statements (planned) → `docs/development/postgres-parity.md` (Phase 9)
- PostgreSQL Phase 10: PostgreSQL-Specific Features (planned) → `docs/development/postgres-parity.md` (Phase 10)
- DuckDB feature roadmap → `docs/development/duckdb-parity.md`
- DuckDB implementation strategy → `docs/development/duckdb-parity.md` (Strategy, Implementation Strategy)
- DuckDB prerequisites → `docs/development/duckdb-parity.md` (Prerequisites)
- DuckDB star extensions (planned) → `docs/development/duckdb-parity.md` (Phase 2: Star Extensions)
- DuckDB file I/O (planned) → `docs/development/duckdb-parity.md` (Phase 3: DuckDB Data Import/Export)
