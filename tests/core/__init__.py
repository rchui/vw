"""Core vw tests - ANSI SQL standard functionality.

This directory contains tests for ANSI SQL standard features that should work
identically across all SQL dialects (PostgreSQL, DuckDB, MySQL, etc.).

Tests in this directory use vw.postgres for rendering (PostgreSQL is the reference
dialect), but only test features that are part of the SQL standard, not PostgreSQL-
specific extensions.

**What belongs here:**
- ANSI SQL-92, SQL:1999, SQL:2003, SQL:2008 standard features
- Features that work identically across all SQL dialects
- Examples: =, <, >, COUNT, SUM, INNER JOIN, CASE/WHEN, CTEs, UNION

**What belongs in tests/postgres:**
- PostgreSQL-specific extensions or syntax
- Examples: ILIKE, ROLLUP/CUBE/GROUPING SETS, DISTINCT ON, LATERAL,
  DATE_TRUNC, NOW(), ARRAY_AGG, JSON functions, FOR UPDATE, FETCH WITH TIES
"""
