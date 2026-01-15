"""Unit tests for DDL (CREATE TABLE) functionality."""

import pytest

import vw
from vw import dtypes
from vw.ddl import CreateTable


def describe_create_table():
    """Tests for CreateTable class."""

    def it_renders_empty_table(render_context: vw.RenderContext) -> None:
        result = CreateTable(name="users").__vw_render__(render_context)
        assert result == "CREATE TABLE users ()"

    def it_renders_table_with_schema(render_context: vw.RenderContext) -> None:
        result = CreateTable(
            name="users",
            schema={
                "id": dtypes.integer(),
                "name": dtypes.varchar(100),
            },
        ).__vw_render__(render_context)
        assert result == "CREATE TABLE users (id INTEGER, name VARCHAR(100))"

    def it_renders_temporary_table(render_context: vw.RenderContext) -> None:
        result = CreateTable(name="temp_data").temporary().__vw_render__(render_context)
        assert result == "CREATE TEMPORARY TABLE temp_data ()"

    def it_renders_if_not_exists(render_context: vw.RenderContext) -> None:
        result = CreateTable(name="cache").if_not_exists().__vw_render__(render_context)
        assert result == "CREATE TABLE IF NOT EXISTS cache ()"

    def it_renders_or_replace(render_context: vw.RenderContext) -> None:
        result = CreateTable(name="snapshot").or_replace().__vw_render__(render_context)
        assert result == "CREATE OR REPLACE TABLE snapshot ()"

    def it_combines_modifiers(render_context: vw.RenderContext) -> None:
        result = CreateTable(name="temp").temporary().if_not_exists().__vw_render__(render_context)
        assert result == "CREATE TEMPORARY TABLE IF NOT EXISTS temp ()"

    def it_combines_or_replace_and_temporary(render_context: vw.RenderContext) -> None:
        result = CreateTable(name="snapshot").or_replace().temporary().__vw_render__(render_context)
        assert result == "CREATE OR REPLACE TEMPORARY TABLE snapshot ()"


def describe_create_table_as_select():
    """Tests for CREATE TABLE AS SELECT (CTAS)."""

    def it_renders_ctas(render_context: vw.RenderContext) -> None:
        query = vw.Source(name="orders").select(vw.col("*"))
        result = CreateTable(name="backup").as_select(query).__vw_render__(render_context)
        assert result == "CREATE TABLE backup AS SELECT * FROM orders"

    def it_renders_ctas_with_temporary(render_context: vw.RenderContext) -> None:
        query = vw.Source(name="orders").select(vw.col("*"))
        result = CreateTable(name="temp_orders").as_select(query).temporary().__vw_render__(render_context)
        assert result == "CREATE TEMPORARY TABLE temp_orders AS SELECT * FROM orders"

    def it_renders_ctas_with_if_not_exists(render_context: vw.RenderContext) -> None:
        query = vw.Source(name="orders").select(vw.col("id"), vw.col("total"))
        result = CreateTable(name="order_summary").as_select(query).if_not_exists().__vw_render__(render_context)
        assert result == "CREATE TABLE IF NOT EXISTS order_summary AS SELECT id, total FROM orders"

    def it_renders_ctas_with_complex_query(render_context: vw.RenderContext) -> None:
        query = (
            vw.Source(name="orders")
            .select(vw.col("customer_id"), vw.col("SUM(total)").alias("total_amount"))
            .where(vw.col("status") == vw.col("'completed'"))
            .group_by(vw.col("customer_id"))
        )
        result = CreateTable(name="customer_totals").as_select(query).__vw_render__(render_context)
        assert result == (
            "CREATE TABLE customer_totals AS "
            "SELECT customer_id, SUM(total) AS total_amount FROM orders "
            "WHERE (status = 'completed') GROUP BY customer_id"
        )


def describe_source_create_table():
    """Tests for Source.create_table() method."""

    def it_creates_empty_table(render_context: vw.RenderContext) -> None:
        result = vw.Source(name="users").create_table().__vw_render__(render_context)
        assert result == "CREATE TABLE users ()"

    def it_creates_table_with_schema(render_context: vw.RenderContext) -> None:
        result = (
            vw.Source(name="users")
            .create_table(
                {
                    "id": dtypes.integer(),
                    "email": dtypes.varchar(255),
                }
            )
            .__vw_render__(render_context)
        )
        assert result == "CREATE TABLE users (id INTEGER, email VARCHAR(255))"

    def it_creates_ctas_via_source(render_context: vw.RenderContext) -> None:
        query = vw.Source(name="orders").select(vw.col("*"))
        result = vw.Source(name="backup").create_table().as_select(query).__vw_render__(render_context)
        assert result == "CREATE TABLE backup AS SELECT * FROM orders"

    def it_chains_modifiers_via_source(render_context: vw.RenderContext) -> None:
        query = vw.Source(name="orders").select(vw.col("*"))
        result = (
            vw.Source(name="temp_backup")
            .create_table()
            .as_select(query)
            .temporary()
            .if_not_exists()
            .__vw_render__(render_context)
        )
        assert result == "CREATE TEMPORARY TABLE IF NOT EXISTS temp_backup AS SELECT * FROM orders"


def describe_create_table_render():
    """Tests for CreateTable.render() method."""

    def it_renders_with_default_config() -> None:
        result = CreateTable(name="users", schema={"id": dtypes.integer()}).render()
        assert result.sql == "CREATE TABLE users (id INTEGER)"
        assert result.params == {}

    def it_renders_ctas_with_params() -> None:
        query = vw.Source(name="orders").select(vw.col("*")).where(vw.col("year") == vw.param("year", 2024))
        result = CreateTable(name="backup").as_select(query).render()
        assert result.sql == "CREATE TABLE backup AS SELECT * FROM orders WHERE (year = $year)"
        assert result.params == {"year": 2024}


def describe_sqlserver_ctas():
    """Tests for SQL Server CTAS behavior."""

    def it_raises_not_implemented_for_sqlserver() -> None:
        query = vw.Source(name="orders").select(vw.col("*"))
        stmt = CreateTable(name="backup").as_select(query)
        config = vw.RenderConfig(dialect=vw.Dialect.SQLSERVER)

        with pytest.raises(NotImplementedError) as exc_info:
            stmt.render(config)

        assert "SQL Server" in str(exc_info.value)
        assert "SELECT INTO" in str(exc_info.value)
