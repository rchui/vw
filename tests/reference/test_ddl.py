"""Unit tests for DDL functionality."""

import pytest

import vw.reference as vw
from vw.reference import dtypes
from vw.reference.ddl import CreateTable, CreateView, DropTable, DropView


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


def describe_drop_table():
    """Tests for DropTable class."""

    def it_renders_drop_table(render_context: vw.RenderContext) -> None:
        result = DropTable(name="users").__vw_render__(render_context)
        assert result == "DROP TABLE users"

    def it_renders_drop_table_if_exists(render_context: vw.RenderContext) -> None:
        result = DropTable(name="users").if_exists().__vw_render__(render_context)
        assert result == "DROP TABLE IF EXISTS users"

    def it_renders_drop_table_cascade(render_context: vw.RenderContext) -> None:
        result = DropTable(name="users").cascade().__vw_render__(render_context)
        assert result == "DROP TABLE users CASCADE"

    def it_renders_drop_table_if_exists_cascade(render_context: vw.RenderContext) -> None:
        result = DropTable(name="users").if_exists().cascade().__vw_render__(render_context)
        assert result == "DROP TABLE IF EXISTS users CASCADE"


def describe_create_view():
    """Tests for CreateView class."""

    def it_renders_create_view(render_context: vw.RenderContext) -> None:
        query = vw.Source(name="users").select(vw.col("*"))
        result = CreateView(name="active_users", query=query).__vw_render__(render_context)
        assert result == "CREATE VIEW active_users AS SELECT * FROM users"

    def it_renders_create_or_replace_view(render_context: vw.RenderContext) -> None:
        query = vw.Source(name="users").select(vw.col("id"), vw.col("name"))
        result = CreateView(name="user_names", query=query).or_replace().__vw_render__(render_context)
        assert result == "CREATE OR REPLACE VIEW user_names AS SELECT id, name FROM users"

    def it_renders_view_with_complex_query(render_context: vw.RenderContext) -> None:
        query = (
            vw.Source(name="orders")
            .select(vw.col("customer_id"), vw.col("SUM(total)").alias("total"))
            .group_by(vw.col("customer_id"))
        )
        result = CreateView(name="customer_totals", query=query).__vw_render__(render_context)
        assert result == (
            "CREATE VIEW customer_totals AS SELECT customer_id, SUM(total) AS total FROM orders GROUP BY customer_id"
        )


def describe_drop_view():
    """Tests for DropView class."""

    def it_renders_drop_view(render_context: vw.RenderContext) -> None:
        result = DropView(name="active_users").__vw_render__(render_context)
        assert result == "DROP VIEW active_users"

    def it_renders_drop_view_if_exists(render_context: vw.RenderContext) -> None:
        result = DropView(name="active_users").if_exists().__vw_render__(render_context)
        assert result == "DROP VIEW IF EXISTS active_users"

    def it_renders_drop_view_cascade(render_context: vw.RenderContext) -> None:
        result = DropView(name="active_users").cascade().__vw_render__(render_context)
        assert result == "DROP VIEW active_users CASCADE"

    def it_renders_drop_view_if_exists_cascade(render_context: vw.RenderContext) -> None:
        result = DropView(name="active_users").if_exists().cascade().__vw_render__(render_context)
        assert result == "DROP VIEW IF EXISTS active_users CASCADE"


def describe_table_accessor():
    """Tests for Source.table accessor."""

    def it_creates_empty_table(render_context: vw.RenderContext) -> None:
        result = vw.Source(name="users").table.create().__vw_render__(render_context)
        assert result == "CREATE TABLE users ()"

    def it_creates_table_with_schema(render_context: vw.RenderContext) -> None:
        result = (
            vw.Source(name="users")
            .table.create(
                {
                    "id": dtypes.integer(),
                    "email": dtypes.varchar(255),
                }
            )
            .__vw_render__(render_context)
        )
        assert result == "CREATE TABLE users (id INTEGER, email VARCHAR(255))"

    def it_creates_ctas_via_accessor(render_context: vw.RenderContext) -> None:
        query = vw.Source(name="orders").select(vw.col("*"))
        result = vw.Source(name="backup").table.create().as_select(query).__vw_render__(render_context)
        assert result == "CREATE TABLE backup AS SELECT * FROM orders"

    def it_chains_modifiers_via_accessor(render_context: vw.RenderContext) -> None:
        query = vw.Source(name="orders").select(vw.col("*"))
        result = (
            vw.Source(name="temp_backup")
            .table.create()
            .as_select(query)
            .temporary()
            .if_not_exists()
            .__vw_render__(render_context)
        )
        assert result == "CREATE TEMPORARY TABLE IF NOT EXISTS temp_backup AS SELECT * FROM orders"

    def it_drops_table_via_accessor(render_context: vw.RenderContext) -> None:
        result = vw.Source(name="users").table.drop().__vw_render__(render_context)
        assert result == "DROP TABLE users"

    def it_drops_table_if_exists_via_accessor(render_context: vw.RenderContext) -> None:
        result = vw.Source(name="users").table.drop().if_exists().__vw_render__(render_context)
        assert result == "DROP TABLE IF EXISTS users"

    def it_drops_table_cascade_via_accessor(render_context: vw.RenderContext) -> None:
        result = vw.Source(name="users").table.drop().cascade().__vw_render__(render_context)
        assert result == "DROP TABLE users CASCADE"


def describe_view_accessor():
    """Tests for Source.view accessor."""

    def it_creates_view_via_accessor(render_context: vw.RenderContext) -> None:
        query = vw.Source(name="users").select(vw.col("*"))
        result = vw.Source(name="all_users").view.create(query).__vw_render__(render_context)
        assert result == "CREATE VIEW all_users AS SELECT * FROM users"

    def it_creates_or_replace_view_via_accessor(render_context: vw.RenderContext) -> None:
        query = vw.Source(name="users").select(vw.col("id"), vw.col("name"))
        result = vw.Source(name="user_names").view.create(query).or_replace().__vw_render__(render_context)
        assert result == "CREATE OR REPLACE VIEW user_names AS SELECT id, name FROM users"

    def it_drops_view_via_accessor(render_context: vw.RenderContext) -> None:
        result = vw.Source(name="all_users").view.drop().__vw_render__(render_context)
        assert result == "DROP VIEW all_users"

    def it_drops_view_if_exists_via_accessor(render_context: vw.RenderContext) -> None:
        result = vw.Source(name="all_users").view.drop().if_exists().__vw_render__(render_context)
        assert result == "DROP VIEW IF EXISTS all_users"

    def it_drops_view_cascade_via_accessor(render_context: vw.RenderContext) -> None:
        result = vw.Source(name="all_users").view.drop().cascade().__vw_render__(render_context)
        assert result == "DROP VIEW all_users CASCADE"


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


def describe_drop_table_render():
    """Tests for DropTable.render() method."""

    def it_renders_with_default_config() -> None:
        result = DropTable(name="users").render()
        assert result.sql == "DROP TABLE users"
        assert result.params == {}

    def it_renders_if_exists_with_default_config() -> None:
        result = DropTable(name="users").if_exists().render()
        assert result.sql == "DROP TABLE IF EXISTS users"
        assert result.params == {}


def describe_create_view_render():
    """Tests for CreateView.render() method."""

    def it_renders_with_default_config() -> None:
        query = vw.Source(name="users").select(vw.col("*"))
        result = CreateView(name="all_users", query=query).render()
        assert result.sql == "CREATE VIEW all_users AS SELECT * FROM users"
        assert result.params == {}

    def it_renders_with_params() -> None:
        query = vw.Source(name="users").select(vw.col("*")).where(vw.col("status") == vw.param("status", "active"))
        result = CreateView(name="active_users", query=query).render()
        assert result.sql == "CREATE VIEW active_users AS SELECT * FROM users WHERE (status = $status)"
        assert result.params == {"status": "active"}


def describe_drop_view_render():
    """Tests for DropView.render() method."""

    def it_renders_with_default_config() -> None:
        result = DropView(name="all_users").render()
        assert result.sql == "DROP VIEW all_users"
        assert result.params == {}

    def it_renders_if_exists_with_default_config() -> None:
        result = DropView(name="all_users").if_exists().render()
        assert result.sql == "DROP VIEW IF EXISTS all_users"
        assert result.params == {}


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
