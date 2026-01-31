"""Integration tests for DDL statements."""

import pytest

import vw.reference as vw
from tests.utils import sql
from vw.reference import dtypes
from vw.reference.functions import F


def describe_create_table_with_schema():
    """Tests for CREATE TABLE with explicit schema."""

    def it_creates_simple_table(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            CREATE TABLE users (id INTEGER, name VARCHAR(100), email VARCHAR(255))
        """
        stmt = vw.Source(name="users").table.create(
            {
                "id": dtypes.integer(),
                "name": dtypes.varchar(100),
                "email": dtypes.varchar(255),
            }
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_creates_table_with_numeric_types(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            CREATE TABLE products (
                id BIGINT,
                price DECIMAL(10,2),
                quantity INTEGER,
                discount FLOAT
            )
        """
        stmt = vw.Source(name="products").table.create(
            {
                "id": dtypes.bigint(),
                "price": dtypes.decimal(10, 2),
                "quantity": dtypes.integer(),
                "discount": dtypes.float(),
            }
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_creates_table_with_datetime_types(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            CREATE TABLE events (
                id INTEGER,
                event_date DATE,
                event_time TIME,
                created_at TIMESTAMP
            )
        """
        stmt = vw.Source(name="events").table.create(
            {
                "id": dtypes.integer(),
                "event_date": dtypes.date(),
                "event_time": dtypes.time(),
                "created_at": dtypes.timestamp(),
            }
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_creates_table_with_text_and_binary_types(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            CREATE TABLE documents (
                id UUID,
                title TEXT,
                content TEXT,
                data BYTEA
            )
        """
        stmt = vw.Source(name="documents").table.create(
            {
                "id": dtypes.uuid(),
                "title": dtypes.text(),
                "content": dtypes.text(),
                "data": dtypes.bytea(),
            }
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_creates_table_with_json_types(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            CREATE TABLE configs (id INTEGER, settings JSON, metadata JSONB)
        """
        stmt = vw.Source(name="configs").table.create(
            {
                "id": dtypes.integer(),
                "settings": dtypes.json(),
                "metadata": dtypes.jsonb(),
            }
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_create_table_modifiers():
    """Tests for CREATE TABLE with modifiers."""

    def it_creates_temporary_table(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            CREATE TEMPORARY TABLE temp_results (id INTEGER, value FLOAT)
        """
        stmt = (
            vw.Source(name="temp_results")
            .table.create(
                {
                    "id": dtypes.integer(),
                    "value": dtypes.float(),
                }
            )
            .temporary()
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_creates_table_if_not_exists(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            CREATE TABLE IF NOT EXISTS cache (key VARCHAR(255), value TEXT)
        """
        stmt = (
            vw.Source(name="cache")
            .table.create(
                {
                    "key": dtypes.varchar(255),
                    "value": dtypes.text(),
                }
            )
            .if_not_exists()
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_creates_or_replaces_table(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            CREATE OR REPLACE TABLE snapshots (id INTEGER, data JSONB)
        """
        stmt = (
            vw.Source(name="snapshots")
            .table.create(
                {
                    "id": dtypes.integer(),
                    "data": dtypes.jsonb(),
                }
            )
            .or_replace()
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_combines_all_modifiers(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            CREATE OR REPLACE TEMPORARY TABLE IF NOT EXISTS temp_data (value INTEGER)
        """
        stmt = (
            vw.Source(name="temp_data")
            .table.create({"value": dtypes.integer()})
            .or_replace()
            .temporary()
            .if_not_exists()
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_create_table_as_select():
    """Tests for CREATE TABLE AS SELECT (CTAS)."""

    def it_creates_table_from_simple_select(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            CREATE TABLE backup AS SELECT * FROM orders
        """
        query = vw.Source(name="orders").select(vw.col("*"))
        stmt = vw.Source(name="backup").table.create().as_select(query)
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_creates_table_from_filtered_select(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            CREATE TABLE active_users AS
            SELECT id, name, email FROM users WHERE (status = 'active')
        """
        query = (
            vw.Source(name="users")
            .select(vw.col("id"), vw.col("name"), vw.col("email"))
            .where(vw.col("status") == vw.col("'active'"))
        )
        stmt = vw.Source(name="active_users").table.create().as_select(query)
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_creates_table_from_aggregated_select(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            CREATE TABLE customer_totals AS
            SELECT customer_id, SUM(amount) AS total, COUNT(*) AS order_count
            FROM orders
            GROUP BY customer_id
        """
        query = (
            vw.Source(name="orders")
            .select(
                vw.col("customer_id"),
                F.sum(vw.col("amount")).alias("total"),
                F.count().alias("order_count"),
            )
            .group_by(vw.col("customer_id"))
        )
        stmt = vw.Source(name="customer_totals").table.create().as_select(query)
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_creates_table_from_joined_select(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            CREATE TABLE order_details AS
            SELECT o.id, o.total, c.name AS customer_name
            FROM orders AS o
            INNER JOIN customers AS c ON (o.customer_id = c.id)
        """
        orders = vw.Source(name="orders").alias("o")
        customers = vw.Source(name="customers").alias("c")
        query = orders.join.inner(customers, on=[vw.col("o.customer_id") == vw.col("c.id")]).select(
            vw.col("o.id"), vw.col("o.total"), vw.col("c.name").alias("customer_name")
        )
        stmt = vw.Source(name="order_details").table.create().as_select(query)
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_creates_temporary_table_from_select(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            CREATE TEMPORARY TABLE temp_results AS
            SELECT * FROM results WHERE (score > 90)
        """
        query = vw.Source(name="results").select(vw.col("*")).where(vw.col("score") > vw.col("90"))
        stmt = vw.Source(name="temp_results").table.create().as_select(query).temporary()
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_creates_table_with_parameters(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            CREATE TABLE year_orders AS
            SELECT * FROM orders WHERE (year = $year) AND (status = $status)
        """
        query = (
            vw.Source(name="orders")
            .select(vw.col("*"))
            .where(vw.col("year") == vw.param("year", 2024))
            .where(vw.col("status") == vw.param("status", "completed"))
        )
        stmt = vw.Source(name="year_orders").table.create().as_select(query)
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"year": 2024, "status": "completed"})


def describe_drop_table():
    """Tests for DROP TABLE."""

    def it_drops_table(render_config: vw.RenderConfig) -> None:
        expected_sql = "DROP TABLE users"
        stmt = vw.Source(name="users").table.drop()
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_drops_table_if_exists(render_config: vw.RenderConfig) -> None:
        expected_sql = "DROP TABLE IF EXISTS users"
        stmt = vw.Source(name="users").table.drop().if_exists()
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_drops_table_cascade(render_config: vw.RenderConfig) -> None:
        expected_sql = "DROP TABLE users CASCADE"
        stmt = vw.Source(name="users").table.drop().cascade()
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_drops_table_if_exists_cascade(render_config: vw.RenderConfig) -> None:
        expected_sql = "DROP TABLE IF EXISTS users CASCADE"
        stmt = vw.Source(name="users").table.drop().if_exists().cascade()
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})


def describe_create_view():
    """Tests for CREATE VIEW."""

    def it_creates_view_from_simple_select(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            CREATE VIEW active_users AS SELECT * FROM users WHERE (status = 'active')
        """
        query = vw.Source(name="users").select(vw.col("*")).where(vw.col("status") == vw.col("'active'"))
        stmt = vw.Source(name="active_users").view.create(query)
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_creates_or_replace_view(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            CREATE OR REPLACE VIEW user_summary AS
            SELECT id, name, email FROM users
        """
        query = vw.Source(name="users").select(vw.col("id"), vw.col("name"), vw.col("email"))
        stmt = vw.Source(name="user_summary").view.create(query).or_replace()
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_creates_view_with_aggregation(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            CREATE VIEW customer_totals AS
            SELECT customer_id, SUM(amount) AS total, COUNT(*) AS order_count
            FROM orders
            GROUP BY customer_id
        """
        query = (
            vw.Source(name="orders")
            .select(
                vw.col("customer_id"),
                F.sum(vw.col("amount")).alias("total"),
                F.count().alias("order_count"),
            )
            .group_by(vw.col("customer_id"))
        )
        stmt = vw.Source(name="customer_totals").view.create(query)
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_creates_view_with_joins(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            CREATE VIEW order_details AS
            SELECT o.id, o.total, c.name AS customer_name
            FROM orders AS o
            INNER JOIN customers AS c ON (o.customer_id = c.id)
        """
        orders = vw.Source(name="orders").alias("o")
        customers = vw.Source(name="customers").alias("c")
        query = orders.join.inner(customers, on=[vw.col("o.customer_id") == vw.col("c.id")]).select(
            vw.col("o.id"), vw.col("o.total"), vw.col("c.name").alias("customer_name")
        )
        stmt = vw.Source(name="order_details").view.create(query)
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_creates_view_with_parameters(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            CREATE VIEW filtered_orders AS
            SELECT * FROM orders WHERE (year = $year)
        """
        query = vw.Source(name="orders").select(vw.col("*")).where(vw.col("year") == vw.param("year", 2024))
        stmt = vw.Source(name="filtered_orders").view.create(query)
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"year": 2024})


def describe_drop_view():
    """Tests for DROP VIEW."""

    def it_drops_view(render_config: vw.RenderConfig) -> None:
        expected_sql = "DROP VIEW active_users"
        stmt = vw.Source(name="active_users").view.drop()
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_drops_view_if_exists(render_config: vw.RenderConfig) -> None:
        expected_sql = "DROP VIEW IF EXISTS active_users"
        stmt = vw.Source(name="active_users").view.drop().if_exists()
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_drops_view_cascade(render_config: vw.RenderConfig) -> None:
        expected_sql = "DROP VIEW active_users CASCADE"
        stmt = vw.Source(name="active_users").view.drop().cascade()
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_drops_view_if_exists_cascade(render_config: vw.RenderConfig) -> None:
        expected_sql = "DROP VIEW IF EXISTS active_users CASCADE"
        stmt = vw.Source(name="active_users").view.drop().if_exists().cascade()
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})


def describe_create_table_with_cte():
    """Tests for CREATE TABLE AS SELECT with CTEs."""

    def it_creates_table_from_cte(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active AS (SELECT * FROM users WHERE (status = 'active'))
            CREATE TABLE active_users_backup AS SELECT * FROM active
        """
        active_cte = vw.cte(
            "active",
            vw.Source(name="users").select(vw.col("*")).where(vw.col("status") == vw.col("'active'")),
        )
        query = active_cte.select(vw.col("*"))
        stmt = vw.Source(name="active_users_backup").table.create().as_select(query)
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_create_view_with_cte():
    """Tests for CREATE VIEW with CTEs."""

    def it_creates_view_from_cte(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active AS (SELECT * FROM users WHERE (status = 'active'))
            CREATE VIEW active_users_view AS SELECT * FROM active
        """
        active_cte = vw.cte(
            "active",
            vw.Source(name="users").select(vw.col("*")).where(vw.col("status") == vw.col("'active'")),
        )
        query = active_cte.select(vw.col("*"))
        stmt = vw.Source(name="active_users_view").view.create(query)
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_sqlserver_ctas():
    """Tests for SQL Server CTAS behavior."""

    def it_raises_not_implemented_for_ctas() -> None:
        query = vw.Source(name="orders").select(vw.col("*"))
        stmt = vw.Source(name="backup").table.create().as_select(query)
        config = vw.RenderConfig(dialect=vw.Dialect.SQLSERVER)

        with pytest.raises(NotImplementedError) as exc_info:
            stmt.render(config)

        assert "SQL Server" in str(exc_info.value)
        assert "SELECT INTO" in str(exc_info.value)

    def it_allows_schema_based_create_for_sqlserver() -> None:
        stmt = vw.Source(name="users").table.create(
            {
                "id": dtypes.integer(),
                "name": dtypes.varchar(100),
            }
        )
        config = vw.RenderConfig(dialect=vw.Dialect.SQLSERVER)
        result = stmt.render(config)
        assert result.sql == "CREATE TABLE users (id INTEGER, name VARCHAR(100))"
