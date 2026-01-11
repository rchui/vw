"""Integration tests for CASE/WHEN expressions."""

import vw
from tests.utils import sql


def describe_case_expressions():
    """Tests for CASE/WHEN/THEN/ELSE expressions."""

    def it_generates_simple_case_with_otherwise(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END FROM users
        """
        stmt = vw.Source(name="users").select(
            vw.when(vw.col("status") == vw.col("'active'")).then(vw.col("1")).otherwise(vw.col("0"))
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_case_without_otherwise(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT CASE WHEN status = 'active' THEN 1 END FROM users
        """
        stmt = vw.Source(name="users").select(vw.when(vw.col("status") == vw.col("'active'")).then(vw.col("1")))
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_case_with_multiple_branches(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT CASE WHEN status = 'active' THEN 1 WHEN status = 'pending' THEN 2 ELSE 0 END FROM users
        """
        stmt = vw.Source(name="users").select(
            vw.when(vw.col("status") == vw.col("'active'"))
            .then(vw.col("1"))
            .when(vw.col("status") == vw.col("'pending'"))
            .then(vw.col("2"))
            .otherwise(vw.col("0"))
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_case_with_many_branches(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT CASE
                WHEN status = 'active' THEN 'A'
                WHEN status = 'pending' THEN 'P'
                WHEN status = 'inactive' THEN 'I'
                WHEN status = 'deleted' THEN 'D'
                ELSE 'U'
            END FROM users
        """
        stmt = vw.Source(name="users").select(
            vw.when(vw.col("status") == vw.col("'active'"))
            .then(vw.col("'A'"))
            .when(vw.col("status") == vw.col("'pending'"))
            .then(vw.col("'P'"))
            .when(vw.col("status") == vw.col("'inactive'"))
            .then(vw.col("'I'"))
            .when(vw.col("status") == vw.col("'deleted'"))
            .then(vw.col("'D'"))
            .otherwise(vw.col("'U'"))
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_case_with_complex_conditions(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT CASE WHEN age >= 18 THEN 'adult' WHEN age >= 13 THEN 'teen' ELSE 'child' END FROM users
        """
        stmt = vw.Source(name="users").select(
            vw.when(vw.col("age") >= vw.col("18"))
            .then(vw.col("'adult'"))
            .when(vw.col("age") >= vw.col("13"))
            .then(vw.col("'teen'"))
            .otherwise(vw.col("'child'"))
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_case_with_parameters(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT CASE WHEN status = $active_status THEN $active_value ELSE $default_value END FROM users
        """
        stmt = vw.Source(name="users").select(
            vw.when(vw.col("status") == vw.param("active_status", "active"))
            .then(vw.param("active_value", 1))
            .otherwise(vw.param("default_value", 0))
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"active_status": "active", "active_value": 1, "default_value": 0},
        )

    def it_generates_case_with_alias(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END AS is_active FROM users
        """
        stmt = vw.Source(name="users").select(
            vw.when(vw.col("status") == vw.col("'active'")).then(vw.col("1")).otherwise(vw.col("0")).alias("is_active")
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_case_in_where_clause(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM orders WHERE (CASE WHEN priority = 'high' THEN 1 ELSE 0 END = 1)
        """
        stmt = (
            vw.Source(name="orders")
            .select(vw.col("*"))
            .where(
                vw.when(vw.col("priority") == vw.col("'high'")).then(vw.col("1")).otherwise(vw.col("0")) == vw.col("1")
            )
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_multiple_case_expressions(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT
                CASE WHEN status = 'active' THEN 1 ELSE 0 END AS is_active,
                CASE WHEN role = 'admin' THEN 1 ELSE 0 END AS is_admin
            FROM users
        """
        stmt = vw.Source(name="users").select(
            vw.when(vw.col("status") == vw.col("'active'")).then(vw.col("1")).otherwise(vw.col("0")).alias("is_active"),
            vw.when(vw.col("role") == vw.col("'admin'")).then(vw.col("1")).otherwise(vw.col("0")).alias("is_admin"),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_nested_case_expressions(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT CASE WHEN status = 'active' THEN CASE WHEN role = 'admin' THEN 'active_admin' ELSE 'active_user' END ELSE 'inactive' END FROM users
        """
        inner_case = (
            vw.when(vw.col("role") == vw.col("'admin'"))
            .then(vw.col("'active_admin'"))
            .otherwise(vw.col("'active_user'"))
        )
        stmt = vw.Source(name="users").select(
            vw.when(vw.col("status") == vw.col("'active'")).then(inner_case).otherwise(vw.col("'inactive'"))
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})
