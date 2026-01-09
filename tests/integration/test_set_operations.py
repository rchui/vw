"""Integration tests for set operations: UNION, UNION ALL, INTERSECT, EXCEPT."""

import vw
from tests.utils import sql


def describe_set_operations():
    """Tests for UNION, UNION ALL, INTERSECT, EXCEPT."""

    def it_generates_union(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            (SELECT id FROM users) UNION (SELECT id FROM admins)
        """
        query1 = vw.Source(name="users").select(vw.col("id"))
        query2 = vw.Source(name="admins").select(vw.col("id"))
        result = (query1 | query2).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_union_all(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            (SELECT id FROM users) UNION ALL (SELECT id FROM admins)
        """
        query1 = vw.Source(name="users").select(vw.col("id"))
        query2 = vw.Source(name="admins").select(vw.col("id"))
        result = (query1 + query2).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_intersect(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            (SELECT id FROM users) INTERSECT (SELECT id FROM premium_users)
        """
        query1 = vw.Source(name="users").select(vw.col("id"))
        query2 = vw.Source(name="premium_users").select(vw.col("id"))
        result = (query1 & query2).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_except(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            (SELECT id FROM users) EXCEPT (SELECT id FROM banned_users)
        """
        query1 = vw.Source(name="users").select(vw.col("id"))
        query2 = vw.Source(name="banned_users").select(vw.col("id"))
        result = (query1 - query2).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_chains_multiple_unions(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            (
                (SELECT id FROM users)
                UNION
                (SELECT id FROM admins)
            )
            UNION
            (SELECT id FROM guests)
        """
        query1 = vw.Source(name="users").select(vw.col("id"))
        query2 = vw.Source(name="admins").select(vw.col("id"))
        query3 = vw.Source(name="guests").select(vw.col("id"))
        result = (query1 | query2 | query3).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_mixes_set_operations(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            ((SELECT id FROM users) UNION (SELECT id FROM admins)) EXCEPT (SELECT id FROM banned)
        """
        users = vw.Source(name="users").select(vw.col("id"))
        admins = vw.Source(name="admins").select(vw.col("id"))
        banned = vw.Source(name="banned").select(vw.col("id"))
        result = ((users | admins) - banned).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_preserves_parameters_in_set_operations(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            (SELECT id FROM users WHERE (status = :status1))
            UNION
            (SELECT id FROM admins WHERE (status = :status2))
        """
        query1 = vw.Source(name="users").select(vw.col("id")).where(vw.col("status") == vw.param("status1", "active"))
        query2 = (
            vw.Source(name="admins").select(vw.col("id")).where(vw.col("status") == vw.param("status2", "verified"))
        )
        result = (query1 | query2).render(config=render_config)
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"status1": "active", "status2": "verified"},
        )

    def it_uses_set_operation_as_subquery(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM ((SELECT id FROM users) UNION (SELECT id FROM admins)) AS all_ids
        """
        query1 = vw.Source(name="users").select(vw.col("id"))
        query2 = vw.Source(name="admins").select(vw.col("id"))
        combined = (query1 | query2).alias("all_ids")
        result = combined.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})
