"""Integration tests for Common Table Expressions (CTEs)."""

import vw.reference as vw
from tests.utils import sql


def describe_basic_ctes():
    """Tests for basic CTE functionality."""

    def it_generates_basic_cte(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active_users AS (
                SELECT *
                FROM users
                WHERE (status = 'active')
            )
            SELECT * FROM active_users
        """
        active_users = vw.cte(
            "active_users",
            vw.Source(name="users").select(vw.col("*")).where(vw.col("status") == vw.col("'active'")),
        )
        result = active_users.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_cte_with_qualified_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active_users AS (SELECT id, name FROM users)
            SELECT active_users.id, active_users.name FROM active_users
        """
        active_users = vw.cte(
            "active_users",
            vw.Source(name="users").select(vw.col("id"), vw.col("name")),
        )
        result = active_users.select(active_users.col("id"), active_users.col("name")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_cte_with_parameters(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active_users AS (SELECT * FROM users WHERE (status = $status))
            SELECT * FROM active_users
        """
        status = vw.param("status", "active")
        active_users = vw.cte(
            "active_users",
            vw.Source(name="users").select(vw.col("*")).where(vw.col("status") == status),
        )
        result = active_users.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"status": "active"})


def describe_cte_with_joins():
    """Tests for CTEs used in joins."""

    def it_generates_cte_in_join(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active_users AS (
                SELECT id, name
                FROM users
                WHERE (active = true)
            )
            SELECT orders.id, active_users.name
            FROM orders
            INNER JOIN active_users
                ON (orders.user_id = active_users.id)
        """
        active_users = vw.cte(
            "active_users",
            vw.Source(name="users").select(vw.col("id"), vw.col("name")).where(vw.col("active") == vw.col("true")),
        )
        orders = vw.Source(name="orders")
        result = (
            orders.join.inner(active_users, on=[orders.col("user_id") == active_users.col("id")])
            .select(orders.col("id"), active_users.col("name"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_multiple_ctes():
    """Tests for multiple CTEs."""

    def it_generates_multiple_ctes(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH
                active_users AS (
                    SELECT * FROM users WHERE (active = true)
                ),
                recent_orders AS (
                    SELECT * FROM orders WHERE (created_at > '2024-01-01')
                )
            SELECT *
            FROM active_users
            INNER JOIN recent_orders ON (active_users.id = recent_orders.user_id)
        """
        active_users = vw.cte(
            "active_users",
            vw.Source(name="users").select(vw.col("*")).where(vw.col("active") == vw.col("true")),
        )
        recent_orders = vw.cte(
            "recent_orders",
            vw.Source(name="orders").select(vw.col("*")).where(vw.col("created_at") > vw.col("'2024-01-01'")),
        )
        result = (
            active_users.join.inner(recent_orders, on=[active_users.col("id") == recent_orders.col("user_id")])
            .select(vw.col("*"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_cte_referencing_another_cte(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH base_users AS (SELECT * FROM users),
                 active_users AS (SELECT * FROM base_users WHERE (active = true))
            SELECT * FROM active_users
        """
        base_users = vw.cte(
            "base_users",
            vw.Source(name="users").select(vw.col("*")),
        )
        active_users = vw.cte(
            "active_users",
            base_users.select(vw.col("*")).where(vw.col("active") == vw.col("true")),
        )
        result = active_users.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_cte_with_clauses():
    """Tests for CTEs with various SQL clauses."""

    def it_generates_cte_with_group_by(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH customer_totals AS (SELECT customer_id, SUM(total) AS total FROM orders GROUP BY customer_id)
            SELECT * FROM customer_totals
        """
        customer_totals = vw.cte(
            "customer_totals",
            vw.Source(name="orders")
            .select(vw.col("customer_id"), vw.col("SUM(total)").alias("total"))
            .group_by(vw.col("customer_id")),
        )
        result = customer_totals.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_cte_with_having(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active_orders AS (SELECT * FROM orders WHERE (status = 'active'))
            SELECT customer_id, SUM(total)
            FROM active_orders
            GROUP BY customer_id
            HAVING (SUM(total) > 500)
        """
        active_orders = vw.cte(
            "active_orders",
            vw.Source(name="orders").select(vw.col("*")).where(vw.col("status") == vw.col("'active'")),
        )
        result = (
            active_orders.select(vw.col("customer_id"), vw.col("SUM(total)"))
            .group_by(vw.col("customer_id"))
            .having(vw.col("SUM(total)") > vw.col("500"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_cte_with_limit(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active_users AS (SELECT * FROM users WHERE (active = true))
            SELECT * FROM active_users ORDER BY id ASC LIMIT 10
        """
        active_users = vw.cte(
            "active_users",
            vw.Source(name="users").select(vw.col("*")).where(vw.col("active") == vw.col("true")),
        )
        result = active_users.select(vw.col("*")).order_by(vw.col("id").asc()).limit(10).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_recursive_ctes():
    """Tests for recursive CTEs."""

    def it_generates_recursive_cte_for_hierarchy(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH RECURSIVE org_hierarchy AS (
                (SELECT id, name, manager_id, 1 AS level
                FROM employees
                WHERE (manager_id IS NULL))
                UNION ALL
                (SELECT e.id, e.name, e.manager_id, h.level + 1
                FROM employees AS e
                INNER JOIN org_hierarchy AS h ON (e.manager_id = h.id))
            )
            SELECT * FROM org_hierarchy
        """
        # Anchor: top-level employees (no manager)
        anchor = (
            vw.Source(name="employees")
            .select(vw.col("id"), vw.col("name"), vw.col("manager_id"), vw.col("1").alias("level"))
            .where(vw.col("manager_id").is_null())
        )
        # Recursive: join employees to the CTE
        e = vw.Source(name="employees").alias("e")
        h = vw.Source(name="org_hierarchy").alias("h")
        recursive = e.join.inner(h, on=[vw.col("e.manager_id") == vw.col("h.id")]).select(
            vw.col("e.id"), vw.col("e.name"), vw.col("e.manager_id"), (vw.col("h.level") + vw.col("1"))
        )
        org = vw.cte("org_hierarchy", anchor + recursive, recursive=True)
        result = org.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_recursive_cte_for_graph_traversal(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH RECURSIVE paths AS (
                (SELECT source, target, 1 AS hops
                FROM edges
                WHERE (source = 'A'))
                UNION ALL
                (SELECT p.source, e.target, p.hops + 1
                FROM paths AS p
                INNER JOIN edges AS e ON (p.target = e.source))
            )
            SELECT * FROM paths
        """
        # Anchor: start from node 'A'
        anchor = (
            vw.Source(name="edges")
            .select(vw.col("source"), vw.col("target"), vw.col("1").alias("hops"))
            .where(vw.col("source") == vw.col("'A'"))
        )
        # Recursive: follow edges
        p = vw.Source(name="paths").alias("p")
        e = vw.Source(name="edges").alias("e")
        recursive = p.join.inner(e, on=[vw.col("p.target") == vw.col("e.source")]).select(
            vw.col("p.source"), vw.col("e.target"), vw.col("p.hops") + vw.col("1")
        )
        paths = vw.cte("paths", anchor + recursive, recursive=True)
        result = paths.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_recursive_cte_with_parameters(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH RECURSIVE descendants AS (
                (SELECT id, name, parent_id
                FROM categories
                WHERE (id = $root_id))
                UNION ALL
                (SELECT c.id, c.name, c.parent_id
                FROM categories AS c
                INNER JOIN descendants AS d ON (c.parent_id = d.id))
            )
            SELECT * FROM descendants
        """
        root_id = vw.param("root_id", 1)
        anchor = (
            vw.Source(name="categories")
            .select(vw.col("id"), vw.col("name"), vw.col("parent_id"))
            .where(vw.col("id") == root_id)
        )
        c = vw.Source(name="categories").alias("c")
        d = vw.Source(name="descendants").alias("d")
        recursive = c.join.inner(d, on=[vw.col("c.parent_id") == vw.col("d.id")]).select(
            vw.col("c.id"), vw.col("c.name"), vw.col("c.parent_id")
        )
        descendants = vw.cte("descendants", anchor + recursive, recursive=True)
        result = descendants.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"root_id": 1})

    def it_generates_recursive_cte_with_union_instead_of_union_all(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH RECURSIVE unique_paths AS (
                (SELECT node FROM nodes WHERE (node = 'start'))
                UNION
                (SELECT e.target FROM unique_paths AS p INNER JOIN edges AS e ON (p.node = e.source))
            )
            SELECT * FROM unique_paths
        """
        anchor = vw.Source(name="nodes").select(vw.col("node")).where(vw.col("node") == vw.col("'start'"))
        p = vw.Source(name="unique_paths").alias("p")
        e = vw.Source(name="edges").alias("e")
        recursive = p.join.inner(e, on=[vw.col("p.node") == vw.col("e.source")]).select(vw.col("e.target"))
        # Use | for UNION (deduplicates) instead of + for UNION ALL
        paths = vw.cte("unique_paths", anchor | recursive, recursive=True)
        result = paths.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_mixed_recursive_and_non_recursive_ctes(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH RECURSIVE
                active_employees AS (SELECT * FROM employees WHERE (active = true)),
                org_hierarchy AS (
                    (SELECT id, manager_id FROM active_employees WHERE (manager_id IS NULL))
                    UNION ALL
                    (SELECT e.id, e.manager_id
                    FROM active_employees AS e
                    INNER JOIN org_hierarchy AS h ON (e.manager_id = h.id))
                )
            SELECT * FROM org_hierarchy
        """
        # Non-recursive CTE: filter active employees
        active_employees = vw.cte(
            "active_employees",
            vw.Source(name="employees").select(vw.col("*")).where(vw.col("active") == vw.col("true")),
        )
        # Recursive CTE: build hierarchy from active employees
        anchor = active_employees.select(vw.col("id"), vw.col("manager_id")).where(vw.col("manager_id").is_null())
        e = vw.Source(name="active_employees").alias("e")
        h = vw.Source(name="org_hierarchy").alias("h")
        recursive = e.join.inner(h, on=[vw.col("e.manager_id") == vw.col("h.id")]).select(
            vw.col("e.id"), vw.col("e.manager_id")
        )
        org = vw.cte("org_hierarchy", anchor + recursive, recursive=True)
        result = org.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})
