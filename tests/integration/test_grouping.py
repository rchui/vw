"""Integration tests for GROUPING SETS, CUBE, ROLLUP, and GROUPING() function."""

import vw
from tests.utils import sql
from vw.functions import F


def describe_rollup():
    """Tests for ROLLUP in GROUP BY."""

    def it_generates_rollup_with_single_column(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT year, SUM(amount) FROM sales GROUP BY ROLLUP (year)
        """
        stmt = (
            vw.Source(name="sales").select(vw.col("year"), F.sum(vw.col("amount"))).group_by(vw.rollup(vw.col("year")))
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_rollup_with_multiple_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT year, quarter, month, SUM(amount)
            FROM sales
            GROUP BY ROLLUP (year, quarter, month)
        """
        stmt = (
            vw.Source(name="sales")
            .select(vw.col("year"), vw.col("quarter"), vw.col("month"), F.sum(vw.col("amount")))
            .group_by(vw.rollup(vw.col("year"), vw.col("quarter"), vw.col("month")))
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_rollup_with_where_clause(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT year, quarter, SUM(amount)
            FROM sales
            WHERE (status = 'completed')
            GROUP BY ROLLUP (year, quarter)
        """
        stmt = (
            vw.Source(name="sales")
            .select(vw.col("year"), vw.col("quarter"), F.sum(vw.col("amount")))
            .where(vw.col("status") == vw.col("'completed'"))
            .group_by(vw.rollup(vw.col("year"), vw.col("quarter")))
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_rollup_with_having_clause(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT year, SUM(amount)
            FROM sales
            GROUP BY ROLLUP (year)
            HAVING (SUM(amount) > 1000)
        """
        stmt = (
            vw.Source(name="sales")
            .select(vw.col("year"), F.sum(vw.col("amount")))
            .group_by(vw.rollup(vw.col("year")))
            .having(F.sum(vw.col("amount")) > vw.col("1000"))
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_cube():
    """Tests for CUBE in GROUP BY."""

    def it_generates_cube_with_single_column(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT region, SUM(amount) FROM sales GROUP BY CUBE (region)
        """
        stmt = (
            vw.Source(name="sales")
            .select(vw.col("region"), F.sum(vw.col("amount")))
            .group_by(vw.cube(vw.col("region")))
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_cube_with_multiple_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT region, product, SUM(amount)
            FROM sales
            GROUP BY CUBE (region, product)
        """
        stmt = (
            vw.Source(name="sales")
            .select(vw.col("region"), vw.col("product"), F.sum(vw.col("amount")))
            .group_by(vw.cube(vw.col("region"), vw.col("product")))
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_cube_with_parameters(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT region, product, SUM(amount)
            FROM sales
            WHERE (year = $year)
            GROUP BY CUBE (region, product)
        """
        stmt = (
            vw.Source(name="sales")
            .select(vw.col("region"), vw.col("product"), F.sum(vw.col("amount")))
            .where(vw.col("year") == vw.param("year", 2024))
            .group_by(vw.cube(vw.col("region"), vw.col("product")))
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"year": 2024})


def describe_grouping_sets():
    """Tests for GROUPING SETS in GROUP BY."""

    def it_generates_grouping_sets_with_single_set(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT year, region, SUM(amount)
            FROM sales
            GROUP BY GROUPING SETS ((year, region))
        """
        stmt = (
            vw.Source(name="sales")
            .select(vw.col("year"), vw.col("region"), F.sum(vw.col("amount")))
            .group_by(vw.grouping_sets((vw.col("year"), vw.col("region"))))
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_grouping_sets_with_multiple_sets(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT year, region, SUM(amount)
            FROM sales
            GROUP BY GROUPING SETS ((year, region), (year), (region))
        """
        stmt = (
            vw.Source(name="sales")
            .select(vw.col("year"), vw.col("region"), F.sum(vw.col("amount")))
            .group_by(
                vw.grouping_sets(
                    (vw.col("year"), vw.col("region")),
                    (vw.col("year"),),
                    (vw.col("region"),),
                )
            )
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_grouping_sets_with_empty_set_for_grand_total(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT year, region, SUM(amount)
            FROM sales
            GROUP BY GROUPING SETS ((year, region), (year), ())
        """
        stmt = (
            vw.Source(name="sales")
            .select(vw.col("year"), vw.col("region"), F.sum(vw.col("amount")))
            .group_by(
                vw.grouping_sets(
                    (vw.col("year"), vw.col("region")),
                    (vw.col("year"),),
                    (),
                )
            )
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_only_grand_total(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT SUM(amount)
            FROM sales
            GROUP BY GROUPING SETS (())
        """
        stmt = vw.Source(name="sales").select(F.sum(vw.col("amount"))).group_by(vw.grouping_sets(()))
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_mixed_grouping():
    """Tests for mixing regular columns with grouping constructs."""

    def it_generates_column_with_rollup(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT country, state, city, SUM(amount)
            FROM sales
            GROUP BY country, ROLLUP (state, city)
        """
        stmt = (
            vw.Source(name="sales")
            .select(vw.col("country"), vw.col("state"), vw.col("city"), F.sum(vw.col("amount")))
            .group_by(vw.col("country"), vw.rollup(vw.col("state"), vw.col("city")))
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_column_with_cube(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT year, region, product, SUM(amount)
            FROM sales
            GROUP BY year, CUBE (region, product)
        """
        stmt = (
            vw.Source(name="sales")
            .select(vw.col("year"), vw.col("region"), vw.col("product"), F.sum(vw.col("amount")))
            .group_by(vw.col("year"), vw.cube(vw.col("region"), vw.col("product")))
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_grouping_function():
    """Tests for the GROUPING() function."""

    def it_generates_grouping_with_single_column(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT year, SUM(amount), GROUPING(year)
            FROM sales
            GROUP BY ROLLUP (year)
        """
        stmt = (
            vw.Source(name="sales")
            .select(vw.col("year"), F.sum(vw.col("amount")), F.grouping(vw.col("year")))
            .group_by(vw.rollup(vw.col("year")))
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_grouping_with_multiple_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT year, region, SUM(amount), GROUPING(year, region)
            FROM sales
            GROUP BY CUBE (year, region)
        """
        stmt = (
            vw.Source(name="sales")
            .select(
                vw.col("year"),
                vw.col("region"),
                F.sum(vw.col("amount")),
                F.grouping(vw.col("year"), vw.col("region")),
            )
            .group_by(vw.cube(vw.col("year"), vw.col("region")))
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_grouping_with_alias(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT year, SUM(amount), GROUPING(year) AS is_total
            FROM sales
            GROUP BY ROLLUP (year)
        """
        stmt = (
            vw.Source(name="sales")
            .select(
                vw.col("year"),
                F.sum(vw.col("amount")),
                F.grouping(vw.col("year")).alias("is_total"),
            )
            .group_by(vw.rollup(vw.col("year")))
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_uses_grouping_in_case_expression(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT
                CASE WHEN GROUPING(year) = 1 THEN 'All Years' ELSE year END AS year_label,
                SUM(amount)
            FROM sales
            GROUP BY ROLLUP (year)
        """
        stmt = (
            vw.Source(name="sales")
            .select(
                vw.when(F.grouping(vw.col("year")) == vw.col("1"))
                .then(vw.col("'All Years'"))
                .otherwise(vw.col("year"))
                .alias("year_label"),
                F.sum(vw.col("amount")),
            )
            .group_by(vw.rollup(vw.col("year")))
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_full_queries():
    """Tests for complete queries with grouping constructs."""

    def it_generates_full_rollup_report(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT year, quarter, SUM(revenue) AS total_revenue, COUNT(*) AS order_count
            FROM orders
            WHERE (status = $status)
            GROUP BY ROLLUP (year, quarter)
            HAVING (SUM(revenue) > $min_revenue)
            ORDER BY year ASC, quarter ASC
        """
        stmt = (
            vw.Source(name="orders")
            .select(
                vw.col("year"),
                vw.col("quarter"),
                F.sum(vw.col("revenue")).alias("total_revenue"),
                F.count().alias("order_count"),
            )
            .where(vw.col("status") == vw.param("status", "completed"))
            .group_by(vw.rollup(vw.col("year"), vw.col("quarter")))
            .having(F.sum(vw.col("revenue")) > vw.param("min_revenue", 10000))
            .order_by(vw.col("year").asc(), vw.col("quarter").asc())
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"status": "completed", "min_revenue": 10000},
        )

    def it_generates_full_cube_analysis(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT
                region,
                product_category,
                SUM(sales) AS total_sales,
                AVG(sales) AS avg_sales,
                GROUPING(region, product_category) AS grouping_level
            FROM sales_data
            GROUP BY CUBE (region, product_category)
            ORDER BY GROUPING(region, product_category) ASC
        """
        stmt = (
            vw.Source(name="sales_data")
            .select(
                vw.col("region"),
                vw.col("product_category"),
                F.sum(vw.col("sales")).alias("total_sales"),
                F.avg(vw.col("sales")).alias("avg_sales"),
                F.grouping(vw.col("region"), vw.col("product_category")).alias("grouping_level"),
            )
            .group_by(vw.cube(vw.col("region"), vw.col("product_category")))
            .order_by(F.grouping(vw.col("region"), vw.col("product_category")).asc())
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})
