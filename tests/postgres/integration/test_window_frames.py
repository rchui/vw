"""Integration tests for window frame clauses."""

from tests.utils import sql
from vw.postgres import (
    CURRENT_ROW,
    UNBOUNDED_FOLLOWING,
    UNBOUNDED_PRECEDING,
    F,
    col,
    following,
    preceding,
    render,
    source,
)


def describe_window_frames():
    """Test window frame clauses (ROWS BETWEEN, RANGE BETWEEN, EXCLUDE)."""

    def describe_rows_between():
        """Test ROWS BETWEEN frame clauses."""

        def test_rows_unbounded_preceding_to_current_row():
            """ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW."""
            q = source("sales").select(
                col("date"),
                F.sum(col("amount"))
                .over(order_by=[col("date").asc()])
                .rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
                .alias("running_total"),
            )
            result = render(q)
            assert result.query == sql(
                "SELECT date, SUM(amount) OVER (ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total FROM sales"
            )
            assert result.params == {}

        def test_rows_current_row_to_unbounded_following():
            """ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING."""
            q = source("sales").select(
                col("date"),
                F.sum(col("amount"))
                .over(order_by=[col("date").asc()])
                .rows_between(CURRENT_ROW, UNBOUNDED_FOLLOWING)
                .alias("future_total"),
            )
            result = render(q)
            assert result.query == sql(
                "SELECT date, SUM(amount) OVER (ORDER BY date ASC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS future_total FROM sales"
            )
            assert result.params == {}

        def test_rows_n_preceding_to_current_row():
            """ROWS BETWEEN n PRECEDING AND CURRENT ROW."""
            q = source("sales").select(
                col("date"),
                F.avg(col("amount"))
                .over(order_by=[col("date").asc()])
                .rows_between(preceding(3), CURRENT_ROW)
                .alias("moving_avg"),
            )
            result = render(q)
            assert result.query == sql(
                "SELECT date, AVG(amount) OVER (ORDER BY date ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS moving_avg FROM sales"
            )
            assert result.params == {}

        def test_rows_n_preceding_to_n_following():
            """ROWS BETWEEN n PRECEDING AND n FOLLOWING."""
            q = source("sales").select(
                col("date"),
                F.avg(col("amount"))
                .over(order_by=[col("date").asc()])
                .rows_between(preceding(2), following(2))
                .alias("centered_avg"),
            )
            result = render(q)
            assert result.query == sql(
                "SELECT date, AVG(amount) OVER (ORDER BY date ASC ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS centered_avg FROM sales"
            )
            assert result.params == {}

        def test_rows_current_row_to_n_following():
            """ROWS BETWEEN CURRENT ROW AND n FOLLOWING."""
            q = source("sales").select(
                col("date"),
                F.sum(col("amount"))
                .over(order_by=[col("date").asc()])
                .rows_between(CURRENT_ROW, following(5))
                .alias("next_5_total"),
            )
            result = render(q)
            assert result.query == sql(
                "SELECT date, SUM(amount) OVER (ORDER BY date ASC ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) AS next_5_total FROM sales"
            )
            assert result.params == {}

    def describe_range_between():
        """Test RANGE BETWEEN frame clauses."""

        def test_range_unbounded_preceding_to_current_row():
            """RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW."""
            q = source("sales").select(
                col("date"),
                F.sum(col("amount"))
                .over(order_by=[col("date").asc()])
                .range_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
                .alias("running_total"),
            )
            result = render(q)
            assert result.query == sql(
                "SELECT date, SUM(amount) OVER (ORDER BY date ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total FROM sales"
            )
            assert result.params == {}

        def test_range_current_row_to_unbounded_following():
            """RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING."""
            q = source("sales").select(
                col("date"),
                F.sum(col("amount"))
                .over(order_by=[col("date").asc()])
                .range_between(CURRENT_ROW, UNBOUNDED_FOLLOWING)
                .alias("future_total"),
            )
            result = render(q)
            assert result.query == sql(
                "SELECT date, SUM(amount) OVER (ORDER BY date ASC RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS future_total FROM sales"
            )
            assert result.params == {}

    def describe_exclude():
        """Test EXCLUDE clause."""

        def test_exclude_current_row():
            """EXCLUDE CURRENT ROW."""
            q = source("sales").select(
                col("date"),
                F.sum(col("amount"))
                .over(order_by=[col("date").asc()])
                .rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
                .exclude("CURRENT ROW")
                .alias("total_without_current"),
            )
            result = render(q)
            assert result.query == sql(
                "SELECT date, SUM(amount) OVER (ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS total_without_current FROM sales"
            )
            assert result.params == {}

        def test_exclude_group():
            """EXCLUDE GROUP."""
            q = source("sales").select(
                col("date"),
                F.sum(col("amount"))
                .over(order_by=[col("date").asc()])
                .rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
                .exclude("GROUP")
                .alias("total_without_group"),
            )
            result = render(q)
            assert result.query == sql(
                "SELECT date, SUM(amount) OVER (ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP) AS total_without_group FROM sales"
            )
            assert result.params == {}

        def test_exclude_ties():
            """EXCLUDE TIES."""
            q = source("sales").select(
                col("date"),
                F.sum(col("amount"))
                .over(order_by=[col("date").asc()])
                .rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
                .exclude("TIES")
                .alias("total_without_ties"),
            )
            result = render(q)
            assert result.query == sql(
                "SELECT date, SUM(amount) OVER (ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) AS total_without_ties FROM sales"
            )
            assert result.params == {}

        def test_exclude_no_others():
            """EXCLUDE NO OTHERS."""
            q = source("sales").select(
                col("date"),
                F.sum(col("amount"))
                .over(order_by=[col("date").asc()])
                .rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
                .exclude("NO OTHERS")
                .alias("total_with_all"),
            )
            result = render(q)
            assert result.query == sql(
                "SELECT date, SUM(amount) OVER (ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS) AS total_with_all FROM sales"
            )
            assert result.params == {}

    def describe_with_partition_by():
        """Test frame clauses with PARTITION BY."""

        def test_rows_between_with_partition_by():
            """ROWS BETWEEN with PARTITION BY."""
            q = source("sales").select(
                col("department"),
                col("date"),
                F.sum(col("amount"))
                .over(partition_by=[col("department")], order_by=[col("date").asc()])
                .rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
                .alias("dept_running_total"),
            )
            result = render(q)
            assert result.query == sql(
                "SELECT department, date, SUM(amount) OVER (PARTITION BY department ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS dept_running_total FROM sales"
            )
            assert result.params == {}

        def test_range_between_with_partition_by():
            """RANGE BETWEEN with PARTITION BY."""
            q = source("sales").select(
                col("department"),
                col("date"),
                F.avg(col("amount"))
                .over(partition_by=[col("department")], order_by=[col("date").asc()])
                .range_between(preceding(7), CURRENT_ROW)
                .alias("dept_7day_avg"),
            )
            result = render(q)
            assert result.query == sql(
                "SELECT department, date, AVG(amount) OVER (PARTITION BY department ORDER BY date ASC RANGE BETWEEN 7 PRECEDING AND CURRENT ROW) AS dept_7day_avg FROM sales"
            )
            assert result.params == {}
