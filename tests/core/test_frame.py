"""Tests for window frame boundary definitions."""

from vw.core.states import (
    CurrentRow,
    Following,
    FrameClause,
    Preceding,
    UnboundedFollowing,
    UnboundedPreceding,
)
from vw.postgres import (
    CURRENT_ROW,
    UNBOUNDED_FOLLOWING,
    UNBOUNDED_PRECEDING,
    F,
    col,
    following,
    preceding,
    render,
)


def describe_unbounded_preceding_constant() -> None:
    """Test UNBOUNDED_PRECEDING constant."""

    def it_is_unbounded_preceding_instance() -> None:
        """UNBOUNDED_PRECEDING should be UnboundedPreceding instance."""
        assert isinstance(UNBOUNDED_PRECEDING, UnboundedPreceding)

    def it_is_frozen() -> None:
        """UnboundedPreceding should be immutable (frozen dataclass)."""
        import dataclasses

        import pytest

        with pytest.raises(dataclasses.FrozenInstanceError):
            UNBOUNDED_PRECEDING.foo = "bar"  # type: ignore

    def it_renders_correctly() -> None:
        """UNBOUNDED_PRECEDING should render as 'UNBOUNDED PRECEDING'."""
        expr = F.sum(col("amount")).over(order_by=[col("date")]).rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
        result = render(expr)
        assert "UNBOUNDED PRECEDING" in result.query


def describe_unbounded_following_constant() -> None:
    """Test UNBOUNDED_FOLLOWING constant."""

    def it_is_unbounded_following_instance() -> None:
        """UNBOUNDED_FOLLOWING should be UnboundedFollowing instance."""
        assert isinstance(UNBOUNDED_FOLLOWING, UnboundedFollowing)

    def it_is_frozen() -> None:
        """UnboundedFollowing should be immutable (frozen dataclass)."""
        import dataclasses

        import pytest

        with pytest.raises(dataclasses.FrozenInstanceError):
            UNBOUNDED_FOLLOWING.foo = "bar"  # type: ignore

    def it_renders_correctly() -> None:
        """UNBOUNDED_FOLLOWING should render as 'UNBOUNDED FOLLOWING'."""
        expr = F.sum(col("amount")).over(order_by=[col("date")]).rows_between(CURRENT_ROW, UNBOUNDED_FOLLOWING)
        result = render(expr)
        assert "UNBOUNDED FOLLOWING" in result.query


def describe_current_row_constant() -> None:
    """Test CURRENT_ROW constant."""

    def it_is_current_row_instance() -> None:
        """CURRENT_ROW should be CurrentRow instance."""
        assert isinstance(CURRENT_ROW, CurrentRow)

    def it_is_frozen() -> None:
        """CurrentRow should be immutable (frozen dataclass)."""
        import dataclasses

        import pytest

        with pytest.raises(dataclasses.FrozenInstanceError):
            CURRENT_ROW.foo = "bar"  # type: ignore

    def it_renders_correctly() -> None:
        """CURRENT_ROW should render as 'CURRENT ROW'."""
        expr = F.sum(col("amount")).over(order_by=[col("date")]).rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
        result = render(expr)
        assert "CURRENT ROW" in result.query


def describe_preceding_function() -> None:
    """Test preceding(n) function."""

    def it_creates_preceding_instance() -> None:
        """preceding(n) should create Preceding instance."""
        p = preceding(3)
        assert isinstance(p, Preceding)

    def it_stores_count() -> None:
        """preceding(n) should store count value."""
        p = preceding(5)
        assert p.count == 5

    def it_accepts_different_counts() -> None:
        """preceding(n) should work with different count values."""
        assert preceding(1).count == 1
        assert preceding(10).count == 10
        assert preceding(100).count == 100

    def it_is_frozen() -> None:
        """Preceding should be immutable (frozen dataclass)."""
        import dataclasses

        import pytest

        p = preceding(3)
        with pytest.raises(dataclasses.FrozenInstanceError):
            p.count = 5  # type: ignore

    def it_renders_correctly() -> None:
        """preceding(n) should render as 'n PRECEDING'."""
        expr = F.avg(col("amount")).over(order_by=[col("date")]).rows_between(preceding(3), CURRENT_ROW)
        result = render(expr)
        assert "3 PRECEDING" in result.query

    def it_renders_with_different_counts() -> None:
        """preceding(n) should render different values correctly."""
        expr1 = F.sum(col("x")).over(order_by=[col("id")]).rows_between(preceding(1), CURRENT_ROW)
        assert "1 PRECEDING" in render(expr1).query

        expr2 = F.sum(col("x")).over(order_by=[col("id")]).rows_between(preceding(7), CURRENT_ROW)
        assert "7 PRECEDING" in render(expr2).query


def describe_following_function() -> None:
    """Test following(n) function."""

    def it_creates_following_instance() -> None:
        """following(n) should create Following instance."""
        f = following(3)
        assert isinstance(f, Following)

    def it_stores_count() -> None:
        """following(n) should store count value."""
        f = following(5)
        assert f.count == 5

    def it_accepts_different_counts() -> None:
        """following(n) should work with different count values."""
        assert following(1).count == 1
        assert following(10).count == 10
        assert following(100).count == 100

    def it_is_frozen() -> None:
        """Following should be immutable (frozen dataclass)."""
        import dataclasses

        import pytest

        f = following(3)
        with pytest.raises(dataclasses.FrozenInstanceError):
            f.count = 5  # type: ignore

    def it_renders_correctly() -> None:
        """following(n) should render as 'n FOLLOWING'."""
        expr = F.sum(col("amount")).over(order_by=[col("date")]).rows_between(CURRENT_ROW, following(5))
        result = render(expr)
        assert "5 FOLLOWING" in result.query

    def it_renders_with_different_counts() -> None:
        """following(n) should render different values correctly."""
        expr1 = F.sum(col("x")).over(order_by=[col("id")]).rows_between(CURRENT_ROW, following(1))
        assert "1 FOLLOWING" in render(expr1).query

        expr2 = F.sum(col("x")).over(order_by=[col("id")]).rows_between(CURRENT_ROW, following(10))
        assert "10 FOLLOWING" in render(expr2).query


def describe_frame_clause_state() -> None:
    """Test FrameClause state dataclass."""

    def it_creates_with_rows_mode() -> None:
        """FrameClause should store ROWS mode."""
        frame = FrameClause(mode="ROWS", start=UNBOUNDED_PRECEDING, end=CURRENT_ROW)
        assert frame.mode == "ROWS"
        assert isinstance(frame.start, UnboundedPreceding)
        assert isinstance(frame.end, CurrentRow)

    def it_creates_with_range_mode() -> None:
        """FrameClause should store RANGE mode."""
        frame = FrameClause(mode="RANGE", start=UNBOUNDED_PRECEDING, end=CURRENT_ROW)
        assert frame.mode == "RANGE"
        assert isinstance(frame.start, UnboundedPreceding)
        assert isinstance(frame.end, CurrentRow)

    def it_creates_with_preceding_boundary() -> None:
        """FrameClause should work with preceding() boundaries."""
        frame = FrameClause(mode="ROWS", start=preceding(3), end=CURRENT_ROW)
        assert isinstance(frame.start, Preceding)
        assert frame.start.count == 3

    def it_creates_with_following_boundary() -> None:
        """FrameClause should work with following() boundaries."""
        frame = FrameClause(mode="ROWS", start=CURRENT_ROW, end=following(5))
        assert isinstance(frame.end, Following)
        assert frame.end.count == 5

    def it_creates_with_none_start() -> None:
        """FrameClause should allow None for start (defaults applied at render time)."""
        frame = FrameClause(mode="ROWS", start=None, end=CURRENT_ROW)
        assert frame.start is None
        assert isinstance(frame.end, CurrentRow)

    def it_creates_with_none_end() -> None:
        """FrameClause should allow None for end (defaults applied at render time)."""
        frame = FrameClause(mode="ROWS", start=UNBOUNDED_PRECEDING, end=None)
        assert isinstance(frame.start, UnboundedPreceding)
        assert frame.end is None

    def it_creates_with_exclude_clause() -> None:
        """FrameClause should store EXCLUDE clause."""
        frame = FrameClause(mode="ROWS", start=UNBOUNDED_PRECEDING, end=CURRENT_ROW, exclude="CURRENT ROW")
        assert frame.exclude == "CURRENT ROW"

    def it_creates_without_exclude_clause() -> None:
        """FrameClause should allow None for exclude."""
        frame = FrameClause(mode="ROWS", start=UNBOUNDED_PRECEDING, end=CURRENT_ROW)
        assert frame.exclude is None

    def it_is_frozen() -> None:
        """FrameClause should be immutable (frozen dataclass)."""
        import dataclasses

        import pytest

        frame = FrameClause(mode="ROWS", start=UNBOUNDED_PRECEDING, end=CURRENT_ROW)
        with pytest.raises(dataclasses.FrozenInstanceError):
            frame.mode = "RANGE"  # type: ignore


def describe_rows_between_rendering() -> None:
    """Test ROWS BETWEEN frame clause rendering."""

    def it_renders_unbounded_preceding_to_current_row() -> None:
        """ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW."""
        expr = F.sum(col("amount")).over(order_by=[col("date")]).rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
        result = render(expr)
        assert "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" in result.query

    def it_renders_current_row_to_unbounded_following() -> None:
        """ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING."""
        expr = F.sum(col("amount")).over(order_by=[col("date")]).rows_between(CURRENT_ROW, UNBOUNDED_FOLLOWING)
        result = render(expr)
        assert "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING" in result.query

    def it_renders_n_preceding_to_current_row() -> None:
        """ROWS BETWEEN n PRECEDING AND CURRENT ROW."""
        expr = F.avg(col("amount")).over(order_by=[col("date")]).rows_between(preceding(3), CURRENT_ROW)
        result = render(expr)
        assert "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW" in result.query

    def it_renders_n_preceding_to_n_following() -> None:
        """ROWS BETWEEN n PRECEDING AND n FOLLOWING."""
        expr = F.avg(col("amount")).over(order_by=[col("date")]).rows_between(preceding(2), following(2))
        result = render(expr)
        assert "ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING" in result.query

    def it_renders_current_row_to_n_following() -> None:
        """ROWS BETWEEN CURRENT ROW AND n FOLLOWING."""
        expr = F.sum(col("amount")).over(order_by=[col("date")]).rows_between(CURRENT_ROW, following(5))
        result = render(expr)
        assert "ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING" in result.query

    def it_renders_unbounded_preceding_to_n_preceding() -> None:
        """ROWS BETWEEN UNBOUNDED PRECEDING AND n PRECEDING."""
        expr = F.sum(col("amount")).over(order_by=[col("date")]).rows_between(UNBOUNDED_PRECEDING, preceding(1))
        result = render(expr)
        assert "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING" in result.query

    def it_renders_n_following_to_unbounded_following() -> None:
        """ROWS BETWEEN n FOLLOWING AND UNBOUNDED FOLLOWING."""
        expr = F.sum(col("amount")).over(order_by=[col("date")]).rows_between(following(1), UNBOUNDED_FOLLOWING)
        result = render(expr)
        assert "ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING" in result.query


def describe_range_between_rendering() -> None:
    """Test RANGE BETWEEN frame clause rendering."""

    def it_renders_unbounded_preceding_to_current_row() -> None:
        """RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW."""
        expr = F.sum(col("amount")).over(order_by=[col("date")]).range_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
        result = render(expr)
        assert "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" in result.query

    def it_renders_current_row_to_unbounded_following() -> None:
        """RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING."""
        expr = F.sum(col("amount")).over(order_by=[col("date")]).range_between(CURRENT_ROW, UNBOUNDED_FOLLOWING)
        result = render(expr)
        assert "RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING" in result.query

    def it_renders_n_preceding_to_current_row() -> None:
        """RANGE BETWEEN n PRECEDING AND CURRENT ROW."""
        expr = F.avg(col("amount")).over(order_by=[col("date")]).range_between(preceding(7), CURRENT_ROW)
        result = render(expr)
        assert "RANGE BETWEEN 7 PRECEDING AND CURRENT ROW" in result.query

    def it_distinguishes_from_rows_mode() -> None:
        """RANGE should render differently from ROWS."""
        rows_expr = F.sum(col("x")).over(order_by=[col("id")]).rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
        range_expr = F.sum(col("x")).over(order_by=[col("id")]).range_between(UNBOUNDED_PRECEDING, CURRENT_ROW)

        rows_result = render(rows_expr)
        range_result = render(range_expr)

        assert "ROWS BETWEEN" in rows_result.query
        assert "RANGE BETWEEN" in range_result.query
        assert "ROWS BETWEEN" not in range_result.query
        assert "RANGE BETWEEN" not in rows_result.query


def describe_exclude_clause_rendering() -> None:
    """Test EXCLUDE clause rendering."""

    def it_renders_exclude_current_row() -> None:
        """EXCLUDE CURRENT ROW should render correctly."""
        expr = (
            F.sum(col("amount"))
            .over(order_by=[col("date")])
            .rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
            .exclude("CURRENT ROW")
        )
        result = render(expr)
        assert "EXCLUDE CURRENT ROW" in result.query

    def it_renders_exclude_group() -> None:
        """EXCLUDE GROUP should render correctly."""
        expr = (
            F.sum(col("amount"))
            .over(order_by=[col("date")])
            .rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
            .exclude("GROUP")
        )
        result = render(expr)
        assert "EXCLUDE GROUP" in result.query

    def it_renders_exclude_ties() -> None:
        """EXCLUDE TIES should render correctly."""
        expr = (
            F.sum(col("amount"))
            .over(order_by=[col("date")])
            .rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
            .exclude("TIES")
        )
        result = render(expr)
        assert "EXCLUDE TIES" in result.query

    def it_renders_exclude_no_others() -> None:
        """EXCLUDE NO OTHERS should render correctly."""
        expr = (
            F.sum(col("amount"))
            .over(order_by=[col("date")])
            .rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
            .exclude("NO OTHERS")
        )
        result = render(expr)
        assert "EXCLUDE NO OTHERS" in result.query

    def it_renders_without_exclude_when_none() -> None:
        """Frame without exclude should not include EXCLUDE keyword."""
        expr = F.sum(col("amount")).over(order_by=[col("date")]).rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
        result = render(expr)
        assert "EXCLUDE" not in result.query

    def it_renders_exclude_with_range_between() -> None:
        """EXCLUDE should work with RANGE BETWEEN."""
        expr = (
            F.sum(col("amount"))
            .over(order_by=[col("date")])
            .range_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
            .exclude("CURRENT ROW")
        )
        result = render(expr)
        assert "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW" in result.query


def describe_frame_boundary_combinations() -> None:
    """Test various valid combinations of frame boundaries."""

    def it_handles_preceding_to_preceding() -> None:
        """n PRECEDING to n PRECEDING should render correctly."""
        expr = F.sum(col("x")).over(order_by=[col("id")]).rows_between(preceding(5), preceding(2))
        result = render(expr)
        assert "ROWS BETWEEN 5 PRECEDING AND 2 PRECEDING" in result.query

    def it_handles_following_to_following() -> None:
        """n FOLLOWING to n FOLLOWING should render correctly."""
        expr = F.sum(col("x")).over(order_by=[col("id")]).rows_between(following(1), following(3))
        result = render(expr)
        assert "ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING" in result.query

    def it_handles_preceding_to_following() -> None:
        """n PRECEDING to n FOLLOWING should render correctly."""
        expr = F.sum(col("x")).over(order_by=[col("id")]).rows_between(preceding(1), following(1))
        result = render(expr)
        assert "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING" in result.query

    def it_handles_unbounded_preceding_to_unbounded_following() -> None:
        """UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING should render correctly."""
        expr = F.sum(col("x")).over(order_by=[col("id")]).rows_between(UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING)
        result = render(expr)
        assert "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING" in result.query


def describe_frame_with_window_functions() -> None:
    """Test frame clauses with different window functions."""

    def it_works_with_sum() -> None:
        """Frame clauses should work with SUM."""
        expr = F.sum(col("amount")).over(order_by=[col("date")]).rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
        result = render(expr)
        assert result.query.startswith("SUM(amount)")
        assert "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" in result.query

    def it_works_with_avg() -> None:
        """Frame clauses should work with AVG."""
        expr = F.avg(col("price")).over(order_by=[col("date")]).rows_between(preceding(7), CURRENT_ROW)
        result = render(expr)
        assert result.query.startswith("AVG(price)")
        assert "ROWS BETWEEN 7 PRECEDING AND CURRENT ROW" in result.query

    def it_works_with_count() -> None:
        """Frame clauses should work with COUNT."""
        expr = F.count(col("id")).over(order_by=[col("created")]).rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
        result = render(expr)
        assert result.query.startswith("COUNT(id)")
        assert "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" in result.query

    def it_works_with_min() -> None:
        """Frame clauses should work with MIN."""
        expr = F.min(col("score")).over(order_by=[col("date")]).range_between(preceding(3), following(3))
        result = render(expr)
        assert result.query.startswith("MIN(score)")
        assert "RANGE BETWEEN 3 PRECEDING AND 3 FOLLOWING" in result.query

    def it_works_with_max() -> None:
        """Frame clauses should work with MAX."""
        expr = F.max(col("value")).over(order_by=[col("timestamp")]).rows_between(CURRENT_ROW, following(10))
        result = render(expr)
        assert result.query.startswith("MAX(value)")
        assert "ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING" in result.query


def describe_identity_comparison() -> None:
    """Test that frame boundary instances use identity comparison."""

    def it_unbounded_preceding_instances_not_equal() -> None:
        """Two UnboundedPreceding instances should not be equal (eq=False)."""
        u1 = UnboundedPreceding()
        u2 = UnboundedPreceding()
        assert u1 is not u2
        # eq=False means identity comparison
        assert (u1 == u2) == (u1 is u2)

    def it_unbounded_following_instances_not_equal() -> None:
        """Two UnboundedFollowing instances should not be equal (eq=False)."""
        u1 = UnboundedFollowing()
        u2 = UnboundedFollowing()
        assert u1 is not u2
        # eq=False means identity comparison
        assert (u1 == u2) == (u1 is u2)

    def it_current_row_instances_not_equal() -> None:
        """Two CurrentRow instances should not be equal (eq=False)."""
        c1 = CurrentRow()
        c2 = CurrentRow()
        assert c1 is not c2
        # eq=False means identity comparison
        assert (c1 == c2) == (c1 is c2)

    def it_preceding_instances_not_equal() -> None:
        """Two Preceding instances with same count should not be equal (eq=False)."""
        p1 = preceding(3)
        p2 = preceding(3)
        assert p1 is not p2
        assert p1.count == p2.count  # Same count
        # eq=False means identity comparison
        assert (p1 == p2) == (p1 is p2)

    def it_following_instances_not_equal() -> None:
        """Two Following instances with same count should not be equal (eq=False)."""
        f1 = following(5)
        f2 = following(5)
        assert f1 is not f2
        assert f1.count == f2.count  # Same count
        # eq=False means identity comparison
        assert (f1 == f2) == (f1 is f2)


def describe_constants_are_singletons() -> None:
    """Test that module-level constants are single instances."""

    def it_unbounded_preceding_is_singleton() -> None:
        """UNBOUNDED_PRECEDING should always reference the same instance."""
        from vw.postgres import UNBOUNDED_PRECEDING as UP1
        from vw.postgres import UNBOUNDED_PRECEDING as UP2

        assert UP1 is UP2
        assert UP1 is UNBOUNDED_PRECEDING

    def it_unbounded_following_is_singleton() -> None:
        """UNBOUNDED_FOLLOWING should always reference the same instance."""
        from vw.postgres import UNBOUNDED_FOLLOWING as UF1
        from vw.postgres import UNBOUNDED_FOLLOWING as UF2

        assert UF1 is UF2
        assert UF1 is UNBOUNDED_FOLLOWING

    def it_current_row_is_singleton() -> None:
        """CURRENT_ROW should always reference the same instance."""
        from vw.postgres import CURRENT_ROW as CR1
        from vw.postgres import CURRENT_ROW as CR2

        assert CR1 is CR2
        assert CR1 is CURRENT_ROW
