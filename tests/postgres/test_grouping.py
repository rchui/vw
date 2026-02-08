"""Tests for ROLLUP, CUBE, GROUPING SETS, and GROUPING() in vw.postgres."""

from vw.core.render import RenderContext
from vw.core.states import Column, Cube, GroupingSets, Rollup
from vw.postgres import F, col, cube, grouping_sets, ref, render, rollup
from vw.postgres.render import render_state


def _ctx() -> RenderContext:
    from vw.core.render import ParamStyle, RenderConfig

    return RenderContext(config=RenderConfig(param_style=ParamStyle.DOLLAR))


# --- Rollup ---------------------------------------------------------------- #


def test_rollup_renders_single_column() -> None:
    state = Rollup(columns=(Column(name="region"),))
    assert render_state(state, _ctx()) == "ROLLUP (region)"


def test_rollup_renders_multiple_columns() -> None:
    state = Rollup(columns=(Column(name="region"), Column(name="product")))
    assert render_state(state, _ctx()) == "ROLLUP (region, product)"


def test_rollup_factory_renders_correctly() -> None:
    expr = rollup(col("region"), col("product"))
    assert isinstance(expr.state, Rollup)
    assert render_state(expr.state, _ctx()) == "ROLLUP (region, product)"


# --- Cube ------------------------------------------------------------------ #


def test_cube_renders_single_column() -> None:
    state = Cube(columns=(Column(name="region"),))
    assert render_state(state, _ctx()) == "CUBE (region)"


def test_cube_renders_multiple_columns() -> None:
    state = Cube(columns=(Column(name="region"), Column(name="product")))
    assert render_state(state, _ctx()) == "CUBE (region, product)"


def test_cube_factory_renders_correctly() -> None:
    expr = cube(col("region"), col("product"))
    assert isinstance(expr.state, Cube)
    assert render_state(expr.state, _ctx()) == "CUBE (region, product)"


# --- GroupingSets ---------------------------------------------------------- #


def test_grouping_sets_renders_single_set() -> None:
    state = GroupingSets(sets=((Column(name="region"),),))
    assert render_state(state, _ctx()) == "GROUPING SETS ((region))"


def test_grouping_sets_renders_multiple_sets() -> None:
    state = GroupingSets(
        sets=(
            (Column(name="region"), Column(name="product")),
            (Column(name="region"),),
            (),
        )
    )
    assert render_state(state, _ctx()) == "GROUPING SETS ((region, product), (region), ())"


def test_grouping_sets_renders_empty_set_for_grand_total() -> None:
    state = GroupingSets(sets=((),))
    assert render_state(state, _ctx()) == "GROUPING SETS (())"


def test_grouping_sets_factory_renders_correctly() -> None:
    expr = grouping_sets(
        (col("region"), col("product")),
        (col("region"),),
        (),
    )
    assert isinstance(expr.state, GroupingSets)
    assert render_state(expr.state, _ctx()) == "GROUPING SETS ((region, product), (region), ())"


# --- GROUPING() function --------------------------------------------------- #


def test_grouping_function_renders_single_column() -> None:
    result = render(ref("sales").select(F.grouping(col("region"))))
    assert result.query == "SELECT GROUPING(region) FROM sales"


def test_grouping_function_renders_multiple_columns() -> None:
    result = render(ref("sales").select(F.grouping(col("region"), col("product"))))
    assert result.query == "SELECT GROUPING(region, product) FROM sales"


# --- Integration: full query rendering ------------------------------------- #


def test_rollup_in_group_by() -> None:
    result = render(
        ref("sales")
        .select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
        .group_by(rollup(col("region"), col("product")))
    )
    assert result.query == "SELECT region, product, SUM(amount) AS total FROM sales GROUP BY ROLLUP (region, product)"


def test_cube_in_group_by() -> None:
    result = render(
        ref("sales")
        .select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
        .group_by(cube(col("region"), col("product")))
    )
    assert result.query == "SELECT region, product, SUM(amount) AS total FROM sales GROUP BY CUBE (region, product)"


def test_grouping_sets_in_group_by() -> None:
    result = render(
        ref("sales")
        .select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
        .group_by(
            grouping_sets(
                (col("region"), col("product")),
                (col("region"),),
                (),
            )
        )
    )
    assert (
        result.query
        == "SELECT region, product, SUM(amount) AS total FROM sales GROUP BY GROUPING SETS ((region, product), (region), ())"
    )


def test_grouping_sets_with_grouping_function() -> None:
    result = render(
        ref("sales")
        .select(
            col("region"),
            col("product"),
            F.sum(col("amount")).alias("total"),
            F.grouping(col("region")).alias("grp_region"),
        )
        .group_by(
            grouping_sets(
                (col("region"), col("product")),
                (col("region"),),
                (),
            )
        )
    )
    assert result.query == (
        "SELECT region, product, SUM(amount) AS total, GROUPING(region) AS grp_region "
        "FROM sales "
        "GROUP BY GROUPING SETS ((region, product), (region), ())"
    )
