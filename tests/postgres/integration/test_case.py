"""Integration tests for CASE/WHEN expressions."""

from tests.utils import sql
from vw.postgres import col, param, ref, render, when


def describe_basic_case() -> None:
    def it_renders_single_when_with_else() -> None:
        expected_sql = """
            SELECT CASE WHEN status = $a THEN $one ELSE $zero END
            FROM users
        """

        q = ref("users").select(
            when(col("status") == param("a", "active")).then(param("one", 1)).otherwise(param("zero", 0))
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"a": "active", "one": 1, "zero": 0}

    def it_renders_single_when_without_else() -> None:
        expected_sql = """
            SELECT CASE WHEN status = $a THEN $one END
            FROM users
        """

        q = ref("users").select(when(col("status") == param("a", "active")).then(param("one", 1)).end())
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"a": "active", "one": 1}


def describe_multiple_when() -> None:
    def it_renders_multiple_when_clauses() -> None:
        expected_sql = """
            SELECT CASE WHEN age >= $adult THEN $alabel WHEN age >= $teen THEN $tlabel ELSE $other END
            FROM users
        """

        q = ref("users").select(
            when(col("age") >= param("adult", 18))
            .then(param("alabel", "adult"))
            .when(col("age") >= param("teen", 13))
            .then(param("tlabel", "teen"))
            .otherwise(param("other", "child"))
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"adult": 18, "alabel": "adult", "teen": 13, "tlabel": "teen", "other": "child"}

    def it_renders_three_when_clauses() -> None:
        expected_sql = """
            SELECT CASE WHEN score >= $a THEN $alabel WHEN score >= $b THEN $blabel WHEN score >= $c THEN $clabel ELSE $dlabel END
            FROM results
        """

        q = ref("results").select(
            when(col("score") >= param("a", 90))
            .then(param("alabel", "A"))
            .when(col("score") >= param("b", 80))
            .then(param("blabel", "B"))
            .when(col("score") >= param("c", 70))
            .then(param("clabel", "C"))
            .otherwise(param("dlabel", "F"))
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"a": 90, "alabel": "A", "b": 80, "blabel": "B", "c": 70, "clabel": "C", "dlabel": "F"}


def describe_case_in_select() -> None:
    def it_renders_case_as_aliased_column() -> None:
        expected_sql = """
            SELECT id, CASE WHEN active = $t THEN $yes ELSE $no END AS status_label
            FROM users
        """

        q = ref("users").select(
            col("id"),
            when(col("active") == param("t", True))
            .then(param("yes", "yes"))
            .otherwise(param("no", "no"))
            .alias("status_label"),
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"t": True, "yes": "yes", "no": "no"}


def describe_case_in_where() -> None:
    def it_renders_case_in_where_clause() -> None:
        expected_sql = """
            SELECT id
            FROM orders
            WHERE CASE WHEN priority = $high THEN $one ELSE $zero END > $threshold
        """

        q = (
            ref("orders")
            .select(col("id"))
            .where(
                (when(col("priority") == param("high", "high")).then(param("one", 1)).otherwise(param("zero", 0)))
                > param("threshold", 0)
            )
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"high": "high", "one": 1, "zero": 0, "threshold": 0}


def describe_nested_case() -> None:
    def it_renders_nested_case_as_result() -> None:
        expected_sql = """
            SELECT CASE WHEN tier = $p THEN CASE WHEN score >= $hi THEN $gold ELSE $silver END ELSE $bronze END
            FROM members
        """

        inner = when(col("score") >= param("hi", 90)).then(param("gold", "gold")).otherwise(param("silver", "silver"))
        q = ref("members").select(
            when(col("tier") == param("p", "premium")).then(inner).otherwise(param("bronze", "bronze"))
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"p": "premium", "hi": 90, "gold": "gold", "silver": "silver", "bronze": "bronze"}


def describe_case_with_parameters() -> None:
    def it_collects_all_parameters() -> None:
        expected_sql = """
            SELECT id, CASE WHEN status = $s1 THEN $r1 WHEN status = $s2 THEN $r2 ELSE $r3 END
            FROM orders
        """

        q = ref("orders").select(
            col("id"),
            when(col("status") == param("s1", "pending"))
            .then(param("r1", 1))
            .when(col("status") == param("s2", "shipped"))
            .then(param("r2", 2))
            .otherwise(param("r3", 0)),
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"s1": "pending", "r1": 1, "s2": "shipped", "r2": 2, "r3": 0}
