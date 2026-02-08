"""Integration tests for null handling scalar functions."""

from tests.utils import sql
from vw.postgres import F, col, param, ref, render


def describe_null_handling_functions():
    """Test null handling functions (COALESCE, NULLIF, GREATEST, LEAST)."""

    def describe_coalesce():
        """Test COALESCE function."""

        def test_coalesce_two_args():
            expected_sql = """SELECT COALESCE(nickname, name) FROM users"""

            q = ref("users").select(F.coalesce(col("nickname"), col("name")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_coalesce_three_args():
            expected_sql = """SELECT COALESCE(nickname, display_name, name) FROM users"""

            q = ref("users").select(F.coalesce(col("nickname"), col("display_name"), col("name")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_coalesce_with_alias():
            expected_sql = """SELECT COALESCE(nickname, name) AS label FROM users"""

            q = ref("users").select(F.coalesce(col("nickname"), col("name")).alias("label"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_coalesce_with_param():
            expected_sql = """SELECT COALESCE(nickname, $fallback) FROM users"""

            q = ref("users").select(F.coalesce(col("nickname"), param("fallback", "unknown")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"fallback": "unknown"}

    def describe_nullif():
        """Test NULLIF function."""

        def test_nullif_basic():
            expected_sql = """SELECT NULLIF(value, sentinel) FROM data"""

            q = ref("data").select(F.nullif(col("value"), col("sentinel")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_nullif_with_param():
            expected_sql = """SELECT NULLIF(score, $zero) FROM results"""

            q = ref("results").select(F.nullif(col("score"), param("zero", 0)))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"zero": 0}

        def test_nullif_with_alias():
            expected_sql = """SELECT NULLIF(value, sentinel) AS clean_value FROM data"""

            q = ref("data").select(F.nullif(col("value"), col("sentinel")).alias("clean_value"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_greatest():
        """Test GREATEST function."""

        def test_greatest_two_args():
            expected_sql = """SELECT GREATEST(a, b) FROM data"""

            q = ref("data").select(F.greatest(col("a"), col("b")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_greatest_three_args():
            expected_sql = """SELECT GREATEST(a, b, c) FROM data"""

            q = ref("data").select(F.greatest(col("a"), col("b"), col("c")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_greatest_with_alias():
            expected_sql = """SELECT GREATEST(a, b) AS max_val FROM data"""

            q = ref("data").select(F.greatest(col("a"), col("b")).alias("max_val"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_least():
        """Test LEAST function."""

        def test_least_two_args():
            expected_sql = """SELECT LEAST(a, b) FROM data"""

            q = ref("data").select(F.least(col("a"), col("b")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_least_three_args():
            expected_sql = """SELECT LEAST(a, b, c) FROM data"""

            q = ref("data").select(F.least(col("a"), col("b"), col("c")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_least_with_alias():
            expected_sql = """SELECT LEAST(a, b) AS min_val FROM data"""

            q = ref("data").select(F.least(col("a"), col("b")).alias("min_val"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}
