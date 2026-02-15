"""Integration tests for DuckDB file reading."""

from tests.utils import sql
from vw.duckdb import CSV, JSON, JSONL, F, Parquet, col, file, lit, render


def describe_csv_file_reading():
    def it_reads_csv_with_header():
        expected_sql = """
            SELECT name, age
            FROM read_csv('data/users.csv', header = TRUE)
        """

        query = file("data/users.csv", format=CSV(header=True)).select(col("name"), col("age"))
        result = render(query)

        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_filters_csv_data():
        expected_sql = """
            SELECT name, city
            FROM read_csv('data/users.csv', header = TRUE)
            WHERE age > 28
        """

        query = (
            file("data/users.csv", format=CSV(header=True)).select(col("name"), col("city")).where(col("age") > lit(28))
        )
        result = render(query)

        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_aggregates_csv_data():
        expected_sql = """
            SELECT category, SUM(amount) AS total
            FROM read_csv('sales.csv', header = TRUE)
            GROUP BY category
            ORDER BY category ASC
        """

        query = (
            file("sales.csv", format=CSV(header=True))
            .select(col("category"), F.sum(col("amount")).alias("total"))
            .group_by(col("category"))
            .order_by(col("category").asc())
        )
        result = render(query)

        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_reads_csv_with_custom_delimiter():
        expected_sql = """
            SELECT name, age
            FROM read_csv('data.psv', header = TRUE, delim = '|')
        """

        query = file("data.psv", format=CSV(header=True, delim="|")).select(col("name"), col("age"))
        result = render(query)

        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_reads_csv_with_skip_and_all_varchar():
        expected_sql = """
            SELECT *
            FROM read_csv('raw_data.csv', all_varchar = TRUE, skip = 2)
        """

        query = file("raw_data.csv", format=CSV(all_varchar=True, skip=2)).select(col("*"))
        result = render(query)

        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_parquet_file_reading():
    def it_reads_parquet_file():
        expected_sql = """
            SELECT name, age
            FROM read_parquet('data/users.parquet')
        """

        query = file("data/users.parquet", format=Parquet()).select(col("name"), col("age"))
        result = render(query)

        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_filters_parquet_data():
        expected_sql = """
            SELECT name
            FROM read_parquet('users.parquet')
            WHERE age >= 30
        """

        query = file("users.parquet", format=Parquet()).select(col("name")).where(col("age") >= lit(30))
        result = render(query)

        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_reads_parquet_with_options():
        expected_sql = """
            SELECT *
            FROM read_parquet('data.parquet', filename = TRUE, hive_partitioning = TRUE)
        """

        query = file("data.parquet", format=Parquet(filename=True, hive_partitioning=True)).select(col("*"))
        result = render(query)

        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_json_file_reading():
    def it_reads_json_file():
        expected_sql = """
            SELECT name, age
            FROM read_json('data/users.json')
        """

        query = file("data/users.json", format=JSON()).select(col("name"), col("age"))
        result = render(query)

        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_reads_json_with_options():
        expected_sql = """
            SELECT *
            FROM read_json('data.json', ignore_errors = TRUE, compression = 'gzip')
        """

        query = file("data.json", format=JSON(ignore_errors=True, compression="gzip")).select(col("*"))
        result = render(query)

        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_jsonl_file_reading():
    def it_reads_jsonl_file():
        expected_sql = """
            SELECT name, age
            FROM read_json('events.jsonl', format = 'newline_delimited')
        """

        query = file("events.jsonl", format=JSONL()).select(col("name"), col("age"))
        result = render(query)

        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_filters_jsonl_data():
        expected_sql = """
            SELECT name
            FROM read_json('events.jsonl', format = 'newline_delimited')
            WHERE age < 30
            ORDER BY name ASC
        """

        query = (
            file("events.jsonl", format=JSONL())
            .select(col("name"))
            .where(col("age") < lit(30))
            .order_by(col("name").asc())
        )
        result = render(query)

        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_reads_jsonl_with_options():
        expected_sql = """
            SELECT *
            FROM read_json('data.jsonl', format = 'newline_delimited', ignore_errors = TRUE)
        """

        query = file("data.jsonl", format=JSONL(ignore_errors=True)).select(col("*"))
        result = render(query)

        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_multiple_file_reading():
    def it_reads_multiple_csv_files():
        expected_sql = """
            SELECT name, score
            FROM read_csv(['file1.csv', 'file2.csv'], header = TRUE)
            ORDER BY name ASC
        """

        query = (
            file("file1.csv", "file2.csv", format=CSV(header=True))
            .select(col("name"), col("score"))
            .order_by(col("name").asc())
        )
        result = render(query)

        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_reads_wildcard_csv_files():
        expected_sql = """
            SELECT *
            FROM read_csv('data/*.csv', header = TRUE, union_by_name = TRUE)
        """

        query = file("data/*.csv", format=CSV(header=True, union_by_name=True)).select(col("*"))
        result = render(query)

        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_file_with_joins():
    def it_joins_csv_file_with_table():
        expected_sql = """
            SELECT u.name, f.score
            FROM users AS u
            INNER JOIN read_csv('scores.csv', header = TRUE) AS f ON (u.id = f.user_id)
        """

        from vw.duckdb import ref

        users = ref("users").alias("u")
        scores = file("scores.csv", format=CSV(header=True)).alias("f")

        query = users.select(users.col("name"), scores.col("score")).join.inner(
            scores, on=[users.col("id") == scores.col("user_id")]
        )
        result = render(query)

        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_file_with_cte():
    def it_uses_file_in_cte():
        expected_sql = """
            WITH user_scores AS (
                SELECT name, score
                FROM read_csv('scores.csv', header = TRUE)
                WHERE score > 90
            )
            SELECT name, score
            FROM user_scores
            ORDER BY score DESC
        """

        from vw.duckdb import cte

        high_scores = cte(
            "user_scores",
            file("scores.csv", format=CSV(header=True)).select(col("name"), col("score")).where(col("score") > lit(90)),
        )

        query = high_scores.select(col("name"), col("score")).order_by(col("score").desc())
        result = render(query)

        assert result.query == sql(expected_sql)
        assert result.params == {}
