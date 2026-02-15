"""Tests for DuckDB file reading functionality."""

from vw.core.states import File
from vw.duckdb import CSV, JSON, JSONL, F, Parquet, col, file, lit, render


def describe_file_factory():
    def it_creates_file_with_single_path():
        f = file("test.csv", format=CSV())
        assert isinstance(f.state, File)
        assert f.state.paths == ("test.csv",)
        assert isinstance(f.state.format, CSV)

    def it_creates_file_with_multiple_paths():
        f = file("f1.csv", "f2.csv", format=CSV())
        assert isinstance(f.state, File)
        assert f.state.paths == ("f1.csv", "f2.csv")

    def it_accepts_csv_with_options():
        f = file("test.csv", format=CSV(header=True, delim="|"))
        assert isinstance(f.state, File)
        assert isinstance(f.state.format, CSV)
        assert f.state.format.header is True
        assert f.state.format.delim == "|"


def describe_csv_basic_rendering():
    def it_renders_file_with_no_options():
        query = file("test.csv", format=CSV())
        result = render(query)
        assert result.query == "FROM read_csv('test.csv')"

    def it_renders_file_with_header_option():
        query = file("test.csv", format=CSV(header=True))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', header = TRUE)"

    def it_renders_file_with_header_false():
        query = file("test.csv", format=CSV(header=False))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', header = FALSE)"

    def it_renders_file_with_delim_option():
        query = file("test.csv", format=CSV(delim="|"))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', delim = '|')"

    def it_renders_file_with_multiple_options():
        query = file("test.csv", format=CSV(header=True, delim="|", skip=10))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', header = TRUE, delim = '|', skip = 10)"


def describe_csv_string_options():
    def it_renders_quote_option():
        query = file("test.csv", format=CSV(quote="'"))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', quote = ''')"

    def it_renders_escape_option():
        query = file("test.csv", format=CSV(escape="\\"))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', escape = '\\')"

    def it_renders_compression_option():
        query = file("test.csv", format=CSV(compression="gzip"))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', compression = 'gzip')"

    def it_renders_dateformat_option():
        query = file("test.csv", format=CSV(dateformat="%Y-%m-%d"))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', dateformat = '%Y-%m-%d')"

    def it_renders_timestampformat_option():
        query = file("test.csv", format=CSV(timestampformat="%Y-%m-%d %H:%M:%S"))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', timestampformat = '%Y-%m-%d %H:%M:%S')"

    def it_renders_decimal_separator_option():
        query = file("test.csv", format=CSV(decimal_separator=","))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', decimal_separator = ',')"


def describe_csv_boolean_options():
    def it_renders_all_varchar_option():
        query = file("test.csv", format=CSV(all_varchar=True))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', all_varchar = TRUE)"

    def it_renders_null_padding_option():
        query = file("test.csv", format=CSV(null_padding=True))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', null_padding = TRUE)"

    def it_renders_ignore_errors_option():
        query = file("test.csv", format=CSV(ignore_errors=True))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', ignore_errors = TRUE)"

    def it_renders_parallel_option():
        query = file("test.csv", format=CSV(parallel=False))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', parallel = FALSE)"

    def it_renders_filename_option():
        query = file("test.csv", format=CSV(filename=True))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', filename = TRUE)"

    def it_renders_hive_partitioning_option():
        query = file("test.csv", format=CSV(hive_partitioning=True))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', hive_partitioning = TRUE)"

    def it_renders_union_by_name_option():
        query = file("test.csv", format=CSV(union_by_name=True))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', union_by_name = TRUE)"


def describe_csv_integer_options():
    def it_renders_skip_option():
        query = file("test.csv", format=CSV(skip=5))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', skip = 5)"

    def it_renders_sample_size_option():
        query = file("test.csv", format=CSV(sample_size=1000))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', sample_size = 1000)"

    def it_renders_max_line_size_option():
        query = file("test.csv", format=CSV(max_line_size=1048576))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', max_line_size = 1048576)"


def describe_csv_complex_options():
    def it_renders_columns_option():
        query = file("test.csv", format=CSV(columns={"id": "INTEGER", "name": "VARCHAR"}))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', columns = {'id': 'INTEGER', 'name': 'VARCHAR'})"

    def it_renders_auto_type_candidates_option():
        query = file("test.csv", format=CSV(auto_type_candidates=["INTEGER", "VARCHAR"]))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', auto_type_candidates = ['INTEGER', 'VARCHAR'])"

    def it_renders_names_option():
        query = file("test.csv", format=CSV(names=["id", "name", "email"]))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', names = ['id', 'name', 'email'])"

    def it_renders_types_option():
        query = file("test.csv", format=CSV(types=["INTEGER", "VARCHAR", "VARCHAR"]))
        result = render(query)
        assert result.query == "FROM read_csv('test.csv', types = ['INTEGER', 'VARCHAR', 'VARCHAR'])"


def describe_multiple_files():
    def it_renders_multiple_file_paths():
        query = file("f1.csv", "f2.csv", format=CSV(header=True))
        result = render(query)
        assert result.query == "FROM read_csv(['f1.csv', 'f2.csv'], header = TRUE)"

    def it_renders_three_file_paths():
        query = file("f1.csv", "f2.csv", "f3.csv", format=CSV())
        result = render(query)
        assert result.query == "FROM read_csv(['f1.csv', 'f2.csv', 'f3.csv'])"


def describe_file_with_select():
    def it_renders_file_with_select():
        query = file("test.csv", format=CSV(header=True)).select(col("name"))
        result = render(query)
        assert result.query == "SELECT name FROM read_csv('test.csv', header = TRUE)"

    def it_renders_file_with_multiple_columns():
        query = file("test.csv", format=CSV(header=True)).select(col("name"), col("email"))
        result = render(query)
        assert result.query == "SELECT name, email FROM read_csv('test.csv', header = TRUE)"

    def it_renders_file_with_star():
        query = file("test.csv", format=CSV(header=True)).select(col("*"))
        result = render(query)
        assert result.query == "SELECT * FROM read_csv('test.csv', header = TRUE)"


def describe_file_with_where():
    def it_renders_file_with_where():
        query = file("test.csv", format=CSV(header=True)).select(col("name")).where(col("age") > lit(18))
        result = render(query)
        assert result.query == "SELECT name FROM read_csv('test.csv', header = TRUE) WHERE age > 18"

    def it_renders_file_with_multiple_where_conditions():
        query = (
            file("test.csv", format=CSV(header=True))
            .select(col("name"))
            .where(col("age") > lit(18), col("active") == lit(True))
        )
        result = render(query)
        assert result.query == "SELECT name FROM read_csv('test.csv', header = TRUE) WHERE age > 18 AND active = TRUE"


def describe_file_with_aggregation():
    def it_renders_file_with_group_by():
        query = file("test.csv", format=CSV(header=True)).select(col("category"), F.count()).group_by(col("category"))
        result = render(query)
        assert result.query == "SELECT category, COUNT(*) FROM read_csv('test.csv', header = TRUE) GROUP BY category"

    def it_renders_file_with_having():
        query = (
            file("test.csv", format=CSV(header=True))
            .select(col("category"), F.count().alias("cnt"))
            .group_by(col("category"))
            .having(F.count() > lit(5))
        )
        result = render(query)
        assert (
            result.query
            == "SELECT category, COUNT(*) AS cnt FROM read_csv('test.csv', header = TRUE) GROUP BY category HAVING COUNT(*) > 5"
        )


def describe_file_with_order_and_limit():
    def it_renders_file_with_order_by():
        query = file("test.csv", format=CSV(header=True)).select(col("name")).order_by(col("name").asc())
        result = render(query)
        assert result.query == "SELECT name FROM read_csv('test.csv', header = TRUE) ORDER BY name ASC"

    def it_renders_file_with_limit():
        query = file("test.csv", format=CSV(header=True)).select(col("name")).limit(10)
        result = render(query)
        assert result.query == "SELECT name FROM read_csv('test.csv', header = TRUE) LIMIT 10"


def describe_file_with_alias():
    def it_renders_file_with_alias():
        query = file("test.csv", format=CSV(header=True)).alias("t").select(col("name"))
        result = render(query)
        assert result.query == "SELECT name FROM read_csv('test.csv', header = TRUE) AS t"

    def it_renders_file_with_qualified_column():
        f = file("test.csv", format=CSV(header=True)).alias("t")
        query = f.select(f.col("name"))
        result = render(query)
        assert result.query == "SELECT t.name FROM read_csv('test.csv', header = TRUE) AS t"


def describe_parquet_format():
    def it_renders_parquet_with_no_options():
        query = file("data.parquet", format=Parquet())
        result = render(query)
        assert result.query == "FROM read_parquet('data.parquet')"

    def it_renders_parquet_with_filename():
        query = file("data.parquet", format=Parquet(filename=True))
        result = render(query)
        assert result.query == "FROM read_parquet('data.parquet', filename = TRUE)"

    def it_renders_parquet_with_multiple_options():
        query = file("data.parquet", format=Parquet(binary_as_string=True, hive_partitioning=True))
        result = render(query)
        assert result.query == "FROM read_parquet('data.parquet', binary_as_string = TRUE, hive_partitioning = TRUE)"

    def it_renders_parquet_with_select():
        query = file("data.parquet", format=Parquet()).select(col("id"), col("name"))
        result = render(query)
        assert result.query == "SELECT id, name FROM read_parquet('data.parquet')"


def describe_json_format():
    def it_renders_json_with_no_options():
        query = file("data.json", format=JSON())
        result = render(query)
        assert result.query == "FROM read_json('data.json')"

    def it_renders_json_with_ignore_errors():
        query = file("data.json", format=JSON(ignore_errors=True))
        result = render(query)
        assert result.query == "FROM read_json('data.json', ignore_errors = TRUE)"

    def it_renders_json_with_columns():
        query = file("data.json", format=JSON(columns={"id": "INTEGER", "name": "VARCHAR"}))
        result = render(query)
        assert result.query == "FROM read_json('data.json', columns = {'id': 'INTEGER', 'name': 'VARCHAR'})"

    def it_renders_json_with_multiple_options():
        query = file("data.json", format=JSON(ignore_errors=True, compression="gzip"))
        result = render(query)
        assert result.query == "FROM read_json('data.json', ignore_errors = TRUE, compression = 'gzip')"

    def it_renders_json_with_select():
        query = file("data.json", format=JSON()).select(col("id"))
        result = render(query)
        assert result.query == "SELECT id FROM read_json('data.json')"


def describe_jsonl_format():
    def it_renders_jsonl_with_no_options():
        query = file("events.jsonl", format=JSONL())
        result = render(query)
        assert result.query == "FROM read_json('events.jsonl', format = 'newline_delimited')"

    def it_renders_jsonl_with_ignore_errors():
        query = file("events.jsonl", format=JSONL(ignore_errors=True))
        result = render(query)
        assert result.query == "FROM read_json('events.jsonl', format = 'newline_delimited', ignore_errors = TRUE)"

    def it_renders_jsonl_with_columns():
        query = file("events.jsonl", format=JSONL(columns={"event": "VARCHAR"}))
        result = render(query)
        assert (
            result.query
            == "FROM read_json('events.jsonl', format = 'newline_delimited', columns = {'event': 'VARCHAR'})"
        )

    def it_renders_jsonl_with_select():
        query = file("events.jsonl", format=JSONL()).select(col("event_type"))
        result = render(query)
        assert result.query == "SELECT event_type FROM read_json('events.jsonl', format = 'newline_delimited')"
