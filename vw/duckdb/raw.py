"""Raw SQL API for DuckDB."""
from vw.core.base import Factories
from vw.core.raw import RawAPI
from vw.duckdb.base import Expression, RowSet

raw = RawAPI(factories=Factories(expr=Expression, rowset=RowSet))
