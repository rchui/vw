"""Raw SQL API for Snowflake."""

from vw.core.base import Factories
from vw.core.raw import RawAPI
from vw.snowflake.base import Expression, RowSet

raw = RawAPI(factories=Factories(expr=Expression, rowset=RowSet))
