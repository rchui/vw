"""Raw SQL API for PostgreSQL."""

from vw.core.base import Factories
from vw.core.raw import RawAPI
from vw.postgres.base import Expression, RowSet

# Instantiate with PostgreSQL factories
raw = RawAPI(factories=Factories(expr=Expression, rowset=RowSet))
