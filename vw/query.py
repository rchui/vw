"""SQL query builder with method chaining."""

from collections.abc import Sequence
from dataclasses import dataclass, field

from vw.expr import Column, Expression


@dataclass
class InnerJoin:
    """Represents an INNER JOIN operation."""

    right: "Source"
    on: Sequence[Expression] = field(default_factory=list)

    def __vw_render__(self) -> str:
        """Return the SQL representation of the inner join."""
        join_sql = f"INNER JOIN {self.right.__vw_render__()}"
        if self.on:
            conditions = [expr.__vw_render__() for expr in self.on]
            join_sql += f" ON {' AND '.join(conditions)}"
        return join_sql


class JoinAccessor:
    """Accessor for join operations on a Source."""

    def __init__(self, source: "Source"):
        self._source = source

    def inner(self, right: "Source", *, on: Sequence[Expression] = ()) -> "Source":
        """
        Perform an INNER JOIN with another source.

        Args:
            right: The source to join with.
            on: Sequence of join condition expressions. Multiple conditions are combined with AND.

        Returns:
            A new Source with the join applied.

        Example:
            >>> users = Source("users")
            >>> orders = Source("orders")
            >>> users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        """
        return Source(
            name=self._source.name,
            joins=self._source.joins + [InnerJoin(right=right, on=on)],
        )


@dataclass
class Source:
    """Represents a SQL data source (table, view, etc.)."""

    name: str
    joins: list[InnerJoin] = field(default_factory=list)

    @property
    def join(self) -> JoinAccessor:
        """Access join operations."""
        return JoinAccessor(self)

    def col(self, column_name: str, /) -> Column:
        """
        Create a column reference qualified with this source's name.

        Args:
            column_name: Column name to qualify.

        Returns:
            A Column with the source name as a prefix.

        Example:
            >>> Source("users").col("id")  # Returns Column("users.id")
        """
        return Column(f"{self.name}.{column_name}")

    def select(self, *columns: Expression) -> "Statement":
        """
        Select columns from this source.

        Args:
            *columns: Expression objects to select.

        Returns:
            A Statement object for method chaining.

        Example:
            >>> from vw import col
            >>> Source("users").select(col("*"))
            >>> Source("users").select(col("id"), col("name"))
        """
        return Statement(source=self, columns=list(columns))

    def __vw_render__(self) -> str:
        """Return the SQL representation of the source."""
        sql = self.name
        if self.joins:
            for join_obj in self.joins:
                sql += f" {join_obj.__vw_render__()}"
        return sql


@dataclass
class Statement:
    """Represents a SQL statement."""

    source: Source
    columns: list[Expression]

    def render(self) -> str:
        """
        Render the SQL statement string.

        Returns:
            The constructed SQL statement.
        """
        # Render columns
        rendered_columns = [col.__vw_render__() for col in self.columns]
        columns_str = ", ".join(rendered_columns)
        source_str = self.source.__vw_render__()

        return f"SELECT {columns_str} FROM {source_str}"

    def __vw_render__(self) -> str:
        """Return the SQL representation of the statement."""
        return self.render()
