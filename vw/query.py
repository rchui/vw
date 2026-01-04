"""SQL query builder with method chaining."""

from dataclasses import dataclass

from vw.expr import Expression


@dataclass
class Source:
    """Represents a SQL data source (table, view, etc.)."""

    name: str

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
        return self.name


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
