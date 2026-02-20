from __future__ import annotations

from dataclasses import dataclass, replace

from vw.core.base import Expression as CoreExpression
from vw.core.base import RowSet as CoreRowSet
from vw.duckdb.star import StarAccessor


@dataclass(eq=False, frozen=True, kw_only=True)
class Expression(CoreExpression["Expression", "RowSet"]): ...


@dataclass(eq=False, frozen=True, kw_only=True)
class RowSet(CoreRowSet["Expression", "RowSet"]):
    @property
    def star(self) -> StarAccessor:
        """Access DuckDB star extensions (EXCLUDE, REPLACE).

        Returns:
            Callable StarAccessor for star expressions.

        Example:
            >>> users.star()                                    # SELECT *
            >>> users.star(users.star.exclude(col("password"))) # SELECT * EXCLUDE (password)
            >>> users.star(users.star.replace(name=F.upper(col("name")))) # SELECT * REPLACE (...)
        """
        return StarAccessor(rowset=self)

    def sample(
        self,
        *,
        percent: float | None = None,
        rows: int | None = None,
        method: str | None = None,
        seed: int | None = None,
    ) -> RowSet:
        """Add USING SAMPLE clause (DuckDB-specific).

        Exactly one of `percent` or `rows` must be provided.

        Args:
            percent: Sample percentage (0-100).
            rows: Number of rows to sample.
            method: Sampling method ("reservoir", "bernoulli", "system").
            seed: Random seed for reproducibility (REPEATABLE clause).

        Returns:
            A new RowSet with the USING SAMPLE modifier appended.

        Examples:
            >>> source("big_table").sample(percent=10)
            >>> source("big_table").sample(rows=1000)
            >>> source("big_table").sample(method="reservoir", rows=1000)
            >>> source("big_table").sample(percent=10, seed=42)
        """
        from vw.duckdb.states import UsingSample

        if percent is None and rows is None:
            raise ValueError("sample() requires exactly one of: percent or rows")
        if percent is not None and rows is not None:
            raise ValueError("sample() accepts only one of: percent or rows (not both)")

        modifier = UsingSample(percent=percent, rows=rows, method=method, seed=seed)
        new_state = replace(self.state, modifiers=self.state.modifiers + (modifier,))
        return RowSet(state=new_state, factories=self.factories)
