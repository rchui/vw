from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from vw.core.base import Expression, Factories, RowSet


@dataclass(frozen=True)
class RawAPI:
    """Namespace for raw SQL escape hatches.

    WARNING: All methods in this namespace bypass vw's safety guarantees:
    - No syntax validation until query execution
    - No SQL injection protection if you concatenate strings into templates
    - No type checking
    - No dialect portability

    Best practices:
    - Only use for features vw doesn't support yet
    - Always pass user input via param(), never f-strings
    - Use {name} placeholders for dependent expressions
    - Consider filing a feature request for native support
    - Add comments explaining why raw SQL is needed

    Example:
        >>> from vw.postgres import raw, col, param
        >>> # Good: Using param() for user input
        >>> raw.expr("custom_func({input})", input=param("val", user_value))
        >>>
        >>> # Bad: String concatenation
        >>> raw.expr(f"custom_func({user_value})")  # SQL INJECTION RISK!
    """

    factories: Factories

    def expr(self, template: str, /, **kwargs: Expression) -> Expression:
        """Create a raw SQL expression with named parameter substitution.

        Use {name} placeholders in the template. Each kwarg provides an expression
        to substitute for that placeholder. Rendering happens at render time to
        support context-dependent features like CTEs.

        Args:
            template: Raw SQL template string with {name} placeholders.
            **kwargs: Named expressions to substitute into the template.

        Returns:
            An Expression wrapping a RawExpr state.

        Example:
            >>> # PostgreSQL-specific operator
            >>> raw.expr("{a} @> {b}", a=col("tags"), b=param("tag", "python"))
            >>>
            >>> # Custom function
            >>> raw.expr("generate_series(1, {n})", n=param("count", 10))
            >>>
            >>> # Complex expression
            >>> raw.expr(
            ...     "GREATEST({a}, {b}, {c})",
            ...     a=col("x"), b=col("y"), c=param("default", 0)
            ... )
            >>>
            >>> # Use in join conditions
            >>> buildings.join.inner(
            ...     parcels,
            ...     on=[raw.expr("ST_Within({a}, {b})", a=col("b.geom"), b=col("p.geom"))]
            ... )
        """
        from vw.core.states import RawExpr

        # Store template and named parameters for lazy rendering
        params_tuple = tuple((k, v.state) for k, v in kwargs.items())
        return self.factories.expr(state=RawExpr(sql=template, params=params_tuple), factories=self.factories)

    def rowset(self, template: str, /, **kwargs: Expression) -> RowSet:
        """Create a raw SQL source/rowset with named parameter substitution.

        Use {name} placeholders in the template. Each kwarg provides an expression
        to substitute for that placeholder. Rendering happens at render time to
        support context-dependent features like CTEs.

        Args:
            template: Raw SQL source template string with {name} placeholders.
            **kwargs: Named expressions to substitute into the template.

        Returns:
            A RowSet wrapping a RawSource state.

        Example:
            >>> # PostgreSQL table function
            >>> raw.rowset("generate_series(1, {n}) AS t(num)", n=param("max", 10))
            >>>
            >>> # LATERAL with unnest
            >>> raw.rowset(
            ...     "LATERAL unnest({arr}) AS t(elem)",
            ...     arr=col("array_col")
            ... )
            >>>
            >>> # JSON table function
            >>> raw.rowset(
            ...     "json_to_recordset({data}) AS t(id INT, name TEXT)",
            ...     data=col("json_data")
            ... )
            >>>
            >>> # Use in FROM clause
            >>> series = raw.rowset("generate_series(1, 10) AS t(n)")
            >>> result = series.select(col("n"))
        """
        from vw.core.states import RawSource

        # Store template and named parameters for lazy rendering
        params_tuple = tuple((k, v.state) for k, v in kwargs.items())
        return self.factories.rowset(state=RawSource(sql=template, params=params_tuple), factories=self.factories)

    def func(self, name: str, /, *args: Expression) -> Expression:
        """Create a function call with the given name and arguments.

        Use this for database-specific functions not yet supported by vw.
        For functions requiring special syntax (WITHIN GROUP, FILTER, etc.),
        use raw.expr() instead.

        Args:
            name: The function name (e.g., "custom_func", "my_agg").
            *args: Positional arguments to pass to the function.

        Returns:
            An Expression wrapping a Function state.

        Example:
            >>> raw.func("gen_random_uuid")
            >>> raw.func("custom_hash", col("email"))
            >>> raw.func("percentile_cont", param("pct", 0.95))
        """
        from vw.core.states import Function

        args_tuple = tuple(arg.state for arg in args)
        return self.factories.expr(state=Function(name=name.upper(), args=args_tuple), factories=self.factories)
