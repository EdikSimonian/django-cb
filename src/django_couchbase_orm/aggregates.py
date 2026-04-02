"""Aggregation functions for QuerySet.aggregate()."""

from __future__ import annotations

from django_couchbase_orm.query.n1ql import _validate_identifier


class AggregateFunc:
    """Base class for aggregate functions."""

    func_name: str = ""

    def __init__(self, field: str):
        self.field = field

    def to_n1ql(self, field_map: dict[str, str]) -> str:
        db_field = field_map.get(self.field, self.field)
        _validate_identifier(db_field)
        return f"{self.func_name}(d.`{db_field}`)"


class Count(AggregateFunc):
    """COUNT() aggregate."""

    func_name = "COUNT"

    def to_n1ql(self, field_map: dict[str, str]) -> str:
        if self.field == "*":
            return "COUNT(*)"
        return super().to_n1ql(field_map)


class Sum(AggregateFunc):
    """SUM() aggregate."""

    func_name = "SUM"


class Avg(AggregateFunc):
    """AVG() aggregate."""

    func_name = "AVG"


class Min(AggregateFunc):
    """MIN() aggregate."""

    func_name = "MIN"


class Max(AggregateFunc):
    """MAX() aggregate."""

    func_name = "MAX"


def _build_agg_expression(agg: AggregateFunc, field_map: dict[str, str]) -> str:
    """Convert an aggregate function to a N1QL expression string."""
    if not isinstance(agg, AggregateFunc):
        raise TypeError(f"Expected an aggregate function, got {type(agg).__name__}")
    return agg.to_n1ql(field_map)
