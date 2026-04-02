"""Tests for aggregation functions."""

import pytest

from django_couchbase_orm.aggregates import Avg, Count, Max, Min, Sum, _build_agg_expression


class TestAggregateClasses:
    def test_count(self):
        agg = Count("name")
        result = agg.to_n1ql({"name": "name"})
        assert result == "COUNT(d.`name`)"

    def test_count_star(self):
        agg = Count("*")
        result = agg.to_n1ql({})
        assert result == "COUNT(*)"

    def test_sum(self):
        agg = Sum("abv")
        result = agg.to_n1ql({"abv": "abv"})
        assert result == "SUM(d.`abv`)"

    def test_avg(self):
        agg = Avg("abv")
        result = agg.to_n1ql({"abv": "abv"})
        assert result == "AVG(d.`abv`)"

    def test_min(self):
        agg = Min("abv")
        result = agg.to_n1ql({"abv": "abv"})
        assert result == "MIN(d.`abv`)"

    def test_max(self):
        agg = Max("abv")
        result = agg.to_n1ql({"abv": "abv"})
        assert result == "MAX(d.`abv`)"

    def test_field_mapping(self):
        """Aggregate should use db_field name."""
        agg = Avg("email_address")
        result = agg.to_n1ql({"email_address": "emailAddress"})
        assert result == "AVG(d.`emailAddress`)"

    def test_invalid_field_name(self):
        agg = Avg("field`injection")
        with pytest.raises(ValueError, match="Invalid identifier"):
            agg.to_n1ql({"field`injection": "field`injection"})


class TestBuildAggExpression:
    def test_valid(self):
        result = _build_agg_expression(Avg("score"), {"score": "score"})
        assert result == "AVG(d.`score`)"

    def test_invalid_type(self):
        with pytest.raises(TypeError, match="aggregate function"):
            _build_agg_expression("not_an_agg", {})
