"""Tests for Q objects."""

import pytest

from django_cb.query.n1ql import N1QLQuery
from django_cb.queryset.q import Q


class TestQBasic:
    def test_single_kwarg(self):
        q = Q(name="Alice")
        assert len(q.children) == 1
        assert q.children[0] == ("name", "Alice")
        assert q.connector == Q.AND
        assert q.negated is False

    def test_multiple_kwargs_are_and(self):
        q = Q(name="Alice", age=30)
        assert len(q.children) == 2

    def test_repr(self):
        q = Q(name="Alice")
        assert "name='Alice'" in repr(q)


class TestQCombination:
    def test_and(self):
        q = Q(name="Alice") & Q(age=30)
        assert q.connector == Q.AND
        assert len(q.children) == 2

    def test_or(self):
        q = Q(name="Alice") | Q(name="Bob")
        assert q.connector == Q.OR
        assert len(q.children) == 2

    def test_not(self):
        q = ~Q(deleted=True)
        assert q.negated is True

    def test_double_not(self):
        q = ~~Q(deleted=True)
        assert q.negated is False

    def test_complex_expression(self):
        q = (Q(name="Alice") | Q(name="Bob")) & ~Q(deleted=True)
        assert q.connector == Q.AND
        assert len(q.children) == 2

    def test_type_error(self):
        with pytest.raises(TypeError):
            Q(name="Alice") & "not a Q"


class TestQResolve:
    def _make_query(self):
        return N1QLQuery("b", "s", "c")

    def test_simple_resolve(self):
        q = Q(name="Alice")
        query = self._make_query()
        clause = q.resolve(query)
        assert "d.`name` = $1" in clause

    def test_and_resolve(self):
        q = Q(name="Alice") & Q(age__gte=18)
        query = self._make_query()
        clause = q.resolve(query)
        assert "d.`name` = $1" in clause
        assert "d.`age` >= $2" in clause
        assert " AND " in clause

    def test_or_resolve(self):
        q = Q(name="Alice") | Q(name="Bob")
        query = self._make_query()
        clause = q.resolve(query)
        assert "d.`name` = $1" in clause
        assert "d.`name` = $2" in clause
        assert " OR " in clause

    def test_not_resolve(self):
        q = ~Q(deleted=True)
        query = self._make_query()
        clause = q.resolve(query)
        assert "NOT" in clause
        assert "d.`deleted` = $1" in clause

    def test_complex_resolve(self):
        q = (Q(status="active") | Q(role="admin")) & Q(age__gte=18)
        query = self._make_query()
        clause = q.resolve(query)
        assert "d.`age` >= " in clause
        # Should have both status and role
        assert "d.`status` = " in clause
        assert "d.`role` = " in clause

    def test_empty_q(self):
        q = Q()
        query = self._make_query()
        clause = q.resolve(query)
        assert clause == ""

    def test_multiple_kwargs_resolve(self):
        q = Q(name="Alice", age=30)
        query = self._make_query()
        clause = q.resolve(query)
        assert "d.`name` = " in clause
        assert "d.`age` = " in clause

    def test_field_map(self):
        """Resolve should map Python field names to DB field names."""
        q = Q(first_name="Alice")
        query = self._make_query()
        clause = q.resolve(query, field_map={"first_name": "firstName"})
        assert "d.`firstName` = $1" in clause

    def test_field_map_with_lookup(self):
        q = Q(first_name__contains="Ali")
        query = self._make_query()
        clause = q.resolve(query, field_map={"first_name": "firstName"})
        assert "CONTAINS(d.`firstName`, $1)" in clause

    def test_params_correct(self):
        q = Q(name="Alice") & Q(age=30)
        query = self._make_query()
        q.resolve(query)
        assert query._params == ["Alice", 30]
