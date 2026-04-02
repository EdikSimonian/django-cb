"""Tests for QuerySet — query building, chaining, and SQL generation."""

import pytest
from django.test import override_settings
from unittest.mock import patch, MagicMock

from django_cb.document import Document
from django_cb.fields.simple import IntegerField, StringField, FloatField, BooleanField
from django_cb.queryset.q import Q
from django_cb.queryset.queryset import QuerySet


COUCHBASE_SETTINGS = {
    "default": {
        "CONNECTION_STRING": "couchbase://localhost",
        "USERNAME": "admin",
        "PASSWORD": "password",
        "BUCKET": "testbucket",
        "SCOPE": "_default",
    }
}


class QSUser(Document):
    name = StringField(required=True)
    age = IntegerField()
    email = StringField(db_field="emailAddress")
    active = BooleanField(default=True)

    class Meta:
        collection_name = "users"
        doc_type_field = "_type"


class QSBeer(Document):
    name = StringField(required=True)
    abv = FloatField()
    style = StringField()
    brewery_id = StringField()

    class Meta:
        collection_name = "_default"
        doc_type_field = "type"


# ============================================================
# Query building tests (don't execute, just check SQL generation)
# ============================================================


class TestQuerySetBuild:
    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_all(self):
        qs = QSUser.objects.all()
        query = qs._build_query()
        stmt, params = query.build()
        assert "FROM `testbucket`.`_default`.`users` AS d" in stmt
        assert "d.`_type` = $1" in stmt
        assert params[0] == "qsuser"

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_filter_simple(self):
        qs = QSUser.objects.filter(name="Alice")
        query = qs._build_query()
        stmt, params = query.build()
        assert "d.`name` = $2" in stmt
        assert "Alice" in params

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_filter_lookup(self):
        qs = QSUser.objects.filter(age__gte=18)
        query = qs._build_query()
        stmt, params = query.build()
        assert "d.`age` >= $2" in stmt
        assert 18 in params

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_filter_chaining(self):
        qs = QSUser.objects.filter(age__gte=18).filter(active=True)
        query = qs._build_query()
        stmt, params = query.build()
        assert "d.`age` >= " in stmt
        assert "d.`active` = " in stmt
        assert 18 in params
        assert True in params

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_filter_db_field_mapping(self):
        """Filter on Python field name should use db_field in SQL."""
        qs = QSUser.objects.filter(email="alice@example.com")
        query = qs._build_query()
        stmt, params = query.build()
        assert "d.`emailAddress` = " in stmt

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_filter_db_field_with_lookup(self):
        qs = QSUser.objects.filter(email__contains="@example")
        query = qs._build_query()
        stmt, params = query.build()
        assert "CONTAINS(d.`emailAddress`, " in stmt

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_exclude(self):
        qs = QSUser.objects.exclude(active=False)
        query = qs._build_query()
        stmt, params = query.build()
        assert "NOT (d.`active` = " in stmt

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_filter_and_exclude(self):
        qs = QSUser.objects.filter(age__gte=18).exclude(name="Admin")
        query = qs._build_query()
        stmt, params = query.build()
        assert "d.`age` >= " in stmt
        assert "NOT (d.`name` = " in stmt

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_filter_with_q(self):
        qs = QSUser.objects.filter(Q(name="Alice") | Q(name="Bob"))
        query = qs._build_query()
        stmt, params = query.build()
        assert " OR " in stmt
        assert "Alice" in params
        assert "Bob" in params

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_order_by(self):
        qs = QSUser.objects.order_by("-age", "name")
        query = qs._build_query()
        stmt, _ = query.build()
        assert "ORDER BY d.`age` DESC, d.`name` ASC" in stmt

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_order_by_db_field(self):
        """order_by with Python field name should map to db_field."""
        qs = QSUser.objects.order_by("email")
        query = qs._build_query()
        stmt, _ = query.build()
        assert "d.`emailAddress`" in stmt

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_slice(self):
        qs = QSUser.objects.all()[:10]
        query = qs._build_query()
        stmt, params = query.build()
        assert "LIMIT" in stmt
        assert 10 in params

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_slice_with_offset(self):
        qs = QSUser.objects.all()[20:30]
        query = qs._build_query()
        stmt, params = query.build()
        assert "LIMIT" in stmt
        assert "OFFSET" in stmt
        assert 10 in params  # 30 - 20
        assert 20 in params

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_values(self):
        qs = QSUser.objects.values("name", "age")
        query = qs._build_query()
        stmt, _ = query.build()
        assert "d.`name`" in stmt
        assert "d.`age`" in stmt

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_values_with_db_field(self):
        qs = QSUser.objects.values("email")
        query = qs._build_query()
        stmt, _ = query.build()
        assert "d.`emailAddress`" in stmt

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_complex_query(self):
        qs = (
            QSUser.objects
            .filter(Q(age__gte=18) | Q(active=True))
            .exclude(name__startswith="test")
            .order_by("-age")
        )[:50]
        query = qs._build_query()
        stmt, params = query.build()
        assert "OR" in stmt
        assert "NOT" in stmt
        assert "ORDER BY" in stmt
        assert "LIMIT" in stmt


class TestQuerySetChaining:
    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_clone_independence(self):
        """Chaining should not modify the original QuerySet."""
        qs1 = QSUser.objects.all()
        qs2 = qs1.filter(name="Alice")
        qs3 = qs1.filter(name="Bob")

        q2 = qs2._build_query()
        q3 = qs3._build_query()
        stmt2, _ = q2.build()
        stmt3, _ = q3.build()

        assert "Alice" not in stmt3
        assert "Bob" not in stmt2

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_none_returns_empty(self):
        qs = QSUser.objects.none()
        assert list(qs) == []
        assert qs.count() == 0  # Should not hit DB

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_none_cached_empty(self):
        """none() should have an empty result cache, so count doesn't query."""
        qs = QSUser.objects.none()
        # _result_cache should be set to []
        assert qs._result_cache == []


class TestQuerySetIndex:
    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_negative_index_raises(self):
        with pytest.raises(ValueError, match="Negative"):
            QSUser.objects.all()[-1]

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_invalid_index_type(self):
        with pytest.raises(TypeError):
            QSUser.objects.all()["key"]


class TestQuerySetNoneCount:
    """Test that none().count() returns 0 without executing a query."""

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_none_count(self):
        qs = QSUser.objects.none()
        # Since _result_cache is [], len() returns 0
        assert len(qs) == 0

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_none_bool(self):
        qs = QSUser.objects.none()
        assert not qs

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_none_first(self):
        qs = QSUser.objects.none()
        # first() clones, so won't use cache. Let's test the iteration path.
        assert list(qs) == []


# ============================================================
# Manager delegation tests
# ============================================================


class TestManagerDelegation:
    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_filter_returns_queryset(self):
        qs = QSUser.objects.filter(name="Alice")
        assert isinstance(qs, QuerySet)

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_exclude_returns_queryset(self):
        qs = QSUser.objects.exclude(active=False)
        assert isinstance(qs, QuerySet)

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_order_by_returns_queryset(self):
        qs = QSUser.objects.order_by("name")
        assert isinstance(qs, QuerySet)

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_values_returns_queryset(self):
        qs = QSUser.objects.values("name")
        assert isinstance(qs, QuerySet)

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_all_returns_queryset(self):
        qs = QSUser.objects.all()
        assert isinstance(qs, QuerySet)

    def test_get_with_no_args_raises(self):
        with pytest.raises(ValueError, match="at least one"):
            QSUser.objects.get()
