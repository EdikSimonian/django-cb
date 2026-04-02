"""Tests for QuerySet execution paths using mocked Couchbase cluster."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from django.test import override_settings

from django_couchbase_orm.document import Document
from django_couchbase_orm.fields.simple import FloatField, IntegerField, StringField
from django_couchbase_orm.queryset.q import Q
from django_couchbase_orm.queryset.queryset import QuerySet

COUCHBASE_SETTINGS = {
    "default": {
        "CONNECTION_STRING": "couchbase://localhost",
        "USERNAME": "admin",
        "PASSWORD": "password",
        "BUCKET": "testbucket",
        "SCOPE": "_default",
    }
}


class ExDoc(Document):
    name = StringField(required=True)
    age = IntegerField()
    score = FloatField()

    class Meta:
        collection_name = "exdocs"


def _mock_query_result(rows):
    """Create a mock cluster.query() result that iterates over rows."""
    mock_result = MagicMock()
    mock_result.__iter__ = MagicMock(return_value=iter(rows))
    mock_result.metadata.return_value.metrics.return_value = None
    return mock_result


def _mock_cluster(rows=None):
    """Create a mock cluster that returns rows from query()."""
    cluster = MagicMock()
    cluster.query.return_value = _mock_query_result(rows or [])
    return cluster


# ============================================================
# QuerySet execution (sync)
# ============================================================


class TestQuerySetExecution:
    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_execute_returns_documents(self):
        rows = [
            {"__id": "doc1", "name": "Alice", "age": 30, "_type": "exdoc"},
            {"__id": "doc2", "name": "Bob", "age": 25, "_type": "exdoc"},
        ]
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster(rows)):
            results = list(ExDoc.objects.all())
        assert len(results) == 2
        assert results[0].name == "Alice"
        assert results[1].name == "Bob"

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_execute_values_returns_dicts(self):
        rows = [
            {"__id": "doc1", "name": "Alice"},
            {"__id": "doc2", "name": "Bob"},
        ]
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster(rows)):
            results = list(ExDoc.objects.values("name"))
        assert len(results) == 2
        assert results[0]["name"] == "Alice"

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_count_via_query(self):
        rows = [{"__count": 42}]
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster(rows)):
            count = ExDoc.objects.count()
        assert count == 42

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_count_empty_result(self):
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster([])):
            count = ExDoc.objects.count()
        assert count == 0

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_exists_true(self):
        rows = [{"__id": "doc1", "name": "Alice", "_type": "exdoc"}]
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster(rows)):
            assert ExDoc.objects.filter(name="Alice").exists() is True

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_exists_false(self):
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster([])):
            assert ExDoc.objects.filter(name="Nobody").exists() is False

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_first_found(self):
        rows = [{"__id": "doc1", "name": "Alice", "_type": "exdoc"}]
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster(rows)):
            doc = ExDoc.objects.first()
        assert doc is not None
        assert doc.name == "Alice"

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_first_empty(self):
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster([])):
            doc = ExDoc.objects.first()
        assert doc is None

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_last(self):
        rows = [
            {"__id": "doc1", "name": "Alice", "_type": "exdoc"},
            {"__id": "doc2", "name": "Bob", "_type": "exdoc"},
        ]
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster(rows)):
            doc = ExDoc.objects.last()
        assert doc.name == "Bob"

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_get_success(self):
        rows = [{"__id": "doc1", "name": "Alice", "_type": "exdoc"}]
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster(rows)):
            doc = ExDoc.objects.filter(name="Alice").get()
        assert doc.name == "Alice"

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_get_not_found(self):
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster([])):
            with pytest.raises(ExDoc.DoesNotExist):
                ExDoc.objects.filter(name="Nobody").get()

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_get_multiple(self):
        rows = [
            {"__id": "doc1", "name": "Alice", "_type": "exdoc"},
            {"__id": "doc2", "name": "Alice", "_type": "exdoc"},
        ]
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster(rows)):
            with pytest.raises(ExDoc.MultipleObjectsReturned):
                ExDoc.objects.filter(name="Alice").get()

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_get_with_kwargs(self):
        rows = [{"__id": "doc1", "name": "Alice", "age": 30, "_type": "exdoc"}]
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster(rows)):
            doc = ExDoc.objects.get(name="Alice", age=30)
        assert doc.name == "Alice"

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_len(self):
        rows = [
            {"__id": "doc1", "name": "A", "_type": "exdoc"},
            {"__id": "doc2", "name": "B", "_type": "exdoc"},
        ]
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster(rows)):
            qs = ExDoc.objects.all()
            assert len(qs) == 2

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_bool_true(self):
        rows = [{"__id": "doc1", "name": "A", "_type": "exdoc"}]
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster(rows)):
            assert bool(ExDoc.objects.all())

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_bool_false(self):
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster([])):
            assert not bool(ExDoc.objects.all())

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_repr(self):
        rows = [{"__id": "doc1", "name": "A", "_type": "exdoc"}]
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster(rows)):
            r = repr(ExDoc.objects.all())
        assert "QuerySet" in r

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_index_access(self):
        rows = [{"__id": "doc1", "name": "Alice", "_type": "exdoc"}]
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster(rows)):
            doc = ExDoc.objects.all()[0]
        assert doc.name == "Alice"

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_index_out_of_range(self):
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster([])):
            with pytest.raises(IndexError):
                ExDoc.objects.all()[0]

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_iterator(self):
        rows = [
            {"__id": "doc1", "name": "A", "_type": "exdoc"},
            {"__id": "doc2", "name": "B", "_type": "exdoc"},
        ]
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster(rows)):
            results = list(ExDoc.objects.iterator())
        assert len(results) == 2

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_raw(self):
        rows = [{"x": 1}, {"x": 2}]
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster(rows)):
            results = ExDoc.objects.raw("SELECT * FROM bucket", [])
        assert len(results) == 2

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_raw_no_params(self):
        rows = [{"x": 1}]
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster(rows)):
            results = ExDoc.objects.raw("SELECT 1")
        assert len(results) == 1

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_update(self):
        mock_cluster = _mock_cluster([])
        metrics = MagicMock()
        metrics.mutation_count.return_value = 5
        mock_cluster.query.return_value.metadata.return_value.metrics.return_value = metrics
        with patch("django_couchbase_orm.connection.get_cluster", return_value=mock_cluster):
            count = ExDoc.objects.filter(name="old").update(name="new")
        assert count == 5

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_update_empty_kwargs(self):
        assert ExDoc.objects.filter(name="x").update() == 0

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_delete_bulk(self):
        mock_cluster = _mock_cluster([])
        metrics = MagicMock()
        metrics.mutation_count.return_value = 3
        mock_cluster.query.return_value.metadata.return_value.metrics.return_value = metrics
        with patch("django_couchbase_orm.connection.get_cluster", return_value=mock_cluster):
            count = ExDoc.objects.filter(name="old").delete()
        assert count == 3

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_aggregate(self):
        rows = [{"avg_age": 30.5, "total": 100}]
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster(rows)):
            from django_couchbase_orm.aggregates import Avg, Count

            result = ExDoc.objects.all().aggregate(avg_age=Avg("age"), total=Count("*"))
        assert result["avg_age"] == 30.5
        assert result["total"] == 100

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_aggregate_empty(self):
        with patch("django_couchbase_orm.connection.get_cluster", return_value=_mock_cluster([])):
            from django_couchbase_orm.aggregates import Avg

            result = ExDoc.objects.all().aggregate(avg_age=Avg("age"))
        assert result["avg_age"] is None


# ============================================================
# Async execution
# ============================================================


def _mock_async_query_result(rows):
    """Create a mock async query result."""
    mock_result = MagicMock()

    async def async_iter():
        for row in rows:
            yield row

    mock_result.__aiter__ = lambda self: async_iter()
    return mock_result


def _mock_async_cluster(rows=None):
    cluster = MagicMock()
    cluster.query.return_value = _mock_async_query_result(rows or [])
    return cluster


class TestAsyncQuerySetExecution:
    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    @pytest.mark.asyncio
    async def test_alist(self):
        rows = [{"__id": "d1", "name": "Alice", "_type": "exdoc"}]
        with patch(
            "django_couchbase_orm.async_connection.get_async_cluster",
            new_callable=AsyncMock,
            return_value=_mock_async_cluster(rows),
        ):
            results = await ExDoc.objects.all().alist()
        assert len(results) == 1
        assert results[0].name == "Alice"

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    @pytest.mark.asyncio
    async def test_acount(self):
        rows = [{"__count": 42}]
        with patch(
            "django_couchbase_orm.async_connection.get_async_cluster",
            new_callable=AsyncMock,
            return_value=_mock_async_cluster(rows),
        ):
            count = await ExDoc.objects.acount()
        assert count == 42

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    @pytest.mark.asyncio
    async def test_acount_empty(self):
        with patch(
            "django_couchbase_orm.async_connection.get_async_cluster",
            new_callable=AsyncMock,
            return_value=_mock_async_cluster([]),
        ):
            count = await ExDoc.objects.acount()
        assert count == 0

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    @pytest.mark.asyncio
    async def test_afirst(self):
        rows = [{"__id": "d1", "name": "Alice", "_type": "exdoc"}]
        with patch(
            "django_couchbase_orm.async_connection.get_async_cluster",
            new_callable=AsyncMock,
            return_value=_mock_async_cluster(rows),
        ):
            doc = await ExDoc.objects.afirst()
        assert doc.name == "Alice"

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    @pytest.mark.asyncio
    async def test_afirst_empty(self):
        with patch(
            "django_couchbase_orm.async_connection.get_async_cluster",
            new_callable=AsyncMock,
            return_value=_mock_async_cluster([]),
        ):
            doc = await ExDoc.objects.afirst()
        assert doc is None

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    @pytest.mark.asyncio
    async def test_aexists_true(self):
        rows = [{"__id": "d1", "name": "A", "_type": "exdoc"}]
        with patch(
            "django_couchbase_orm.async_connection.get_async_cluster",
            new_callable=AsyncMock,
            return_value=_mock_async_cluster(rows),
        ):
            assert await ExDoc.objects.filter(name="A").aexists() is True

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    @pytest.mark.asyncio
    async def test_aexists_false(self):
        with patch(
            "django_couchbase_orm.async_connection.get_async_cluster",
            new_callable=AsyncMock,
            return_value=_mock_async_cluster([]),
        ):
            assert await ExDoc.objects.filter(name="Nobody").aexists() is False

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    @pytest.mark.asyncio
    async def test_aget_success(self):
        rows = [{"__id": "d1", "name": "Alice", "_type": "exdoc"}]
        with patch(
            "django_couchbase_orm.async_connection.get_async_cluster",
            new_callable=AsyncMock,
            return_value=_mock_async_cluster(rows),
        ):
            doc = await ExDoc.objects.filter(name="Alice").aget()
        assert doc.name == "Alice"

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    @pytest.mark.asyncio
    async def test_aget_not_found(self):
        with patch(
            "django_couchbase_orm.async_connection.get_async_cluster",
            new_callable=AsyncMock,
            return_value=_mock_async_cluster([]),
        ):
            with pytest.raises(ExDoc.DoesNotExist):
                await ExDoc.objects.filter(name="Nobody").aget()

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    @pytest.mark.asyncio
    async def test_aget_multiple(self):
        rows = [
            {"__id": "d1", "name": "A", "_type": "exdoc"},
            {"__id": "d2", "name": "A", "_type": "exdoc"},
        ]
        with patch(
            "django_couchbase_orm.async_connection.get_async_cluster",
            new_callable=AsyncMock,
            return_value=_mock_async_cluster(rows),
        ):
            with pytest.raises(ExDoc.MultipleObjectsReturned):
                await ExDoc.objects.filter(name="A").aget()

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    @pytest.mark.asyncio
    async def test_async_iteration(self):
        rows = [
            {"__id": "d1", "name": "A", "_type": "exdoc"},
            {"__id": "d2", "name": "B", "_type": "exdoc"},
        ]
        with patch(
            "django_couchbase_orm.async_connection.get_async_cluster",
            new_callable=AsyncMock,
            return_value=_mock_async_cluster(rows),
        ):
            results = []
            async for doc in ExDoc.objects.all():
                results.append(doc)
        assert len(results) == 2


# ============================================================
# Manager async delegation
# ============================================================


class TestManagerAsyncDelegation:
    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    @pytest.mark.asyncio
    async def test_manager_acount(self):
        rows = [{"__count": 10}]
        with patch(
            "django_couchbase_orm.async_connection.get_async_cluster",
            new_callable=AsyncMock,
            return_value=_mock_async_cluster(rows),
        ):
            count = await ExDoc.objects.acount()
        assert count == 10

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    @pytest.mark.asyncio
    async def test_manager_afirst(self):
        rows = [{"__id": "d1", "name": "First", "_type": "exdoc"}]
        with patch(
            "django_couchbase_orm.async_connection.get_async_cluster",
            new_callable=AsyncMock,
            return_value=_mock_async_cluster(rows),
        ):
            doc = await ExDoc.objects.afirst()
        assert doc.name == "First"

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    @pytest.mark.asyncio
    async def test_manager_alist(self):
        rows = [{"__id": "d1", "name": "A", "_type": "exdoc"}]
        with patch(
            "django_couchbase_orm.async_connection.get_async_cluster",
            new_callable=AsyncMock,
            return_value=_mock_async_cluster(rows),
        ):
            results = await ExDoc.objects.alist()
        assert len(results) == 1
