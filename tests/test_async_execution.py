"""Tests for async connection, Document CRUD, and Manager with mocked acouchbase."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from django.test import override_settings

from django_couchbase_orm.async_connection import (
    _async_connections,
    get_async_bucket,
    get_async_cluster,
    get_async_collection,
    close_async_connections,
    reset_async_connections,
)
from django_couchbase_orm.document import Document
from django_couchbase_orm.fields.simple import IntegerField, StringField

COUCHBASE_SETTINGS = {
    "default": {
        "CONNECTION_STRING": "couchbase://localhost",
        "USERNAME": "admin",
        "PASSWORD": "password",
        "BUCKET": "testbucket",
        "SCOPE": "_default",
    }
}


class AsyncTestDoc(Document):
    name = StringField(required=True)
    score = IntegerField(default=0)

    class Meta:
        collection_name = "async_test_docs"


# ============================================================
# Async connection
# ============================================================


class TestAsyncConnection:
    def setup_method(self):
        reset_async_connections()

    def teardown_method(self):
        reset_async_connections()

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    @pytest.mark.asyncio
    async def test_get_async_cluster(self):
        mock_cluster = MagicMock()
        mock_cluster.wait_until_ready = AsyncMock()

        with patch.dict("sys.modules", {
            "acouchbase": MagicMock(),
            "acouchbase.cluster": MagicMock(
                AsyncCluster=MagicMock(connect=AsyncMock(return_value=mock_cluster))
            ),
        }):
            cluster = await get_async_cluster("default")
            assert cluster is mock_cluster

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    @pytest.mark.asyncio
    async def test_get_async_cluster_cached(self):
        mock_cluster = MagicMock()
        _async_connections["cluster:default"] = mock_cluster
        cluster = await get_async_cluster("default")
        assert cluster is mock_cluster

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    @pytest.mark.asyncio
    async def test_get_async_bucket(self):
        mock_bucket = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster.bucket.return_value = mock_bucket
        _async_connections["cluster:default"] = mock_cluster

        bucket = await get_async_bucket("default")
        assert bucket is mock_bucket

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    @pytest.mark.asyncio
    async def test_get_async_bucket_cached(self):
        mock_bucket = MagicMock()
        _async_connections["bucket:default"] = mock_bucket
        bucket = await get_async_bucket("default")
        assert bucket is mock_bucket

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    @pytest.mark.asyncio
    async def test_get_async_collection(self):
        mock_coll = MagicMock()
        mock_scope = MagicMock()
        mock_scope.collection.return_value = mock_coll
        mock_bucket = MagicMock()
        mock_bucket.scope.return_value = mock_scope
        _async_connections["bucket:default"] = mock_bucket

        coll = await get_async_collection("default", "_default", "test_coll")
        assert coll is mock_coll

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    @pytest.mark.asyncio
    async def test_get_async_collection_cached(self):
        mock_coll = MagicMock()
        _async_connections["collection:default:_default:test_coll"] = mock_coll
        coll = await get_async_collection("default", "_default", "test_coll")
        assert coll is mock_coll

    @pytest.mark.asyncio
    async def test_close_async_connections(self):
        mock_cluster = MagicMock()
        _async_connections["cluster:default"] = mock_cluster
        _async_connections["bucket:default"] = MagicMock()
        await close_async_connections()
        assert len(_async_connections) == 0

    def test_reset_async_connections(self):
        _async_connections["test"] = "value"
        reset_async_connections()
        assert len(_async_connections) == 0


# ============================================================
# Async Document CRUD
# ============================================================


class TestAsyncDocumentCRUD:
    @pytest.mark.asyncio
    async def test_asave(self):
        mock_coll = AsyncMock()
        mock_coll.upsert.return_value = MagicMock(cas=12345)

        doc = AsyncTestDoc(name="test")
        with patch.object(doc, "_aget_collection", return_value=mock_coll):
            await doc.asave()
        assert doc._is_new is False
        assert doc._cas == 12345
        mock_coll.upsert.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_asave_validates(self):
        from django_couchbase_orm.exceptions import ValidationError

        doc = AsyncTestDoc()  # name is required
        with pytest.raises(ValidationError):
            await doc.asave()

    @pytest.mark.asyncio
    async def test_adelete(self):
        mock_coll = AsyncMock()

        doc = AsyncTestDoc(_id="del1", name="bye")
        with patch.object(doc, "_aget_collection", return_value=mock_coll):
            await doc.adelete()
        mock_coll.remove.assert_awaited_once_with("del1")

    @pytest.mark.asyncio
    async def test_areload(self):
        mock_result = MagicMock()
        mock_result.content_as = {dict: {"name": "updated", "score": 99, "_type": "asynctestdoc"}}
        mock_result.cas = 67890

        mock_coll = AsyncMock()
        mock_coll.get.return_value = mock_result

        doc = AsyncTestDoc(_id="r1", name="original")
        with patch.object(doc, "_aget_collection", return_value=mock_coll):
            await doc.areload()
        assert doc.name == "updated"
        assert doc.score == 99
        assert doc._cas == 67890


# ============================================================
# Async Manager
# ============================================================


class TestAsyncManager:
    @pytest.mark.asyncio
    async def test_aget_by_pk(self):
        mock_result = MagicMock()
        mock_result.content_as = {dict: {"name": "Alice", "_type": "asynctestdoc"}}
        mock_result.cas = 111

        mock_coll = AsyncMock()
        mock_coll.get.return_value = mock_result

        with patch(
            "django_couchbase_orm.async_connection.get_async_collection",
            new_callable=AsyncMock,
            return_value=mock_coll,
        ):
            doc = await AsyncTestDoc.objects.aget(pk="doc1")
        assert doc.name == "Alice"
        assert doc._cas == 111

    @pytest.mark.asyncio
    async def test_aget_by_pk_not_found(self):
        from couchbase.exceptions import DocumentNotFoundException

        mock_coll = AsyncMock()
        mock_coll.get.side_effect = DocumentNotFoundException()

        with patch(
            "django_couchbase_orm.async_connection.get_async_collection",
            new_callable=AsyncMock,
            return_value=mock_coll,
        ):
            with pytest.raises(AsyncTestDoc.DoesNotExist):
                await AsyncTestDoc.objects.aget(pk="nonexistent")

    @pytest.mark.asyncio
    async def test_acreate(self):
        mock_coll = AsyncMock()
        mock_coll.upsert.return_value = MagicMock(cas=222)

        with patch(
            "django_couchbase_orm.async_connection.get_async_collection",
            new_callable=AsyncMock,
            return_value=mock_coll,
        ):
            doc = await AsyncTestDoc.objects.acreate(name="NewDoc")
        assert doc.name == "NewDoc"
        assert doc._is_new is False
