"""Tests for sub-document operations."""

import pytest
from unittest.mock import MagicMock, patch, call

from django_couchbase_orm.document import Document
from django_couchbase_orm.fields.simple import StringField, IntegerField
from django_couchbase_orm.query.subdoc import SubDocAccessor
from django_couchbase_orm.exceptions import OperationError


class SubDocTestDoc(Document):
    name = StringField()
    score = IntegerField()


class TestSubDocAccessor:
    def test_accessor_via_document(self):
        doc = SubDocTestDoc(_id="test1", name="test")
        assert isinstance(doc.subdoc, SubDocAccessor)

    def test_get(self, patch_collection):
        """Test subdoc get delegates to lookup_in."""
        mock_result = MagicMock()
        mock_result.content_as = {object: lambda idx: "San Francisco"}
        patch_collection.lookup_in = MagicMock(return_value=mock_result)

        doc = SubDocTestDoc(_id="doc1", name="test")
        doc.save()

        result = doc.subdoc.get("address.city")
        patch_collection.lookup_in.assert_called_once()
        args = patch_collection.lookup_in.call_args
        assert args[0][0] == "doc1"

    def test_exists(self, patch_collection):
        mock_result = MagicMock()
        mock_result.exists = MagicMock(return_value=True)
        patch_collection.lookup_in = MagicMock(return_value=mock_result)

        doc = SubDocTestDoc(_id="doc1", name="test")
        doc.save()

        result = doc.subdoc.exists("address")
        assert result is True

    def test_count(self, patch_collection):
        mock_result = MagicMock()
        mock_result.content_as = {int: lambda idx: 5}
        patch_collection.lookup_in = MagicMock(return_value=mock_result)

        doc = SubDocTestDoc(_id="doc1", name="test")
        doc.save()

        result = doc.subdoc.count("tags")
        patch_collection.lookup_in.assert_called_once()

    def test_upsert(self, patch_collection):
        patch_collection.mutate_in = MagicMock()

        doc = SubDocTestDoc(_id="doc1", name="test")
        doc.save()

        doc.subdoc.upsert("address.city", "New York")
        patch_collection.mutate_in.assert_called_once()
        args = patch_collection.mutate_in.call_args
        assert args[0][0] == "doc1"

    def test_insert(self, patch_collection):
        patch_collection.mutate_in = MagicMock()

        doc = SubDocTestDoc(_id="doc1", name="test")
        doc.save()

        doc.subdoc.insert("new_field", "value")
        patch_collection.mutate_in.assert_called_once()

    def test_replace(self, patch_collection):
        patch_collection.mutate_in = MagicMock()

        doc = SubDocTestDoc(_id="doc1", name="test")
        doc.save()

        doc.subdoc.replace("name", "updated")
        patch_collection.mutate_in.assert_called_once()

    def test_remove(self, patch_collection):
        patch_collection.mutate_in = MagicMock()

        doc = SubDocTestDoc(_id="doc1", name="test")
        doc.save()

        doc.subdoc.remove("temp_field")
        patch_collection.mutate_in.assert_called_once()

    def test_array_append(self, patch_collection):
        patch_collection.mutate_in = MagicMock()

        doc = SubDocTestDoc(_id="doc1", name="test")
        doc.save()

        doc.subdoc.array_append("tags", "vip")
        patch_collection.mutate_in.assert_called_once()

    def test_array_prepend(self, patch_collection):
        patch_collection.mutate_in = MagicMock()

        doc = SubDocTestDoc(_id="doc1", name="test")
        doc.save()

        doc.subdoc.array_prepend("tags", "first")
        patch_collection.mutate_in.assert_called_once()

    def test_array_addunique(self, patch_collection):
        patch_collection.mutate_in = MagicMock()

        doc = SubDocTestDoc(_id="doc1", name="test")
        doc.save()

        doc.subdoc.array_addunique("tags", "unique_tag")
        patch_collection.mutate_in.assert_called_once()

    def test_increment(self, patch_collection):
        patch_collection.mutate_in = MagicMock()

        doc = SubDocTestDoc(_id="doc1", name="test")
        doc.save()

        doc.subdoc.increment("login_count", 1)
        patch_collection.mutate_in.assert_called_once()

    def test_decrement(self, patch_collection):
        patch_collection.mutate_in = MagicMock()

        doc = SubDocTestDoc(_id="doc1", name="test")
        doc.save()

        doc.subdoc.decrement("login_count", 1)
        patch_collection.mutate_in.assert_called_once()

    def test_multi_lookup(self, patch_collection):
        mock_result = MagicMock()
        patch_collection.lookup_in = MagicMock(return_value=mock_result)

        doc = SubDocTestDoc(_id="doc1", name="test")
        doc.save()

        import couchbase.subdocument as SD
        doc.subdoc.multi_lookup(SD.get("name"), SD.exists("score"))
        patch_collection.lookup_in.assert_called_once()
        args = patch_collection.lookup_in.call_args
        assert len(args[0][1]) == 2

    def test_multi_mutate(self, patch_collection):
        patch_collection.mutate_in = MagicMock()

        doc = SubDocTestDoc(_id="doc1", name="test")
        doc.save()

        import couchbase.subdocument as SD
        doc.subdoc.multi_mutate(
            SD.upsert("name", "new_name"),
            SD.increment("score", 10),
        )
        patch_collection.mutate_in.assert_called_once()
        args = patch_collection.mutate_in.call_args
        assert len(args[0][1]) == 2

    def test_error_wrapping(self, patch_collection):
        patch_collection.lookup_in = MagicMock(side_effect=Exception("network error"))

        doc = SubDocTestDoc(_id="doc1", name="test")
        doc.save()

        with pytest.raises(OperationError, match="Sub-document get failed"):
            doc.subdoc.get("some.path")
