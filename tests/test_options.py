"""Tests for DocumentOptions."""

import pytest

from django_couchbase_orm.fields.simple import IntegerField, StringField
from django_couchbase_orm.options import DocumentOptions


class TestDocumentOptions:
    def test_defaults(self):
        opts = DocumentOptions()
        assert opts.collection_name == ""
        assert opts.scope_name == "_default"
        assert opts.bucket_alias == "default"
        assert opts.doc_type_field == "_type"
        assert opts.abstract is False
        assert opts.fields == {}

    def test_apply_meta(self):
        class Meta:
            collection_name = "users"
            scope_name = "app"
            bucket_alias = "secondary"
            abstract = False

        opts = DocumentOptions(Meta)
        assert opts.collection_name == "users"
        assert opts.scope_name == "app"
        assert opts.bucket_alias == "secondary"

    def test_get_field(self):
        opts = DocumentOptions()
        f = StringField()
        f.name = "name"
        opts.fields["name"] = f
        assert opts.get_field("name") is f

    def test_get_field_missing(self):
        opts = DocumentOptions()
        with pytest.raises(KeyError, match="does not exist"):
            opts.get_field("nonexistent")

    def test_get_field_by_db_name(self):
        opts = DocumentOptions()
        f = StringField(db_field="user_name")
        f.name = "name"
        opts.fields["name"] = f
        assert opts.get_field_by_db_name("user_name") is f
        assert opts.get_field_by_db_name("nonexistent") is None

    def test_repr(self):
        opts = DocumentOptions()
        opts.collection_name = "users"
        r = repr(opts)
        assert "users" in r

    def test_indexes(self):
        class Meta:
            indexes = [{"fields": ["name"], "name": "idx_name"}]

        opts = DocumentOptions(Meta)
        assert len(opts.indexes) == 1
