"""Tests for management commands."""

from io import StringIO
from unittest.mock import MagicMock, patch

import pytest
from django.core.management import call_command
from django.test import override_settings

from django_cb.document import Document
from django_cb.fields.simple import IntegerField, StringField


COUCHBASE_SETTINGS = {
    "default": {
        "CONNECTION_STRING": "couchbase://localhost",
        "USERNAME": "admin",
        "PASSWORD": "password",
        "BUCKET": "testbucket",
        "SCOPE": "_default",
    },
    # Other document classes in tests may use non-default aliases
    "shop_db": {
        "CONNECTION_STRING": "couchbase://localhost",
        "USERNAME": "admin",
        "PASSWORD": "password",
        "BUCKET": "shopbucket",
        "SCOPE": "_default",
    },
    "analytics": {
        "CONNECTION_STRING": "couchbase://localhost",
        "USERNAME": "admin",
        "PASSWORD": "password",
        "BUCKET": "analytics",
        "SCOPE": "_default",
    },
    "secondary": {
        "CONNECTION_STRING": "couchbase://localhost",
        "USERNAME": "admin",
        "PASSWORD": "password",
        "BUCKET": "other",
        "SCOPE": "_default",
    },
}


class IndexedDoc(Document):
    name = StringField()
    age = IntegerField()

    class Meta:
        collection_name = "indexed_docs"
        indexes = [
            {"fields": ["name"], "name": "idx_name"},
            {"fields": ["age", "name"], "name": "idx_age_name"},
        ]


class TestCbEnsureIndexes:
    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_dry_run(self):
        out = StringIO()
        call_command("cb_ensure_indexes", "--dry-run", stdout=out)
        output = out.getvalue()
        assert "DRY RUN" in output

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_dry_run_with_primary(self):
        out = StringIO()
        call_command("cb_ensure_indexes", "--dry-run", "--primary", stdout=out)
        output = out.getvalue()
        assert "PRIMARY INDEX" in output
        assert "DRY RUN" in output

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_indexes_include_fields(self):
        out = StringIO()
        call_command("cb_ensure_indexes", "--dry-run", stdout=out)
        output = out.getvalue()
        assert "idx_name" in output
        assert "idx_age_name" in output


class TestCbCreateCollections:
    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_dry_run(self):
        out = StringIO()
        # Need to mock get_bucket since there's no real cluster
        with patch("django_cb.management.commands.cb_create_collections.get_bucket"):
            call_command("cb_create_collections", "--dry-run", stdout=out)
        output = out.getvalue()
        assert "DRY RUN" in output or "nothing created" in output.lower()
