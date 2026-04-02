"""Tests for select_related prefetching."""

import pytest
from django.test import override_settings

from django_cb.document import Document
from django_cb.fields.reference import ReferenceField
from django_cb.fields.simple import StringField
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


class SRBrewery(Document):
    name = StringField(required=True)

    class Meta:
        collection_name = "sr_breweries"


class SRBeer(Document):
    name = StringField(required=True)
    brewery = ReferenceField(SRBrewery)

    class Meta:
        collection_name = "sr_beers"


class TestSelectRelated:
    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_returns_queryset(self):
        qs = SRBeer.objects.select_related("brewery")
        assert isinstance(qs, QuerySet)
        assert qs._select_related_fields == ["brewery"]

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_chainable(self):
        qs = SRBeer.objects.filter(name="IPA").select_related("brewery").order_by("name")
        assert isinstance(qs, QuerySet)
        assert qs._select_related_fields == ["brewery"]
        assert qs._order_by_fields == ["name"]

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_clone_preserves(self):
        qs1 = SRBeer.objects.select_related("brewery")
        qs2 = qs1.filter(name="IPA")
        assert qs2._select_related_fields == ["brewery"]

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_prefetch_attaches_documents(self, patch_collection):
        """Test that prefetched docs are attached to _prefetched dict."""
        # Store a brewery
        patch_collection._store["brew1"] = {
            "name": "Test Brewery",
            "_type": "srbrewery",
        }
        # Create beer docs that reference it
        beer1 = SRBeer(name="IPA", brewery="brew1")
        beer1._is_new = False

        # Simulate prefetch
        qs = SRBeer.objects.select_related("brewery")
        qs._prefetch_related([beer1])

        assert hasattr(beer1, "_prefetched")
        assert "brewery" in beer1._prefetched
        assert beer1._prefetched["brewery"].name == "Test Brewery"

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_prefetch_missing_ref(self, patch_collection):
        """Prefetch should skip missing referenced documents."""
        beer = SRBeer(name="Orphan Beer", brewery="nonexistent")
        beer._is_new = False

        qs = SRBeer.objects.select_related("brewery")
        qs._prefetch_related([beer])

        # Should not have _prefetched since the ref doesn't exist
        prefetched = getattr(beer, "_prefetched", {})
        assert "brewery" not in prefetched

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_prefetch_none_ref(self, patch_collection):
        """Prefetch should skip None references."""
        beer = SRBeer(name="No Brewery")
        beer._is_new = False

        qs = SRBeer.objects.select_related("brewery")
        qs._prefetch_related([beer])

        prefetched = getattr(beer, "_prefetched", {})
        assert "brewery" not in prefetched

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_prefetch_non_reference_field_ignored(self, patch_collection):
        """select_related on a non-ReferenceField should be a no-op."""
        beer = SRBeer(name="Test")
        beer._is_new = False

        qs = SRBeer.objects.select_related("name")
        qs._prefetch_related([beer])

        prefetched = getattr(beer, "_prefetched", {})
        assert not prefetched
