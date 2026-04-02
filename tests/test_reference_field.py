"""Tests for ReferenceField."""

import pytest

from django_cb.document import Document
from django_cb.exceptions import ValidationError
from django_cb.fields.reference import ReferenceField
from django_cb.fields.simple import StringField


class RefBrewery(Document):
    name = StringField(required=True)


class RefBeer(Document):
    name = StringField(required=True)
    brewery = ReferenceField(RefBrewery)


class RefBeerStr(Document):
    name = StringField(required=True)
    brewery = ReferenceField("RefBrewery")


class TestReferenceField:
    def test_to_python_string(self):
        f = ReferenceField(RefBrewery)
        f.name = "brewery"
        assert f.to_python("brewery::123") == "brewery::123"

    def test_to_python_none(self):
        f = ReferenceField(RefBrewery)
        f.name = "brewery"
        assert f.to_python(None) is None

    def test_to_json_string(self):
        f = ReferenceField(RefBrewery)
        f.name = "brewery"
        assert f.to_json("brewery::123") == "brewery::123"

    def test_to_json_document_instance(self):
        f = ReferenceField(RefBrewery)
        f.name = "brewery"
        b = RefBrewery(_id="b123", name="Test")
        assert f.to_json(b) == "b123"

    def test_to_json_none(self):
        f = ReferenceField(RefBrewery)
        f.name = "brewery"
        assert f.to_json(None) is None

    def test_validate_string(self):
        f = ReferenceField(RefBrewery)
        f.name = "brewery"
        f.validate("brewery::123")

    def test_validate_document(self):
        f = ReferenceField(RefBrewery)
        f.name = "brewery"
        b = RefBrewery(_id="b123", name="Test")
        f.validate(b)

    def test_validate_none(self):
        f = ReferenceField(RefBrewery)
        f.name = "brewery"
        f.validate(None)

    def test_validate_wrong_type(self):
        f = ReferenceField(RefBrewery)
        f.name = "brewery"
        with pytest.raises(ValidationError, match="expected a string key"):
            f.validate(12345)

    def test_validate_required(self):
        f = ReferenceField(RefBrewery, required=True)
        f.name = "brewery"
        with pytest.raises(ValidationError, match="required"):
            f.validate(None)

    def test_resolve_type_class(self):
        f = ReferenceField(RefBrewery)
        f.name = "brewery"
        assert f._resolve_type() is RefBrewery

    def test_resolve_type_string(self):
        f = ReferenceField("RefBrewery")
        f.name = "brewery"
        assert f._resolve_type() is RefBrewery

    def test_resolve_type_unknown(self):
        f = ReferenceField("NonExistentDoc")
        f.name = "test"
        with pytest.raises(ValidationError, match="Could not resolve"):
            f._resolve_type()

    def test_document_with_reference(self):
        """Test defining a document with a ReferenceField."""
        beer = RefBeer(name="IPA", brewery="brewery::123")
        assert beer.brewery == "brewery::123"
        d = beer.to_dict()
        assert d["brewery"] == "brewery::123"

    def test_document_with_string_reference(self):
        beer = RefBeerStr(name="IPA", brewery="brewery::123")
        assert beer.brewery == "brewery::123"

    def test_from_dict_with_reference(self):
        data = {"name": "IPA", "brewery": "brewery::123", "_type": "refbeer"}
        beer = RefBeer.from_dict("beer::1", data)
        assert beer.brewery == "brewery::123"

    def test_dereference(self, patch_collection):
        """Test dereferencing loads the referenced document."""
        patch_collection._store["b123"] = {
            "name": "Test Brewery",
            "_type": "refbrewery",
        }
        f = ReferenceField(RefBrewery)
        f.name = "brewery"
        result = f.dereference("b123")
        assert result.name == "Test Brewery"

    def test_dereference_not_found(self, patch_collection):
        f = ReferenceField(RefBrewery)
        f.name = "brewery"
        with pytest.raises(RefBrewery.DoesNotExist):
            f.dereference("nonexistent")
