"""Tests for compound fields: ListField, DictField, EmbeddedDocumentField."""

import pytest

from django_cb.exceptions import ValidationError
from django_cb.fields.compound import (
    DictField,
    EmbeddedDocument,
    EmbeddedDocumentField,
    ListField,
)
from django_cb.fields.simple import IntegerField, StringField


# ============================================================
# ListField
# ============================================================


class TestListField:
    def test_basic(self):
        f = ListField()
        f.name = "tags"
        f.validate(["a", "b", "c"])
        assert f.to_python(["a", "b"]) == ["a", "b"]
        assert f.to_json(["a", "b"]) == ["a", "b"]

    def test_none_valid(self):
        f = ListField()
        f.name = "tags"
        f.validate(None)
        assert f.to_python(None) is None
        assert f.to_json(None) is None

    def test_not_a_list(self):
        f = ListField()
        f.name = "tags"
        with pytest.raises(ValidationError, match="expected a list"):
            f.validate("not a list")

    def test_tuple_accepted(self):
        f = ListField()
        f.name = "tags"
        f.validate(("a", "b"))
        assert f.to_python(("a", "b")) == ["a", "b"]

    def test_empty_list(self):
        f = ListField()
        f.name = "tags"
        f.validate([])
        assert f.to_json([]) == []

    def test_typed_field(self):
        f = ListField(field=IntegerField())
        f.name = "scores"
        f.validate([1, 2, 3])

    def test_typed_field_invalid_item(self):
        f = ListField(field=IntegerField())
        f.name = "scores"
        with pytest.raises(ValidationError, match="index 1"):
            f.validate([1, "not_int", 3])

    def test_typed_field_coerces(self):
        f = ListField(field=IntegerField())
        f.name = "scores"
        result = f.to_python(["1", "2", "3"])
        assert result == [1, 2, 3]

    def test_min_length(self):
        f = ListField(min_length=2)
        f.name = "tags"
        f.validate(["a", "b"])
        with pytest.raises(ValidationError, match="too short"):
            f.validate(["a"])

    def test_max_length(self):
        f = ListField(max_length=3)
        f.name = "tags"
        f.validate(["a", "b", "c"])
        with pytest.raises(ValidationError, match="too long"):
            f.validate(["a", "b", "c", "d"])

    def test_required(self):
        f = ListField(required=True)
        f.name = "tags"
        with pytest.raises(ValidationError, match="required"):
            f.validate(None)

    def test_to_python_not_a_list(self):
        f = ListField()
        f.name = "tags"
        with pytest.raises(ValidationError, match="expected a list"):
            f.to_python("string")

    def test_nested_list(self):
        f = ListField()
        f.name = "matrix"
        val = [[1, 2], [3, 4]]
        f.validate(val)
        assert f.to_json(val) == val


# ============================================================
# DictField
# ============================================================


class TestDictField:
    def test_basic(self):
        f = DictField()
        f.name = "meta"
        f.validate({"key": "value"})
        assert f.to_python({"a": 1}) == {"a": 1}
        assert f.to_json({"a": 1}) == {"a": 1}

    def test_none_valid(self):
        f = DictField()
        f.name = "meta"
        f.validate(None)
        assert f.to_python(None) is None
        assert f.to_json(None) is None

    def test_not_a_dict(self):
        f = DictField()
        f.name = "meta"
        with pytest.raises(ValidationError, match="expected a dict"):
            f.validate(["not", "a", "dict"])

    def test_empty_dict(self):
        f = DictField()
        f.name = "meta"
        f.validate({})
        assert f.to_json({}) == {}

    def test_nested_dict(self):
        f = DictField()
        f.name = "meta"
        val = {"geo": {"lat": 37.7, "lon": -122.4}}
        f.validate(val)
        assert f.to_json(val) == val

    def test_required(self):
        f = DictField(required=True)
        f.name = "meta"
        with pytest.raises(ValidationError, match="required"):
            f.validate(None)

    def test_to_python_not_a_dict(self):
        f = DictField()
        f.name = "meta"
        with pytest.raises(ValidationError, match="expected a dict"):
            f.to_python("string")


# ============================================================
# EmbeddedDocument and EmbeddedDocumentField
# ============================================================


class Address(EmbeddedDocument):
    street = StringField()
    city = StringField(required=True)
    state = StringField()
    zip_code = StringField(db_field="zipCode")


class GeoPoint(EmbeddedDocument):
    lat = IntegerField(required=True)
    lon = IntegerField(required=True)


class TestEmbeddedDocument:
    def test_create(self):
        addr = Address(city="SF", state="CA")
        assert addr.city == "SF"
        assert addr.state == "CA"
        assert addr.street is None

    def test_unexpected_field(self):
        with pytest.raises(TypeError, match="Unexpected field"):
            Address(bogus="bad")

    def test_to_dict(self):
        addr = Address(street="123 Main", city="SF", zip_code="94107")
        d = addr.to_dict()
        assert d["city"] == "SF"
        assert d["street"] == "123 Main"
        assert d["zipCode"] == "94107"  # db_field mapping
        assert "state" not in d  # None values excluded

    def test_from_dict(self):
        data = {"street": "456 Oak", "city": "LA", "state": "CA", "zipCode": "90001"}
        addr = Address.from_dict(data)
        assert addr.city == "LA"
        assert addr.zip_code == "90001"

    def test_validate_valid(self):
        addr = Address(city="SF")
        addr.validate()  # should not raise

    def test_validate_missing_required(self):
        addr = Address()
        with pytest.raises(ValidationError) as exc_info:
            addr.validate()
        assert "city" in exc_info.value.errors

    def test_equality(self):
        a1 = Address(city="SF", state="CA")
        a2 = Address(city="SF", state="CA")
        a3 = Address(city="LA", state="CA")
        assert a1 == a2
        assert a1 != a3

    def test_repr(self):
        addr = Address(city="SF")
        assert "Address" in repr(addr)

    def test_setattr(self):
        addr = Address(city="SF")
        addr.city = "LA"
        assert addr.city == "LA"

    def test_defaults(self):
        class WithDefault(EmbeddedDocument):
            name = StringField(default="unknown")

        obj = WithDefault()
        assert obj.name == "unknown"

    def test_roundtrip(self):
        addr = Address(street="123 Main", city="SF", state="CA", zip_code="94107")
        d = addr.to_dict()
        restored = Address.from_dict(d)
        assert restored == addr


class TestEmbeddedDocumentField:
    def test_basic(self):
        f = EmbeddedDocumentField(Address)
        f.name = "address"
        addr = Address(city="SF")
        f.validate(addr)

    def test_to_python_from_dict(self):
        f = EmbeddedDocumentField(Address)
        f.name = "address"
        result = f.to_python({"city": "SF", "state": "CA"})
        assert isinstance(result, Address)
        assert result.city == "SF"

    def test_to_python_from_instance(self):
        f = EmbeddedDocumentField(Address)
        f.name = "address"
        addr = Address(city="SF")
        assert f.to_python(addr) is addr

    def test_to_python_none(self):
        f = EmbeddedDocumentField(Address)
        f.name = "address"
        assert f.to_python(None) is None

    def test_to_json(self):
        f = EmbeddedDocumentField(Address)
        f.name = "address"
        addr = Address(city="SF", zip_code="94107")
        result = f.to_json(addr)
        assert result == {"city": "SF", "zipCode": "94107"}

    def test_to_json_none(self):
        f = EmbeddedDocumentField(Address)
        f.name = "address"
        assert f.to_json(None) is None

    def test_validate_dict_input(self):
        f = EmbeddedDocumentField(Address)
        f.name = "address"
        f.validate({"city": "SF"})  # valid

    def test_validate_invalid_embedded(self):
        f = EmbeddedDocumentField(Address)
        f.name = "address"
        with pytest.raises(ValidationError):
            f.validate({"street": "123"})  # city is required

    def test_validate_wrong_type(self):
        f = EmbeddedDocumentField(Address)
        f.name = "address"
        with pytest.raises(ValidationError, match="expected a"):
            f.validate("not an address")

    def test_required(self):
        f = EmbeddedDocumentField(Address, required=True)
        f.name = "address"
        with pytest.raises(ValidationError, match="required"):
            f.validate(None)

    def test_nested_embedded(self):
        """EmbeddedDocument within EmbeddedDocument."""
        class Location(EmbeddedDocument):
            address = EmbeddedDocumentField(Address)
            geo = EmbeddedDocumentField(GeoPoint)

        loc = Location(
            address=Address(city="SF"),
            geo=GeoPoint(lat=37, lon=-122),
        )
        d = loc.to_dict()
        assert d["address"]["city"] == "SF"
        assert d["geo"]["lat"] == 37
