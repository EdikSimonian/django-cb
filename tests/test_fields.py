"""Comprehensive tests for field types: validation, serialization, defaults, edge cases."""

import uuid

import pytest

from django_cb.exceptions import ValidationError
from django_cb.fields.base import BaseField
from django_cb.fields.simple import (
    BooleanField,
    FloatField,
    IntegerField,
    StringField,
    UUIDField,
)


# ============================================================
# BaseField
# ============================================================


class TestBaseField:
    def test_creation_order_increments(self):
        f1 = BaseField()
        f2 = BaseField()
        assert f2._creation_order > f1._creation_order

    def test_default_static(self):
        f = BaseField(default="hello")
        assert f.get_default() == "hello"

    def test_default_callable(self):
        f = BaseField(default=list)
        d1 = f.get_default()
        d2 = f.get_default()
        assert d1 == []
        assert d1 is not d2  # new list each time

    def test_default_deepcopy(self):
        """Static mutable defaults should be deep-copied."""
        f = BaseField(default=[1, 2, 3])
        d1 = f.get_default()
        d2 = f.get_default()
        d1.append(4)
        assert d2 == [1, 2, 3]

    def test_has_default(self):
        # 0 is not None, so has_default should be True
        assert BaseField(default=0).has_default() is True
        assert BaseField().has_default() is False
        assert BaseField(default="").has_default() is True
        assert BaseField(default=False).has_default() is True

    def test_get_db_field_default(self):
        f = BaseField()
        f.name = "my_field"
        assert f.get_db_field() == "my_field"

    def test_get_db_field_override(self):
        f = BaseField(db_field="myField")
        f.name = "my_field"
        assert f.get_db_field() == "myField"

    def test_validate_required_none(self):
        f = BaseField(required=True)
        f.name = "test"
        with pytest.raises(ValidationError, match="required"):
            f.validate(None)

    def test_validate_required_with_value(self):
        f = BaseField(required=True)
        f.name = "test"
        f.validate("something")  # should not raise

    def test_validate_not_required_none(self):
        f = BaseField(required=False)
        f.name = "test"
        f.validate(None)  # should not raise

    def test_validate_choices_valid(self):
        f = BaseField(choices=["a", "b", "c"])
        f.name = "test"
        f.validate("a")  # should not raise

    def test_validate_choices_invalid(self):
        f = BaseField(choices=["a", "b", "c"])
        f.name = "test"
        with pytest.raises(ValidationError, match="not a valid choice"):
            f.validate("d")

    def test_validate_choices_tuple_format(self):
        """Choices can be (value, label) tuples."""
        f = BaseField(choices=[("a", "Alpha"), ("b", "Beta")])
        f.name = "test"
        f.validate("a")  # should not raise
        with pytest.raises(ValidationError):
            f.validate("Alpha")  # label is not a valid choice

    def test_validate_custom_validator(self):
        def must_be_positive(value):
            if value <= 0:
                raise ValidationError("Must be positive")

        f = BaseField(validators=[must_be_positive])
        f.name = "test"
        f.validate(5)  # should not raise
        with pytest.raises(ValidationError, match="positive"):
            f.validate(-1)

    def test_validate_multiple_validators(self):
        call_log = []

        def v1(value):
            call_log.append("v1")

        def v2(value):
            call_log.append("v2")

        f = BaseField(validators=[v1, v2])
        f.name = "test"
        f.validate("x")
        assert call_log == ["v1", "v2"]

    def test_to_python_passthrough(self):
        f = BaseField()
        assert f.to_python(42) == 42
        assert f.to_python("hello") == "hello"

    def test_to_json_passthrough(self):
        f = BaseField()
        assert f.to_json(42) == 42

    def test_repr(self):
        f = BaseField()
        f.name = "test_field"
        assert "BaseField" in repr(f)
        assert "test_field" in repr(f)


# ============================================================
# StringField
# ============================================================


class TestStringField:
    def test_basic(self):
        f = StringField()
        f.name = "test"
        f.validate("hello")
        assert f.to_python("hello") == "hello"
        assert f.to_json("hello") == "hello"

    def test_to_python_coerces(self):
        f = StringField()
        f.name = "test"
        assert f.to_python(42) == "42"
        assert f.to_python(True) == "True"

    def test_to_python_none(self):
        f = StringField()
        assert f.to_python(None) is None

    def test_to_json_none(self):
        f = StringField()
        assert f.to_json(None) is None

    def test_validate_not_string(self):
        f = StringField()
        f.name = "test"
        with pytest.raises(ValidationError, match="expected a string"):
            f.validate(42)

    def test_validate_bool_not_string(self):
        f = StringField()
        f.name = "test"
        with pytest.raises(ValidationError, match="expected a string"):
            f.validate(True)

    def test_min_length_valid(self):
        f = StringField(min_length=3)
        f.name = "test"
        f.validate("abc")  # exactly 3

    def test_min_length_invalid(self):
        f = StringField(min_length=3)
        f.name = "test"
        with pytest.raises(ValidationError, match="too short"):
            f.validate("ab")

    def test_max_length_valid(self):
        f = StringField(max_length=5)
        f.name = "test"
        f.validate("hello")  # exactly 5

    def test_max_length_invalid(self):
        f = StringField(max_length=5)
        f.name = "test"
        with pytest.raises(ValidationError, match="too long"):
            f.validate("toolong")

    def test_min_and_max_length(self):
        f = StringField(min_length=2, max_length=5)
        f.name = "test"
        f.validate("hi")
        f.validate("hello")
        with pytest.raises(ValidationError):
            f.validate("x")
        with pytest.raises(ValidationError):
            f.validate("toolong")

    def test_regex_valid(self):
        f = StringField(regex=r"^\d{3}-\d{4}$")
        f.name = "test"
        f.validate("123-4567")

    def test_regex_invalid(self):
        f = StringField(regex=r"^\d{3}-\d{4}$")
        f.name = "test"
        with pytest.raises(ValidationError, match="does not match"):
            f.validate("abc-defg")

    def test_empty_string(self):
        f = StringField()
        f.name = "test"
        f.validate("")  # valid — it's a string

    def test_empty_string_with_min_length(self):
        f = StringField(min_length=1)
        f.name = "test"
        with pytest.raises(ValidationError, match="too short"):
            f.validate("")

    def test_required_empty_string(self):
        """An empty string is not None, so it passes required check."""
        f = StringField(required=True)
        f.name = "test"
        f.validate("")  # should not raise

    def test_choices(self):
        f = StringField(choices=["red", "blue", "green"])
        f.name = "color"
        f.validate("red")
        with pytest.raises(ValidationError, match="not a valid choice"):
            f.validate("yellow")


# ============================================================
# IntegerField
# ============================================================


class TestIntegerField:
    def test_basic(self):
        f = IntegerField()
        f.name = "test"
        f.validate(42)
        assert f.to_python(42) == 42
        assert f.to_json(42) == 42

    def test_to_python_coerces(self):
        f = IntegerField()
        f.name = "test"
        assert f.to_python("42") == 42
        assert f.to_python(42.9) == 42

    def test_to_python_invalid(self):
        f = IntegerField()
        f.name = "test"
        with pytest.raises(ValidationError, match="could not convert"):
            f.to_python("not_a_number")

    def test_to_python_none(self):
        f = IntegerField()
        assert f.to_python(None) is None

    def test_validate_not_int(self):
        f = IntegerField()
        f.name = "test"
        with pytest.raises(ValidationError, match="expected an integer"):
            f.validate("42")

    def test_validate_float_not_int(self):
        f = IntegerField()
        f.name = "test"
        with pytest.raises(ValidationError, match="expected an integer"):
            f.validate(42.5)

    def test_validate_bool_not_int(self):
        """bool is a subclass of int but should not be accepted."""
        f = IntegerField()
        f.name = "test"
        with pytest.raises(ValidationError, match="expected an integer"):
            f.validate(True)

    def test_min_value(self):
        f = IntegerField(min_value=0)
        f.name = "test"
        f.validate(0)
        f.validate(100)
        with pytest.raises(ValidationError, match="less than minimum"):
            f.validate(-1)

    def test_max_value(self):
        f = IntegerField(max_value=100)
        f.name = "test"
        f.validate(100)
        with pytest.raises(ValidationError, match="greater than maximum"):
            f.validate(101)

    def test_min_and_max_value(self):
        f = IntegerField(min_value=1, max_value=10)
        f.name = "test"
        f.validate(1)
        f.validate(10)
        with pytest.raises(ValidationError):
            f.validate(0)
        with pytest.raises(ValidationError):
            f.validate(11)

    def test_zero(self):
        f = IntegerField()
        f.name = "test"
        f.validate(0)
        assert f.to_json(0) == 0

    def test_negative(self):
        f = IntegerField()
        f.name = "test"
        f.validate(-42)
        assert f.to_json(-42) == -42

    def test_large_int(self):
        f = IntegerField()
        f.name = "test"
        big = 10**18
        f.validate(big)
        assert f.to_json(big) == big


# ============================================================
# FloatField
# ============================================================


class TestFloatField:
    def test_basic(self):
        f = FloatField()
        f.name = "test"
        f.validate(3.14)
        assert f.to_python(3.14) == 3.14
        assert f.to_json(3.14) == 3.14

    def test_accepts_int(self):
        f = FloatField()
        f.name = "test"
        f.validate(42)  # int is a valid number

    def test_to_python_coerces(self):
        f = FloatField()
        f.name = "test"
        assert f.to_python("3.14") == 3.14
        assert f.to_python(42) == 42.0

    def test_to_python_invalid(self):
        f = FloatField()
        f.name = "test"
        with pytest.raises(ValidationError, match="could not convert"):
            f.to_python("not_a_number")

    def test_to_python_none(self):
        f = FloatField()
        assert f.to_python(None) is None

    def test_validate_not_number(self):
        f = FloatField()
        f.name = "test"
        with pytest.raises(ValidationError, match="expected a number"):
            f.validate("3.14")

    def test_validate_bool_not_number(self):
        f = FloatField()
        f.name = "test"
        with pytest.raises(ValidationError, match="expected a number"):
            f.validate(True)

    def test_min_value(self):
        f = FloatField(min_value=0.0)
        f.name = "test"
        f.validate(0.0)
        with pytest.raises(ValidationError, match="less than minimum"):
            f.validate(-0.1)

    def test_max_value(self):
        f = FloatField(max_value=1.0)
        f.name = "test"
        f.validate(1.0)
        with pytest.raises(ValidationError, match="greater than maximum"):
            f.validate(1.1)

    def test_none_valid(self):
        f = FloatField()
        f.name = "test"
        f.validate(None)

    def test_zero(self):
        f = FloatField()
        f.name = "test"
        f.validate(0.0)
        assert f.to_json(0.0) == 0.0


# ============================================================
# BooleanField
# ============================================================


class TestBooleanField:
    def test_basic(self):
        f = BooleanField()
        f.name = "test"
        f.validate(True)
        f.validate(False)
        assert f.to_python(True) is True
        assert f.to_python(False) is False

    def test_to_json(self):
        f = BooleanField()
        assert f.to_json(True) is True
        assert f.to_json(False) is False

    def test_to_python_coerces(self):
        f = BooleanField()
        assert f.to_python(1) is True
        assert f.to_python(0) is False
        assert f.to_python("") is False

    def test_to_python_none(self):
        f = BooleanField()
        assert f.to_python(None) is None

    def test_validate_not_bool(self):
        f = BooleanField()
        f.name = "test"
        with pytest.raises(ValidationError, match="expected a boolean"):
            f.validate(1)
        with pytest.raises(ValidationError, match="expected a boolean"):
            f.validate("true")

    def test_none_valid(self):
        f = BooleanField()
        f.name = "test"
        f.validate(None)

    def test_required(self):
        f = BooleanField(required=True)
        f.name = "test"
        with pytest.raises(ValidationError, match="required"):
            f.validate(None)
        f.validate(False)  # False is not None


# ============================================================
# UUIDField
# ============================================================


class TestUUIDField:
    def test_basic(self):
        f = UUIDField()
        f.name = "test"
        u = uuid.uuid4()
        f.validate(u)
        assert f.to_python(str(u)) == u
        assert f.to_json(u) == str(u)

    def test_to_python_from_string(self):
        f = UUIDField()
        f.name = "test"
        s = "12345678-1234-5678-1234-567812345678"
        result = f.to_python(s)
        assert isinstance(result, uuid.UUID)
        assert str(result) == s

    def test_to_python_from_uuid(self):
        f = UUIDField()
        u = uuid.uuid4()
        assert f.to_python(u) is u

    def test_to_python_invalid(self):
        f = UUIDField()
        f.name = "test"
        with pytest.raises(ValidationError, match="could not convert"):
            f.to_python("not-a-uuid")

    def test_to_python_none(self):
        f = UUIDField()
        assert f.to_python(None) is None

    def test_to_json_from_uuid(self):
        f = UUIDField()
        u = uuid.uuid4()
        assert f.to_json(u) == str(u)

    def test_to_json_none(self):
        f = UUIDField()
        assert f.to_json(None) is None

    def test_validate_string_uuid(self):
        f = UUIDField()
        f.name = "test"
        f.validate("12345678-1234-5678-1234-567812345678")  # should not raise

    def test_validate_invalid_string(self):
        f = UUIDField()
        f.name = "test"
        with pytest.raises(ValidationError, match="not a valid UUID"):
            f.validate("not-a-uuid")

    def test_auto_generates_default(self):
        f = UUIDField(auto=True)
        assert f.has_default()
        d = f.get_default()
        assert isinstance(d, uuid.UUID)
        # Each call generates a new UUID
        assert f.get_default() != d

    def test_auto_false_no_default(self):
        f = UUIDField(auto=False)
        assert not f.has_default()

    def test_none_valid(self):
        f = UUIDField()
        f.name = "test"
        f.validate(None)

    def test_required(self):
        f = UUIDField(required=True)
        f.name = "test"
        with pytest.raises(ValidationError, match="required"):
            f.validate(None)
