"""Tests for DateTimeField and DateField."""

from datetime import date, datetime, timezone, timedelta
from unittest.mock import patch

import pytest

from django_cb.exceptions import ValidationError
from django_cb.fields.datetime import DateField, DateTimeField


class TestDateTimeField:
    def test_to_python_from_string(self):
        f = DateTimeField()
        f.name = "test"
        result = f.to_python("2024-01-15T10:30:00")
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.hour == 10

    def test_to_python_from_datetime(self):
        f = DateTimeField()
        dt = datetime(2024, 6, 15, 12, 0)
        assert f.to_python(dt) is dt

    def test_to_python_none(self):
        f = DateTimeField()
        assert f.to_python(None) is None

    def test_to_python_invalid(self):
        f = DateTimeField()
        f.name = "test"
        with pytest.raises(ValidationError, match="could not parse"):
            f.to_python("not-a-date")

    def test_to_python_wrong_type(self):
        f = DateTimeField()
        f.name = "test"
        with pytest.raises(ValidationError, match="expected a datetime"):
            f.to_python(12345)

    def test_to_json(self):
        f = DateTimeField()
        f.name = "test"
        dt = datetime(2024, 1, 15, 10, 30, 0)
        result = f.to_json(dt)
        assert result == "2024-01-15T10:30:00"

    def test_to_json_with_tz(self):
        f = DateTimeField()
        f.name = "test"
        dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        result = f.to_json(dt)
        assert "2024-01-15" in result
        assert "10:30" in result

    def test_to_json_string_passthrough(self):
        f = DateTimeField()
        f.name = "test"
        result = f.to_json("2024-01-15T10:30:00")
        assert result == "2024-01-15T10:30:00"

    def test_to_json_none(self):
        f = DateTimeField()
        assert f.to_json(None) is None

    def test_validate_valid(self):
        f = DateTimeField()
        f.name = "test"
        f.validate(datetime.now())
        f.validate("2024-01-15T10:30:00")
        f.validate(None)

    def test_validate_invalid_type(self):
        f = DateTimeField()
        f.name = "test"
        with pytest.raises(ValidationError, match="expected a datetime"):
            f.validate(12345)

    def test_validate_invalid_string(self):
        f = DateTimeField()
        f.name = "test"
        with pytest.raises(ValidationError, match="not a valid ISO"):
            f.validate("not-a-date")

    def test_validate_required(self):
        f = DateTimeField(required=True)
        f.name = "test"
        with pytest.raises(ValidationError, match="required"):
            f.validate(None)

    def test_auto_now(self):
        f = DateTimeField(auto_now=True)
        f.name = "updated"
        result = f.pre_save_value(None, is_new=False)
        assert isinstance(result, datetime)
        assert result.tzinfo == timezone.utc

    def test_auto_now_always_updates(self):
        f = DateTimeField(auto_now=True)
        f.name = "updated"
        old = datetime(2020, 1, 1, tzinfo=timezone.utc)
        result = f.pre_save_value(old, is_new=False)
        assert result > old

    def test_auto_now_add_on_create(self):
        f = DateTimeField(auto_now_add=True)
        f.name = "created"
        result = f.pre_save_value(None, is_new=True)
        assert isinstance(result, datetime)

    def test_auto_now_add_not_on_update(self):
        f = DateTimeField(auto_now_add=True)
        f.name = "created"
        old = datetime(2020, 1, 1, tzinfo=timezone.utc)
        result = f.pre_save_value(old, is_new=False)
        assert result == old

    def test_roundtrip(self):
        f = DateTimeField()
        f.name = "test"
        dt = datetime(2024, 6, 15, 12, 30, 45, tzinfo=timezone.utc)
        json_val = f.to_json(dt)
        restored = f.to_python(json_val)
        assert restored == dt


class TestDateField:
    def test_to_python_from_string(self):
        f = DateField()
        f.name = "test"
        result = f.to_python("2024-01-15")
        assert isinstance(result, date)
        assert result == date(2024, 1, 15)

    def test_to_python_from_date(self):
        f = DateField()
        d = date(2024, 1, 15)
        assert f.to_python(d) is d

    def test_to_python_from_datetime(self):
        f = DateField()
        dt = datetime(2024, 1, 15, 10, 30)
        result = f.to_python(dt)
        assert result == date(2024, 1, 15)

    def test_to_python_none(self):
        f = DateField()
        assert f.to_python(None) is None

    def test_to_python_invalid(self):
        f = DateField()
        f.name = "test"
        with pytest.raises(ValidationError, match="could not parse"):
            f.to_python("not-a-date")

    def test_to_json(self):
        f = DateField()
        f.name = "test"
        assert f.to_json(date(2024, 1, 15)) == "2024-01-15"

    def test_to_json_from_datetime(self):
        f = DateField()
        f.name = "test"
        assert f.to_json(datetime(2024, 1, 15, 10, 30)) == "2024-01-15"

    def test_to_json_none(self):
        f = DateField()
        assert f.to_json(None) is None

    def test_validate_valid(self):
        f = DateField()
        f.name = "test"
        f.validate(date.today())
        f.validate("2024-01-15")
        f.validate(None)

    def test_validate_invalid_type(self):
        f = DateField()
        f.name = "test"
        with pytest.raises(ValidationError, match="expected a date"):
            f.validate(12345)

    def test_validate_invalid_string(self):
        f = DateField()
        f.name = "test"
        with pytest.raises(ValidationError, match="not a valid ISO"):
            f.validate("not-a-date")

    def test_auto_now(self):
        f = DateField(auto_now=True)
        f.name = "updated"
        result = f.pre_save_value(None, is_new=False)
        assert isinstance(result, date)
        assert result == date.today()

    def test_auto_now_add(self):
        f = DateField(auto_now_add=True)
        f.name = "created"
        result = f.pre_save_value(None, is_new=True)
        assert result == date.today()

    def test_auto_now_add_not_on_update(self):
        f = DateField(auto_now_add=True)
        f.name = "created"
        old = date(2020, 1, 1)
        assert f.pre_save_value(old, is_new=False) == old

    def test_roundtrip(self):
        f = DateField()
        f.name = "test"
        d = date(2024, 6, 15)
        json_val = f.to_json(d)
        restored = f.to_python(json_val)
        assert restored == d
