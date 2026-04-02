"""Tests for the exceptions module."""

from django_cb.exceptions import (
    ConnectionError,
    DjangoCbError,
    DocumentDoesNotExist,
    MultipleDocumentsReturned,
    OperationError,
    ValidationError,
)


class TestExceptionHierarchy:
    def test_all_inherit_from_base(self):
        assert issubclass(ValidationError, DjangoCbError)
        assert issubclass(DocumentDoesNotExist, DjangoCbError)
        assert issubclass(MultipleDocumentsReturned, DjangoCbError)
        assert issubclass(ConnectionError, DjangoCbError)
        assert issubclass(OperationError, DjangoCbError)

    def test_can_catch_with_base(self):
        try:
            raise ValidationError("oops")
        except DjangoCbError:
            pass  # should be caught


class TestValidationError:
    def test_message_string(self):
        e = ValidationError("something went wrong")
        assert str(e) == "something went wrong"
        assert e.message == "something went wrong"
        assert e.errors == {}

    def test_message_dict(self):
        errs = {"name": "required", "age": "must be positive"}
        e = ValidationError(errs)
        assert e.errors == errs
        assert "name" in str(e)

    def test_errors_kwarg(self):
        errs = {"field1": "error1"}
        e = ValidationError(errors=errs)
        assert e.errors == errs

    def test_default_message(self):
        e = ValidationError()
        assert e.message == "Validation error"
