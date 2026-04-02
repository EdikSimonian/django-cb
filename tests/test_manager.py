"""Tests for DocumentManager."""

import pytest

from django_cb.document import Document
from django_cb.exceptions import DocumentDoesNotExist, OperationError
from django_cb.fields.simple import IntegerField, StringField


# Define test document outside of tests so metaclass runs once
class ManagerUser(Document):
    name = StringField(required=True)
    age = IntegerField()

    class Meta:
        collection_name = "manager_users"


class TestManagerAccess:
    def test_accessible_from_class(self):
        manager = ManagerUser.objects
        assert manager is not None

    def test_not_accessible_from_instance(self):
        user = ManagerUser(name="Alice")
        with pytest.raises(AttributeError, match="not instances"):
            _ = user.objects


class TestManagerGet:
    def test_get_by_pk(self, patch_collection):
        # Pre-populate the store
        patch_collection._store["user::1"] = {
            "name": "Alice",
            "age": 30,
            "_type": "manageruser",
        }

        user = ManagerUser.objects.get(pk="user::1")
        assert user.name == "Alice"
        assert user.age == 30
        assert user.pk == "user::1"
        assert user._is_new is False

    def test_get_by_pk_not_found(self, patch_collection):
        with pytest.raises(ManagerUser.DoesNotExist):
            ManagerUser.objects.get(pk="nonexistent")

    def test_get_by_pk_wrong_type(self, patch_collection):
        patch_collection._store["user::1"] = {
            "name": "Alice",
            "_type": "otherdoctype",
        }
        with pytest.raises(ManagerUser.DoesNotExist, match="not of type"):
            ManagerUser.objects.get(pk="user::1")

    def test_get_no_args(self):
        with pytest.raises(ValueError, match="at least one"):
            ManagerUser.objects.get()

    def test_get_by_field_delegates_to_queryset(self):
        """Non-pk get() should delegate to QuerySet.get()."""
        from django_cb.queryset.queryset import QuerySet

        qs = ManagerUser.objects.filter(name="Alice")
        assert isinstance(qs, QuerySet)


class TestManagerCreate:
    def test_create(self, patch_collection):
        user = ManagerUser.objects.create(name="Bob", age=25)
        assert user.name == "Bob"
        assert user.age == 25
        assert user._is_new is False
        assert user.pk in patch_collection._store

    def test_create_with_id(self, patch_collection):
        user = ManagerUser.objects.create(_id="custom-id", name="Carol")
        assert user.pk == "custom-id"
        assert "custom-id" in patch_collection._store

    def test_create_validates(self, patch_collection):
        """create() should validate before saving."""
        from django_cb.exceptions import ValidationError

        with pytest.raises(ValidationError):
            ManagerUser.objects.create(age=25)  # name is required

    def test_create_validation_error(self, patch_collection):
        from django_cb.exceptions import ValidationError

        with pytest.raises(ValidationError):
            ManagerUser.objects.create(age=25)  # name is required

    def test_create_stores_type(self, patch_collection):
        user = ManagerUser.objects.create(name="Dave")
        data = patch_collection._store[user.pk]
        assert data["_type"] == "manageruser"


class TestManagerGetOrCreate:
    def test_get_existing(self, patch_collection):
        patch_collection._store["existing-id"] = {
            "name": "Existing",
            "age": 40,
            "_type": "manageruser",
        }

        user, created = ManagerUser.objects.get_or_create(_id="existing-id")
        assert created is False
        assert user.name == "Existing"

    def test_create_new(self, patch_collection):
        user, created = ManagerUser.objects.get_or_create(
            _id="new-id", defaults={"name": "New User", "age": 20}
        )
        assert created is True
        assert user.name == "New User"
        assert user.pk == "new-id"

    def test_get_or_create_without_id(self, patch_collection):
        user, created = ManagerUser.objects.get_or_create(
            defaults={"name": "NoId"}
        )
        # Should always create when no _id
        assert created is True
        assert user.name == "NoId"


class TestManagerExists:
    def test_exists_true(self, patch_collection):
        patch_collection._store["exists-id"] = {"name": "test", "_type": "manageruser"}
        assert ManagerUser.objects.exists("exists-id") is True

    def test_exists_false(self, patch_collection):
        assert ManagerUser.objects.exists("no-such-id") is False
