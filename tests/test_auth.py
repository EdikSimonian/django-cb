"""Tests for the Couchbase auth backend and User model."""

import pytest
from unittest.mock import MagicMock, patch

from django_cb.contrib.auth.models import User
from django_cb.contrib.auth.backend import CouchbaseAuthBackend


class TestUserModel:
    def test_create_user_fields(self):
        user = User(username="alice", email="alice@example.com")
        assert user.username == "alice"
        assert user.email == "alice@example.com"
        assert user.is_active is True
        assert user.is_staff is False
        assert user.is_superuser is False

    def test_set_password(self):
        user = User(username="alice")
        user.set_password("secret123")
        assert user.password is not None
        assert user.password != "secret123"  # Should be hashed
        assert user.password.startswith("pbkdf2_sha256$") or user.password.startswith("argon2")

    def test_check_password(self):
        user = User(username="alice")
        user.set_password("secret123")
        assert user.check_password("secret123") is True
        assert user.check_password("wrong") is False

    def test_check_password_empty(self):
        user = User(username="alice")
        assert user.check_password("anything") is False

    def test_set_unusable_password(self):
        user = User(username="alice")
        user.set_unusable_password()
        assert user.has_usable_password() is False
        assert user.check_password("anything") is False

    def test_has_usable_password(self):
        user = User(username="alice")
        user.set_password("secret")
        assert user.has_usable_password() is True

    def test_is_authenticated(self):
        user = User(username="alice")
        assert user.is_authenticated is True

    def test_is_anonymous(self):
        user = User(username="alice")
        assert user.is_anonymous is False

    def test_get_username(self):
        user = User(username="alice")
        assert user.get_username() == "alice"

    def test_get_full_name(self):
        user = User(username="alice", first_name="Alice", last_name="Smith")
        assert user.get_full_name() == "Alice Smith"

    def test_get_full_name_partial(self):
        user = User(username="alice", first_name="Alice")
        assert user.get_full_name() == "Alice"

    def test_get_full_name_empty(self):
        user = User(username="alice")
        assert user.get_full_name() == ""

    def test_get_short_name(self):
        user = User(username="alice", first_name="Alice")
        assert user.get_short_name() == "Alice"

    def test_get_short_name_fallback(self):
        user = User(username="alice")
        assert user.get_short_name() == "alice"

    def test_str(self):
        user = User(username="alice")
        assert str(user) == "alice"

    def test_to_dict_includes_hashed_password(self):
        user = User(username="alice")
        user.set_password("secret")
        d = user.to_dict()
        assert d["username"] == "alice"
        assert "secret" not in d["password"]

    def test_create_user(self, patch_collection):
        user = User.create_user("bob", "bob@example.com", "pass123")
        assert user.username == "bob"
        assert user.email == "bob@example.com"
        assert user.check_password("pass123")
        assert user._is_new is False
        assert user.pk in patch_collection._store

    def test_create_user_no_password(self, patch_collection):
        user = User.create_user("nopass")
        assert user.has_usable_password() is False

    def test_create_superuser(self, patch_collection):
        user = User.create_superuser("admin", "admin@example.com", "admin123")
        assert user.is_staff is True
        assert user.is_superuser is True
        assert user.is_active is True
        assert user.check_password("admin123")


class TestCouchbaseAuthBackend:
    def test_authenticate_success(self, patch_collection):
        # Create a user first
        user = User.create_user("alice", "alice@example.com", "secret123")

        # Mock get_by_username to return the user
        backend = CouchbaseAuthBackend()
        with patch.object(User, "get_by_username", return_value=user):
            result = backend.authenticate(None, username="alice", password="secret123")
        assert result is not None
        assert result.username == "alice"

    def test_authenticate_wrong_password(self, patch_collection):
        user = User.create_user("alice", "alice@example.com", "secret123")

        backend = CouchbaseAuthBackend()
        with patch.object(User, "get_by_username", return_value=user):
            result = backend.authenticate(None, username="alice", password="wrong")
        assert result is None

    def test_authenticate_nonexistent_user(self, patch_collection):
        backend = CouchbaseAuthBackend()
        with patch.object(User, "get_by_username", side_effect=User.DoesNotExist):
            with patch.object(User, "get_by_email", side_effect=User.DoesNotExist):
                result = backend.authenticate(None, username="ghost", password="pass")
        assert result is None

    def test_authenticate_by_email(self, patch_collection):
        user = User.create_user("alice", "alice@example.com", "secret123")

        backend = CouchbaseAuthBackend()
        with patch.object(User, "get_by_username", side_effect=User.DoesNotExist):
            with patch.object(User, "get_by_email", return_value=user):
                result = backend.authenticate(None, username="alice@example.com", password="secret123")
        assert result is not None

    def test_authenticate_inactive_user(self, patch_collection):
        user = User.create_user("inactive", "i@example.com", "pass", is_active=False)

        backend = CouchbaseAuthBackend()
        with patch.object(User, "get_by_username", return_value=user):
            result = backend.authenticate(None, username="inactive", password="pass")
        assert result is None

    def test_authenticate_none_args(self):
        backend = CouchbaseAuthBackend()
        assert backend.authenticate(None) is None
        assert backend.authenticate(None, username="alice") is None
        assert backend.authenticate(None, password="pass") is None

    def test_get_user(self, patch_collection):
        user = User.create_user("alice", password="pass")

        backend = CouchbaseAuthBackend()
        result = backend.get_user(user.pk)
        assert result is not None
        assert result.username == "alice"

    def test_get_user_not_found(self, patch_collection):
        backend = CouchbaseAuthBackend()
        result = backend.get_user("nonexistent-id")
        assert result is None

    def test_has_perm_always_false(self):
        backend = CouchbaseAuthBackend()
        user = User(username="alice")
        assert backend.has_perm(user, "some.perm") is False
        assert backend.has_module_perms(user, "some_app") is False
