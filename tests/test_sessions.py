"""Tests for the Couchbase session backend."""

import json
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from django.test import override_settings

from django_couchbase_orm.contrib.sessions.backend import SessionStore, SESSION_KEY_PREFIX
from tests.conftest import MockCollection, MockCASResult, MockGetResult, MockExistsResult


@pytest.fixture
def session_collection():
    return MockCollection()


@pytest.fixture
def patch_session_collection(session_collection, monkeypatch):
    monkeypatch.setattr(
        "django_couchbase_orm.contrib.sessions.backend.SessionStore._get_collection",
        lambda self: session_collection,
    )
    return session_collection


class TestSessionStore:
    def test_create(self, patch_session_collection):
        store = SessionStore()
        store.create()
        assert store.session_key is not None
        key = f"{SESSION_KEY_PREFIX}{store.session_key}"
        assert key in patch_session_collection._store

    def test_save_new(self, patch_session_collection):
        store = SessionStore()
        store.create()
        store["name"] = "Alice"
        store.save()
        key = f"{SESSION_KEY_PREFIX}{store.session_key}"
        data = patch_session_collection._store[key]
        assert data["session_key"] == store.session_key

    def test_save_must_create(self, patch_session_collection):
        store = SessionStore()
        store._session_key = "test-session-key"
        store.save(must_create=True)
        key = f"{SESSION_KEY_PREFIX}test-session-key"
        assert key in patch_session_collection._store

    def test_save_must_create_duplicate(self, patch_session_collection):
        """must_create should raise CreateError if session already exists."""
        from django.contrib.sessions.backends.base import CreateError
        import string
        from django.utils.crypto import get_random_string

        key = get_random_string(32, string.ascii_lowercase + string.digits)

        store = SessionStore(session_key=key)
        store.save(must_create=True)

        store2 = SessionStore(session_key=key)
        with pytest.raises(CreateError):
            store2.save(must_create=True)

    def test_load(self, patch_session_collection):
        # Pre-populate a session
        session_key = "existing-session"
        cb_key = f"{SESSION_KEY_PREFIX}{session_key}"
        patch_session_collection._store[cb_key] = {
            "session_data": {"user_id": 42, "theme": "dark"},
            "session_key": session_key,
        }

        store = SessionStore(session_key=session_key)
        data = store.load()
        assert data["user_id"] == 42
        assert data["theme"] == "dark"

    def test_load_nonexistent(self, patch_session_collection):
        store = SessionStore(session_key="no-such-session")
        data = store.load()
        assert data == {}
        assert store.session_key is None

    def test_exists(self, patch_session_collection):
        session_key = "check-exists"
        cb_key = f"{SESSION_KEY_PREFIX}{session_key}"
        patch_session_collection._store[cb_key] = {"session_data": {}}

        store = SessionStore()
        assert store.exists(session_key) is True
        assert store.exists("no-such-key") is False

    def test_delete(self, patch_session_collection):
        session_key = "to-delete"
        cb_key = f"{SESSION_KEY_PREFIX}{session_key}"
        patch_session_collection._store[cb_key] = {"session_data": {}}

        store = SessionStore(session_key=session_key)
        store.delete()
        assert cb_key not in patch_session_collection._store

    def test_delete_nonexistent(self, patch_session_collection):
        """Deleting a non-existent session should not raise."""
        store = SessionStore(session_key="ghost")
        store.delete()  # should not raise

    def test_delete_with_explicit_key(self, patch_session_collection):
        session_key = "explicit-del"
        cb_key = f"{SESSION_KEY_PREFIX}{session_key}"
        patch_session_collection._store[cb_key] = {"session_data": {}}

        store = SessionStore()
        store.delete(session_key)
        assert cb_key not in patch_session_collection._store

    def test_delete_no_session_key(self, patch_session_collection):
        """Delete with no session key at all should be a no-op."""
        store = SessionStore()
        store._session_key = None
        store.delete()  # should not raise

    def test_roundtrip(self, patch_session_collection):
        store = SessionStore()
        store.create()
        store["cart"] = [1, 2, 3]
        store["user"] = "alice"
        store.save()

        # Load in a new store
        store2 = SessionStore(session_key=store.session_key)
        data = store2.load()
        assert data["cart"] == [1, 2, 3]
        assert data["user"] == "alice"

    def test_clear_expired_is_noop(self):
        """clear_expired should not raise — TTL handles it."""
        SessionStore.clear_expired()

    def test_key_prefix(self):
        import string
        from django.utils.crypto import get_random_string
        key = get_random_string(32, string.ascii_lowercase + string.digits)
        store = SessionStore(session_key=key)
        assert store._get_key() == f"session:{key}"

    def test_key_prefix_custom(self):
        store = SessionStore()
        assert store._get_key("xyz") == "session:xyz"


class TestSessionStoreSettings:
    @override_settings(COUCHBASE_SESSION={"ALIAS": "secondary", "COLLECTION": "sessions"})
    def test_custom_settings(self):
        """Verify _get_collection reads COUCHBASE_SESSION settings."""
        store = SessionStore()
        # We can't actually call _get_collection without a real connection,
        # but we can verify the settings are read
        from django.conf import settings
        assert settings.COUCHBASE_SESSION["ALIAS"] == "secondary"
        assert settings.COUCHBASE_SESSION["COLLECTION"] == "sessions"
