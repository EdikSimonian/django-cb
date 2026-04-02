import os
from unittest.mock import MagicMock, PropertyMock

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_settings")
django.setup()


class MockCASResult:
    """Mocks a Couchbase MutationResult with a CAS value."""

    def __init__(self, cas=12345):
        self.cas = cas


class MockGetResult:
    """Mocks a Couchbase GetResult."""

    def __init__(self, data: dict, cas=12345):
        self._data = data
        self.cas = cas
        self.content_as = {dict: data}


class MockExistsResult:
    """Mocks a Couchbase ExistsResult."""

    def __init__(self, exists=True):
        self.exists = exists


class MockCollection:
    """Mock Couchbase Collection for unit tests."""

    def __init__(self):
        self._store: dict[str, dict] = {}
        self._cas_counter = 0

    def _next_cas(self):
        self._cas_counter += 1
        return self._cas_counter

    def upsert(self, key, data, *args, **kwargs):
        self._store[key] = data
        return MockCASResult(self._next_cas())

    def insert(self, key, data, *args, **kwargs):
        if key in self._store:
            from couchbase.exceptions import DocumentExistsException

            raise DocumentExistsException()
        self._store[key] = data
        return MockCASResult(self._next_cas())

    def get(self, key, *args, **kwargs):
        if key not in self._store:
            from couchbase.exceptions import DocumentNotFoundException

            raise DocumentNotFoundException()
        return MockGetResult(self._store[key], self._next_cas())

    def remove(self, key, *args, **kwargs):
        if key not in self._store:
            from couchbase.exceptions import DocumentNotFoundException

            raise DocumentNotFoundException()
        del self._store[key]
        return MockCASResult(self._next_cas())

    def exists(self, key, *args, **kwargs):
        return MockExistsResult(key in self._store)


@pytest.fixture
def mock_collection():
    """Provide a MockCollection and patch get_collection to return it."""
    collection = MockCollection()
    return collection


@pytest.fixture
def patch_collection(mock_collection, monkeypatch):
    """Patch django_cb.connection.get_collection to return the mock collection."""
    monkeypatch.setattr(
        "django_cb.connection.get_collection",
        lambda **kwargs: mock_collection,
    )
    monkeypatch.setattr(
        "django_cb.connection.get_collection",
        lambda alias="default", scope=None, collection=None: mock_collection,
    )
    return mock_collection
