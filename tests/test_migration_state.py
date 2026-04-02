"""Tests for MigrationState — in-memory state tracking and persistence."""

import pytest

from django_couchbase_orm.migrations.state import MIGRATION_STATE_KEY, MigrationState
from tests.conftest import MockCollection


class TestMigrationStateInMemory:
    """Test MigrationState without any Couchbase interaction."""

    def test_fresh_state_is_empty(self):
        state = MigrationState()
        assert state.applied == {}
        assert state._cas is None

    def test_record_applied(self):
        state = MigrationState()
        state.record_applied("myapp", "0001_initial")
        assert state.is_applied("myapp", "0001_initial")
        assert not state.is_applied("myapp", "0002_add_field")

    def test_record_unapplied(self):
        state = MigrationState()
        state.record_applied("myapp", "0001_initial")
        state.record_unapplied("myapp", "0001_initial")
        assert not state.is_applied("myapp", "0001_initial")

    def test_unapply_nonexistent_is_noop(self):
        state = MigrationState()
        state.record_unapplied("myapp", "0001_initial")
        assert not state.is_applied("myapp", "0001_initial")

    def test_applied_migrations_all(self):
        state = MigrationState()
        state.record_applied("app1", "0001_initial")
        state.record_applied("app2", "0001_initial")
        state.record_applied("app1", "0002_add_field")
        result = state.applied_migrations()
        assert result == [
            "app1::0001_initial",
            "app1::0002_add_field",
            "app2::0001_initial",
        ]

    def test_applied_migrations_filtered_by_app(self):
        state = MigrationState()
        state.record_applied("app1", "0001_initial")
        state.record_applied("app2", "0001_initial")
        state.record_applied("app1", "0002_add_field")
        result = state.applied_migrations("app1")
        assert result == ["app1::0001_initial", "app1::0002_add_field"]

    def test_applied_migrations_empty_app(self):
        state = MigrationState()
        state.record_applied("app1", "0001_initial")
        result = state.applied_migrations("nonexistent")
        assert result == []

    def test_record_applied_stores_timestamp(self):
        state = MigrationState()
        state.record_applied("myapp", "0001_initial")
        entry = state.applied["myapp::0001_initial"]
        assert "applied_at" in entry

    def test_to_dict(self):
        state = MigrationState()
        state.record_applied("myapp", "0001_initial")
        d = state.to_dict()
        assert d["_type"] == MigrationState.DOC_TYPE
        assert "myapp::0001_initial" in d["applied"]

    def test_from_dict(self):
        data = {
            "_type": MigrationState.DOC_TYPE,
            "applied": {
                "myapp::0001_initial": {"applied_at": "2025-01-01T00:00:00+00:00"},
            },
        }
        state = MigrationState.from_dict(data, cas=42)
        assert state.is_applied("myapp", "0001_initial")
        assert state._cas == 42

    def test_from_dict_empty(self):
        state = MigrationState.from_dict({})
        assert state.applied == {}

    def test_roundtrip(self):
        state = MigrationState()
        state.record_applied("a", "0001_x")
        state.record_applied("b", "0001_y")
        restored = MigrationState.from_dict(state.to_dict())
        assert restored.is_applied("a", "0001_x")
        assert restored.is_applied("b", "0001_y")

    def test_repr(self):
        state = MigrationState()
        assert "0 applied" in repr(state)
        state.record_applied("a", "0001")
        assert "1 applied" in repr(state)


class TestMigrationStatePersistence:
    """Test save/load with mocked Couchbase collection."""

    def test_save_and_load(self, monkeypatch):
        collection = MockCollection()
        monkeypatch.setattr(
            "django_couchbase_orm.migrations.state.get_collection",
            lambda alias="default": collection,
        )

        state = MigrationState()
        state.record_applied("myapp", "0001_initial")
        state.save()

        # Verify saved
        assert MIGRATION_STATE_KEY in collection._store

        # Load it back
        loaded = MigrationState.load()
        assert loaded.is_applied("myapp", "0001_initial")

    def test_load_when_no_document_exists(self, monkeypatch):
        collection = MockCollection()
        monkeypatch.setattr(
            "django_couchbase_orm.migrations.state.get_collection",
            lambda alias="default": collection,
        )

        state = MigrationState.load()
        assert state.applied == {}

    def test_save_updates_cas(self, monkeypatch):
        collection = MockCollection()
        monkeypatch.setattr(
            "django_couchbase_orm.migrations.state.get_collection",
            lambda alias="default": collection,
        )

        state = MigrationState()
        assert state._cas is None
        state.save()
        assert state._cas is not None

    def test_save_with_custom_bucket_alias(self, monkeypatch):
        collection = MockCollection()
        monkeypatch.setattr(
            "django_couchbase_orm.migrations.state.get_collection",
            lambda alias="default": collection,
        )

        state = MigrationState()
        state.record_applied("myapp", "0001_initial")
        # Should call get_collection with the alias
        state.save(bucket_alias="default")
        assert MIGRATION_STATE_KEY in collection._store
