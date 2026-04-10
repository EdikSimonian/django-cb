"""Production readiness tests — error handling, logging, resource management.

Verifies that:
- Errors are logged, not silently swallowed
- manager.exists() only catches DocumentNotFoundException
- Connection pool cleanup works
- N1QL errors are logged at appropriate levels
- close()/ensure_connection() cycle works without segfault
"""

import logging
import uuid

import pytest

from django_couchbase_orm.document import Document
from django_couchbase_orm.exceptions import OperationError
from django_couchbase_orm.fields.simple import IntegerField, StringField

from tests.conftest import couchbase_available, flush_collection

LOCAL_COUCHBASE = {
    "default": {
        "CONNECTION_STRING": "couchbase://localhost",
        "USERNAME": "Administrator",
        "PASSWORD": "password",
        "BUCKET": "testbucket",
        "SCOPE": "_default",
    }
}

integration_mark = pytest.mark.skipif(
    not couchbase_available, reason="Local Couchbase not available"
)


class ProdDoc(Document):
    name = StringField(required=True)
    value = IntegerField(default=0)

    class Meta:
        collection_name = "edge_test_docs"


def _flush():
    flush_collection("edge_test_docs")


# ============================================================
# manager.exists() error handling
# ============================================================


@integration_mark
class TestManagerExistsErrorHandling:
    """exists() should only catch DocumentNotFoundException, not all exceptions."""

    @pytest.fixture(autouse=True)
    def _setup(self, settings):
        settings.COUCHBASE = LOCAL_COUCHBASE
        from django_couchbase_orm.connection import reset_connections

        reset_connections()
        _flush()
        yield
        _flush()

    def test_exists_true_for_existing(self):
        doc = ProdDoc(name="exists_test")
        doc.save()
        assert ProdDoc.objects.exists(doc.pk) is True

    def test_exists_false_for_missing(self):
        assert ProdDoc.objects.exists("nonexistent_pk_xyz") is False

    def test_exists_propagates_connection_errors(self):
        """exists() should NOT silently return False for connection failures."""
        from unittest.mock import PropertyMock, patch

        # Simulate a connection error (not a DocumentNotFoundException)
        with patch.object(
            type(ProdDoc.objects),
            "_collection",
            new_callable=PropertyMock,
            side_effect=RuntimeError("Connection refused"),
        ):
            with pytest.raises(RuntimeError, match="Connection refused"):
                ProdDoc.objects.exists("some_pk")


# ============================================================
# Logging on errors
# ============================================================


@integration_mark
class TestErrorLogging:
    """Verify that errors are logged instead of silently swallowed."""

    @pytest.fixture(autouse=True)
    def _setup(self, settings):
        settings.COUCHBASE = LOCAL_COUCHBASE
        from django_couchbase_orm.connection import reset_connections

        reset_connections()

    def test_close_connections_logs_on_error(self, caplog):
        """close_connections() should log if cluster.close() fails."""
        from unittest.mock import MagicMock

        from django_couchbase_orm.connection import _connections, _lock, close_connections

        mock_cluster = MagicMock()
        mock_cluster.close.side_effect = RuntimeError("close failed")

        with _lock:
            _connections["cluster:test_log"] = mock_cluster

        with caplog.at_level(logging.WARNING, logger="django_couchbase_orm.connection"):
            close_connections()

        assert "close failed" in caplog.text

    def test_share_backend_connection_logs_on_error(self, caplog):
        """share_backend_connection() should log debug message on failure."""
        from unittest.mock import patch

        from django_couchbase_orm.connection import share_backend_connection

        with patch("django.db.connections") as mock_connections:
            mock_connections.__getitem__.side_effect = Exception("connection error")

            with caplog.at_level(logging.DEBUG, logger="django_couchbase_orm.connection"):
                share_backend_connection("nonexistent")

        assert "non-critical" in caplog.text or "connection error" in caplog.text

    def test_introspection_logs_on_collection_list_error(self, caplog):
        """get_table_list() should log when collection API fails."""
        from unittest.mock import MagicMock

        from django_couchbase_orm.db.backends.couchbase.introspection import DatabaseIntrospection

        introspection = DatabaseIntrospection.__new__(DatabaseIntrospection)
        mock_conn = MagicMock()
        mock_conn.couchbase_bucket.collections.side_effect = RuntimeError("API error")
        mock_conn.settings_dict = {"OPTIONS": {"SCOPE": "_default"}}
        introspection.connection = mock_conn

        with caplog.at_level(logging.WARNING, logger="django.db.backends.couchbase.introspection"):
            result = introspection.get_table_list(None)

        assert result == []
        assert "Error listing collections" in caplog.text


# ============================================================
# Connection resource management
# ============================================================


@integration_mark
class TestConnectionResourceManagement:
    """Test that connection pool handles lifecycle correctly."""

    @pytest.fixture(autouse=True)
    def _setup(self, settings):
        settings.COUCHBASE = LOCAL_COUCHBASE
        from django_couchbase_orm.connection import reset_connections

        reset_connections()

    def test_reset_cached_clusters(self):
        """reset_cached_clusters() should clear the module-level cache."""
        from django_couchbase_orm.db.backends.couchbase.base import (
            _cached_clusters,
            reset_cached_clusters,
        )

        # Simulate a cached entry
        _cached_clusters["test:key"] = ("cluster", "bucket")
        assert len(_cached_clusters) >= 1

        reset_cached_clusters()
        assert "test:key" not in _cached_clusters

    def test_get_cluster_returns_same_instance(self):
        """Connection pool should return the same cluster on repeated calls."""
        from django_couchbase_orm.connection import get_cluster

        c1 = get_cluster()
        c2 = get_cluster()
        assert c1 is c2

    def test_reset_connections_clears_pool(self):
        """reset_connections() should allow a fresh connection on next call."""
        from django_couchbase_orm.connection import _connections, get_cluster, reset_connections

        get_cluster()  # Populate cache
        assert len(_connections) >= 1

        reset_connections()
        assert len(_connections) == 0

    def test_close_then_ensure_connection_works(self):
        """Django backend close/reopen cycle should work without crash."""
        from django.db import connection

        connection.ensure_connection()
        assert connection.connection is not None

        # close() is a no-op for Couchbase — doesn't null connection
        connection.close()

        # Should work — either reuses existing or creates new
        connection.ensure_connection()
        assert connection.connection is not None


# ============================================================
# N1QL error handling
# ============================================================


@integration_mark
class TestN1QLErrorHandling:
    """Verify N1QL errors are handled and logged appropriately."""

    @pytest.fixture(autouse=True)
    def _setup(self, settings):
        settings.COUCHBASE = LOCAL_COUCHBASE
        from django_couchbase_orm.connection import reset_connections

        reset_connections()
        _flush()
        yield
        _flush()

    def test_update_returns_int_not_cursor(self):
        """Regression: update() must return int."""
        ProdDoc(name="test_update", value=1).save()
        count = ProdDoc.objects.filter(name="test_update").update(value=2)
        assert isinstance(count, int)
        assert count == 1

    def test_delete_returns_int(self):
        """Regression: delete() must return int."""
        ProdDoc(name="test_delete", value=1).save()
        count = ProdDoc.objects.filter(name="test_delete").delete()
        assert isinstance(count, int)
        assert count == 1

    def test_count_returns_int(self):
        """count() must return int."""
        ProdDoc(name="test_count", value=1).save()
        count = ProdDoc.objects.count()
        assert isinstance(count, int)
        assert count >= 1


# ============================================================
# Django backend error handling
# ============================================================


@integration_mark
@pytest.mark.django_db(transaction=True)
class TestDjangoBackendErrorHandling:
    """Test Django backend error propagation."""

    def test_create_user_returns_integer_pk(self):
        """Auto-increment PK must be an integer."""
        from django.contrib.auth.models import User

        username = f"prod_test_{uuid.uuid4().hex[:8]}"
        user = User.objects.create_user(username, f"{username}@test.com", "pass123")
        assert isinstance(user.pk, int)
        user.delete()

    def test_update_returns_count(self):
        """QuerySet.update() must return integer row count."""
        from django.contrib.auth.models import User

        username = f"prod_upd_{uuid.uuid4().hex[:8]}"
        User.objects.create_user(username, f"{username}@test.com", "pass123")
        count = User.objects.filter(username=username).update(first_name="Test")
        assert isinstance(count, int)
        assert count == 1
        User.objects.filter(username=username).first().delete()

    def test_save_existing_model(self):
        """model.save() on existing instance must not raise TypeError."""
        from django.contrib.auth.models import User

        username = f"prod_save_{uuid.uuid4().hex[:8]}"
        user = User.objects.create_user(username, f"{username}@test.com", "pass123")
        user.first_name = "Updated"
        user.save()  # Must not raise — calls _update() > 0 internally
        user.refresh_from_db()
        assert user.first_name == "Updated"
        user.delete()

    def test_filter_returns_results(self):
        """Basic filter must return results after create."""
        from django.contrib.auth.models import Group

        name = f"prod_grp_{uuid.uuid4().hex[:8]}"
        g = Group.objects.create(name=name)
        results = list(Group.objects.filter(name=name))
        assert len(results) == 1
        assert results[0].name == name
        g.delete()
