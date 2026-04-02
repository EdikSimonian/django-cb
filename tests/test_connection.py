"""Tests for connection management."""

from unittest.mock import MagicMock, patch

import pytest
from django.test import override_settings

from django_couchbase_orm.connection import (
    _connections,
    _get_config,
    close_connections,
    get_bucket,
    get_cluster,
    get_collection,
    reset_connections,
)
from django_couchbase_orm.exceptions import ConnectionError


VALID_SETTINGS = {
    "default": {
        "CONNECTION_STRING": "couchbase://localhost",
        "USERNAME": "admin",
        "PASSWORD": "password",
        "BUCKET": "test_bucket",
        "SCOPE": "_default",
    },
    "secondary": {
        "CONNECTION_STRING": "couchbase://remote",
        "USERNAME": "user",
        "PASSWORD": "pass",
        "BUCKET": "other_bucket",
    },
}


class TestGetConfig:
    @override_settings(COUCHBASE=VALID_SETTINGS)
    def test_valid_config(self):
        config = _get_config("default")
        assert config["CONNECTION_STRING"] == "couchbase://localhost"
        assert config["BUCKET"] == "test_bucket"

    @override_settings(COUCHBASE=VALID_SETTINGS)
    def test_secondary_alias(self):
        config = _get_config("secondary")
        assert config["BUCKET"] == "other_bucket"

    @override_settings(COUCHBASE=VALID_SETTINGS)
    def test_missing_alias(self):
        with pytest.raises(ConnectionError, match="not defined"):
            _get_config("nonexistent")

    @override_settings()
    def test_no_couchbase_setting(self):
        # Remove COUCHBASE from settings
        from django.conf import settings
        if hasattr(settings, 'COUCHBASE'):
            delattr(settings, 'COUCHBASE')
        with pytest.raises(ConnectionError, match="not defined"):
            _get_config("default")

    @override_settings(COUCHBASE={"default": {"CONNECTION_STRING": "couchbase://localhost"}})
    def test_missing_required_keys(self):
        with pytest.raises(ConnectionError, match="Missing required"):
            _get_config("default")


class TestResetConnections:
    def test_reset_clears_cache(self):
        _connections["test_key"] = "test_value"
        reset_connections()
        assert "test_key" not in _connections

    def test_close_connections(self):
        mock_cluster = MagicMock()
        _connections["cluster:default"] = mock_cluster
        _connections["bucket:default"] = MagicMock()
        close_connections()
        mock_cluster.close.assert_called_once()
        assert len(_connections) == 0


class TestGetCluster:
    @override_settings(COUCHBASE=VALID_SETTINGS)
    @patch("django_couchbase_orm.connection.Cluster", create=True)
    def test_get_cluster_creates_and_caches(self, mock_cluster_class):
        """Test that get_cluster creates a cluster and caches it."""
        reset_connections()

        # We need to mock the couchbase imports inside get_cluster
        mock_cluster_instance = MagicMock()
        mock_authenticator = MagicMock()
        mock_options = MagicMock()

        mock_cluster_cls = MagicMock()
        mock_cluster_cls.connect.return_value = mock_cluster_instance

        with patch.dict("sys.modules", {
            "couchbase": MagicMock(),
            "couchbase.auth": MagicMock(PasswordAuthenticator=MagicMock(return_value=mock_authenticator)),
            "couchbase.cluster": MagicMock(Cluster=mock_cluster_cls),
            "couchbase.options": MagicMock(
                ClusterOptions=MagicMock(return_value=mock_options),
                ClusterTimeoutOptions=MagicMock(),
            ),
        }):
            cluster = get_cluster("default")
            assert cluster is mock_cluster_instance

            # Second call should return cached
            cluster2 = get_cluster("default")
            assert cluster2 is mock_cluster_instance

        reset_connections()


class TestGetCollection:
    @override_settings(COUCHBASE=VALID_SETTINGS)
    def test_get_collection_with_mock(self):
        """Test collection retrieval with mocked bucket."""
        reset_connections()

        mock_collection = MagicMock()
        mock_scope = MagicMock()
        mock_scope.collection.return_value = mock_collection
        mock_bucket = MagicMock()
        mock_bucket.scope.return_value = mock_scope

        _connections["bucket:default"] = mock_bucket

        coll = get_collection(alias="default", scope="_default", collection="users")
        mock_bucket.scope.assert_called_with("_default")
        mock_scope.collection.assert_called_with("users")
        assert coll is mock_collection

        reset_connections()

    @override_settings(COUCHBASE=VALID_SETTINGS)
    def test_get_collection_defaults(self):
        """Test that scope defaults to config value."""
        reset_connections()

        mock_collection = MagicMock()
        mock_scope = MagicMock()
        mock_scope.collection.return_value = mock_collection
        mock_bucket = MagicMock()
        mock_bucket.scope.return_value = mock_scope

        _connections["bucket:default"] = mock_bucket

        coll = get_collection(alias="default", collection="users")
        mock_bucket.scope.assert_called_with("_default")

        reset_connections()

    @override_settings(COUCHBASE=VALID_SETTINGS)
    def test_get_collection_caching(self):
        """Test that collections are cached."""
        reset_connections()

        mock_collection = MagicMock()
        mock_scope = MagicMock()
        mock_scope.collection.return_value = mock_collection
        mock_bucket = MagicMock()
        mock_bucket.scope.return_value = mock_scope

        _connections["bucket:default"] = mock_bucket

        coll1 = get_collection(alias="default", scope="_default", collection="users")
        coll2 = get_collection(alias="default", scope="_default", collection="users")
        assert coll1 is coll2

        reset_connections()
