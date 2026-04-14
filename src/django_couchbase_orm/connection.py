import logging
import threading
from datetime import timedelta
from typing import Any

from django.conf import settings

from django_couchbase_orm.exceptions import ConnectionError

logger = logging.getLogger("django_couchbase_orm.connection")

_connections: dict[str, Any] = {}
_lock = threading.RLock()


def _get_config(alias: str = "default") -> dict:
    """Retrieve Couchbase configuration for the given alias from Django settings."""
    couchbase_settings = getattr(settings, "COUCHBASE", None)
    if couchbase_settings is None:
        # Try to auto-derive from DATABASES if a Couchbase backend is configured.
        get_or_create_couchbase_settings()
        couchbase_settings = getattr(settings, "COUCHBASE", None)
    if couchbase_settings is None:
        raise ConnectionError("COUCHBASE setting is not defined in Django settings.")
    if alias not in couchbase_settings:
        raise ConnectionError(f"Couchbase alias '{alias}' is not defined in COUCHBASE settings.")
    config = couchbase_settings[alias]
    required_keys = ("CONNECTION_STRING", "USERNAME", "PASSWORD", "BUCKET")
    missing = [k for k in required_keys if k not in config]
    if missing:
        raise ConnectionError(f"Missing required Couchbase settings for alias '{alias}': {', '.join(missing)}")
    return config


def _is_cluster_alive(cluster) -> bool:
    """Check if a cached cluster connection is still usable."""
    try:
        # The Couchbase SDK raises RuntimeError if the cluster is closed.
        cluster.ping()
        return True
    except (RuntimeError, Exception):
        return False


def get_cluster(alias: str = "default"):
    """Get or create a cached Cluster instance for the given alias.

    Thread-safe with lazy initialization. Handles closed clusters by
    removing them from cache and creating a fresh connection.
    """
    cache_key = f"cluster:{alias}"
    if cache_key in _connections:
        cluster = _connections[cache_key]
        if _is_cluster_alive(cluster):
            return cluster
        # Cluster was closed (e.g., by Django test teardown). Remove stale entries.
        with _lock:
            for key in list(_connections.keys()):
                if key.split(":")[1] == alias if ":" in key else False:
                    _connections.pop(key, None)

    with _lock:
        # Double-check after acquiring lock
        if cache_key in _connections:
            return _connections[cache_key]

        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions, ClusterTimeoutOptions

        config = _get_config(alias)

        authenticator = PasswordAuthenticator(config["USERNAME"], config["PASSWORD"])

        timeout_config = config.get("OPTIONS", {}).get("timeout_options", {})
        timeout_kwargs = {}
        if "kv_timeout" in timeout_config:
            timeout_kwargs["kv_timeout"] = timedelta(seconds=timeout_config["kv_timeout"])
        if "query_timeout" in timeout_config:
            timeout_kwargs["query_timeout"] = timedelta(seconds=timeout_config["query_timeout"])

        cluster_kwargs = {
            "timeout_options": ClusterTimeoutOptions(**timeout_kwargs) if timeout_kwargs else None,
        }
        # OpenTelemetry tracing support (SDK 4.6+).
        tracer = config.get("OPTIONS", {}).get("TRACER")
        if tracer is not None:
            cluster_kwargs["tracer"] = tracer

        cluster_opts = ClusterOptions(authenticator, **cluster_kwargs)

        # Apply WAN development profile for Capella (TLS) connections
        if config["CONNECTION_STRING"].startswith("couchbases://"):
            cluster_opts.apply_profile("wan_development")

        cluster = Cluster.connect(config["CONNECTION_STRING"], cluster_opts)

        wait_timeout = config.get("OPTIONS", {}).get("wait_until_ready_timeout", 20)
        cluster.wait_until_ready(timedelta(seconds=wait_timeout))

        _connections[cache_key] = cluster
        return cluster


def get_bucket(alias: str = "default"):
    """Get the Bucket instance for the given alias."""
    cache_key = f"bucket:{alias}"
    if cache_key in _connections:
        return _connections[cache_key]

    with _lock:
        if cache_key in _connections:
            return _connections[cache_key]

        config = _get_config(alias)
        cluster = get_cluster(alias)
        bucket = cluster.bucket(config["BUCKET"])
        _connections[cache_key] = bucket
        return bucket


def get_collection(alias: str = "default", scope: str | None = None, collection: str | None = None):
    """Get a Collection instance.

    Args:
        alias: The Couchbase connection alias from Django settings.
        scope: The scope name. Defaults to the configured scope or '_default'.
        collection: The collection name. Defaults to '_default'.
    """
    config = _get_config(alias)
    scope_name = scope or config.get("SCOPE", "_default")
    collection_name = collection or "_default"

    cache_key = f"collection:{alias}:{scope_name}:{collection_name}"
    if cache_key in _connections:
        return _connections[cache_key]

    with _lock:
        if cache_key in _connections:
            return _connections[cache_key]

        bucket = get_bucket(alias)
        scope_obj = bucket.scope(scope_name)
        coll = scope_obj.collection(collection_name)
        _connections[cache_key] = coll
        return coll


def close_connections():
    """Close all cached cluster connections and clear the cache."""
    with _lock:
        for key in list(_connections.keys()):
            if key.startswith("cluster:"):
                try:
                    _connections[key].close()
                except Exception as e:
                    logger.warning("Error closing cluster connection '%s': %s", key, e)
        _connections.clear()


def reset_connections():
    """Reset the connection cache. Useful for testing."""
    with _lock:
        _connections.clear()


def cleanup_stale_connections():
    """Remove stale bucket/collection entries for dead clusters.

    Call periodically in long-running servers to prevent unbounded
    growth of the _connections cache.
    """
    with _lock:
        stale_aliases = set()
        for key, obj in list(_connections.items()):
            if key.startswith("cluster:"):
                if not _is_cluster_alive(obj):
                    alias = key.split(":", 1)[1]
                    stale_aliases.add(alias)

        for alias in stale_aliases:
            for key in list(_connections.keys()):
                parts = key.split(":")
                if len(parts) >= 2 and parts[1] == alias:
                    _connections.pop(key, None)
            logger.info("Removed stale connection cache entries for alias '%s'", alias)


def share_backend_connection(db_alias="default"):
    """Share the DB backend's Couchbase cluster with the Document API.

    Call this to avoid creating two separate cluster connections when both
    the Document API (settings.COUCHBASE) and the Django DB backend
    (settings.DATABASES) are configured for the same Couchbase cluster.

    This reads the DATABASES config and populates the Document API's
    connection cache so both systems use the same cluster object.
    """
    from django.db import connections

    try:
        conn = connections[db_alias]
        if conn.vendor != "couchbase":
            return

        conn.ensure_connection()
        cluster = conn.connection
        if cluster is None:
            return

        # Derive the COUCHBASE alias from DATABASES config.
        db_settings = conn.settings_dict
        cb_alias = db_settings.get("OPTIONS", {}).get("COUCHBASE_ALIAS", "default")

        cache_key = f"cluster:{cb_alias}"
        with _lock:
            if cache_key not in _connections:
                _connections[cache_key] = cluster

        bucket_name = db_settings.get("NAME", "default")
        bucket_cache_key = f"bucket:{cb_alias}"
        with _lock:
            if bucket_cache_key not in _connections:
                # Use the backend's cached bucket to avoid a second open_bucket call
                # which can segfault in some Couchbase SDK versions.
                if hasattr(conn, "_bucket") and conn._bucket is not None:
                    _connections[bucket_cache_key] = conn._bucket
                else:
                    _connections[bucket_cache_key] = cluster.bucket(bucket_name)

    except Exception as e:
        logger.debug("Could not share backend connection (non-critical): %s", e)


def get_or_create_couchbase_settings():
    """Auto-generate settings.COUCHBASE from settings.DATABASES if not set.

    Allows projects using only the DB backend (settings.DATABASES) to also
    use the Document API without manually duplicating connection config.
    """
    from django.conf import settings

    if getattr(settings, "COUCHBASE", None) is not None:
        return

    # Look for a Couchbase DB backend in DATABASES.
    for alias, db_config in getattr(settings, "DATABASES", {}).items():
        engine = db_config.get("ENGINE", "")
        if "couchbase" in engine:
            password = db_config.get("PASSWORD", "")
            if not password:
                logger.warning(
                    "Couchbase password not configured for DATABASES alias '%s'. "
                    "Set PASSWORD in your DATABASES settings.",
                    alias,
                )
            cb_config = {
                "CONNECTION_STRING": db_config.get("HOST", "couchbase://localhost"),
                "USERNAME": db_config.get("USER", "Administrator"),
                "PASSWORD": password,
                "BUCKET": db_config.get("NAME", "default"),
                "SCOPE": db_config.get("OPTIONS", {}).get("SCOPE", "_default"),
            }
            settings.COUCHBASE = {"default": cb_config}
            return
