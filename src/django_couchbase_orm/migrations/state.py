"""Migration state tracking — stores which migrations have been applied in Couchbase."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from django_couchbase_orm.connection import get_collection

# Document key used to store migration state
MIGRATION_STATE_KEY = "_cb_migrations"


class MigrationState:
    """Tracks applied migrations in a Couchbase document.

    The migration state document is stored in the default scope/collection
    of the configured bucket with the key ``_cb_migrations``. Its structure::

        {
            "_type": "_cb_migration_state",
            "applied": {
                "app_label::0001_initial": {
                    "applied_at": "2025-01-15T10:30:00+00:00"
                },
                ...
            }
        }

    This class provides a pure in-memory representation that can be loaded
    from and persisted to Couchbase, making it easy to test without a live
    cluster.
    """

    DOC_TYPE = "_cb_migration_state"

    def __init__(self) -> None:
        self.applied: dict[str, dict[str, Any]] = {}
        self._cas: int | None = None

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def is_applied(self, app_label: str, name: str) -> bool:
        """Return True if the given migration has been applied."""
        return self._key(app_label, name) in self.applied

    def applied_migrations(self, app_label: str | None = None) -> list[str]:
        """Return sorted list of applied migration keys, optionally filtered by app."""
        keys = list(self.applied.keys())
        if app_label is not None:
            prefix = f"{app_label}::"
            keys = [k for k in keys if k.startswith(prefix)]
        return sorted(keys)

    # ------------------------------------------------------------------
    # Mutators (in-memory only — call ``save`` to persist)
    # ------------------------------------------------------------------

    def record_applied(self, app_label: str, name: str) -> None:
        """Record a migration as applied."""
        self.applied[self._key(app_label, name)] = {
            "applied_at": datetime.now(timezone.utc).isoformat(),
        }

    def record_unapplied(self, app_label: str, name: str) -> None:
        """Remove a migration from the applied set."""
        self.applied.pop(self._key(app_label, name), None)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "_type": self.DOC_TYPE,
            "applied": self.applied,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any], cas: int | None = None) -> MigrationState:
        state = cls()
        state.applied = data.get("applied", {})
        state._cas = cas
        return state

    def save(self, bucket_alias: str = "default") -> None:
        """Persist the migration state to Couchbase."""
        collection = get_collection(alias=bucket_alias)
        result = collection.upsert(MIGRATION_STATE_KEY, self.to_dict())
        self._cas = result.cas

    @classmethod
    def load(cls, bucket_alias: str = "default") -> MigrationState:
        """Load migration state from Couchbase, or return empty state if not found."""
        collection = get_collection(alias=bucket_alias)
        try:
            result = collection.get(MIGRATION_STATE_KEY)
            data = result.content_as[dict]
            return cls.from_dict(data, cas=result.cas)
        except Exception:
            # Document doesn't exist yet — return fresh state
            return cls()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _key(app_label: str, name: str) -> str:
        return f"{app_label}::{name}"

    def __repr__(self) -> str:
        return f"<MigrationState: {len(self.applied)} applied>"
