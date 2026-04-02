"""Migration base class — defines a single migration with dependencies and operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from django_couchbase_orm.migrations.operations import Operation


class Migration:
    """Base class for all migration files.

    Subclass this in your migration files::

        from django_couchbase_orm.migrations import Migration, operations

        class Migration(Migration):
            app_label = "myapp"
            dependencies = [("myapp", "0001_initial")]
            operations = [
                operations.CreateCollection("beers"),
                operations.AddField("beer", "rating", default=0),
            ]
    """

    # The app this migration belongs to (set by subclass or auto-detected)
    app_label: str = ""

    # List of (app_label, migration_name) this migration depends on.
    # Ensures correct ordering.
    dependencies: list[tuple[str, str]] = []

    # Ordered list of Operation instances to apply.
    operations: list[Operation] = []

    # Human-readable name — defaults to the module name.
    name: str = ""

    @property
    def migration_key(self) -> str:
        """Unique key for this migration: 'app_label::name'."""
        return f"{self.app_label}::{self.name}"

    @property
    def is_reversible(self) -> bool:
        """True if every operation in this migration supports reverse."""
        return all(op.reversible for op in self.operations)

    def __repr__(self) -> str:
        return f"<Migration: {self.app_label}.{self.name}>"
