"""Management command to apply or revert Couchbase migrations."""

from __future__ import annotations

from django.core.management.base import BaseCommand

from django_couchbase_orm.migrations.executor import (
    MigrationContext,
    MigrationExecutor,
    MigrationLoader,
)
from django_couchbase_orm.migrations.state import MigrationState


class Command(BaseCommand):
    help = "Apply or revert Couchbase migrations"

    def add_arguments(self, parser):
        parser.add_argument(
            "app_label",
            nargs="?",
            default=None,
            help="App label to migrate. If omitted, all apps are migrated.",
        )
        parser.add_argument(
            "migration_name",
            nargs="?",
            default=None,
            help="Target migration name. Applies up to (or reverts down to) this migration.",
        )
        parser.add_argument(
            "--fake",
            action="store_true",
            help="Mark migrations as applied without actually running them.",
        )
        parser.add_argument(
            "--list",
            "-l",
            action="store_true",
            dest="show_list",
            help="Show all migrations and their applied status.",
        )
        parser.add_argument(
            "--bucket",
            default="default",
            help="Couchbase bucket alias (default: 'default').",
        )

    def handle(self, *args, **options):
        app_label = options["app_label"]
        migration_name = options["migration_name"]
        fake = options["fake"]
        show_list = options["show_list"]
        bucket_alias = options["bucket"]

        # Load migrations
        app_labels = [app_label] if app_label else None
        loader = MigrationLoader(app_labels=app_labels)

        if show_list:
            self._show_list(loader, bucket_alias)
            return

        # Build executor
        state = MigrationState.load(bucket_alias)
        context = MigrationContext(bucket_alias)
        executor = MigrationExecutor(loader=loader, state=state, context=context, bucket_alias=bucket_alias)

        # Determine target
        target = None
        if app_label and migration_name:
            target = f"{app_label}::{migration_name}"

        # Execute
        plan = executor.plan(target)
        if not plan:
            self.stdout.write("No migrations to apply.")
            return

        self.stdout.write(f"Running {len(plan)} migration(s):")
        for key, direction in plan:
            marker = "APPLY" if direction == "apply" else "REVERT"
            self.stdout.write(f"  [{marker}] {key}")

        applied = executor.migrate(target, fake=fake)

        if fake:
            self.stdout.write(self.style.WARNING(f"Faked {len(applied)} migration(s)."))
        else:
            self.stdout.write(self.style.SUCCESS(f"Applied {len(applied)} migration(s)."))

    def _show_list(self, loader: MigrationLoader, bucket_alias: str) -> None:
        """Display all migrations with applied/pending status."""
        state = MigrationState.load(bucket_alias)
        order = loader.resolve_order()

        if not order:
            self.stdout.write("No migrations found.")
            return

        current_app = None
        for key in order:
            app_label, name = key.split("::")
            if app_label != current_app:
                current_app = app_label
                self.stdout.write(f"\n{app_label}:")

            applied = state.is_applied(app_label, name)
            marker = self.style.SUCCESS("[X]") if applied else "[ ]"
            self.stdout.write(f"  {marker} {name}")
