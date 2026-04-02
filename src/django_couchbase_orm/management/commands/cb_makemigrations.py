"""Management command to auto-detect Document changes and generate migration files."""

from __future__ import annotations

import json
import os

from django.apps import apps
from django.core.management.base import BaseCommand

from django_couchbase_orm.migrations.autodetector import MigrationAutodetector, snapshot_state
from django_couchbase_orm.migrations.writer import MigrationWriter, next_migration_name

# State file stored alongside migrations to track the last-known Document state
STATE_FILENAME = ".cb_state.json"


class Command(BaseCommand):
    help = "Generate Couchbase migration files by detecting Document class changes"

    def add_arguments(self, parser):
        parser.add_argument(
            "app_label",
            nargs="?",
            default=None,
            help="App label to generate migrations for. Defaults to first app with Document classes.",
        )
        parser.add_argument(
            "--name",
            "-n",
            default="",
            help="Custom suffix for the migration file name.",
        )
        parser.add_argument(
            "--empty",
            action="store_true",
            help="Generate an empty migration file for manual editing.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be generated without writing files.",
        )
        parser.add_argument(
            "--initial",
            action="store_true",
            help="Treat this as the initial migration (ignore previous state).",
        )

    def handle(self, *args, **options):
        app_label = options["app_label"]
        custom_name = options["name"]
        empty = options["empty"]
        dry_run = options["dry_run"]
        initial = options["initial"]

        # Determine target app
        if app_label:
            try:
                app_config = apps.get_app_config(app_label)
            except LookupError:
                self.stderr.write(self.style.ERROR(f"App '{app_label}' not found."))
                return
        else:
            # Use the first installed app
            all_configs = list(apps.get_app_configs())
            # Prefer user apps over django_couchbase_orm itself
            user_configs = [c for c in all_configs if c.label != "django_couchbase_orm"]
            app_config = user_configs[0] if user_configs else (all_configs[0] if all_configs else None)
            if app_config is None:
                self.stderr.write(self.style.ERROR("No app found to generate migrations for."))
                return
            app_label = app_config.label

        # Resolve migration directory
        migrations_dir = self._get_migrations_dir(app_config)

        if empty:
            # Generate an empty migration
            name = next_migration_name(migrations_dir, prefix=custom_name or "empty")
            writer = MigrationWriter(app_label, name, operations=[], dependencies=[])
            if dry_run:
                self.stdout.write(self.style.WARNING("[DRY RUN] Would create:"))
                self.stdout.write(writer.as_string())
            else:
                path = writer.write(migrations_dir)
                self.stdout.write(self.style.SUCCESS(f"Created empty migration: {path}"))
            return

        # Load previous state
        state_path = os.path.join(migrations_dir, STATE_FILENAME)
        if initial or not os.path.exists(state_path):
            old_state = {"documents": {}}
        else:
            with open(state_path) as f:
                old_state = json.load(f)

        # Snapshot current state and detect changes
        new_state = snapshot_state()
        detector = MigrationAutodetector(old_state, new_state)

        if not detector.has_changes():
            self.stdout.write("No changes detected.")
            return

        operations = detector.all_operations()

        # Determine dependencies — depend on previous migration in this app if any
        dependencies = []
        if os.path.isdir(migrations_dir):
            existing = sorted(
                f[:-3]
                for f in os.listdir(migrations_dir)
                if f.endswith(".py") and not f.startswith("_") and f != STATE_FILENAME
            )
            if existing:
                dependencies.append((app_label, existing[-1]))

        # Generate migration name
        if custom_name:
            name = next_migration_name(migrations_dir, prefix=custom_name)
        elif not old_state.get("documents"):
            name = next_migration_name(migrations_dir, prefix="initial")
        else:
            name = next_migration_name(migrations_dir, prefix="auto")

        writer = MigrationWriter(app_label, name, operations, dependencies)

        if dry_run:
            self.stdout.write(self.style.WARNING("[DRY RUN] Would create:"))
            self.stdout.write(writer.as_string())
            for op in operations:
                self.stdout.write(f"  - {op.describe()}")
            return

        # Write migration file
        path = writer.write(migrations_dir)
        self.stdout.write(self.style.SUCCESS(f"Created migration: {path}"))
        for op in operations:
            self.stdout.write(f"  - {op.describe()}")

        # Save current state
        os.makedirs(os.path.dirname(state_path), exist_ok=True)
        with open(state_path, "w") as f:
            json.dump(new_state, f, indent=2)

    def _get_migrations_dir(self, app_config) -> str:
        """Return the cb_migrations directory for the given app."""
        app_dir = os.path.dirname(app_config.module.__file__)
        return os.path.join(app_dir, "cb_migrations")
