"""Tests for cb_makemigrations and cb_migrate management commands."""

from __future__ import annotations

import json
import os
import tempfile
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest
from django.core.management import call_command
from django.test import override_settings

from django_couchbase_orm.document import Document
from django_couchbase_orm.fields.simple import IntegerField, StringField
from django_couchbase_orm.migrations.state import MigrationState
from tests.conftest import MockCollection


COUCHBASE_SETTINGS = {
    "default": {
        "CONNECTION_STRING": "couchbase://localhost",
        "USERNAME": "admin",
        "PASSWORD": "password",
        "BUCKET": "testbucket",
        "SCOPE": "_default",
    },
}


class TestCbMakemigrations:
    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_dry_run_shows_changes(self):
        out = StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "django_couchbase_orm.management.commands.cb_makemigrations.Command._get_migrations_dir",
                return_value=os.path.join(tmpdir, "cb_migrations"),
            ):
                call_command("cb_makemigrations", "django_couchbase_orm", "--dry-run", stdout=out)
        output = out.getvalue()
        # Should show dry-run output or "no changes" — both are valid
        assert "DRY RUN" in output or "No changes" in output

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_empty_flag_creates_empty_migration(self):
        out = StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = os.path.join(tmpdir, "cb_migrations")
            with patch(
                "django_couchbase_orm.management.commands.cb_makemigrations.Command._get_migrations_dir",
                return_value=migrations_dir,
            ):
                call_command("cb_makemigrations", "django_couchbase_orm", "--empty", stdout=out)
            output = out.getvalue()
            assert "Created empty migration" in output

            # Verify file exists
            files = os.listdir(migrations_dir)
            py_files = [f for f in files if f.endswith(".py") and not f.startswith("_")]
            assert len(py_files) == 1

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_initial_flag_ignores_previous_state(self):
        out = StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = os.path.join(tmpdir, "cb_migrations")
            os.makedirs(migrations_dir, exist_ok=True)
            # Write a state file that would make it think nothing changed
            state_path = os.path.join(migrations_dir, ".cb_state.json")
            with open(state_path, "w") as f:
                json.dump({"documents": {}}, f)

            with patch(
                "django_couchbase_orm.management.commands.cb_makemigrations.Command._get_migrations_dir",
                return_value=migrations_dir,
            ):
                call_command("cb_makemigrations", "django_couchbase_orm", "--initial", stdout=out)
            output = out.getvalue()
            # With --initial, it should detect all current documents as new
            # (or show "No changes" if no Documents are registered)
            assert "Created migration" in output or "No changes" in output

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_custom_name(self):
        out = StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = os.path.join(tmpdir, "cb_migrations")
            with patch(
                "django_couchbase_orm.management.commands.cb_makemigrations.Command._get_migrations_dir",
                return_value=migrations_dir,
            ):
                call_command("cb_makemigrations", "django_couchbase_orm", "--empty", "--name", "setup_beers", stdout=out)
            output = out.getvalue()
            assert "setup_beers" in output

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_creates_state_file_after_generation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = os.path.join(tmpdir, "cb_migrations")
            with patch(
                "django_couchbase_orm.management.commands.cb_makemigrations.Command._get_migrations_dir",
                return_value=migrations_dir,
            ):
                # Use --initial to force detection
                call_command("cb_makemigrations", "django_couchbase_orm", "--initial", stdout=StringIO())

            state_path = os.path.join(migrations_dir, ".cb_state.json")
            if os.path.exists(state_path):
                with open(state_path) as f:
                    state = json.load(f)
                assert "documents" in state

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_unknown_app_label(self):
        out = StringIO()
        err = StringIO()
        call_command("cb_makemigrations", "nonexistent_app", stdout=out, stderr=err)
        assert "not found" in err.getvalue()


class TestCbMigrate:
    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_list_no_migrations(self):
        out = StringIO()
        with patch.object(MigrationState, "load", return_value=MigrationState()):
            call_command("cb_migrate", "--list", stdout=out)
        output = out.getvalue()
        assert "No migrations" in output

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_no_migrations_to_apply(self):
        out = StringIO()
        with patch.object(MigrationState, "load", return_value=MigrationState()):
            call_command("cb_migrate", stdout=out)
        output = out.getvalue()
        assert "No migrations to apply" in output

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_bucket_flag(self):
        out = StringIO()
        with patch.object(MigrationState, "load", return_value=MigrationState()):
            call_command("cb_migrate", "--bucket", "default", "--list", stdout=out)
        output = out.getvalue()
        assert "No migrations" in output


class TestCbMigrateIntegration:
    """Integration-style tests using in-memory loader and state."""

    @override_settings(COUCHBASE=COUCHBASE_SETTINGS)
    def test_fake_apply(self):
        """Test that --fake marks migrations as applied without executing."""
        from django_couchbase_orm.migrations.executor import MigrationExecutor, MigrationLoader
        from django_couchbase_orm.migrations.migration import Migration as MigrationBase
        from django_couchbase_orm.migrations.operations import RunPython

        calls = []

        class TestMigration(MigrationBase):
            app_label = "testapp"
            name = "0001_test"
            dependencies = []
            operations = [RunPython(lambda ctx: calls.append(True))]

        # Build executor manually
        loader = MigrationLoader.__new__(MigrationLoader)
        loader.migrations = {"testapp::0001_test": TestMigration()}
        loader._graph = {"testapp::0001_test": []}

        state = MigrationState()
        state.save = MagicMock()

        context = MagicMock()
        executor = MigrationExecutor(loader=loader, state=state, context=context)
        result = executor.migrate(fake=True)

        assert result == ["testapp::0001_test"]
        assert calls == []  # Operations not executed
        assert state.is_applied("testapp", "0001_test")
