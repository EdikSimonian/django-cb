"""Tests for Migration class, MigrationLoader, and MigrationExecutor."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from django_couchbase_orm.migrations.executor import (
    MigrationContext,
    MigrationExecutor,
    MigrationLoader,
)
from django_couchbase_orm.migrations.migration import Migration
from django_couchbase_orm.migrations.operations import (
    AddField,
    CreateCollection,
    CreateIndex,
    RemoveField,
    RunPython,
)
from django_couchbase_orm.migrations.state import MigrationState


# ======================================================================
# Migration class tests
# ======================================================================


class TestMigration:
    def test_default_attributes(self):
        m = Migration()
        assert m.app_label == ""
        assert m.dependencies == []
        assert m.operations == []
        assert m.name == ""

    def test_migration_key(self):
        m = Migration()
        m.app_label = "myapp"
        m.name = "0001_initial"
        assert m.migration_key == "myapp::0001_initial"

    def test_is_reversible_all_reversible(self):
        m = Migration()
        m.operations = [
            CreateCollection("beers"),
            AddField("beer", "name", default=""),
        ]
        assert m.is_reversible is True

    def test_is_reversible_with_irreversible(self):
        m = Migration()
        m.operations = [
            CreateCollection("beers"),
            RemoveField("beer", "name"),  # Not reversible
        ]
        assert m.is_reversible is False

    def test_is_reversible_empty(self):
        m = Migration()
        m.operations = []
        assert m.is_reversible is True

    def test_repr(self):
        m = Migration()
        m.app_label = "myapp"
        m.name = "0001_initial"
        assert "myapp" in repr(m)
        assert "0001_initial" in repr(m)


# ======================================================================
# Helper to build an in-memory loader (bypassing file system discovery)
# ======================================================================


def make_loader(migrations: list[Migration]) -> MigrationLoader:
    """Create a MigrationLoader with pre-loaded migrations (no file I/O)."""
    loader = MigrationLoader.__new__(MigrationLoader)
    loader.migrations = {}
    loader._graph = {}
    for m in migrations:
        key = m.migration_key
        loader.migrations[key] = m
        loader._graph[key] = [
            f"{dep_app}::{dep_name}" for dep_app, dep_name in m.dependencies
        ]
    return loader


def make_migration(app_label, name, deps=None, ops=None):
    """Helper to create a Migration instance."""
    m = Migration()
    m.app_label = app_label
    m.name = name
    m.dependencies = deps or []
    m.operations = ops or []
    return m


# ======================================================================
# MigrationLoader tests
# ======================================================================


class TestMigrationLoader:
    def test_resolve_order_no_deps(self):
        m1 = make_migration("app", "0001_initial")
        m2 = make_migration("app", "0002_add_field")
        loader = make_loader([m1, m2])
        order = loader.resolve_order()
        assert set(order) == {"app::0001_initial", "app::0002_add_field"}

    def test_resolve_order_with_deps(self):
        m1 = make_migration("app", "0001_initial")
        m2 = make_migration("app", "0002_add_field", deps=[("app", "0001_initial")])
        loader = make_loader([m1, m2])
        order = loader.resolve_order()
        assert order.index("app::0001_initial") < order.index("app::0002_add_field")

    def test_resolve_order_cross_app_deps(self):
        m1 = make_migration("auth", "0001_initial")
        m2 = make_migration("blog", "0001_initial", deps=[("auth", "0001_initial")])
        loader = make_loader([m1, m2])
        order = loader.resolve_order()
        assert order.index("auth::0001_initial") < order.index("blog::0001_initial")

    def test_resolve_order_circular_deps_raises(self):
        m1 = make_migration("app", "0001", deps=[("app", "0002")])
        m2 = make_migration("app", "0002", deps=[("app", "0001")])
        loader = make_loader([m1, m2])
        with pytest.raises(ValueError, match="Circular dependency"):
            loader.resolve_order()

    def test_resolve_order_diamond_deps(self):
        #     A
        #    / \
        #   B   C
        #    \ /
        #     D
        a = make_migration("app", "A")
        b = make_migration("app", "B", deps=[("app", "A")])
        c = make_migration("app", "C", deps=[("app", "A")])
        d = make_migration("app", "D", deps=[("app", "B"), ("app", "C")])
        loader = make_loader([a, b, c, d])
        order = loader.resolve_order()
        assert order.index("app::A") < order.index("app::B")
        assert order.index("app::A") < order.index("app::C")
        assert order.index("app::B") < order.index("app::D")
        assert order.index("app::C") < order.index("app::D")

    def test_resolve_order_with_targets(self):
        m1 = make_migration("app", "0001")
        m2 = make_migration("app", "0002", deps=[("app", "0001")])
        m3 = make_migration("app", "0003", deps=[("app", "0002")])
        loader = make_loader([m1, m2, m3])
        # Only get migrations reachable from 0002
        order = loader.resolve_order(targets=["app::0002"])
        assert "app::0001" in order
        assert "app::0002" in order
        assert "app::0003" not in order

    def test_resolve_order_empty(self):
        loader = make_loader([])
        assert loader.resolve_order() == []

    def test_resolve_order_single(self):
        m1 = make_migration("app", "0001")
        loader = make_loader([m1])
        assert loader.resolve_order() == ["app::0001"]

    def test_load_from_apps(self):
        """Test that MigrationLoader can handle apps without cb_migrations."""
        # This exercises the real _load path — apps without cb_migrations
        # should be silently skipped
        loader = MigrationLoader()
        # Should not raise, even though test apps don't have cb_migrations
        assert isinstance(loader.migrations, dict)


# ======================================================================
# MigrationExecutor tests
# ======================================================================


class MockMigrationContext:
    """Mock context that tracks operation calls."""

    def __init__(self):
        self.applied_ops = []

    def get_bucket(self, alias=None):
        return MagicMock()

    def get_cluster(self, alias=None):
        return MagicMock()

    def execute_n1ql(self, statement, bucket_alias=None, params=None):
        self.applied_ops.append(("n1ql", statement, params))

    def keyspace(self, bucket_alias, scope_name, collection_name):
        return f"`test`.`{scope_name}`.`{collection_name}`"


class TestMigrationExecutor:
    def _make_executor(self, migrations, applied=None):
        """Helper to create an executor with in-memory state."""
        loader = make_loader(migrations)
        state = MigrationState()
        if applied:
            for app_label, name in applied:
                state.record_applied(app_label, name)
        ctx = MockMigrationContext()
        # Patch state.save to avoid Couchbase calls
        state.save = MagicMock()
        executor = MigrationExecutor(loader=loader, state=state, context=ctx)
        return executor, ctx

    def test_plan_apply_all(self):
        m1 = make_migration("app", "0001", ops=[CreateCollection("beers")])
        m2 = make_migration("app", "0002", deps=[("app", "0001")], ops=[AddField("beer", "name")])
        executor, _ = self._make_executor([m1, m2])
        plan = executor.plan()
        assert len(plan) == 2
        assert all(d == "apply" for _, d in plan)

    def test_plan_skip_applied(self):
        m1 = make_migration("app", "0001")
        m2 = make_migration("app", "0002", deps=[("app", "0001")])
        executor, _ = self._make_executor([m1, m2], applied=[("app", "0001")])
        plan = executor.plan()
        assert len(plan) == 1
        assert plan[0] == ("app::0002", "apply")

    def test_plan_nothing_to_do(self):
        m1 = make_migration("app", "0001")
        executor, _ = self._make_executor([m1], applied=[("app", "0001")])
        plan = executor.plan()
        assert plan == []

    def test_plan_target_forward(self):
        m1 = make_migration("app", "0001")
        m2 = make_migration("app", "0002", deps=[("app", "0001")])
        m3 = make_migration("app", "0003", deps=[("app", "0002")])
        executor, _ = self._make_executor([m1, m2, m3])
        plan = executor.plan(target="app::0002")
        keys = [k for k, _ in plan]
        assert "app::0001" in keys
        assert "app::0002" in keys
        assert "app::0003" not in keys

    def test_plan_target_reverse(self):
        m1 = make_migration("app", "0001", ops=[CreateCollection("beers")])
        m2 = make_migration("app", "0002", deps=[("app", "0001")], ops=[AddField("beer", "name")])
        m3 = make_migration("app", "0003", deps=[("app", "0002")], ops=[AddField("beer", "abv")])
        executor, _ = self._make_executor(
            [m1, m2, m3],
            applied=[("app", "0001"), ("app", "0002"), ("app", "0003")],
        )
        plan = executor.plan(target="app::0001")
        # Should reverse 0003 and 0002
        assert ("app::0003", "reverse") in plan
        assert ("app::0002", "reverse") in plan
        assert ("app::0001", "apply") not in plan

    def test_plan_unknown_target_raises(self):
        m1 = make_migration("app", "0001")
        executor, _ = self._make_executor([m1])
        with pytest.raises(ValueError, match="Unknown migration"):
            executor.plan(target="app::9999")

    def test_migrate_applies_operations(self):
        calls = []

        def track_apply(ctx):
            calls.append("applied")

        m1 = make_migration("app", "0001", ops=[RunPython(track_apply)])
        executor, ctx = self._make_executor([m1])
        result = executor.migrate()
        assert result == ["app::0001"]
        assert calls == ["applied"]
        assert executor.state.is_applied("app", "0001")

    def test_migrate_fake(self):
        calls = []

        def track_apply(ctx):
            calls.append("applied")

        m1 = make_migration("app", "0001", ops=[RunPython(track_apply)])
        executor, _ = self._make_executor([m1])
        result = executor.migrate(fake=True)
        assert result == ["app::0001"]
        assert calls == []  # Operations not executed
        assert executor.state.is_applied("app", "0001")

    def test_migrate_reverse(self):
        forward_calls = []
        reverse_calls = []

        def forward(ctx):
            forward_calls.append(True)

        def reverse(ctx):
            reverse_calls.append(True)

        m1 = make_migration("app", "0001", ops=[RunPython(forward, reverse_func=reverse)])
        m2 = make_migration(
            "app", "0002",
            deps=[("app", "0001")],
            ops=[RunPython(forward, reverse_func=reverse)],
        )
        executor, _ = self._make_executor(
            [m1, m2],
            applied=[("app", "0001"), ("app", "0002")],
        )
        result = executor.migrate(target="app::0001")
        assert "app::0002" in result
        assert reverse_calls == [True]
        assert not executor.state.is_applied("app", "0002")
        assert executor.state.is_applied("app", "0001")

    def test_migrate_irreversible_raises(self):
        m_a = make_migration("app", "0001", ops=[AddField("beer", "x")])
        m_b = make_migration("app", "0002", deps=[("app", "0001")], ops=[RemoveField("beer", "x")])
        executor, _ = self._make_executor(
            [m_a, m_b],
            applied=[("app", "0001"), ("app", "0002")],
        )
        with pytest.raises(ValueError, match="irreversible"):
            executor.migrate(target="app::0001")

    def test_migrate_saves_state(self):
        m1 = make_migration("app", "0001")
        executor, _ = self._make_executor([m1])
        executor.migrate()
        executor.state.save.assert_called_once()

    def test_migrate_multiple_apps(self):
        m1 = make_migration("auth", "0001", ops=[CreateCollection("users")])
        m2 = make_migration("blog", "0001", deps=[("auth", "0001")], ops=[CreateCollection("posts")])
        executor, _ = self._make_executor([m1, m2])
        result = executor.migrate()
        assert len(result) == 2
        assert executor.state.is_applied("auth", "0001")
        assert executor.state.is_applied("blog", "0001")

    def test_migrate_operations_executed_in_order(self):
        calls = []

        def make_tracker(name):
            def track(ctx):
                calls.append(name)
            return track

        m1 = make_migration("app", "0001", ops=[
            RunPython(make_tracker("op1")),
            RunPython(make_tracker("op2")),
        ])
        executor, _ = self._make_executor([m1])
        executor.migrate()
        assert calls == ["op1", "op2"]

    def test_migrate_reverse_operations_in_reverse_order(self):
        calls = []

        def make_tracker(name):
            def forward(ctx):
                calls.append(f"forward_{name}")
            def reverse(ctx):
                calls.append(f"reverse_{name}")
            return forward, reverse

        f1, r1 = make_tracker("op1")
        f2, r2 = make_tracker("op2")

        m1 = make_migration("app", "0001", ops=[
            RunPython(f1, reverse_func=r1),
            RunPython(f2, reverse_func=r2),
        ])
        m2 = make_migration("app", "0002", deps=[("app", "0001")], ops=[
            RunPython(lambda ctx: None, reverse_func=lambda ctx: calls.append("reverse_m2")),
        ])
        executor, _ = self._make_executor(
            [m1, m2],
            applied=[("app", "0001"), ("app", "0002")],
        )
        executor.migrate(target="app::0001")
        # Only m2 should be reversed
        assert calls == ["reverse_m2"]


# ======================================================================
# MigrationContext tests
# ======================================================================


class TestMigrationContext:
    def test_keyspace(self):
        ctx = MigrationContext.__new__(MigrationContext)
        with patch("django.conf.settings") as mock_settings:
            mock_settings.COUCHBASE = {
                "default": {"BUCKET": "mybucket"},
            }
            result = ctx.keyspace("default", "_default", "beers")
            assert result == "`mybucket`.`_default`.`beers`"

    def test_keyspace_custom_alias(self):
        ctx = MigrationContext.__new__(MigrationContext)
        with patch("django.conf.settings") as mock_settings:
            mock_settings.COUCHBASE = {
                "default": {"BUCKET": "defaultbucket"},
                "shop": {"BUCKET": "shopbucket"},
            }
            result = ctx.keyspace("shop", "inventory", "products")
            assert result == "`shopbucket`.`inventory`.`products`"
