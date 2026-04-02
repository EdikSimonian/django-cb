"""Migration executor — resolves dependencies and applies/reverts migrations."""

from __future__ import annotations

import importlib
import pkgutil
from collections import defaultdict
from typing import Any

from django_couchbase_orm.connection import get_bucket, get_cluster
from django_couchbase_orm.migrations.migration import Migration
from django_couchbase_orm.migrations.state import MigrationState


class MigrationContext:
    """Provides operations access to Couchbase resources.

    Passed to every ``Operation.apply()`` and ``Operation.reverse()`` call.
    Abstracts away connection details so operations remain testable.
    """

    def __init__(self, bucket_alias: str = "default") -> None:
        self.default_bucket_alias = bucket_alias

    def get_bucket(self, alias: str | None = None):
        return get_bucket(alias or self.default_bucket_alias)

    def get_cluster(self, alias: str | None = None):
        return get_cluster(alias or self.default_bucket_alias)

    def execute_n1ql(self, statement: str, bucket_alias: str | None = None, params: list | None = None) -> Any:
        """Execute a N1QL statement against the cluster."""
        from couchbase.options import QueryOptions

        cluster = self.get_cluster(bucket_alias)
        opts = QueryOptions(positional_parameters=params) if params else QueryOptions()
        return cluster.query(statement, opts)

    def keyspace(self, bucket_alias: str, scope_name: str, collection_name: str) -> str:
        """Build a fully-qualified keyspace string."""
        from django.conf import settings

        config = settings.COUCHBASE.get(bucket_alias, settings.COUCHBASE["default"])
        bucket_name = config["BUCKET"]
        return f"`{bucket_name}`.`{scope_name}`.`{collection_name}`"


class MigrationLoader:
    """Discovers and loads migration classes from Python packages.

    Migration files live in ``<app_package>.cb_migrations`` (configurable).
    Each file must define a class named ``Migration`` that extends
    ``django_couchbase_orm.migrations.Migration``.
    """

    MIGRATION_PACKAGE = "cb_migrations"

    def __init__(self, app_labels: list[str] | None = None) -> None:
        self.migrations: dict[str, Migration] = {}  # key -> Migration instance
        self._graph: dict[str, list[str]] = {}  # key -> dependency keys
        self._load(app_labels)

    def _load(self, app_labels: list[str] | None) -> None:
        """Load migrations from all installed apps (or given subset)."""
        from django.apps import apps

        app_configs = apps.get_app_configs()
        for app_config in app_configs:
            if app_labels and app_config.label not in app_labels:
                continue
            self._load_app(app_config)

    def _load_app(self, app_config) -> None:
        """Load all migrations for a single app."""
        app_label = app_config.label
        module_name = f"{app_config.name}.{self.MIGRATION_PACKAGE}"
        try:
            package = importlib.import_module(module_name)
        except ImportError:
            return  # No cb_migrations package — skip

        package_path = getattr(package, "__path__", None)
        if package_path is None:
            return

        for importer, mod_name, is_pkg in pkgutil.iter_modules(package_path):
            if is_pkg or mod_name.startswith("_"):
                continue
            full_module = f"{module_name}.{mod_name}"
            try:
                module = importlib.import_module(full_module)
            except ImportError:
                continue

            migration_cls = getattr(module, "Migration", None)
            if migration_cls is None:
                continue

            instance = migration_cls()
            # Allow the migration file to omit app_label — fill it in
            if not instance.app_label:
                instance.app_label = app_label
            if not instance.name:
                instance.name = mod_name

            key = instance.migration_key
            self.migrations[key] = instance
            self._graph[key] = [f"{dep_app}::{dep_name}" for dep_app, dep_name in instance.dependencies]

    def resolve_order(self, targets: list[str] | None = None) -> list[str]:
        """Return migration keys in dependency-safe topological order.

        If ``targets`` is provided, only include migrations reachable from
        those targets. Otherwise return all migrations in order.
        """
        if targets is not None:
            reachable = set()
            stack = list(targets)
            while stack:
                key = stack.pop()
                if key in reachable:
                    continue
                reachable.add(key)
                for dep in self._graph.get(key, []):
                    stack.append(dep)
            keys = reachable
        else:
            keys = set(self._graph.keys())

        # Kahn's algorithm for topological sort
        in_degree: dict[str, int] = defaultdict(int)
        adj: dict[str, list[str]] = defaultdict(list)
        for key in keys:
            if key not in in_degree:
                in_degree[key] = 0
            for dep in self._graph.get(key, []):
                if dep in keys:
                    adj[dep].append(key)
                    in_degree[key] += 1

        queue = sorted(k for k in keys if in_degree[k] == 0)
        result = []
        while queue:
            node = queue.pop(0)
            result.append(node)
            for neighbor in sorted(adj.get(node, [])):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(result) != len(keys):
            applied = set(result)
            cycle_members = keys - applied
            raise ValueError(f"Circular dependency detected among migrations: {cycle_members}")

        return result


class MigrationExecutor:
    """Applies and reverts migrations in dependency order."""

    def __init__(
        self,
        loader: MigrationLoader | None = None,
        state: MigrationState | None = None,
        context: MigrationContext | None = None,
        bucket_alias: str = "default",
    ) -> None:
        self.loader = loader or MigrationLoader()
        self.state = state or MigrationState.load(bucket_alias)
        self.context = context or MigrationContext(bucket_alias)
        self.bucket_alias = bucket_alias

    def plan(self, target: str | None = None) -> list[tuple[str, str]]:
        """Return a plan of (key, direction) pairs.

        ``direction`` is ``"apply"`` or ``"reverse"``.

        If ``target`` is given as ``app_label::name``, migrate to that
        specific migration (applying or reverting as needed). If ``None``,
        apply all unapplied migrations.
        """
        order = self.loader.resolve_order()

        if target is None:
            # Apply all unapplied
            return [(key, "apply") for key in order if not self.state.is_applied(*key.split("::"))]

        if target not in self.loader.migrations:
            raise ValueError(f"Unknown migration: {target}")

        target_idx = order.index(target)
        plan = []

        # Apply everything up to and including target
        for key in order[: target_idx + 1]:
            app_label, name = key.split("::")
            if not self.state.is_applied(app_label, name):
                plan.append((key, "apply"))

        # Reverse everything after target that's currently applied (in reverse order)
        for key in reversed(order[target_idx + 1 :]):
            app_label, name = key.split("::")
            if self.state.is_applied(app_label, name):
                plan.append((key, "reverse"))

        return plan

    def migrate(self, target: str | None = None, fake: bool = False) -> list[str]:
        """Execute the migration plan.

        Returns list of migration keys that were applied/reverted.

        If ``fake`` is True, record migrations as applied without executing
        operations (useful for marking existing state).
        """
        plan = self.plan(target)
        applied_keys = []

        for key, direction in plan:
            migration = self.loader.migrations[key]
            app_label, name = key.split("::")

            if direction == "apply":
                if not fake:
                    for op in migration.operations:
                        op.apply(self.context)
                self.state.record_applied(app_label, name)
            else:
                if not migration.is_reversible:
                    raise ValueError(f"Cannot reverse migration {key}: contains irreversible operations")
                if not fake:
                    for op in reversed(migration.operations):
                        op.reverse(self.context)
                self.state.record_unapplied(app_label, name)

            applied_keys.append(key)

        # Persist state
        self.state.save(self.bucket_alias)
        return applied_keys
