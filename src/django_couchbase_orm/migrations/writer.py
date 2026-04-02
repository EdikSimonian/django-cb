"""Migration writer — serializes Migration objects to Python source files."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from django_couchbase_orm.migrations.operations import (
    AddField,
    AlterField,
    CreateCollection,
    CreateIndex,
    CreateScope,
    DropCollection,
    DropIndex,
    DropScope,
    RemoveField,
    RenameField,
    RunN1QL,
    RunPython,
)


def _repr_value(value: Any) -> str:
    """Return a repr()-safe string for a value."""
    if isinstance(value, str):
        return repr(value)
    if value is None:
        return "None"
    return repr(value)


def _serialize_operation(op) -> str:
    """Serialize an Operation instance to a Python constructor call."""
    if isinstance(op, CreateScope):
        return f"CreateScope({_repr_value(op.scope_name)}, bucket_alias={_repr_value(op.bucket_alias)})"

    if isinstance(op, DropScope):
        return f"DropScope({_repr_value(op.scope_name)}, bucket_alias={_repr_value(op.bucket_alias)})"

    if isinstance(op, CreateCollection):
        return (
            f"CreateCollection({_repr_value(op.collection_name)}, "
            f"scope_name={_repr_value(op.scope_name)}, "
            f"bucket_alias={_repr_value(op.bucket_alias)})"
        )

    if isinstance(op, DropCollection):
        return (
            f"DropCollection({_repr_value(op.collection_name)}, "
            f"scope_name={_repr_value(op.scope_name)}, "
            f"bucket_alias={_repr_value(op.bucket_alias)})"
        )

    if isinstance(op, CreateIndex):
        parts = [
            f"index_name={_repr_value(op.index_name)}",
            f"fields={op.fields!r}",
            f"collection_name={_repr_value(op.collection_name)}",
            f"scope_name={_repr_value(op.scope_name)}",
            f"bucket_alias={_repr_value(op.bucket_alias)}",
        ]
        if op.where:
            parts.append(f"where={_repr_value(op.where)}")
        return f"CreateIndex({', '.join(parts)})"

    if isinstance(op, DropIndex):
        return (
            f"DropIndex(index_name={_repr_value(op.index_name)}, "
            f"collection_name={_repr_value(op.collection_name)}, "
            f"scope_name={_repr_value(op.scope_name)}, "
            f"bucket_alias={_repr_value(op.bucket_alias)})"
        )

    if isinstance(op, AddField):
        return (
            f"AddField(document_type={_repr_value(op.document_type)}, "
            f"field_name={_repr_value(op.field_name)}, "
            f"field_db_name={_repr_value(op.field_db_name)}, "
            f"default={_repr_value(op.default)}, "
            f"collection_name={_repr_value(op.collection_name)}, "
            f"scope_name={_repr_value(op.scope_name)}, "
            f"bucket_alias={_repr_value(op.bucket_alias)})"
        )

    if isinstance(op, RemoveField):
        return (
            f"RemoveField(document_type={_repr_value(op.document_type)}, "
            f"field_name={_repr_value(op.field_name)}, "
            f"field_db_name={_repr_value(op.field_db_name)}, "
            f"collection_name={_repr_value(op.collection_name)}, "
            f"scope_name={_repr_value(op.scope_name)}, "
            f"bucket_alias={_repr_value(op.bucket_alias)})"
        )

    if isinstance(op, RenameField):
        return (
            f"RenameField(document_type={_repr_value(op.document_type)}, "
            f"old_name={_repr_value(op.old_name)}, "
            f"new_name={_repr_value(op.new_name)}, "
            f"collection_name={_repr_value(op.collection_name)}, "
            f"scope_name={_repr_value(op.scope_name)}, "
            f"bucket_alias={_repr_value(op.bucket_alias)})"
        )

    if isinstance(op, AlterField):
        return (
            f"AlterField(document_type={_repr_value(op.document_type)}, "
            f"field_name={_repr_value(op.field_name)}, "
            f"expression={_repr_value(op.expression)}, "
            f"collection_name={_repr_value(op.collection_name)}, "
            f"scope_name={_repr_value(op.scope_name)}, "
            f"bucket_alias={_repr_value(op.bucket_alias)})"
        )

    if isinstance(op, RunN1QL):
        parts = [f"statement={_repr_value(op.statement)}"]
        if op.params:
            parts.append(f"params={op.params!r}")
        parts.append(f"bucket_alias={_repr_value(op.bucket_alias)}")
        if op.reverse_statement:
            parts.append(f"reverse_statement={_repr_value(op.reverse_statement)}")
        if op.reverse_params:
            parts.append(f"reverse_params={op.reverse_params!r}")
        return f"RunN1QL({', '.join(parts)})"

    if isinstance(op, RunPython):
        # RunPython cannot be auto-serialized — leave a placeholder
        func_name = getattr(op.func, "__name__", "unknown")
        return f"RunPython({func_name})  # TODO: define {func_name} above"

    return f"# Unsupported operation: {op!r}"


class MigrationWriter:
    """Generates a Python migration file from a list of operations."""

    def __init__(
        self,
        app_label: str,
        name: str,
        operations: list,
        dependencies: list[tuple[str, str]] | None = None,
    ) -> None:
        self.app_label = app_label
        self.name = name
        self.operations = operations
        self.dependencies = dependencies or []

    def as_string(self) -> str:
        """Generate the full Python source for the migration file."""
        # Collect which operation classes are used
        op_classes = set()
        for op in self.operations:
            op_classes.add(type(op).__name__)

        imports = ", ".join(sorted(op_classes))

        # Serialize operations
        op_lines = []
        for op in self.operations:
            serialized = _serialize_operation(op)
            op_lines.append(f"        {serialized},")

        ops_block = "\n".join(op_lines) if op_lines else "        # No operations"

        # Serialize dependencies
        dep_lines = []
        for app, dep_name in self.dependencies:
            dep_lines.append(f"        ({_repr_value(app)}, {_repr_value(dep_name)}),")
        deps_block = "\n".join(dep_lines)

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        lines = [
            f"# Generated by django-couchbase-orm on {timestamp}",
            "",
            "from django_couchbase_orm.migrations import Migration as BaseMigration",
        ]
        if op_classes:
            lines.append(f"from django_couchbase_orm.migrations.operations import {imports}")
        lines += [
            "",
            "",
            "class Migration(BaseMigration):",
            f"    app_label = {_repr_value(self.app_label)}",
            f"    name = {_repr_value(self.name)}",
            "",
            "    dependencies = [",
        ]
        if deps_block:
            lines.append(deps_block)
        lines += [
            "    ]",
            "",
            "    operations = [",
            ops_block,
            "    ]",
            "",
        ]
        return "\n".join(lines) + "\n"

    def write(self, directory: str) -> str:
        """Write the migration file to disk.

        Creates the directory (and ``__init__.py``) if needed.
        Returns the full path to the written file.
        """
        os.makedirs(directory, exist_ok=True)

        init_path = os.path.join(directory, "__init__.py")
        if not os.path.exists(init_path):
            with open(init_path, "w") as f:
                pass  # Empty __init__.py

        file_path = os.path.join(directory, f"{self.name}.py")
        with open(file_path, "w") as f:
            f.write(self.as_string())

        return file_path


def next_migration_name(directory: str, prefix: str = "") -> str:
    """Generate the next sequential migration name for a directory.

    Scans existing migration files like ``0001_initial.py``, ``0002_add_field.py``
    and returns the next number with the given prefix, e.g. ``0003_add_field``.
    """
    max_num = 0
    if os.path.isdir(directory):
        for filename in os.listdir(directory):
            if filename.endswith(".py") and not filename.startswith("_"):
                try:
                    num = int(filename.split("_", 1)[0])
                    max_num = max(max_num, num)
                except (ValueError, IndexError):
                    pass

    next_num = max_num + 1
    suffix = f"_{prefix}" if prefix else "_auto"
    return f"{next_num:04d}{suffix}"
