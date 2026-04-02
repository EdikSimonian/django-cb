"""Migration operations — atomic units of schema and data change."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from django_couchbase_orm.migrations.executor import MigrationContext


class Operation:
    """Base class for all migration operations.

    Every operation must implement ``apply`` (forward) and ``describe``
    (human-readable summary). Operations that support rollback should
    also implement ``reverse``.

    ``reversible`` is ``False`` by default; subclasses that can be
    undone must set it to ``True``.
    """

    reversible: bool = False

    def apply(self, context: MigrationContext) -> None:
        raise NotImplementedError

    def reverse(self, context: MigrationContext) -> None:
        raise NotImplementedError(f"{self.__class__.__name__} is not reversible")

    def describe(self) -> str:
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {self.describe()}>"


# ======================================================================
# Infrastructure operations — scopes, collections, indexes
# ======================================================================


class CreateScope(Operation):
    """Create a Couchbase scope."""

    reversible = True

    def __init__(self, scope_name: str, bucket_alias: str = "default") -> None:
        self.scope_name = scope_name
        self.bucket_alias = bucket_alias

    def apply(self, context: MigrationContext) -> None:
        bucket = context.get_bucket(self.bucket_alias)
        cm = bucket.collections()
        try:
            cm.create_scope(self.scope_name)
        except Exception as e:
            if "already exists" not in str(e).lower():
                raise

    def reverse(self, context: MigrationContext) -> None:
        bucket = context.get_bucket(self.bucket_alias)
        cm = bucket.collections()
        try:
            cm.drop_scope(self.scope_name)
        except Exception as e:
            if "not found" not in str(e).lower():
                raise

    def describe(self) -> str:
        return f"Create scope '{self.scope_name}'"


class DropScope(Operation):
    """Drop a Couchbase scope."""

    reversible = False  # Cannot recreate contents

    def __init__(self, scope_name: str, bucket_alias: str = "default") -> None:
        self.scope_name = scope_name
        self.bucket_alias = bucket_alias

    def apply(self, context: MigrationContext) -> None:
        bucket = context.get_bucket(self.bucket_alias)
        cm = bucket.collections()
        try:
            cm.drop_scope(self.scope_name)
        except Exception as e:
            if "not found" not in str(e).lower():
                raise

    def describe(self) -> str:
        return f"Drop scope '{self.scope_name}'"


class CreateCollection(Operation):
    """Create a Couchbase collection within a scope."""

    reversible = True

    def __init__(
        self,
        collection_name: str,
        scope_name: str = "_default",
        bucket_alias: str = "default",
    ) -> None:
        self.collection_name = collection_name
        self.scope_name = scope_name
        self.bucket_alias = bucket_alias

    def apply(self, context: MigrationContext) -> None:
        bucket = context.get_bucket(self.bucket_alias)
        cm = bucket.collections()
        try:
            from couchbase.management.collections import CollectionSpec

            cm.create_collection(CollectionSpec(self.collection_name, self.scope_name))
        except Exception as e:
            if "already exists" not in str(e).lower():
                raise

    def reverse(self, context: MigrationContext) -> None:
        bucket = context.get_bucket(self.bucket_alias)
        cm = bucket.collections()
        try:
            from couchbase.management.collections import CollectionSpec

            cm.drop_collection(CollectionSpec(self.collection_name, self.scope_name))
        except Exception as e:
            if "not found" not in str(e).lower():
                raise

    def describe(self) -> str:
        return f"Create collection '{self.scope_name}.{self.collection_name}'"


class DropCollection(Operation):
    """Drop a Couchbase collection."""

    reversible = False  # Data is lost

    def __init__(
        self,
        collection_name: str,
        scope_name: str = "_default",
        bucket_alias: str = "default",
    ) -> None:
        self.collection_name = collection_name
        self.scope_name = scope_name
        self.bucket_alias = bucket_alias

    def apply(self, context: MigrationContext) -> None:
        bucket = context.get_bucket(self.bucket_alias)
        cm = bucket.collections()
        try:
            from couchbase.management.collections import CollectionSpec

            cm.drop_collection(CollectionSpec(self.collection_name, self.scope_name))
        except Exception as e:
            if "not found" not in str(e).lower():
                raise

    def describe(self) -> str:
        return f"Drop collection '{self.scope_name}.{self.collection_name}'"


class CreateIndex(Operation):
    """Create a secondary N1QL index."""

    reversible = True

    def __init__(
        self,
        index_name: str,
        fields: list[str],
        collection_name: str = "_default",
        scope_name: str = "_default",
        bucket_alias: str = "default",
        where: str | None = None,
    ) -> None:
        self.index_name = index_name
        self.fields = fields
        self.collection_name = collection_name
        self.scope_name = scope_name
        self.bucket_alias = bucket_alias
        self.where = where

    def apply(self, context: MigrationContext) -> None:
        keyspace = context.keyspace(self.bucket_alias, self.scope_name, self.collection_name)
        fields_str = ", ".join(f"`{f}`" for f in self.fields)
        stmt = f"CREATE INDEX `{self.index_name}` ON {keyspace}({fields_str})"
        if self.where:
            stmt += f" WHERE {self.where}"
        stmt += ' WITH {"defer_build": false}'
        try:
            context.execute_n1ql(stmt, self.bucket_alias)
        except Exception as e:
            if "already exists" not in str(e).lower():
                raise

    def reverse(self, context: MigrationContext) -> None:
        keyspace = context.keyspace(self.bucket_alias, self.scope_name, self.collection_name)
        stmt = f"DROP INDEX `{self.index_name}` ON {keyspace}"
        try:
            context.execute_n1ql(stmt, self.bucket_alias)
        except Exception as e:
            if "not found" not in str(e).lower():
                raise

    def describe(self) -> str:
        return f"Create index '{self.index_name}' on ({', '.join(self.fields)})"


class DropIndex(Operation):
    """Drop a secondary N1QL index."""

    reversible = False

    def __init__(
        self,
        index_name: str,
        collection_name: str = "_default",
        scope_name: str = "_default",
        bucket_alias: str = "default",
    ) -> None:
        self.index_name = index_name
        self.collection_name = collection_name
        self.scope_name = scope_name
        self.bucket_alias = bucket_alias

    def apply(self, context: MigrationContext) -> None:
        keyspace = context.keyspace(self.bucket_alias, self.scope_name, self.collection_name)
        stmt = f"DROP INDEX `{self.index_name}` ON {keyspace}"
        try:
            context.execute_n1ql(stmt, self.bucket_alias)
        except Exception as e:
            if "not found" not in str(e).lower():
                raise

    def describe(self) -> str:
        return f"Drop index '{self.index_name}'"


# ======================================================================
# Data / field operations — transform documents in-place
# ======================================================================


class AddField(Operation):
    """Add a field with a default value to all existing documents of a type.

    Uses a N1QL UPDATE to set the field on all documents matching the
    ``_type`` discriminator.
    """

    reversible = True

    def __init__(
        self,
        document_type: str,
        field_name: str,
        field_db_name: str | None = None,
        default: Any = None,
        collection_name: str = "_default",
        scope_name: str = "_default",
        bucket_alias: str = "default",
    ) -> None:
        self.document_type = document_type
        self.field_name = field_name
        self.field_db_name = field_db_name or field_name
        self.default = default
        self.collection_name = collection_name
        self.scope_name = scope_name
        self.bucket_alias = bucket_alias

    def apply(self, context: MigrationContext) -> None:
        keyspace = context.keyspace(self.bucket_alias, self.scope_name, self.collection_name)
        stmt = f"UPDATE {keyspace} SET `{self.field_db_name}` = $1 WHERE `_type` = $2"
        context.execute_n1ql(stmt, self.bucket_alias, [self.default, self.document_type])

    def reverse(self, context: MigrationContext) -> None:
        keyspace = context.keyspace(self.bucket_alias, self.scope_name, self.collection_name)
        stmt = f"UPDATE {keyspace} UNSET `{self.field_db_name}` WHERE `_type` = $2"
        context.execute_n1ql(stmt, self.bucket_alias, [self.document_type])

    def describe(self) -> str:
        return f"Add field '{self.field_name}' to '{self.document_type}' (default={self.default!r})"


class RemoveField(Operation):
    """Remove a field from all existing documents of a type."""

    reversible = False  # Data is lost

    def __init__(
        self,
        document_type: str,
        field_name: str,
        field_db_name: str | None = None,
        collection_name: str = "_default",
        scope_name: str = "_default",
        bucket_alias: str = "default",
    ) -> None:
        self.document_type = document_type
        self.field_name = field_name
        self.field_db_name = field_db_name or field_name
        self.collection_name = collection_name
        self.scope_name = scope_name
        self.bucket_alias = bucket_alias

    def apply(self, context: MigrationContext) -> None:
        keyspace = context.keyspace(self.bucket_alias, self.scope_name, self.collection_name)
        stmt = f"UPDATE {keyspace} UNSET `{self.field_db_name}` WHERE `_type` = $1"
        context.execute_n1ql(stmt, self.bucket_alias, [self.document_type])

    def describe(self) -> str:
        return f"Remove field '{self.field_name}' from '{self.document_type}'"


class RenameField(Operation):
    """Rename a field in all existing documents of a type."""

    reversible = True

    def __init__(
        self,
        document_type: str,
        old_name: str,
        new_name: str,
        collection_name: str = "_default",
        scope_name: str = "_default",
        bucket_alias: str = "default",
    ) -> None:
        self.document_type = document_type
        self.old_name = old_name
        self.new_name = new_name
        self.collection_name = collection_name
        self.scope_name = scope_name
        self.bucket_alias = bucket_alias

    def apply(self, context: MigrationContext) -> None:
        keyspace = context.keyspace(self.bucket_alias, self.scope_name, self.collection_name)
        stmt = (
            f"UPDATE {keyspace} SET `{self.new_name}` = `{self.old_name}`, "
            f"`{self.old_name}` = NULL "
            f"WHERE `_type` = $1 AND `{self.old_name}` IS NOT NULL"
        )
        context.execute_n1ql(stmt, self.bucket_alias, [self.document_type])
        # Clean up the old field
        stmt_unset = f"UPDATE {keyspace} UNSET `{self.old_name}` WHERE `_type` = $1"
        context.execute_n1ql(stmt_unset, self.bucket_alias, [self.document_type])

    def reverse(self, context: MigrationContext) -> None:
        keyspace = context.keyspace(self.bucket_alias, self.scope_name, self.collection_name)
        stmt = (
            f"UPDATE {keyspace} SET `{self.old_name}` = `{self.new_name}`, "
            f"`{self.new_name}` = NULL "
            f"WHERE `_type` = $1 AND `{self.new_name}` IS NOT NULL"
        )
        context.execute_n1ql(stmt, self.bucket_alias, [self.document_type])
        stmt_unset = f"UPDATE {keyspace} UNSET `{self.new_name}` WHERE `_type` = $1"
        context.execute_n1ql(stmt_unset, self.bucket_alias, [self.document_type])

    def describe(self) -> str:
        return f"Rename field '{self.old_name}' to '{self.new_name}' on '{self.document_type}'"


class AlterField(Operation):
    """Transform a field's values using a N1QL expression.

    Example: convert a string field to uppercase::

        AlterField("beer", "name", "UPPER(`name`)")
    """

    reversible = False

    def __init__(
        self,
        document_type: str,
        field_name: str,
        expression: str,
        collection_name: str = "_default",
        scope_name: str = "_default",
        bucket_alias: str = "default",
    ) -> None:
        self.document_type = document_type
        self.field_name = field_name
        self.expression = expression
        self.collection_name = collection_name
        self.scope_name = scope_name
        self.bucket_alias = bucket_alias

    def apply(self, context: MigrationContext) -> None:
        keyspace = context.keyspace(self.bucket_alias, self.scope_name, self.collection_name)
        stmt = f"UPDATE {keyspace} SET `{self.field_name}` = {self.expression} WHERE `_type` = $1"
        context.execute_n1ql(stmt, self.bucket_alias, [self.document_type])

    def describe(self) -> str:
        return f"Alter field '{self.field_name}' on '{self.document_type}' using {self.expression}"


# ======================================================================
# Escape-hatch operations — raw N1QL and Python callables
# ======================================================================


class RunN1QL(Operation):
    """Execute an arbitrary N1QL statement."""

    reversible = False

    def __init__(
        self,
        statement: str,
        params: list[Any] | None = None,
        bucket_alias: str = "default",
        reverse_statement: str | None = None,
        reverse_params: list[Any] | None = None,
    ) -> None:
        self.statement = statement
        self.params = params
        self.bucket_alias = bucket_alias
        self.reverse_statement = reverse_statement
        self.reverse_params = reverse_params
        if reverse_statement:
            self.reversible = True

    def apply(self, context: MigrationContext) -> None:
        context.execute_n1ql(self.statement, self.bucket_alias, self.params)

    def reverse(self, context: MigrationContext) -> None:
        if not self.reverse_statement:
            raise NotImplementedError("RunN1QL has no reverse statement")
        context.execute_n1ql(self.reverse_statement, self.bucket_alias, self.reverse_params)

    def describe(self) -> str:
        truncated = self.statement[:60] + ("..." if len(self.statement) > 60 else "")
        return f"Run N1QL: {truncated}"


class RunPython(Operation):
    """Execute an arbitrary Python callable.

    The callable receives the ``MigrationContext`` as its only argument.
    An optional ``reverse_func`` makes the operation reversible.

    Example::

        def populate_slugs(context):
            # Custom data migration logic
            ...

        RunPython(populate_slugs)
    """

    reversible = False

    def __init__(
        self,
        func: Any,
        reverse_func: Any | None = None,
    ) -> None:
        self.func = func
        self.reverse_func = reverse_func
        if reverse_func:
            self.reversible = True

    def apply(self, context: MigrationContext) -> None:
        self.func(context)

    def reverse(self, context: MigrationContext) -> None:
        if not self.reverse_func:
            raise NotImplementedError("RunPython has no reverse function")
        self.reverse_func(context)

    def describe(self) -> str:
        name = getattr(self.func, "__name__", repr(self.func))
        return f"Run Python: {name}"
