"""Auto-detector — diffs current Document classes against stored state to generate operations."""

from __future__ import annotations

import json
from typing import Any

from django_couchbase_orm.document import get_document_registry
from django_couchbase_orm.migrations.operations import (
    AddField,
    CreateCollection,
    CreateIndex,
    CreateScope,
    DropCollection,
    DropIndex,
    RemoveField,
)


def _serialize_field(field) -> dict[str, Any]:
    """Serialize a field's metadata for state comparison."""
    return {
        "type": type(field).__name__,
        "db_field": field.get_db_field(),
        "required": field.required,
        "default": _safe_default(field),
    }


def _safe_default(field) -> Any:
    """Extract a JSON-safe default value from a field."""
    if field.default is None:
        return None
    if callable(field.default):
        # Can't serialize callables — store a sentinel
        return "__callable__"
    try:
        json.dumps(field.default)
        return field.default
    except (TypeError, ValueError):
        return "__non_serializable__"


def snapshot_state() -> dict[str, Any]:
    """Capture the current state of all registered Document classes.

    Returns a dict suitable for JSON serialization::

        {
            "documents": {
                "Beer": {
                    "collection_name": "beer",
                    "scope_name": "_default",
                    "bucket_alias": "default",
                    "doc_type_value": "beer",
                    "fields": {
                        "name": {"type": "StringField", "db_field": "name", ...},
                        ...
                    },
                    "indexes": [...]
                },
                ...
            }
        }
    """
    registry = get_document_registry()
    documents = {}

    for name, doc_cls in sorted(registry.items()):
        meta = doc_cls._meta
        if meta.abstract:
            continue

        fields = {}
        for field_name, field in meta.fields.items():
            fields[field_name] = _serialize_field(field)

        documents[name] = {
            "collection_name": meta.collection_name,
            "scope_name": meta.scope_name,
            "bucket_alias": meta.bucket_alias,
            "doc_type_value": meta.doc_type_value,
            "fields": fields,
            "indexes": meta.indexes,
        }

    return {"documents": documents}


class MigrationAutodetector:
    """Compares two state snapshots and generates migration operations.

    Usage::

        detector = MigrationAutodetector(old_state, new_state)
        operations = detector.detect_changes()
    """

    def __init__(
        self,
        old_state: dict[str, Any] | None = None,
        new_state: dict[str, Any] | None = None,
    ) -> None:
        self.old_state = old_state or {"documents": {}}
        self.new_state = new_state or snapshot_state()

    def detect_changes(self) -> dict[str, list]:
        """Detect differences and return operations grouped by app/document.

        Returns::

            {
                "infrastructure": [...],  # Scope/collection operations
                "fields": [...],          # Field add/remove operations
                "indexes": [...],         # Index operations
            }
        """
        old_docs = self.old_state.get("documents", {})
        new_docs = self.new_state.get("documents", {})

        infra_ops = []
        field_ops = []
        index_ops = []

        # Detect new documents (collections to create)
        for name in sorted(set(new_docs) - set(old_docs)):
            doc = new_docs[name]
            scope = doc["scope_name"]
            collection = doc["collection_name"]
            bucket = doc["bucket_alias"]

            if scope != "_default":
                infra_ops.append(CreateScope(scope, bucket_alias=bucket))
            if collection != "_default":
                infra_ops.append(CreateCollection(collection, scope_name=scope, bucket_alias=bucket))

            # Add fields with defaults for new documents
            for field_name, field_info in doc["fields"].items():
                if field_info["default"] is not None and field_info["default"] != "__callable__":
                    field_ops.append(
                        AddField(
                            document_type=doc["doc_type_value"],
                            field_name=field_name,
                            field_db_name=field_info["db_field"],
                            default=field_info["default"],
                            collection_name=collection,
                            scope_name=scope,
                            bucket_alias=bucket,
                        )
                    )

            # Add indexes for new documents
            for idx in doc.get("indexes", []):
                index_ops.append(
                    CreateIndex(
                        index_name=idx["name"],
                        fields=idx["fields"],
                        collection_name=collection,
                        scope_name=scope,
                        bucket_alias=bucket,
                        where=idx.get("where"),
                    )
                )

        # Detect removed documents (collections to drop)
        for name in sorted(set(old_docs) - set(new_docs)):
            doc = old_docs[name]
            collection = doc["collection_name"]
            scope = doc["scope_name"]
            bucket = doc["bucket_alias"]

            # Drop indexes first
            for idx in doc.get("indexes", []):
                index_ops.append(
                    DropIndex(
                        index_name=idx["name"],
                        collection_name=collection,
                        scope_name=scope,
                        bucket_alias=bucket,
                    )
                )

            if collection != "_default":
                infra_ops.append(DropCollection(collection, scope_name=scope, bucket_alias=bucket))

        # Detect changes within existing documents
        for name in sorted(set(old_docs) & set(new_docs)):
            old_doc = old_docs[name]
            new_doc = new_docs[name]
            collection = new_doc["collection_name"]
            scope = new_doc["scope_name"]
            bucket = new_doc["bucket_alias"]
            doc_type = new_doc["doc_type_value"]

            old_fields = old_doc.get("fields", {})
            new_fields = new_doc.get("fields", {})

            # New fields
            for field_name in sorted(set(new_fields) - set(old_fields)):
                field_info = new_fields[field_name]
                default = field_info["default"]
                if default == "__callable__" or default == "__non_serializable__":
                    default = None
                field_ops.append(
                    AddField(
                        document_type=doc_type,
                        field_name=field_name,
                        field_db_name=field_info["db_field"],
                        default=default,
                        collection_name=collection,
                        scope_name=scope,
                        bucket_alias=bucket,
                    )
                )

            # Removed fields
            for field_name in sorted(set(old_fields) - set(new_fields)):
                field_info = old_fields[field_name]
                field_ops.append(
                    RemoveField(
                        document_type=doc_type,
                        field_name=field_name,
                        field_db_name=field_info["db_field"],
                        collection_name=collection,
                        scope_name=scope,
                        bucket_alias=bucket,
                    )
                )

            # Index changes
            old_indexes = {idx["name"]: idx for idx in old_doc.get("indexes", [])}
            new_indexes = {idx["name"]: idx for idx in new_doc.get("indexes", [])}

            for idx_name in sorted(set(new_indexes) - set(old_indexes)):
                idx = new_indexes[idx_name]
                index_ops.append(
                    CreateIndex(
                        index_name=idx_name,
                        fields=idx["fields"],
                        collection_name=collection,
                        scope_name=scope,
                        bucket_alias=bucket,
                        where=idx.get("where"),
                    )
                )

            for idx_name in sorted(set(old_indexes) - set(new_indexes)):
                index_ops.append(
                    DropIndex(
                        index_name=idx_name,
                        collection_name=collection,
                        scope_name=scope,
                        bucket_alias=bucket,
                    )
                )

            # Index definition changed (fields or where clause changed)
            for idx_name in sorted(set(old_indexes) & set(new_indexes)):
                old_idx = old_indexes[idx_name]
                new_idx = new_indexes[idx_name]
                if old_idx != new_idx:
                    index_ops.append(
                        DropIndex(
                            index_name=idx_name,
                            collection_name=collection,
                            scope_name=scope,
                            bucket_alias=bucket,
                        )
                    )
                    index_ops.append(
                        CreateIndex(
                            index_name=idx_name,
                            fields=new_idx["fields"],
                            collection_name=collection,
                            scope_name=scope,
                            bucket_alias=bucket,
                            where=new_idx.get("where"),
                        )
                    )

        return {
            "infrastructure": infra_ops,
            "fields": field_ops,
            "indexes": index_ops,
        }

    def has_changes(self) -> bool:
        """Return True if any changes were detected."""
        changes = self.detect_changes()
        return any(changes.values())

    def all_operations(self) -> list:
        """Return all operations in a safe execution order (infra, fields, indexes)."""
        changes = self.detect_changes()
        return changes["infrastructure"] + changes["fields"] + changes["indexes"]
