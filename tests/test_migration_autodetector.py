"""Tests for MigrationAutodetector — detecting Document class changes."""

from __future__ import annotations

import pytest

from django_couchbase_orm.migrations.autodetector import (
    MigrationAutodetector,
    _safe_default,
    _serialize_field,
    snapshot_state,
)
from django_couchbase_orm.migrations.operations import (
    AddField,
    CreateCollection,
    CreateIndex,
    CreateScope,
    DropCollection,
    DropIndex,
    RemoveField,
)


# ======================================================================
# State snapshot helpers
# ======================================================================


def make_doc_state(
    name="TestDoc",
    collection_name="testdoc",
    scope_name="_default",
    bucket_alias="default",
    doc_type_value="testdoc",
    fields=None,
    indexes=None,
):
    """Build a single document state dict for testing."""
    return {
        name: {
            "collection_name": collection_name,
            "scope_name": scope_name,
            "bucket_alias": bucket_alias,
            "doc_type_value": doc_type_value,
            "fields": fields or {},
            "indexes": indexes or [],
        }
    }


def make_field_state(type_name="StringField", db_field=None, required=False, default=None):
    """Build a single field state dict."""
    return {
        "type": type_name,
        "db_field": db_field or "fieldname",
        "required": required,
        "default": default,
    }


# ======================================================================
# _serialize_field / _safe_default tests
# ======================================================================


class TestSerializeField:
    def test_serialize_simple_field(self):
        from django_couchbase_orm.fields.simple import StringField

        f = StringField(required=True, default="hello")
        f.name = "name"
        result = _serialize_field(f)
        assert result["type"] == "StringField"
        assert result["db_field"] == "name"
        assert result["required"] is True
        assert result["default"] == "hello"

    def test_serialize_field_with_db_field(self):
        from django_couchbase_orm.fields.simple import IntegerField

        f = IntegerField(db_field="custom_name")
        f.name = "my_field"
        result = _serialize_field(f)
        assert result["db_field"] == "custom_name"

    def test_safe_default_none(self):
        from django_couchbase_orm.fields.simple import StringField

        f = StringField()
        assert _safe_default(f) is None

    def test_safe_default_callable(self):
        from django_couchbase_orm.fields.simple import StringField

        f = StringField(default=list)
        assert _safe_default(f) == "__callable__"

    def test_safe_default_serializable(self):
        from django_couchbase_orm.fields.simple import IntegerField

        f = IntegerField(default=42)
        assert _safe_default(f) == 42


# ======================================================================
# snapshot_state tests
# ======================================================================


class TestSnapshotState:
    def test_returns_dict_with_documents_key(self):
        state = snapshot_state()
        assert "documents" in state
        assert isinstance(state["documents"], dict)

    def test_captures_registered_documents(self):
        # The document registry may contain documents from other test files;
        # just verify the structure is correct
        state = snapshot_state()
        for name, doc in state["documents"].items():
            assert "collection_name" in doc
            assert "scope_name" in doc
            assert "bucket_alias" in doc
            assert "fields" in doc
            assert "indexes" in doc


# ======================================================================
# MigrationAutodetector tests
# ======================================================================


class TestAutodetectorNewDocument:
    """Test detection of entirely new Document classes."""

    def test_detect_new_document_creates_collection(self):
        old = {"documents": {}}
        new = {"documents": make_doc_state("Beer", collection_name="beer")}
        detector = MigrationAutodetector(old, new)
        changes = detector.detect_changes()
        infra = changes["infrastructure"]
        assert any(isinstance(op, CreateCollection) and op.collection_name == "beer" for op in infra)

    def test_detect_new_document_with_custom_scope(self):
        old = {"documents": {}}
        new = {"documents": make_doc_state("Beer", scope_name="brewing", collection_name="beer")}
        detector = MigrationAutodetector(old, new)
        changes = detector.detect_changes()
        infra = changes["infrastructure"]
        assert any(isinstance(op, CreateScope) and op.scope_name == "brewing" for op in infra)
        assert any(isinstance(op, CreateCollection) and op.scope_name == "brewing" for op in infra)

    def test_no_scope_creation_for_default(self):
        old = {"documents": {}}
        new = {"documents": make_doc_state("Beer", scope_name="_default")}
        detector = MigrationAutodetector(old, new)
        changes = detector.detect_changes()
        infra = changes["infrastructure"]
        assert not any(isinstance(op, CreateScope) for op in infra)

    def test_no_collection_creation_for_default(self):
        old = {"documents": {}}
        new = {"documents": make_doc_state("Beer", collection_name="_default")}
        detector = MigrationAutodetector(old, new)
        changes = detector.detect_changes()
        infra = changes["infrastructure"]
        assert not any(isinstance(op, CreateCollection) for op in infra)

    def test_detect_new_document_with_indexes(self):
        old = {"documents": {}}
        new = {
            "documents": make_doc_state(
                "Beer",
                collection_name="beer",
                indexes=[{"name": "idx_name", "fields": ["name"]}],
            )
        }
        detector = MigrationAutodetector(old, new)
        changes = detector.detect_changes()
        idx_ops = changes["indexes"]
        assert any(isinstance(op, CreateIndex) and op.index_name == "idx_name" for op in idx_ops)

    def test_detect_new_document_with_default_fields(self):
        old = {"documents": {}}
        new = {
            "documents": make_doc_state(
                "Beer",
                collection_name="beer",
                fields={
                    "rating": make_field_state("IntegerField", db_field="rating", default=0),
                },
            )
        }
        detector = MigrationAutodetector(old, new)
        changes = detector.detect_changes()
        field_ops = changes["fields"]
        assert any(
            isinstance(op, AddField) and op.field_name == "rating" and op.default == 0
            for op in field_ops
        )


class TestAutodetectorRemovedDocument:
    """Test detection of removed Document classes."""

    def test_detect_removed_document_drops_collection(self):
        old = {"documents": make_doc_state("Beer", collection_name="beer")}
        new = {"documents": {}}
        detector = MigrationAutodetector(old, new)
        changes = detector.detect_changes()
        infra = changes["infrastructure"]
        assert any(isinstance(op, DropCollection) and op.collection_name == "beer" for op in infra)

    def test_detect_removed_document_drops_indexes(self):
        old = {
            "documents": make_doc_state(
                "Beer",
                collection_name="beer",
                indexes=[{"name": "idx_name", "fields": ["name"]}],
            )
        }
        new = {"documents": {}}
        detector = MigrationAutodetector(old, new)
        changes = detector.detect_changes()
        idx_ops = changes["indexes"]
        assert any(isinstance(op, DropIndex) and op.index_name == "idx_name" for op in idx_ops)

    def test_no_drop_for_default_collection(self):
        old = {"documents": make_doc_state("Beer", collection_name="_default")}
        new = {"documents": {}}
        detector = MigrationAutodetector(old, new)
        changes = detector.detect_changes()
        infra = changes["infrastructure"]
        assert not any(isinstance(op, DropCollection) for op in infra)


class TestAutodetectorFieldChanges:
    """Test detection of field additions and removals on existing documents."""

    def test_detect_added_field(self):
        old = {"documents": make_doc_state("Beer", fields={})}
        new = {
            "documents": make_doc_state(
                "Beer",
                fields={"rating": make_field_state("IntegerField", db_field="rating", default=0)},
            )
        }
        detector = MigrationAutodetector(old, new)
        changes = detector.detect_changes()
        field_ops = changes["fields"]
        assert any(isinstance(op, AddField) and op.field_name == "rating" for op in field_ops)

    def test_detect_removed_field(self):
        old = {
            "documents": make_doc_state(
                "Beer",
                fields={"rating": make_field_state("IntegerField", db_field="rating")},
            )
        }
        new = {"documents": make_doc_state("Beer", fields={})}
        detector = MigrationAutodetector(old, new)
        changes = detector.detect_changes()
        field_ops = changes["fields"]
        assert any(isinstance(op, RemoveField) and op.field_name == "rating" for op in field_ops)

    def test_detect_multiple_field_changes(self):
        old = {
            "documents": make_doc_state(
                "Beer",
                fields={
                    "name": make_field_state("StringField", db_field="name"),
                    "old_field": make_field_state("StringField", db_field="old_field"),
                },
            )
        }
        new = {
            "documents": make_doc_state(
                "Beer",
                fields={
                    "name": make_field_state("StringField", db_field="name"),
                    "new_field": make_field_state("IntegerField", db_field="new_field", default=0),
                },
            )
        }
        detector = MigrationAutodetector(old, new)
        changes = detector.detect_changes()
        field_ops = changes["fields"]
        assert any(isinstance(op, AddField) and op.field_name == "new_field" for op in field_ops)
        assert any(isinstance(op, RemoveField) and op.field_name == "old_field" for op in field_ops)

    def test_unchanged_field_no_ops(self):
        fields = {"name": make_field_state("StringField", db_field="name")}
        old = {"documents": make_doc_state("Beer", fields=fields)}
        new = {"documents": make_doc_state("Beer", fields=fields)}
        detector = MigrationAutodetector(old, new)
        changes = detector.detect_changes()
        assert changes["fields"] == []

    def test_callable_default_becomes_none(self):
        old = {"documents": make_doc_state("Beer", fields={})}
        new = {
            "documents": make_doc_state(
                "Beer",
                fields={
                    "tags": make_field_state("ListField", db_field="tags", default="__callable__"),
                },
            )
        }
        detector = MigrationAutodetector(old, new)
        changes = detector.detect_changes()
        field_ops = changes["fields"]
        add_ops = [op for op in field_ops if isinstance(op, AddField) and op.field_name == "tags"]
        assert len(add_ops) == 1
        assert add_ops[0].default is None  # Callable defaults can't be serialized


class TestAutodetectorIndexChanges:
    """Test detection of index additions, removals, and modifications."""

    def test_detect_added_index(self):
        old = {"documents": make_doc_state("Beer", indexes=[])}
        new = {
            "documents": make_doc_state(
                "Beer",
                indexes=[{"name": "idx_name", "fields": ["name"]}],
            )
        }
        detector = MigrationAutodetector(old, new)
        changes = detector.detect_changes()
        assert any(isinstance(op, CreateIndex) and op.index_name == "idx_name" for op in changes["indexes"])

    def test_detect_removed_index(self):
        old = {
            "documents": make_doc_state(
                "Beer",
                indexes=[{"name": "idx_name", "fields": ["name"]}],
            )
        }
        new = {"documents": make_doc_state("Beer", indexes=[])}
        detector = MigrationAutodetector(old, new)
        changes = detector.detect_changes()
        assert any(isinstance(op, DropIndex) and op.index_name == "idx_name" for op in changes["indexes"])

    def test_detect_modified_index(self):
        old = {
            "documents": make_doc_state(
                "Beer",
                indexes=[{"name": "idx_name", "fields": ["name"]}],
            )
        }
        new = {
            "documents": make_doc_state(
                "Beer",
                indexes=[{"name": "idx_name", "fields": ["name", "abv"]}],
            )
        }
        detector = MigrationAutodetector(old, new)
        changes = detector.detect_changes()
        idx_ops = changes["indexes"]
        # Should drop old and create new
        assert any(isinstance(op, DropIndex) and op.index_name == "idx_name" for op in idx_ops)
        assert any(isinstance(op, CreateIndex) and op.index_name == "idx_name" for op in idx_ops)

    def test_unchanged_index_no_ops(self):
        indexes = [{"name": "idx_name", "fields": ["name"]}]
        old = {"documents": make_doc_state("Beer", indexes=indexes)}
        new = {"documents": make_doc_state("Beer", indexes=indexes)}
        detector = MigrationAutodetector(old, new)
        changes = detector.detect_changes()
        assert changes["indexes"] == []


class TestAutodetectorHasChanges:
    def test_no_changes(self):
        state = {"documents": make_doc_state("Beer")}
        detector = MigrationAutodetector(state, state)
        assert detector.has_changes() is False

    def test_has_changes(self):
        old = {"documents": {}}
        new = {"documents": make_doc_state("Beer", collection_name="beer")}
        detector = MigrationAutodetector(old, new)
        assert detector.has_changes() is True


class TestAutodetectorAllOperations:
    def test_order_infra_then_fields_then_indexes(self):
        old = {"documents": {}}
        new = {
            "documents": make_doc_state(
                "Beer",
                collection_name="beer",
                scope_name="brewing",
                fields={"rating": make_field_state("IntegerField", db_field="rating", default=0)},
                indexes=[{"name": "idx_rating", "fields": ["rating"]}],
            )
        }
        detector = MigrationAutodetector(old, new)
        ops = detector.all_operations()
        # Infrastructure ops should come first
        infra_end = 0
        for i, op in enumerate(ops):
            if isinstance(op, (CreateScope, CreateCollection)):
                infra_end = i
        field_start = len(ops)
        for i, op in enumerate(ops):
            if isinstance(op, (AddField, RemoveField)):
                field_start = i
                break
        index_start = len(ops)
        for i, op in enumerate(ops):
            if isinstance(op, (CreateIndex, DropIndex)):
                index_start = i
                break

        assert infra_end < field_start or infra_end < index_start

    def test_multiple_documents(self):
        old = {"documents": {}}
        new = {
            "documents": {
                **make_doc_state("Beer", collection_name="beer"),
                **make_doc_state("Brewery", collection_name="brewery"),
            }
        }
        detector = MigrationAutodetector(old, new)
        ops = detector.all_operations()
        collection_names = [
            op.collection_name for op in ops if isinstance(op, CreateCollection)
        ]
        assert "beer" in collection_names
        assert "brewery" in collection_names
