from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from django_couchbase_orm.fields.base import BaseField


class DocumentOptions:
    """Holds metadata about a Document class, accessible via cls._meta.

    Populated by DocumentMetaclass from the inner Meta class and field definitions.
    """

    def __init__(self, meta: type | None = None):
        self.collection_name: str = ""
        self.scope_name: str = "_default"
        self.bucket_alias: str = "default"
        self.doc_type_field: str = "_type"
        self.doc_type_value: str = ""
        self.id_field: str = "id"
        self.fields: OrderedDict[str, BaseField] = OrderedDict()
        self.indexes: list[dict[str, Any]] = []
        self.abstract: bool = False

        if meta:
            self._apply_meta(meta)

    def _apply_meta(self, meta: type) -> None:
        """Apply settings from an inner Meta class."""
        if hasattr(meta, "collection_name"):
            self.collection_name = meta.collection_name
        if hasattr(meta, "scope_name"):
            self.scope_name = meta.scope_name
        if hasattr(meta, "bucket_alias"):
            self.bucket_alias = meta.bucket_alias
        if hasattr(meta, "doc_type_field"):
            self.doc_type_field = meta.doc_type_field
        if hasattr(meta, "id_field"):
            self.id_field = meta.id_field
        if hasattr(meta, "indexes"):
            self.indexes = meta.indexes
        if hasattr(meta, "abstract"):
            self.abstract = meta.abstract

    def get_field(self, name: str) -> BaseField:
        """Get a field by its attribute name."""
        if name not in self.fields:
            raise KeyError(f"Field '{name}' does not exist on this document.")
        return self.fields[name]

    def get_field_by_db_name(self, db_name: str) -> BaseField | None:
        """Get a field by its database field name."""
        for field in self.fields.values():
            if field.get_db_field() == db_name:
                return field
        return None

    def __repr__(self) -> str:
        return f"<DocumentOptions: collection={self.collection_name!r}, fields={list(self.fields.keys())}>"
