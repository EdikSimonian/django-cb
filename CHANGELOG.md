# Changelog

## 0.1.0 (2024)

Initial release.

### Features

- **Document model** with metaclass, typed fields, validation, and CRUD operations
- **Fields**: StringField, IntegerField, FloatField, BooleanField, UUIDField, DateTimeField, DateField, ListField, DictField, EmbeddedDocumentField, ReferenceField
- **QuerySet** with Django-style filtering, ordering, slicing, Q objects, and 16 lookup transforms
- **N1QL query builder** with parameterized queries
- **KV-optimized** `get(pk=...)` for fast single-document lookups
- **Sub-document operations** via `document.subdoc` accessor
- **Signals**: pre_save, post_save, pre_delete, post_delete
- **Session backend**: Couchbase-backed Django sessions with TTL expiry
- **Auth backend**: Couchbase-backed User model and authentication
- **Connection management**: Thread-safe, lazy initialization, multiple connection support, automatic WAN profile for Capella
