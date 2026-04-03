# Testing Guide

The project has **940+ tests** split into two suites: the Document API tests (no Couchbase required) and the Database Backend integration tests (requires a running Couchbase instance).

## Quick Start

```bash
# Run all unit tests (no Couchbase needed)
pytest

# Run backend integration tests (requires local Couchbase)
DJANGO_SETTINGS_MODULE=tests.test_backend_settings pytest tests/test_backend_*.py

# Run everything
pytest && DJANGO_SETTINGS_MODULE=tests.test_backend_settings pytest tests/test_backend_*.py
```

## Prerequisites

### Unit Tests (Document API)

No external services needed. Tests use mocked Couchbase connections.

```bash
pip install -e ".[dev]"
pytest
```

### Integration Tests (Database Backend)

Requires a local Couchbase Server:

```bash
# Start Couchbase via Docker
docker run -d --name couchbase-test \
  -p 8091-8097:8091-8097 \
  -p 11210-11211:11210-11211 \
  couchbase/server:latest

# Wait for startup, then create a bucket via the web UI:
# http://localhost:8091 → Buckets → Add Bucket → "testbucket"
# Default credentials: Administrator / password

# Run backend tests
DJANGO_SETTINGS_MODULE=tests.test_backend_settings pytest tests/test_backend_*.py
```

## Test Suites

### Document API Tests (784 tests)

These test the standalone Document ORM without requiring a database connection.

| File | Tests | What's Tested |
|------|------:|---------------|
| `test_fields.py` | 79 | StringField, IntegerField, FloatField, BooleanField, UUIDField validation, defaults, choices |
| `test_migration_operations.py` | 66 | CreateCollection, CreateIndex, AddField, RemoveField, RenameField, AlterField, RunN1QL, RunPython |
| `test_document.py` | 46 | Document CRUD, save/delete/reload, validation, pk, to_dict/from_dict, Meta options |
| `test_transforms.py` | 42 | All lookup transforms: exact, gt, gte, lt, lte, in, contains, icontains, startswith, regex, between |
| `test_compound_fields.py` | 42 | ListField, DictField, EmbeddedDocument, EmbeddedDocumentField validation and serialization |
| `test_queryset_execution.py` | 41 | QuerySet evaluation, caching, iteration, slicing, chaining |
| `test_integration.py` | 39 | End-to-end CRUD against real Couchbase (marked `@pytest.mark.integration`) |
| `test_migration_writer.py` | 35 | Migration file generation, serialization |
| `test_datetime_fields.py` | 33 | DateTimeField, DateField, auto_now, auto_now_add, ISO format |
| `test_migration_executor.py` | 31 | Migration execution, dependency resolution, forward/reverse |
| `test_queryset.py` | 30 | QuerySet building: filter, exclude, order_by, values, select_related, none |
| `test_migration_autodetector.py` | 29 | Auto-detecting model changes, generating operations |
| `test_auth.py` | 28 | User model, password hashing, authentication backend |
| `test_n1ql.py` | 20 | N1QL query builder, parameterization, keyspace formatting |
| `test_q.py` | 19 | Q objects: AND, OR, NOT, nesting, resolution to N1QL |
| `test_reference_field.py` | 18 | ReferenceField: save, load, dereference, type resolution |
| `test_paginator.py` | 17 | CouchbasePaginator, Page objects, navigation |
| `test_migration_state.py` | 17 | Migration state tracking in Couchbase |
| `test_manager.py` | 17 | DocumentManager: get, create, get_or_create, exists, bulk ops |
| `test_async.py` | 17 | Async QuerySet: alist, acount, afirst, aget, aexists |
| `test_subdoc.py` | 16 | Sub-document operations: get, upsert, array_append, increment |
| `test_sessions.py` | 16 | Couchbase session backend: create, read, modify, delete, expiry |
| `test_async_execution.py` | 15 | Async Document CRUD: asave, adelete, areload |
| `test_connection.py` | 11 | Connection management, config validation, cluster caching |
| `test_migration_commands.py` | 10 | Management commands: cb_makemigrations, cb_migrate |
| `test_aggregates.py` | 10 | Count, Sum, Avg, Min, Max aggregate functions |
| `test_signals.py` | 8 | pre_save, post_save, pre_delete, post_delete signals |
| `test_bulk.py` | 8 | bulk_create, bulk_update |
| `test_select_related.py` | 7 | ReferenceField prefetching, batch KV loading |
| `test_options.py` | 7 | DocumentOptions, Meta class, field introspection |
| `test_exceptions.py` | 6 | DoesNotExist, MultipleObjectsReturned, ValidationError, OperationError |
| `test_management.py` | 4 | cb_ensure_indexes, cb_create_collections commands |

### Database Backend Tests (156 tests)

These test the Django `db/backends/couchbase/` against a real Couchbase instance.

| File | Tests | What's Tested |
|------|------:|---------------|
| `test_backend_crud.py` | 57 | **Phase 1**: Connection, cursor, param conversion, IN clause collapsing, column parsing, Model CRUD (create, get, filter, update, delete, count, exists, first, values, values_list, order_by, slicing), ContentTypes, auth, schema editor, operations, features |
| `test_backend_phase2.py` | 37 | **Phase 2**: select_related (FK JOINs), FK filter traversal, reverse FK, M2M (add, remove, clear, reverse, count), annotate+Count, aggregate, Q objects (AND, OR, NOT), F expressions, UPSERT save, all lookup types (icontains, istartswith, iendswith, iexact, in, isnull, gt/lt), prefetch_related, values().annotate() GROUP BY, ORDER BY desc |
| `test_backend_phase3.py` | 23 | **Phase 3**: Auth permissions (user, group, superuser, inactive), Django admin (index, changelist, add, change, delete, search), ModelForm (create, update), UserCreationForm, AuthenticationForm, DB sessions (create, modify, delete), ContentTypes (get_for_model, model_class), LogEntry |
| `test_backend_phase4.py` | 18 | **Phase 4**: MigrationRecorder, showmigrations, schema editor (create/delete model, M2M, add_field), transactions (atomic basic, nested), custom model CRUD (Article with FK, M2M tags, auto_now, ordering, select_related), introspection |
| `test_backend_phase5.py` | 21 | **Phase 5**: Connection sharing (auto-derived settings, same cluster instance), cross-API data access, subqueries (pk__in, exclude with subquery, queryset IN), bulk_create, bulk_update, complex queries (values_list multiple fields, chained filters, complex Q excludes, none(), null FK, distinct, only/defer, multiple aggregates, reverse FK, F comparisons) |

## Running Specific Tests

```bash
# Single file
pytest tests/test_fields.py

# Single test class
pytest tests/test_fields.py::TestStringField

# Single test
pytest tests/test_fields.py::TestStringField::test_max_length

# By keyword
pytest -k "test_filter"

# Integration tests only (requires Couchbase)
pytest -m integration

# Verbose output
pytest -v

# Stop on first failure
pytest -x
```

## Running Backend Tests

```bash
# All backend tests
DJANGO_SETTINGS_MODULE=tests.test_backend_settings pytest tests/test_backend_*.py -v

# Single phase
DJANGO_SETTINGS_MODULE=tests.test_backend_settings pytest tests/test_backend_phase2.py -v

# Single test class
DJANGO_SETTINGS_MODULE=tests.test_backend_settings pytest tests/test_backend_crud.py::TestModelCRUD -v
```

## Coverage

```bash
# Unit tests coverage
coverage run -m pytest tests/ \
  --ignore=tests/test_backend_*.py \
  --ignore=tests/test_wagtail_*.py \
  --ignore=tests/testapp \
  --ignore=tests/wagtailapp
coverage report --show-missing --include="src/*"

# Backend coverage (requires Couchbase)
DJANGO_SETTINGS_MODULE=tests.test_backend_settings \
  coverage run -m pytest tests/test_backend_*.py
coverage report --show-missing --include="src/*"

# HTML report
coverage html
open htmlcov/index.html
```

## Test Configuration

### Unit Tests

Configured in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["src", "."]
DJANGO_SETTINGS_MODULE = "tests.django_settings"
asyncio_mode = "auto"
```

Settings file: `tests/django_settings.py` — uses mocked Couchbase connections.

### Backend Tests

Settings file: `tests/test_backend_settings.py` — connects to real Couchbase at `localhost`.

```python
DATABASES = {
    "default": {
        "ENGINE": "django_couchbase_orm.db.backends.couchbase",
        "NAME": "testbucket",
        "USER": "Administrator",
        "PASSWORD": "password",
        "HOST": "couchbase://localhost",
    }
}
```

## Writing New Tests

### Unit Test (no Couchbase)

```python
# tests/test_my_feature.py
import pytest
from django_couchbase_orm import Document, StringField

class TestMyFeature:
    def test_something(self):
        class MyDoc(Document):
            name = StringField(required=True)
        doc = MyDoc(name="test")
        assert doc.name == "test"
```

### Integration Test (requires Couchbase)

```python
# tests/test_my_integration.py
import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _couchbase_available(),
        reason="Local Couchbase not available",
    ),
]

class TestMyIntegration:
    def test_real_query(self):
        # Uses real Couchbase connection
        ...
```

### Backend Test (Django models with Couchbase)

```python
# tests/test_backend_my_feature.py
import pytest

pytestmark = [
    pytest.mark.skipif(...),
    pytest.mark.django_db(transaction=True),
]

class TestMyBackendFeature:
    def test_model_crud(self):
        from django.contrib.auth.models import User
        user = User.objects.create_user("test", "test@test.com", "pass")
        assert user.pk is not None
        user.delete()
```

## CI/CD

Tests run automatically on push via GitHub Actions. The CI workflow:

1. Runs unit tests (no Couchbase) on Python 3.10, 3.11, 3.12, 3.13
2. Generates coverage report
3. Updates the coverage badge

Integration tests require a Couchbase instance and run separately.
