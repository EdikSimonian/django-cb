"""Tests for MigrationWriter — serializing migrations to Python files."""

from __future__ import annotations

import os
import tempfile

import pytest

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
from django_couchbase_orm.migrations.writer import (
    MigrationWriter,
    _repr_value,
    _serialize_operation,
    next_migration_name,
)


class TestReprValue:
    def test_string(self):
        assert _repr_value("hello") == "'hello'"

    def test_none(self):
        assert _repr_value(None) == "None"

    def test_int(self):
        assert _repr_value(42) == "42"

    def test_float(self):
        assert _repr_value(3.14) == "3.14"

    def test_bool(self):
        assert _repr_value(True) == "True"

    def test_list(self):
        result = _repr_value([1, 2, 3])
        assert result == "[1, 2, 3]"

    def test_string_with_quotes(self):
        result = _repr_value("it's a test")
        assert "it" in result


class TestSerializeOperation:
    def test_create_scope(self):
        op = CreateScope("myscope", bucket_alias="default")
        result = _serialize_operation(op)
        assert "CreateScope" in result
        assert "'myscope'" in result

    def test_drop_scope(self):
        op = DropScope("myscope")
        result = _serialize_operation(op)
        assert "DropScope" in result
        assert "'myscope'" in result

    def test_create_collection(self):
        op = CreateCollection("beers", scope_name="brewing", bucket_alias="default")
        result = _serialize_operation(op)
        assert "CreateCollection" in result
        assert "'beers'" in result
        assert "'brewing'" in result

    def test_drop_collection(self):
        op = DropCollection("beers")
        result = _serialize_operation(op)
        assert "DropCollection" in result
        assert "'beers'" in result

    def test_create_index(self):
        op = CreateIndex(
            index_name="idx_name",
            fields=["name", "abv"],
            collection_name="beers",
        )
        result = _serialize_operation(op)
        assert "CreateIndex" in result
        assert "'idx_name'" in result
        assert "['name', 'abv']" in result

    def test_create_index_with_where(self):
        op = CreateIndex(
            index_name="idx_active",
            fields=["name"],
            where="_type = 'beer'",
        )
        result = _serialize_operation(op)
        assert "where=" in result

    def test_drop_index(self):
        op = DropIndex(index_name="idx_name")
        result = _serialize_operation(op)
        assert "DropIndex" in result
        assert "'idx_name'" in result

    def test_add_field(self):
        op = AddField(
            document_type="beer",
            field_name="rating",
            field_db_name="beer_rating",
            default=0,
        )
        result = _serialize_operation(op)
        assert "AddField" in result
        assert "'beer'" in result
        assert "'rating'" in result
        assert "'beer_rating'" in result
        assert "0" in result

    def test_remove_field(self):
        op = RemoveField(document_type="beer", field_name="rating")
        result = _serialize_operation(op)
        assert "RemoveField" in result
        assert "'rating'" in result

    def test_rename_field(self):
        op = RenameField(document_type="beer", old_name="desc", new_name="description")
        result = _serialize_operation(op)
        assert "RenameField" in result
        assert "'desc'" in result
        assert "'description'" in result

    def test_alter_field(self):
        op = AlterField(document_type="beer", field_name="name", expression="UPPER(`name`)")
        result = _serialize_operation(op)
        assert "AlterField" in result
        assert "UPPER" in result

    def test_run_n1ql(self):
        op = RunN1QL("SELECT 1", params=[42], bucket_alias="default")
        result = _serialize_operation(op)
        assert "RunN1QL" in result
        assert "'SELECT 1'" in result
        assert "[42]" in result

    def test_run_n1ql_with_reverse(self):
        op = RunN1QL(
            "UPDATE x SET a=1",
            reverse_statement="UPDATE x SET a=0",
            reverse_params=[1],
        )
        result = _serialize_operation(op)
        assert "reverse_statement=" in result
        assert "reverse_params=" in result

    def test_run_python(self):
        def my_func(ctx):
            pass

        op = RunPython(my_func)
        result = _serialize_operation(op)
        assert "RunPython" in result
        assert "my_func" in result
        assert "TODO" in result


class TestMigrationWriter:
    def test_as_string_basic(self):
        ops = [
            CreateCollection("beers"),
            AddField("beer", "name", default=""),
        ]
        writer = MigrationWriter("myapp", "0001_initial", ops)
        source = writer.as_string()
        assert "class Migration" in source
        assert "app_label = 'myapp'" in source
        assert "name = '0001_initial'" in source
        assert "CreateCollection" in source
        assert "AddField" in source
        assert "from django_couchbase_orm.migrations" in source

    def test_as_string_with_dependencies(self):
        writer = MigrationWriter(
            "myapp",
            "0002_add_field",
            operations=[AddField("beer", "rating", default=0)],
            dependencies=[("myapp", "0001_initial")],
        )
        source = writer.as_string()
        assert "('myapp', '0001_initial')" in source

    def test_as_string_empty(self):
        writer = MigrationWriter("myapp", "0001_empty", operations=[])
        source = writer.as_string()
        assert "class Migration" in source
        assert "No operations" in source

    def test_as_string_is_valid_python(self):
        ops = [
            CreateScope("brewing"),
            CreateCollection("beers", scope_name="brewing"),
            CreateIndex(index_name="idx_name", fields=["name"], collection_name="beers"),
            AddField("beer", "rating", default=0),
        ]
        writer = MigrationWriter("myapp", "0001_initial", ops)
        source = writer.as_string()
        # Should be valid Python — compile it
        compile(source, "<test>", "exec")

    def test_as_string_imports_only_used_classes(self):
        writer = MigrationWriter(
            "myapp", "0001", operations=[CreateCollection("beers")]
        )
        source = writer.as_string()
        assert "CreateCollection" in source
        # Should not import unused operation classes
        assert "RenameField" not in source

    def test_write_creates_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = os.path.join(tmpdir, "cb_migrations")
            writer = MigrationWriter("myapp", "0001_initial", [CreateCollection("beers")])
            path = writer.write(migrations_dir)
            assert os.path.exists(path)
            assert path.endswith("0001_initial.py")

            # Check __init__.py was created
            init_path = os.path.join(migrations_dir, "__init__.py")
            assert os.path.exists(init_path)

            # Verify content
            with open(path) as f:
                content = f.read()
            assert "class Migration" in content

    def test_write_existing_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = os.path.join(tmpdir, "cb_migrations")
            os.makedirs(migrations_dir)
            # Write __init__.py first
            with open(os.path.join(migrations_dir, "__init__.py"), "w") as f:
                f.write("# existing")

            writer = MigrationWriter("myapp", "0001_initial", [])
            path = writer.write(migrations_dir)
            assert os.path.exists(path)

            # __init__.py should still have original content
            with open(os.path.join(migrations_dir, "__init__.py")) as f:
                assert "# existing" in f.read()


class TestNextMigrationName:
    def test_empty_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            name = next_migration_name(tmpdir, prefix="initial")
            assert name == "0001_initial"

    def test_nonexistent_directory(self):
        name = next_migration_name("/nonexistent/path", prefix="initial")
        assert name == "0001_initial"

    def test_increments(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create existing migration files
            open(os.path.join(tmpdir, "0001_initial.py"), "w").close()
            open(os.path.join(tmpdir, "0002_add_field.py"), "w").close()
            name = next_migration_name(tmpdir, prefix="add_index")
            assert name == "0003_add_index"

    def test_ignores_init(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "__init__.py"), "w").close()
            name = next_migration_name(tmpdir, prefix="initial")
            assert name == "0001_initial"

    def test_ignores_non_numeric(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "custom_migration.py"), "w").close()
            name = next_migration_name(tmpdir, prefix="initial")
            assert name == "0001_initial"

    def test_default_prefix(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            name = next_migration_name(tmpdir)
            assert name == "0001_auto"

    def test_handles_gaps(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "0001_initial.py"), "w").close()
            open(os.path.join(tmpdir, "0005_jump.py"), "w").close()
            name = next_migration_name(tmpdir, prefix="next")
            assert name == "0006_next"
