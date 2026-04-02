"""Tests for migration operations — each operation's apply, reverse, and describe."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from django_couchbase_orm.migrations.executor import MigrationContext
from django_couchbase_orm.migrations.operations import (
    AddField,
    AlterField,
    CreateCollection,
    CreateIndex,
    CreateScope,
    DropCollection,
    DropIndex,
    DropScope,
    Operation,
    RemoveField,
    RenameField,
    RunN1QL,
    RunPython,
)


class MockContext:
    """Lightweight MigrationContext mock for unit tests."""

    def __init__(self):
        self.n1ql_calls = []
        self.bucket = MagicMock()
        self.bucket.collections.return_value = MagicMock()

    def get_bucket(self, alias=None):
        return self.bucket

    def get_cluster(self, alias=None):
        return MagicMock()

    def execute_n1ql(self, statement, bucket_alias=None, params=None):
        self.n1ql_calls.append((statement, params))

    def keyspace(self, bucket_alias, scope_name, collection_name):
        return f"`testbucket`.`{scope_name}`.`{collection_name}`"


class TestOperationBase:
    def test_base_apply_raises(self):
        op = Operation()
        with pytest.raises(NotImplementedError):
            op.apply(MockContext())

    def test_base_reverse_raises(self):
        op = Operation()
        with pytest.raises(NotImplementedError):
            op.reverse(MockContext())

    def test_base_describe_raises(self):
        op = Operation()
        with pytest.raises(NotImplementedError):
            op.describe()

    def test_base_not_reversible(self):
        assert Operation.reversible is False


class TestCreateScope:
    def test_apply(self):
        ctx = MockContext()
        op = CreateScope("myscope")
        op.apply(ctx)
        ctx.bucket.collections().create_scope.assert_called_with("myscope")

    def test_apply_already_exists(self):
        ctx = MockContext()
        ctx.bucket.collections().create_scope.side_effect = Exception("Scope already exists")
        op = CreateScope("myscope")
        op.apply(ctx)  # Should not raise

    def test_apply_other_error_raises(self):
        ctx = MockContext()
        ctx.bucket.collections().create_scope.side_effect = Exception("Connection failed")
        op = CreateScope("myscope")
        with pytest.raises(Exception, match="Connection failed"):
            op.apply(ctx)

    def test_reverse(self):
        ctx = MockContext()
        op = CreateScope("myscope")
        op.reverse(ctx)
        ctx.bucket.collections().drop_scope.assert_called_with("myscope")

    def test_reverse_not_found(self):
        ctx = MockContext()
        ctx.bucket.collections().drop_scope.side_effect = Exception("Scope not found")
        op = CreateScope("myscope")
        op.reverse(ctx)  # Should not raise

    def test_reversible(self):
        assert CreateScope.reversible is True

    def test_describe(self):
        op = CreateScope("myscope")
        assert "myscope" in op.describe()

    def test_repr(self):
        op = CreateScope("myscope")
        assert "CreateScope" in repr(op)


class TestDropScope:
    def test_apply(self):
        ctx = MockContext()
        op = DropScope("myscope")
        op.apply(ctx)
        ctx.bucket.collections().drop_scope.assert_called_with("myscope")

    def test_apply_not_found(self):
        ctx = MockContext()
        ctx.bucket.collections().drop_scope.side_effect = Exception("Scope not found")
        op = DropScope("myscope")
        op.apply(ctx)  # Should not raise

    def test_not_reversible(self):
        assert DropScope.reversible is False

    def test_describe(self):
        op = DropScope("myscope")
        assert "myscope" in op.describe()


class TestCreateCollection:
    def test_apply(self):
        ctx = MockContext()
        op = CreateCollection("beers", scope_name="brewing")
        with patch("django_couchbase_orm.migrations.operations.CollectionSpec", create=True) as MockSpec:
            # Patch the import inside apply
            import django_couchbase_orm.migrations.operations as ops_mod
            original = ops_mod.CreateCollection.apply

            # Just test that it calls create_collection
            op.apply(ctx)
            ctx.bucket.collections().create_collection.assert_called_once()

    def test_apply_already_exists(self):
        ctx = MockContext()
        ctx.bucket.collections().create_collection.side_effect = Exception("Collection already exists")
        op = CreateCollection("beers")
        op.apply(ctx)  # Should not raise

    def test_reverse(self):
        ctx = MockContext()
        op = CreateCollection("beers", scope_name="brewing")
        op.reverse(ctx)
        ctx.bucket.collections().drop_collection.assert_called_once()

    def test_reversible(self):
        assert CreateCollection.reversible is True

    def test_describe(self):
        op = CreateCollection("beers", scope_name="brewing")
        assert "brewing.beers" in op.describe()


class TestDropCollection:
    def test_apply(self):
        ctx = MockContext()
        op = DropCollection("beers")
        op.apply(ctx)
        ctx.bucket.collections().drop_collection.assert_called_once()

    def test_apply_not_found(self):
        ctx = MockContext()
        ctx.bucket.collections().drop_collection.side_effect = Exception("Collection not found")
        op = DropCollection("beers")
        op.apply(ctx)  # Should not raise

    def test_not_reversible(self):
        assert DropCollection.reversible is False

    def test_describe(self):
        op = DropCollection("beers", scope_name="brewing")
        assert "brewing.beers" in op.describe()


class TestCreateIndex:
    def test_apply(self):
        ctx = MockContext()
        op = CreateIndex(
            index_name="idx_name",
            fields=["name"],
            collection_name="beers",
            scope_name="_default",
        )
        op.apply(ctx)
        assert len(ctx.n1ql_calls) == 1
        stmt = ctx.n1ql_calls[0][0]
        assert "CREATE INDEX" in stmt
        assert "`idx_name`" in stmt
        assert "`name`" in stmt

    def test_apply_with_where(self):
        ctx = MockContext()
        op = CreateIndex(
            index_name="idx_active",
            fields=["name"],
            collection_name="beers",
            where="_type = 'beer'",
        )
        op.apply(ctx)
        stmt = ctx.n1ql_calls[0][0]
        assert "WHERE" in stmt

    def test_apply_already_exists(self):
        ctx = MockContext()
        ctx.execute_n1ql = MagicMock(side_effect=Exception("Index already exists"))
        op = CreateIndex(index_name="idx_name", fields=["name"])
        op.apply(ctx)  # Should not raise

    def test_apply_other_error(self):
        ctx = MockContext()
        ctx.execute_n1ql = MagicMock(side_effect=Exception("Connection failed"))
        op = CreateIndex(index_name="idx_name", fields=["name"])
        with pytest.raises(Exception, match="Connection failed"):
            op.apply(ctx)

    def test_reverse(self):
        ctx = MockContext()
        op = CreateIndex(
            index_name="idx_name",
            fields=["name"],
            collection_name="beers",
        )
        op.reverse(ctx)
        stmt = ctx.n1ql_calls[0][0]
        assert "DROP INDEX" in stmt
        assert "`idx_name`" in stmt

    def test_reversible(self):
        assert CreateIndex.reversible is True

    def test_describe(self):
        op = CreateIndex(index_name="idx_name", fields=["name", "abv"])
        assert "idx_name" in op.describe()
        assert "name" in op.describe()

    def test_multiple_fields(self):
        ctx = MockContext()
        op = CreateIndex(
            index_name="idx_multi",
            fields=["name", "abv", "ibu"],
            collection_name="beers",
        )
        op.apply(ctx)
        stmt = ctx.n1ql_calls[0][0]
        assert "`name`" in stmt
        assert "`abv`" in stmt
        assert "`ibu`" in stmt


class TestDropIndex:
    def test_apply(self):
        ctx = MockContext()
        op = DropIndex(index_name="idx_name", collection_name="beers")
        op.apply(ctx)
        assert "DROP INDEX" in ctx.n1ql_calls[0][0]

    def test_apply_not_found(self):
        ctx = MockContext()
        ctx.execute_n1ql = MagicMock(side_effect=Exception("Index not found"))
        op = DropIndex(index_name="idx_name")
        op.apply(ctx)  # Should not raise

    def test_not_reversible(self):
        assert DropIndex.reversible is False

    def test_describe(self):
        op = DropIndex(index_name="idx_name")
        assert "idx_name" in op.describe()


class TestAddField:
    def test_apply(self):
        ctx = MockContext()
        op = AddField(
            document_type="beer",
            field_name="rating",
            default=0,
            collection_name="beers",
        )
        op.apply(ctx)
        stmt, params = ctx.n1ql_calls[0]
        assert "UPDATE" in stmt
        assert "SET" in stmt
        assert "`rating`" in stmt
        assert params == [0, "beer"]

    def test_apply_with_db_name(self):
        ctx = MockContext()
        op = AddField(
            document_type="beer",
            field_name="rating",
            field_db_name="beer_rating",
            default=5,
            collection_name="beers",
        )
        op.apply(ctx)
        stmt = ctx.n1ql_calls[0][0]
        assert "`beer_rating`" in stmt

    def test_reverse(self):
        ctx = MockContext()
        op = AddField(
            document_type="beer",
            field_name="rating",
            collection_name="beers",
        )
        op.reverse(ctx)
        stmt = ctx.n1ql_calls[0][0]
        assert "UNSET" in stmt
        assert "`rating`" in stmt

    def test_reversible(self):
        assert AddField.reversible is True

    def test_describe(self):
        op = AddField(document_type="beer", field_name="rating", default=0)
        desc = op.describe()
        assert "rating" in desc
        assert "beer" in desc

    def test_default_none(self):
        ctx = MockContext()
        op = AddField(document_type="beer", field_name="notes", default=None)
        op.apply(ctx)
        _, params = ctx.n1ql_calls[0]
        assert params[0] is None

    def test_default_string(self):
        ctx = MockContext()
        op = AddField(document_type="beer", field_name="status", default="draft")
        op.apply(ctx)
        _, params = ctx.n1ql_calls[0]
        assert params[0] == "draft"


class TestRemoveField:
    def test_apply(self):
        ctx = MockContext()
        op = RemoveField(
            document_type="beer",
            field_name="rating",
            collection_name="beers",
        )
        op.apply(ctx)
        stmt, params = ctx.n1ql_calls[0]
        assert "UNSET" in stmt
        assert "`rating`" in stmt
        assert params == ["beer"]

    def test_not_reversible(self):
        assert RemoveField.reversible is False

    def test_describe(self):
        op = RemoveField(document_type="beer", field_name="rating")
        assert "rating" in op.describe()
        assert "beer" in op.describe()


class TestRenameField:
    def test_apply(self):
        ctx = MockContext()
        op = RenameField(
            document_type="beer",
            old_name="desc",
            new_name="description",
            collection_name="beers",
        )
        op.apply(ctx)
        # Should produce two N1QL calls: copy+nullify, then unset
        assert len(ctx.n1ql_calls) == 2
        stmt1 = ctx.n1ql_calls[0][0]
        assert "`description`" in stmt1
        assert "`desc`" in stmt1
        stmt2 = ctx.n1ql_calls[1][0]
        assert "UNSET" in stmt2

    def test_reverse(self):
        ctx = MockContext()
        op = RenameField(
            document_type="beer",
            old_name="desc",
            new_name="description",
            collection_name="beers",
        )
        op.reverse(ctx)
        assert len(ctx.n1ql_calls) == 2
        stmt1 = ctx.n1ql_calls[0][0]
        assert "`desc`" in stmt1
        assert "`description`" in stmt1

    def test_reversible(self):
        assert RenameField.reversible is True

    def test_describe(self):
        op = RenameField(document_type="beer", old_name="desc", new_name="description")
        desc = op.describe()
        assert "desc" in desc
        assert "description" in desc


class TestAlterField:
    def test_apply(self):
        ctx = MockContext()
        op = AlterField(
            document_type="beer",
            field_name="name",
            expression="UPPER(`name`)",
            collection_name="beers",
        )
        op.apply(ctx)
        stmt, params = ctx.n1ql_calls[0]
        assert "SET" in stmt
        assert "UPPER(`name`)" in stmt
        assert params == ["beer"]

    def test_not_reversible(self):
        assert AlterField.reversible is False

    def test_describe(self):
        op = AlterField(document_type="beer", field_name="name", expression="UPPER(`name`)")
        assert "name" in op.describe()
        assert "UPPER" in op.describe()


class TestRunN1QL:
    def test_apply(self):
        ctx = MockContext()
        op = RunN1QL("SELECT 1", params=[42], bucket_alias="default")
        op.apply(ctx)
        assert ctx.n1ql_calls == [("SELECT 1", [42])]

    def test_not_reversible_by_default(self):
        op = RunN1QL("SELECT 1")
        assert op.reversible is False
        with pytest.raises(NotImplementedError):
            op.reverse(MockContext())

    def test_reversible_with_reverse_statement(self):
        op = RunN1QL(
            "UPDATE x SET a=1",
            reverse_statement="UPDATE x SET a=0",
        )
        assert op.reversible is True
        ctx = MockContext()
        op.reverse(ctx)
        assert ctx.n1ql_calls == [("UPDATE x SET a=0", None)]

    def test_reverse_with_params(self):
        op = RunN1QL(
            "UPDATE x SET a=$1",
            params=[1],
            reverse_statement="UPDATE x SET a=$1",
            reverse_params=[0],
        )
        ctx = MockContext()
        op.reverse(ctx)
        assert ctx.n1ql_calls == [("UPDATE x SET a=$1", [0])]

    def test_describe(self):
        op = RunN1QL("SELECT META(d).id FROM `bucket`.`_default`.`beers` AS d WHERE d._type = 'beer'")
        desc = op.describe()
        assert "Run N1QL" in desc
        # Long statements are truncated
        assert len(desc) < 100

    def test_describe_short(self):
        op = RunN1QL("SELECT 1")
        assert "SELECT 1" in op.describe()


class TestRunPython:
    def test_apply(self):
        called = []
        def my_func(ctx):
            called.append(True)

        op = RunPython(my_func)
        op.apply(MockContext())
        assert called == [True]

    def test_not_reversible_by_default(self):
        op = RunPython(lambda ctx: None)
        assert op.reversible is False
        with pytest.raises(NotImplementedError):
            op.reverse(MockContext())

    def test_reversible_with_reverse_func(self):
        forward_calls = []
        reverse_calls = []

        def forward(ctx):
            forward_calls.append(True)

        def reverse(ctx):
            reverse_calls.append(True)

        op = RunPython(forward, reverse_func=reverse)
        assert op.reversible is True

        op.apply(MockContext())
        assert forward_calls == [True]

        op.reverse(MockContext())
        assert reverse_calls == [True]

    def test_describe(self):
        def populate_data(ctx):
            pass

        op = RunPython(populate_data)
        assert "populate_data" in op.describe()

    def test_describe_lambda(self):
        op = RunPython(lambda ctx: None)
        assert "Run Python" in op.describe()

    def test_func_receives_context(self):
        received = []
        def check_ctx(ctx):
            received.append(ctx)

        ctx = MockContext()
        op = RunPython(check_ctx)
        op.apply(ctx)
        assert received[0] is ctx
