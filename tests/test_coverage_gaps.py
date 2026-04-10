"""Tests for coverage gaps identified by the test audit.

Covers: N1QL query building (delete, update, clone), paginator edge cases,
manager operations, signal handling, document options, identifier validation,
QuerySet.none(), nested lookups, and more.
"""

import pytest

from django_couchbase_orm.document import Document
from django_couchbase_orm.exceptions import OperationError, ValidationError
from django_couchbase_orm.fields.base import BaseField
from django_couchbase_orm.fields.compound import (
    DictField,
    EmbeddedDocument,
    EmbeddedDocumentField,
    ListField,
)
from django_couchbase_orm.fields.simple import (
    BooleanField,
    FloatField,
    IntegerField,
    StringField,
)
from django_couchbase_orm.options import DocumentOptions
from django_couchbase_orm.query.n1ql import N1QLQuery, _validate_identifier

from tests.conftest import couchbase_available, flush_collection

integration_mark = pytest.mark.skipif(
    not couchbase_available, reason="Local Couchbase not available"
)

LOCAL_COUCHBASE = {
    "default": {
        "CONNECTION_STRING": "couchbase://localhost",
        "USERNAME": "Administrator",
        "PASSWORD": "password",
        "BUCKET": "testbucket",
        "SCOPE": "_default",
    }
}


# ============================================================
# N1QL identifier validation
# ============================================================


class TestIdentifierValidation:
    def test_valid_simple(self):
        assert _validate_identifier("name") == "name"

    def test_valid_underscore_prefix(self):
        assert _validate_identifier("_type") == "_type"

    def test_valid_alphanumeric(self):
        assert _validate_identifier("field_123") == "field_123"

    def test_invalid_space(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            _validate_identifier("field name")

    def test_invalid_backtick(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            _validate_identifier("`injection`")

    def test_invalid_starts_with_number(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            _validate_identifier("123field")

    def test_invalid_special_chars(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            _validate_identifier("field-name")

    def test_invalid_dot(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            _validate_identifier("a.b")

    def test_invalid_empty(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            _validate_identifier("")

    def test_single_char(self):
        assert _validate_identifier("x") == "x"

    def test_single_underscore(self):
        assert _validate_identifier("_") == "_"


# ============================================================
# N1QL query builder — build_delete
# ============================================================


class TestN1QLBuildDelete:
    def test_delete_no_where(self):
        q = N1QLQuery("b", "s", "c")
        stmt, params = q.build_delete()
        assert stmt == "DELETE FROM `b`.`s`.`c` AS d"
        assert params == []

    def test_delete_with_where(self):
        q = N1QLQuery("b", "s", "c")
        p = q.add_param("exdoc")
        q.where(f"d.`_type` = {p}")
        stmt, params = q.build_delete()
        assert "WHERE" in stmt
        assert "d.`_type` = $1" in stmt
        assert params == ["exdoc"]

    def test_delete_with_use_keys_single(self):
        q = N1QLQuery("b", "s", "c")
        q.use_keys(["doc1"])
        stmt, params = q.build_delete()
        assert "USE KEYS" in stmt
        assert "doc1" in params

    def test_delete_with_use_keys_multiple(self):
        q = N1QLQuery("b", "s", "c")
        q.use_keys(["doc1", "doc2", "doc3"])
        stmt, params = q.build_delete()
        assert "USE KEYS" in stmt
        assert ["doc1", "doc2", "doc3"] in params

    def test_delete_with_where_and_keys(self):
        q = N1QLQuery("b", "s", "c")
        q.use_keys(["doc1"])
        p = q.add_param("active")
        q.where(f"d.`status` = {p}")
        stmt, params = q.build_delete()
        assert "USE KEYS" in stmt
        assert "WHERE" in stmt

    def test_delete_multiple_where_clauses(self):
        q = N1QLQuery("b", "s", "c")
        p1 = q.add_param("exdoc")
        q.where(f"d.`_type` = {p1}")
        p2 = q.add_param("active")
        q.where(f"d.`status` = {p2}")
        stmt, params = q.build_delete()
        assert "AND" in stmt
        assert params == ["exdoc", "active"]


# ============================================================
# N1QL query builder — build_update
# ============================================================


class TestN1QLBuildUpdate:
    def test_update_single_field(self):
        q = N1QLQuery("b", "s", "c")
        stmt, params = q.build_update({"name": "Alice"})
        assert "UPDATE" in stmt
        assert "SET d.`name` = $1" in stmt
        assert params == ["Alice"]

    def test_update_multiple_fields(self):
        q = N1QLQuery("b", "s", "c")
        stmt, params = q.build_update({"name": "Alice", "age": 30})
        assert "SET" in stmt
        assert "d.`name`" in stmt
        assert "d.`age`" in stmt
        assert len(params) == 2

    def test_update_with_where(self):
        q = N1QLQuery("b", "s", "c")
        p = q.add_param("exdoc")
        q.where(f"d.`_type` = {p}")
        stmt, params = q.build_update({"name": "New"})
        assert "WHERE" in stmt
        assert params == ["exdoc", "New"]

    def test_update_with_use_keys(self):
        q = N1QLQuery("b", "s", "c")
        q.use_keys(["doc1"])
        stmt, params = q.build_update({"name": "New"})
        assert "USE KEYS" in stmt

    def test_update_invalid_field_name(self):
        q = N1QLQuery("b", "s", "c")
        with pytest.raises(ValueError, match="Invalid identifier"):
            q.build_update({"invalid field": "value"})

    def test_update_null_value(self):
        q = N1QLQuery("b", "s", "c")
        stmt, params = q.build_update({"name": None})
        assert params == [None]


# ============================================================
# N1QL query builder — clone
# ============================================================


class TestN1QLClone:
    def test_clone_preserves_all_fields(self):
        q = N1QLQuery("b", "s", "c")
        q.select("name", "age")
        q.include_meta_id()
        p = q.add_param("exdoc")
        q.where(f"d.`_type` = {p}")
        q.order_by("name", "-age")
        q.limit(10)
        q.offset(5)
        q.use_keys(["doc1"])

        clone = q.clone()
        assert clone._bucket == "b"
        assert clone._scope == "s"
        assert clone._collection == "c"
        assert clone._select_fields == ["name", "age"]
        assert clone._meta_id is True
        assert clone._where_clauses == q._where_clauses
        assert clone._order_by == ["name", "-age"]
        assert clone._limit == 10
        assert clone._offset == 5
        assert clone._use_keys == ["doc1"]

    def test_clone_is_independent(self):
        q = N1QLQuery("b", "s", "c")
        q.where("d.`name` = $1", ["Alice"])
        clone = q.clone()
        clone.where("d.`age` = $2", [30])

        assert len(q._where_clauses) == 1
        assert len(clone._where_clauses) == 2

    def test_clone_none_fields(self):
        q = N1QLQuery("b", "s", "c")
        clone = q.clone()
        assert clone._select_fields is None
        assert clone._use_keys is None


# ============================================================
# N1QL query builder — build SELECT
# ============================================================


class TestN1QLBuildSelect:
    def test_select_all(self):
        q = N1QLQuery("b", "s", "c")
        stmt, params = q.build()
        assert stmt == "SELECT d.* FROM `b`.`s`.`c` AS d"

    def test_select_with_meta_id(self):
        q = N1QLQuery("b", "s", "c")
        q.include_meta_id()
        stmt, _ = q.build()
        assert "META(d).id AS __id" in stmt

    def test_select_specific_fields(self):
        q = N1QLQuery("b", "s", "c")
        q.select("name", "age")
        stmt, _ = q.build()
        assert "d.`name`" in stmt
        assert "d.`age`" in stmt

    def test_select_count(self):
        q = N1QLQuery("b", "s", "c")
        q.select_count()
        stmt, _ = q.build()
        assert "COUNT(*) AS `__count`" in stmt

    def test_order_by_ascending(self):
        q = N1QLQuery("b", "s", "c")
        q.order_by("name")
        stmt, _ = q.build()
        assert "ORDER BY d.`name` ASC" in stmt

    def test_order_by_descending(self):
        q = N1QLQuery("b", "s", "c")
        q.order_by("-name")
        stmt, _ = q.build()
        assert "ORDER BY d.`name` DESC" in stmt

    def test_limit_and_offset(self):
        q = N1QLQuery("b", "s", "c")
        q.limit(10).offset(20)
        stmt, params = q.build()
        assert "LIMIT" in stmt
        assert "OFFSET" in stmt
        assert 10 in params
        assert 20 in params

    def test_use_keys_single(self):
        q = N1QLQuery("b", "s", "c")
        q.use_keys(["doc1"])
        stmt, params = q.build()
        assert "USE KEYS $1" in stmt
        assert "doc1" in params

    def test_use_keys_multiple(self):
        q = N1QLQuery("b", "s", "c")
        q.use_keys(["doc1", "doc2"])
        stmt, params = q.build()
        assert "USE KEYS $1" in stmt
        assert ["doc1", "doc2"] in params

    def test_add_param_incrementing(self):
        q = N1QLQuery("b", "s", "c")
        assert q.add_param("a") == "$1"
        assert q.add_param("b") == "$2"
        assert q.add_param("c") == "$3"

    def test_keyspace_property(self):
        q = N1QLQuery("mybucket", "myscope", "mycoll")
        assert q.keyspace == "`mybucket`.`myscope`.`mycoll`"

    def test_order_by_invalid_identifier(self):
        q = N1QLQuery("b", "s", "c")
        with pytest.raises(ValueError, match="Invalid identifier"):
            q.order_by("invalid field")

    def test_select_invalid_identifier(self):
        q = N1QLQuery("b", "s", "c")
        with pytest.raises(ValueError, match="Invalid identifier"):
            q.select("bad field")


# ============================================================
# Paginator — unit tests
# ============================================================


class TestPaginatorUnitTests:
    """Test paginator without Couchbase by using a mock queryset."""

    def _make_paginator(self, total, per_page):
        """Create a paginator with a fake queryset."""
        from django_couchbase_orm.paginator import CouchbasePaginator

        class FakeQS:
            def __init__(self, total):
                self._total = total
                self._items = list(range(total))

            def count(self):
                return self._total

            def __getitem__(self, key):
                return self._items[key]

        return CouchbasePaginator(FakeQS(total), per_page=per_page)

    def test_basic_pagination(self):
        p = self._make_paginator(50, 10)
        assert p.count == 50
        assert p.num_pages == 5
        assert list(p.page_range) == [1, 2, 3, 4, 5]

    def test_page_contents(self):
        p = self._make_paginator(25, 10)
        page1 = p.page(1)
        assert len(page1) == 10
        page3 = p.page(3)
        assert len(page3) == 5  # Last page has 5 items

    def test_page_navigation(self):
        p = self._make_paginator(30, 10)
        page1 = p.page(1)
        assert page1.has_next is True
        assert page1.has_previous is False
        assert page1.next_page_number == 2

        page2 = p.page(2)
        assert page2.has_next is True
        assert page2.has_previous is True

        page3 = p.page(3)
        assert page3.has_next is False
        assert page3.has_previous is True
        assert page3.previous_page_number == 2

    def test_page_no_next_raises(self):
        p = self._make_paginator(10, 10)
        page = p.page(1)
        with pytest.raises(ValueError, match="No next page"):
            page.next_page_number

    def test_page_no_previous_raises(self):
        p = self._make_paginator(10, 10)
        page = p.page(1)
        with pytest.raises(ValueError, match="No previous page"):
            page.previous_page_number

    def test_invalid_page_zero(self):
        p = self._make_paginator(10, 10)
        with pytest.raises(ValueError, match="at least 1"):
            p.page(0)

    def test_invalid_page_negative(self):
        p = self._make_paginator(10, 10)
        with pytest.raises(ValueError, match="at least 1"):
            p.page(-1)

    def test_invalid_page_too_high(self):
        p = self._make_paginator(10, 10)
        with pytest.raises(ValueError, match="does not exist"):
            p.page(2)

    def test_invalid_page_string(self):
        p = self._make_paginator(10, 10)
        with pytest.raises(ValueError, match="must be an integer"):
            p.page("abc")

    def test_string_page_number_coerced(self):
        p = self._make_paginator(10, 10)
        page = p.page("1")  # String "1" should work
        assert page.number == 1

    def test_empty_queryset(self):
        p = self._make_paginator(0, 10)
        assert p.count == 0
        assert p.num_pages == 1  # Always at least 1 page
        page = p.page(1)
        assert len(page) == 0
        assert page.has_next is False
        assert page.has_previous is False
        assert bool(page) is False

    def test_per_page_zero_or_negative(self):
        p = self._make_paginator(10, 0)
        assert p.per_page == 1  # Should clamp to 1

        p2 = self._make_paginator(10, -5)
        assert p2.per_page == 1

    def test_start_end_index(self):
        p = self._make_paginator(25, 10)
        page1 = p.page(1)
        assert page1.start_index == 1
        assert page1.end_index == 10

        page3 = p.page(3)
        assert page3.start_index == 21
        assert page3.end_index == 25

    def test_empty_page_indices(self):
        p = self._make_paginator(0, 10)
        page = p.page(1)
        assert page.start_index == 0
        assert page.end_index == 0

    def test_page_repr(self):
        p = self._make_paginator(50, 10)
        page = p.page(3)
        assert repr(page) == "<Page 3 of 5>"

    def test_page_iter(self):
        p = self._make_paginator(5, 10)
        page = p.page(1)
        items = list(page)
        assert items == [0, 1, 2, 3, 4]

    def test_has_other_pages(self):
        p = self._make_paginator(10, 10)
        page = p.page(1)
        assert page.has_other_pages is False

        p2 = self._make_paginator(20, 10)
        page = p2.page(1)
        assert page.has_other_pages is True


# ============================================================
# DocumentOptions
# ============================================================


class TestDocumentOptions:
    def test_defaults(self):
        opts = DocumentOptions()
        assert opts.collection_name == ""
        assert opts.scope_name == "_default"
        assert opts.bucket_alias == "default"
        assert opts.doc_type_field == "_type"
        assert opts.abstract is False

    def test_apply_meta(self):
        class Meta:
            collection_name = "users"
            scope_name = "myapp"
            bucket_alias = "secondary"
            abstract = True

        opts = DocumentOptions(Meta)
        assert opts.collection_name == "users"
        assert opts.scope_name == "myapp"
        assert opts.bucket_alias == "secondary"
        assert opts.abstract is True

    def test_get_field(self):
        opts = DocumentOptions()
        f = StringField()
        f.name = "name"
        opts.fields["name"] = f
        assert opts.get_field("name") is f

    def test_get_field_missing(self):
        opts = DocumentOptions()
        with pytest.raises(KeyError, match="does not exist"):
            opts.get_field("nonexistent")

    def test_get_field_by_db_name(self):
        opts = DocumentOptions()
        f = StringField(db_field="fn")
        f.name = "full_name"
        opts.fields["full_name"] = f
        assert opts.get_field_by_db_name("fn") is f
        assert opts.get_field_by_db_name("nonexistent") is None

    def test_repr(self):
        opts = DocumentOptions()
        opts.collection_name = "users"
        r = repr(opts)
        assert "users" in r


# ============================================================
# Manager — unit tests (no Couchbase)
# ============================================================


class TestManagerAccessFromInstance:
    def test_manager_not_accessible_from_instance(self):
        class MyDoc(Document):
            name = StringField()

            class Meta:
                collection_name = "test"

        doc = MyDoc(name="test")
        with pytest.raises(AttributeError, match="not instances"):
            doc.objects

    def test_manager_accessible_from_class(self):
        class MyDoc(Document):
            name = StringField()

            class Meta:
                collection_name = "test"

        mgr = MyDoc.objects
        assert mgr._document_class is MyDoc


class TestManagerGetNoArgs:
    def test_get_no_args_raises(self):
        class MyDoc(Document):
            name = StringField()

            class Meta:
                collection_name = "test"

        with pytest.raises(ValueError, match="requires at least one"):
            MyDoc.objects.get()


# ============================================================
# QuerySet.none()
# ============================================================


@integration_mark
class TestQuerySetNone:
    @pytest.fixture(autouse=True)
    def _setup(self, settings):
        settings.COUCHBASE = LOCAL_COUCHBASE
        from django_couchbase_orm.connection import reset_connections
        reset_connections()

    def test_none_returns_empty(self):
        qs = GapDoc.objects.none()
        assert list(qs) == []
        assert len(qs) == 0
        assert bool(qs) is False
        assert qs.count() == 0
        assert qs.exists() is False
        assert qs.first() is None
        assert qs.last() is None


# ============================================================
# Signals
# ============================================================


class TestSignalDefinitions:
    def test_signals_exist(self):
        from django_couchbase_orm.signals import post_delete, post_save, pre_delete, pre_save

        assert pre_save is not None
        assert post_save is not None
        assert pre_delete is not None
        assert post_delete is not None


# ============================================================
# Document metaclass and type discriminator
# ============================================================


class TestDocumentMetaclass:
    def test_doc_type_value_auto_generated(self):
        class MyDoc(Document):
            name = StringField()

            class Meta:
                collection_name = "test"

        assert MyDoc._meta.doc_type_value == "mydoc"

    def test_does_not_exist_exception(self):
        class MyDoc(Document):
            name = StringField()

            class Meta:
                collection_name = "test"

        assert hasattr(MyDoc, "DoesNotExist")
        with pytest.raises(MyDoc.DoesNotExist):
            raise MyDoc.DoesNotExist("not found")

    def test_multiple_objects_returned_exception(self):
        class MyDoc(Document):
            name = StringField()

            class Meta:
                collection_name = "test"

        assert hasattr(MyDoc, "MultipleObjectsReturned")
        with pytest.raises(MyDoc.MultipleObjectsReturned):
            raise MyDoc.MultipleObjectsReturned("too many")

    def test_abstract_document(self):
        class BaseDoc(Document):
            name = StringField()

            class Meta:
                abstract = True
                collection_name = "base"

        class ConcreteDoc(BaseDoc):
            age = IntegerField()

            class Meta:
                collection_name = "concrete"

        assert "name" in ConcreteDoc._meta.fields
        assert "age" in ConcreteDoc._meta.fields

    def test_field_ordering_preserved(self):
        class OrderedDoc(Document):
            alpha = StringField()
            beta = IntegerField()
            gamma = FloatField()

            class Meta:
                collection_name = "test"

        field_names = list(OrderedDoc._meta.fields.keys())
        assert field_names == ["alpha", "beta", "gamma"]


# ============================================================
# Field edge cases
# ============================================================


class TestFieldEdgeCases:
    def test_multiple_validators(self):
        def must_be_positive(value):
            if value <= 0:
                raise ValidationError("Must be positive")

        def must_be_even(value):
            if value % 2 != 0:
                raise ValidationError("Must be even")

        f = IntegerField(validators=[must_be_positive, must_be_even])
        f.name = "test"
        f.validate(4)  # passes both

        with pytest.raises(ValidationError, match="Must be positive"):
            f.validate(-2)

        with pytest.raises(ValidationError, match="Must be even"):
            f.validate(3)

    def test_choices_with_tuples(self):
        f = StringField(choices=[("a", "Alpha"), ("b", "Beta")])
        f.name = "test"
        f.validate("a")
        f.validate("b")
        with pytest.raises(ValidationError, match="not a valid choice"):
            f.validate("Alpha")  # Must use value, not label

    def test_string_field_none_valid_when_not_required(self):
        f = StringField(required=False)
        f.name = "test"
        f.validate(None)  # Should not raise

    def test_integer_field_none_valid_when_not_required(self):
        f = IntegerField(required=False)
        f.name = "test"
        f.validate(None)

    def test_required_field_none_raises(self):
        f = StringField(required=True)
        f.name = "test"
        with pytest.raises(ValidationError, match="required"):
            f.validate(None)

    def test_list_field_to_python_non_list_raises(self):
        f = ListField()
        f.name = "test"
        with pytest.raises(ValidationError, match="expected a list"):
            f.to_python("not a list")

    def test_dict_field_to_python_non_dict_raises(self):
        f = DictField()
        f.name = "test"
        with pytest.raises(ValidationError, match="expected a dict"):
            f.to_python([1, 2, 3])

    def test_embedded_document_field_wrong_type(self):
        class Addr(EmbeddedDocument):
            city = StringField()

        f = EmbeddedDocumentField(Addr)
        f.name = "test"
        with pytest.raises(ValidationError):
            f.validate("not an embedded doc")

    def test_float_field_to_json_none(self):
        f = FloatField()
        assert f.to_json(None) is None

    def test_int_field_to_json_none(self):
        f = IntegerField()
        assert f.to_json(None) is None

    def test_bool_field_to_json_none(self):
        f = BooleanField()
        assert f.to_json(None) is None


# ============================================================
# Deeply nested embedded documents
# ============================================================


class TestDeeplyNestedEmbedded:
    def test_three_levels_deep(self):
        class Level3(EmbeddedDocument):
            value = IntegerField()

        class Level2(EmbeddedDocument):
            inner = EmbeddedDocumentField(Level3)

        class Level1(EmbeddedDocument):
            middle = EmbeddedDocumentField(Level2)

        l1 = Level1(middle=Level2(inner=Level3(value=42)))
        d = l1.to_dict()
        assert d == {"middle": {"inner": {"value": 42}}}

        l1_restored = Level1.from_dict(d)
        assert l1_restored.middle.inner.value == 42

    def test_embedded_with_list_field(self):
        class Item(EmbeddedDocument):
            name = StringField()
            tags = ListField(field=StringField())

        item = Item(name="test", tags=["a", "b"])
        d = item.to_dict()
        assert d == {"name": "test", "tags": ["a", "b"]}


# ============================================================
# Document to_dict / from_dict roundtrip edge cases
# ============================================================


class TestDocumentSerialization:
    def test_to_dict_type_discriminator(self):
        class MyDoc(Document):
            name = StringField()

            class Meta:
                collection_name = "test"

        doc = MyDoc(name="Alice")
        d = doc.to_dict()
        assert "_type" in d
        assert d["_type"] == "mydoc"

    def test_from_dict_sets_is_new_false(self):
        class MyDoc(Document):
            name = StringField()

            class Meta:
                collection_name = "test"

        doc = MyDoc.from_dict("key1", {"name": "Alice"})
        assert doc._is_new is False
        assert doc._id == "key1"

    def test_to_dict_with_all_field_types(self):
        class FullDoc(Document):
            name = StringField(required=True)
            count = IntegerField()
            ratio = FloatField()
            active = BooleanField(default=True)
            tags = ListField(field=StringField())
            meta = DictField()

            class Meta:
                collection_name = "test"

        doc = FullDoc(name="test", count=5, ratio=3.14, tags=["a"], meta={"k": "v"})
        d = doc.to_dict()
        assert d["name"] == "test"
        assert d["count"] == 5
        assert d["ratio"] == 3.14
        assert d["active"] is True
        assert d["tags"] == ["a"]
        assert d["meta"] == {"k": "v"}


# ============================================================
# Document clean() hook
# ============================================================


class TestDocumentCleanHook:
    def test_clean_called_during_full_clean(self):
        clean_called = []

        class MyDoc(Document):
            name = StringField(required=True)

            class Meta:
                collection_name = "test"

            def clean(self):
                clean_called.append(True)
                if self._data.get("name") == "forbidden":
                    raise ValidationError("Forbidden name")

        doc = MyDoc(name="ok")
        doc.full_clean()
        assert clean_called == [True]

        doc2 = MyDoc(name="forbidden")
        with pytest.raises(ValidationError, match="Forbidden"):
            doc2.full_clean()


# ============================================================
# Exceptions
# ============================================================


class TestExceptions:
    def test_validation_error_with_message(self):
        err = ValidationError("bad value")
        assert err.message == "bad value"

    def test_validation_error_with_errors_dict(self):
        err = ValidationError(errors={"name": "required", "age": "must be positive"})
        assert err.errors is not None
        assert "name" in err.errors
        assert "age" in err.errors

    def test_operation_error(self):
        err = OperationError("failed to save")
        assert "failed to save" in str(err)


# ============================================================
# Integration tests — require Couchbase
# ============================================================


class GapDoc(Document):
    name = StringField(required=True)
    score = IntegerField(default=0)
    active = BooleanField(default=True)
    tags = ListField(field=StringField())

    class Meta:
        collection_name = "edge_test_docs"


def _flush():
    flush_collection("edge_test_docs")


@integration_mark
class TestManagerIntegration:
    @pytest.fixture(autouse=True)
    def _cleanup(self, settings):
        settings.COUCHBASE = LOCAL_COUCHBASE
        from django_couchbase_orm.connection import reset_connections

        reset_connections()
        _flush()
        yield
        _flush()

    def test_create_and_get_by_pk(self):
        doc = GapDoc.objects.create(name="test")
        loaded = GapDoc.objects.get(pk=doc.pk)
        assert loaded.name == "test"
        assert loaded._is_new is False

    def test_get_nonexistent_raises(self):
        with pytest.raises(GapDoc.DoesNotExist):
            GapDoc.objects.get(pk="nonexistent_pk_xyz")

    def test_get_by_field(self):
        GapDoc(name="unique_abc").save()
        doc = GapDoc.objects.get(name="unique_abc")
        assert doc.name == "unique_abc"

    def test_exists_true_and_false(self):
        doc = GapDoc(name="exists_test")
        doc.save()
        assert GapDoc.objects.exists(doc.pk) is True
        assert GapDoc.objects.exists("nonexistent_xyz") is False

    def test_get_or_create_creates(self):
        doc, created = GapDoc.objects.get_or_create(
            _id="goc_new", defaults={"name": "new_doc"}
        )
        assert created is True
        assert doc.name == "new_doc"

    def test_get_or_create_gets_existing(self):
        GapDoc(_id="goc_exist", name="existing").save()
        doc, created = GapDoc.objects.get_or_create(
            _id="goc_exist", defaults={"name": "should_not_use"}
        )
        assert created is False
        assert doc.name == "existing"

    def test_bulk_create(self):
        docs = [GapDoc(name=f"bulk_{i}") for i in range(5)]
        result = GapDoc.objects.bulk_create(docs)
        assert len(result) == 5
        for doc in result:
            assert doc._is_new is False
            assert doc._cas is not None

    def test_bulk_update(self):
        docs = [GapDoc(name=f"bu_{i}", score=i) for i in range(3)]
        GapDoc.objects.bulk_create(docs)

        for doc in docs:
            doc.score = doc.score + 100

        updated = GapDoc.objects.bulk_update(docs, ["score"])
        assert updated == 3

        for doc in docs:
            reloaded = GapDoc.objects.get(pk=doc.pk)
            assert reloaded.score >= 100

    def test_bulk_update_empty_docs(self):
        assert GapDoc.objects.bulk_update([], ["name"]) == 0

    def test_bulk_update_empty_fields(self):
        docs = [GapDoc(name="test")]
        GapDoc.objects.bulk_create(docs)
        assert GapDoc.objects.bulk_update(docs, []) == 0

    def test_bulk_update_invalid_field(self):
        doc = GapDoc(name="test")
        doc.save()
        with pytest.raises(OperationError):
            GapDoc.objects.bulk_update([doc], ["nonexistent_field"])


@integration_mark
class TestSignalIntegration:
    @pytest.fixture(autouse=True)
    def _cleanup(self, settings):
        settings.COUCHBASE = LOCAL_COUCHBASE
        from django_couchbase_orm.connection import reset_connections

        reset_connections()
        _flush()
        yield
        _flush()

    def test_pre_save_fires(self):
        from django_couchbase_orm.signals import pre_save

        received = []

        def handler(sender, instance, created, **kwargs):
            received.append({"sender": sender, "created": created, "name": instance.name})

        pre_save.connect(handler, sender=GapDoc)
        try:
            doc = GapDoc(name="signal_test")
            doc.save()
            assert len(received) == 1
            assert received[0]["created"] is True
            assert received[0]["name"] == "signal_test"
        finally:
            pre_save.disconnect(handler, sender=GapDoc)

    def test_post_save_fires(self):
        from django_couchbase_orm.signals import post_save

        received = []

        def handler(sender, instance, created, **kwargs):
            received.append(created)

        post_save.connect(handler, sender=GapDoc)
        try:
            doc = GapDoc(name="post_test")
            doc.save()
            assert received == [True]

            doc.name = "updated"
            doc.save()
            assert received == [True, False]
        finally:
            post_save.disconnect(handler, sender=GapDoc)

    def test_pre_delete_fires(self):
        from django_couchbase_orm.signals import pre_delete

        received = []

        def handler(sender, instance, **kwargs):
            received.append(instance.pk)

        pre_delete.connect(handler, sender=GapDoc)
        try:
            doc = GapDoc(name="del_test")
            doc.save()
            pk = doc.pk
            doc.delete()
            assert received == [pk]
        finally:
            pre_delete.disconnect(handler, sender=GapDoc)

    def test_post_delete_fires(self):
        from django_couchbase_orm.signals import post_delete

        received = []

        def handler(sender, instance, **kwargs):
            received.append(True)

        post_delete.connect(handler, sender=GapDoc)
        try:
            doc = GapDoc(name="postdel_test")
            doc.save()
            doc.delete()
            assert received == [True]
        finally:
            post_delete.disconnect(handler, sender=GapDoc)


@integration_mark
class TestAggregateWithFilters:
    @pytest.fixture(autouse=True)
    def _cleanup(self, settings):
        settings.COUCHBASE = LOCAL_COUCHBASE
        from django_couchbase_orm.connection import reset_connections

        reset_connections()
        _flush()
        yield
        _flush()

    def test_aggregate_with_filter(self):
        from django_couchbase_orm.aggregates import Avg, Count

        GapDoc(name="a", score=10, active=True).save()
        GapDoc(name="b", score=20, active=True).save()
        GapDoc(name="c", score=30, active=False).save()

        result = GapDoc.objects.filter(active=True).aggregate(
            avg_score=Avg("score"), total=Count("*")
        )
        assert result["total"] == 2
        assert result["avg_score"] == 15.0

    def test_aggregate_with_exclude(self):
        from django_couchbase_orm.aggregates import Count

        GapDoc(name="a", score=10).save()
        GapDoc(name="b", score=20).save()
        GapDoc(name="c", score=30).save()

        result = GapDoc.objects.exclude(name="b").aggregate(total=Count("*"))
        assert result["total"] == 2

    def test_count_with_filter(self):
        GapDoc(name="x", active=True).save()
        GapDoc(name="y", active=True).save()
        GapDoc(name="z", active=False).save()

        assert GapDoc.objects.filter(active=True).count() == 2
        assert GapDoc.objects.filter(active=False).count() == 1


@integration_mark
class TestQuerySetChaining:
    @pytest.fixture(autouse=True)
    def _cleanup(self, settings):
        settings.COUCHBASE = LOCAL_COUCHBASE
        from django_couchbase_orm.connection import reset_connections

        reset_connections()
        _flush()
        yield
        _flush()

    def test_filter_then_order_then_limit(self):
        for i in range(10):
            GapDoc(name=f"chain_{i:02d}", score=i).save()

        results = list(GapDoc.objects.filter(score__gte=3).order_by("score")[:5])
        assert len(results) == 5
        scores = [r.score for r in results]
        assert scores == [3, 4, 5, 6, 7]

    def test_exclude_then_count(self):
        GapDoc(name="keep1", score=1).save()
        GapDoc(name="keep2", score=2).save()
        GapDoc(name="remove", score=3).save()

        count = GapDoc.objects.exclude(name="remove").count()
        assert count == 2

    def test_filter_filter_stacks(self):
        GapDoc(name="match", score=10, active=True).save()
        GapDoc(name="no_score", score=5, active=True).save()
        GapDoc(name="inactive", score=10, active=False).save()

        results = list(
            GapDoc.objects.filter(active=True).filter(score__gte=10)
        )
        assert len(results) == 1
        assert results[0].name == "match"

    def test_values_with_filter(self):
        GapDoc(name="val_test", score=42).save()
        results = list(GapDoc.objects.filter(name="val_test").values("name", "score"))
        assert len(results) == 1
        assert results[0]["name"] == "val_test"
        assert results[0]["score"] == 42
