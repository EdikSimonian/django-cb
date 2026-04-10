"""Tests for QuerySet execution paths against a real Couchbase instance.

These tests validate that queries execute correctly end-to-end, including
return types (e.g. _update() returns int, not cursor).
"""

import pytest
from django_couchbase_orm.document import Document
from django_couchbase_orm.fields.simple import FloatField, IntegerField, StringField

from tests.conftest import couchbase_available, flush_collection

LOCAL_COUCHBASE = {
    "default": {
        "CONNECTION_STRING": "couchbase://localhost",
        "USERNAME": "Administrator",
        "PASSWORD": "password",
        "BUCKET": "testbucket",
        "SCOPE": "_default",
    }
}


class ExDoc(Document):
    name = StringField(required=True)
    age = IntegerField()
    score = FloatField()

    class Meta:
        collection_name = "exdocs"


def _flush_exdocs():
    flush_collection("exdocs")


pytestmark = [
    pytest.mark.skipif(not couchbase_available, reason="Local Couchbase not available"),
]


class TestQuerySetExecution:
    @pytest.fixture(autouse=True)
    def _cleanup(self, settings):
        settings.COUCHBASE = LOCAL_COUCHBASE
        _flush_exdocs()
        yield
        _flush_exdocs()

    def test_execute_returns_documents(self):
        ExDoc(name="Alice", age=30).save()
        ExDoc(name="Bob", age=25).save()

        results = list(ExDoc.objects.all())
        assert len(results) == 2
        names = {r.name for r in results}
        assert names == {"Alice", "Bob"}

    def test_execute_values_returns_dicts(self):
        ExDoc(name="Alice", age=30).save()
        ExDoc(name="Bob", age=25).save()

        results = list(ExDoc.objects.values("name"))
        assert len(results) == 2
        names = {r["name"] for r in results}
        assert names == {"Alice", "Bob"}

    def test_count_via_query(self):
        for i in range(5):
            ExDoc(name=f"doc_{i}", age=i).save()
        count = ExDoc.objects.count()
        assert count == 5

    def test_count_empty_result(self):
        count = ExDoc.objects.count()
        assert count == 0

    def test_exists_true(self):
        ExDoc(name="Alice", age=30).save()
        assert ExDoc.objects.filter(name="Alice").exists() is True

    def test_exists_false(self):
        assert ExDoc.objects.filter(name="Nobody").exists() is False

    def test_first_found(self):
        ExDoc(name="Alice", age=30).save()
        doc = ExDoc.objects.first()
        assert doc is not None
        assert doc.name == "Alice"

    def test_first_empty(self):
        doc = ExDoc.objects.first()
        assert doc is None

    def test_last(self):
        ExDoc(name="Alice", age=10).save()
        ExDoc(name="Bob", age=20).save()
        doc = ExDoc.objects.last()
        assert doc is not None
        assert doc.name in ("Alice", "Bob")

    def test_get_success(self):
        ExDoc(name="Alice", age=30).save()
        doc = ExDoc.objects.filter(name="Alice").get()
        assert doc.name == "Alice"

    def test_get_not_found(self):
        with pytest.raises(ExDoc.DoesNotExist):
            ExDoc.objects.filter(name="Nobody").get()

    def test_get_multiple(self):
        ExDoc(name="Alice", age=30).save()
        ExDoc(name="Alice", age=25).save()
        with pytest.raises(ExDoc.MultipleObjectsReturned):
            ExDoc.objects.filter(name="Alice").get()

    def test_get_with_kwargs(self):
        ExDoc(name="Alice", age=30).save()
        doc = ExDoc.objects.get(name="Alice", age=30)
        assert doc.name == "Alice"
        assert doc.age == 30

    def test_len(self):
        ExDoc(name="A", age=1).save()
        ExDoc(name="B", age=2).save()
        qs = ExDoc.objects.all()
        assert len(qs) == 2

    def test_bool_true(self):
        ExDoc(name="A", age=1).save()
        assert bool(ExDoc.objects.all())

    def test_bool_false(self):
        assert not bool(ExDoc.objects.all())

    def test_repr(self):
        ExDoc(name="A", age=1).save()
        r = repr(ExDoc.objects.all())
        assert "QuerySet" in r

    def test_index_access(self):
        ExDoc(name="Alice", age=30).save()
        doc = ExDoc.objects.all()[0]
        assert doc.name == "Alice"

    def test_index_out_of_range(self):
        with pytest.raises(IndexError):
            ExDoc.objects.all()[0]

    def test_iterator(self):
        ExDoc(name="A", age=1).save()
        ExDoc(name="B", age=2).save()
        results = list(ExDoc.objects.iterator())
        assert len(results) == 2

    def test_raw(self):
        ExDoc(name="A", age=1).save()
        results = ExDoc.objects.raw(
            "SELECT META().id AS __id, d.* FROM `testbucket`.`_default`.`exdocs` d WHERE d._type = 'exdoc'"
        )
        assert len(results) >= 1

    def test_update_returns_integer(self):
        """Regression test: _update() must return an int, not a cursor object."""
        ExDoc(name="old", age=1).save()
        ExDoc(name="old", age=2).save()
        ExDoc(name="other", age=3).save()

        count = ExDoc.objects.filter(name="old").update(name="new")
        assert isinstance(count, int), f"update() returned {type(count)}, expected int"
        assert count == 2

        assert ExDoc.objects.filter(name="new").count() == 2
        assert ExDoc.objects.filter(name="old").count() == 0

    def test_update_returns_zero_for_no_match(self):
        count = ExDoc.objects.filter(name="nonexistent").update(name="new")
        assert isinstance(count, int)
        assert count == 0

    def test_update_empty_kwargs(self):
        assert ExDoc.objects.filter(name="x").update() == 0

    def test_delete_bulk(self):
        ExDoc(name="del1", age=1).save()
        ExDoc(name="del2", age=2).save()
        ExDoc(name="keep", age=3).save()

        count = ExDoc.objects.filter(name__in=["del1", "del2"]).delete()
        assert isinstance(count, int)
        assert count == 2
        assert ExDoc.objects.count() == 1

    def test_aggregate(self):
        ExDoc(name="A", age=20, score=1.0).save()
        ExDoc(name="B", age=30, score=2.0).save()
        ExDoc(name="C", age=40, score=3.0).save()

        from django_couchbase_orm.aggregates import Avg, Count

        result = ExDoc.objects.all().aggregate(avg_age=Avg("age"), total=Count("*"))
        assert result["avg_age"] == 30.0
        assert result["total"] == 3

    def test_aggregate_empty(self):
        from django_couchbase_orm.aggregates import Avg

        result = ExDoc.objects.all().aggregate(avg_age=Avg("age"))
        assert result["avg_age"] is None
