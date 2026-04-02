"""Comprehensive tests for Document base class, metaclass, and CRUD operations."""

import uuid
from unittest.mock import MagicMock, patch

import pytest

from django_cb.document import Document, _document_registry, get_document_registry
from django_cb.exceptions import (
    DocumentDoesNotExist,
    MultipleDocumentsReturned,
    OperationError,
    ValidationError,
)
from django_cb.fields.simple import BooleanField, FloatField, IntegerField, StringField, UUIDField


# ============================================================
# Document definition tests
# ============================================================


class TestDocumentMetaclass:
    def test_basic_document_definition(self):
        class User(Document):
            name = StringField(required=True)
            age = IntegerField()

        assert "name" in User._meta.fields
        assert "age" in User._meta.fields
        assert User._meta.collection_name == "user"
        assert User._meta.doc_type_value == "user"

    def test_meta_collection_name(self):
        class Product(Document):
            title = StringField()

            class Meta:
                collection_name = "products"

        assert Product._meta.collection_name == "products"

    def test_meta_scope_and_bucket(self):
        class Order(Document):
            total = FloatField()

            class Meta:
                scope_name = "commerce"
                bucket_alias = "shop_db"

        assert Order._meta.scope_name == "commerce"
        assert Order._meta.bucket_alias == "shop_db"

    def test_fields_ordered_by_creation(self):
        class Item(Document):
            z_field = StringField()
            a_field = StringField()

        field_names = list(Item._meta.fields.keys())
        # Should be in declaration order (based on creation counter)
        assert field_names.index("z_field") < field_names.index("a_field")

    def test_does_not_exist_exception(self):
        class Cat(Document):
            name = StringField()

        assert hasattr(Cat, "DoesNotExist")
        assert issubclass(Cat.DoesNotExist, DocumentDoesNotExist)

        # Each class gets its own exception type
        class Dog(Document):
            name = StringField()

        assert Cat.DoesNotExist is not Dog.DoesNotExist

    def test_multiple_objects_returned_exception(self):
        class Widget(Document):
            name = StringField()

        assert hasattr(Widget, "MultipleObjectsReturned")
        assert issubclass(Widget.MultipleObjectsReturned, MultipleDocumentsReturned)

    def test_document_registered(self):
        class Gadget(Document):
            name = StringField()

        assert "Gadget" in get_document_registry()
        assert get_document_registry()["Gadget"] is Gadget

    def test_abstract_document_not_registered(self):
        class BaseModel(Document):
            created_by = StringField()

            class Meta:
                abstract = True

        assert "BaseModel" not in get_document_registry()

    def test_inheritance(self):
        class BaseAnimal(Document):
            name = StringField(required=True)
            sound = StringField()

            class Meta:
                abstract = True

        class Cow(BaseAnimal):
            milk_production = FloatField()

        assert "name" in Cow._meta.fields
        assert "sound" in Cow._meta.fields
        assert "milk_production" in Cow._meta.fields
        assert Cow._meta.collection_name == "cow"

    def test_db_field_mapping(self):
        class Profile(Document):
            first_name = StringField(db_field="firstName")
            last_name = StringField(db_field="lastName")

        assert Profile._meta.fields["first_name"].get_db_field() == "firstName"
        assert Profile._meta.fields["last_name"].get_db_field() == "lastName"


# ============================================================
# Document instance tests
# ============================================================


class TestDocumentInstance:
    def test_init_with_kwargs(self):
        class Person(Document):
            name = StringField()
            age = IntegerField()

        p = Person(name="Alice", age=30)
        assert p.name == "Alice"
        assert p.age == 30

    def test_init_with_id(self):
        class Thing(Document):
            value = StringField()

        t = Thing(_id="custom-id", value="test")
        assert t.pk == "custom-id"

    def test_init_auto_generates_id(self):
        class Thing2(Document):
            value = StringField()

        t = Thing2(value="test")
        assert t.pk is not None
        # Should be a valid UUID
        uuid.UUID(t.pk)

    def test_init_default_values(self):
        class Config(Document):
            enabled = BooleanField(default=True)
            retries = IntegerField(default=3)

        c = Config()
        assert c.enabled is True
        assert c.retries == 3

    def test_init_unexpected_kwargs(self):
        class Simple(Document):
            name = StringField()

        with pytest.raises(TypeError, match="Unexpected keyword arguments"):
            Simple(name="ok", bogus="bad")

    def test_attribute_access(self):
        class Item3(Document):
            title = StringField()

        item = Item3(title="hello")
        assert item.title == "hello"
        item.title = "world"
        assert item.title == "world"

    def test_attribute_access_nonexistent(self):
        class Item4(Document):
            title = StringField()

        item = Item4()
        with pytest.raises(AttributeError):
            _ = item.nonexistent

    def test_pk_property(self):
        class Item5(Document):
            title = StringField()

        item = Item5(_id="abc")
        assert item.pk == "abc"
        item.pk = "def"
        assert item.pk == "def"

    def test_is_new_flag(self):
        class Item6(Document):
            title = StringField()

        item = Item6()
        assert item._is_new is True

    def test_repr(self):
        class Item7(Document):
            title = StringField()

        item = Item7(_id="test-id")
        assert "Item7" in repr(item)
        assert "test-id" in repr(item)

    def test_equality(self):
        class Item8(Document):
            title = StringField()

        a = Item8(_id="same-id")
        b = Item8(_id="same-id")
        c = Item8(_id="different-id")
        assert a == b
        assert a != c

    def test_equality_different_types(self):
        class TypeA(Document):
            name = StringField()

        class TypeB(Document):
            name = StringField()

        a = TypeA(_id="id1")
        b = TypeB(_id="id1")
        assert a != b

    def test_equality_non_document(self):
        class Item9(Document):
            name = StringField()

        assert Item9(_id="x") != "x"

    def test_hash(self):
        class Item10(Document):
            name = StringField()

        a = Item10(_id="id1")
        b = Item10(_id="id1")
        assert hash(a) == hash(b)
        # Can be used in sets/dicts
        s = {a, b}
        assert len(s) == 1


# ============================================================
# Serialization
# ============================================================


class TestDocumentSerialization:
    def test_to_dict(self):
        class Article(Document):
            title = StringField(required=True)
            views = IntegerField(default=0)

        a = Article(title="Hello", views=42)
        d = a.to_dict()
        assert d["title"] == "Hello"
        assert d["views"] == 42
        assert d["_type"] == "article"

    def test_to_dict_none_optional_excluded(self):
        class Sparse(Document):
            required_field = StringField(required=True)
            optional_field = StringField()

        s = Sparse(required_field="value")
        d = s.to_dict()
        assert "required_field" in d
        assert "optional_field" not in d  # None optional fields excluded

    def test_to_dict_none_required_included(self):
        """Required fields with None value should still appear as null."""
        class Strict(Document):
            name = StringField(required=True)

        s = Strict()
        d = s.to_dict()
        assert "name" in d
        assert d["name"] is None

    def test_to_dict_db_field_mapping(self):
        class Mapped(Document):
            first_name = StringField(db_field="firstName")

        m = Mapped(first_name="Alice")
        d = m.to_dict()
        assert "firstName" in d
        assert "first_name" not in d

    def test_to_dict_type_discriminator(self):
        class TypedDoc(Document):
            value = StringField()

        d = TypedDoc(value="x").to_dict()
        assert d["_type"] == "typeddoc"

    def test_to_dict_custom_type_field(self):
        class CustomType(Document):
            value = StringField()

            class Meta:
                doc_type_field = "doc_class"

        d = CustomType(value="x").to_dict()
        assert "doc_class" in d
        assert d["doc_class"] == "customtype"

    def test_from_dict(self):
        class Blog(Document):
            title = StringField()
            views = IntegerField()

        data = {"title": "My Post", "views": 100, "_type": "blog"}
        blog = Blog.from_dict("blog::1", data)
        assert blog.pk == "blog::1"
        assert blog.title == "My Post"
        assert blog.views == 100
        assert blog._is_new is False

    def test_from_dict_with_db_field(self):
        class MappedDoc(Document):
            user_name = StringField(db_field="userName")

        data = {"userName": "alice", "_type": "mappeddoc"}
        doc = MappedDoc.from_dict("key1", data)
        assert doc.user_name == "alice"

    def test_from_dict_missing_fields(self):
        class Partial(Document):
            name = StringField()
            age = IntegerField()

        data = {"name": "Bob", "_type": "partial"}
        doc = Partial.from_dict("key1", data)
        assert doc.name == "Bob"
        assert doc.age is None

    def test_roundtrip(self):
        class RoundTrip(Document):
            name = StringField()
            count = IntegerField()
            active = BooleanField()

        original = RoundTrip(name="test", count=42, active=True)
        data = original.to_dict()
        restored = RoundTrip.from_dict(original.pk, data)
        assert restored.name == original.name
        assert restored.count == original.count
        assert restored.active == original.active


# ============================================================
# Validation
# ============================================================


class TestDocumentValidation:
    def test_full_clean_valid(self):
        class ValidDoc(Document):
            name = StringField(required=True)
            age = IntegerField(min_value=0)

        doc = ValidDoc(name="Alice", age=30)
        doc.full_clean()  # should not raise

    def test_full_clean_invalid(self):
        class StrictDoc(Document):
            name = StringField(required=True)
            age = IntegerField(min_value=0)

        doc = StrictDoc(age=-1)  # name missing, age negative
        with pytest.raises(ValidationError) as exc_info:
            doc.full_clean()
        assert "name" in exc_info.value.errors
        assert "age" in exc_info.value.errors

    def test_full_clean_collects_all_errors(self):
        class MultiError(Document):
            a = StringField(required=True)
            b = StringField(required=True)
            c = IntegerField(required=True)

        doc = MultiError()
        with pytest.raises(ValidationError) as exc_info:
            doc.full_clean()
        assert len(exc_info.value.errors) == 3

    def test_custom_clean(self):
        class CustomClean(Document):
            start = IntegerField()
            end = IntegerField()

            def clean(self):
                if self.start is not None and self.end is not None:
                    if self.start >= self.end:
                        raise ValidationError("start must be less than end")

        doc = CustomClean(start=10, end=5)
        with pytest.raises(ValidationError, match="start must be less than end"):
            doc.full_clean()


# ============================================================
# CRUD operations (mocked)
# ============================================================


class TestDocumentCRUD:
    def test_save(self, patch_collection):
        class SaveDoc(Document):
            name = StringField()

        doc = SaveDoc(name="test")
        doc.save()
        assert doc._is_new is False
        assert doc._cas is not None
        assert doc.pk in patch_collection._store
        assert patch_collection._store[doc.pk]["name"] == "test"
        assert patch_collection._store[doc.pk]["_type"] == "savedoc"

    def test_save_validates_by_default(self, patch_collection):
        class RequiredSave(Document):
            name = StringField(required=True)

        doc = RequiredSave()
        with pytest.raises(ValidationError):
            doc.save()
        # Should not have been saved
        assert doc.pk not in patch_collection._store

    def test_save_skip_validation(self, patch_collection):
        class SkipValid(Document):
            name = StringField(required=True)

        doc = SkipValid()
        doc.save(validate=False)
        assert doc.pk in patch_collection._store

    def test_save_updates_existing(self, patch_collection):
        class UpdateDoc(Document):
            name = StringField()

        doc = UpdateDoc(_id="doc1", name="v1")
        doc.save()
        assert patch_collection._store["doc1"]["name"] == "v1"
        doc.name = "v2"
        doc.save()
        assert patch_collection._store["doc1"]["name"] == "v2"

    def test_delete(self, patch_collection):
        class DeleteDoc(Document):
            name = StringField()

        doc = DeleteDoc(_id="del1", name="bye")
        doc.save()
        assert "del1" in patch_collection._store
        doc.delete()
        assert "del1" not in patch_collection._store

    def test_delete_nonexistent(self, patch_collection):
        class DeleteDoc2(Document):
            name = StringField()

        doc = DeleteDoc2(_id="ghost")
        with pytest.raises(OperationError):
            doc.delete()

    def test_reload(self, patch_collection):
        class ReloadDoc(Document):
            name = StringField()

        doc = ReloadDoc(_id="r1", name="original")
        doc.save()
        # Simulate external change
        patch_collection._store["r1"]["name"] = "changed"
        doc.reload()
        assert doc.name == "changed"
        assert doc._is_new is False

    def test_reload_nonexistent(self, patch_collection):
        class ReloadDoc2(Document):
            name = StringField()

        doc = ReloadDoc2(_id="nonexistent")
        with pytest.raises(OperationError):
            doc.reload()
