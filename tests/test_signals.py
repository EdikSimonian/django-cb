"""Tests for document lifecycle signals."""

import pytest

from django_couchbase_orm.document import Document
from django_couchbase_orm.fields.simple import StringField, IntegerField
from django_couchbase_orm.signals import pre_save, post_save, pre_delete, post_delete


class SignalDoc(Document):
    name = StringField(required=True)
    counter = IntegerField(default=0)


class TestSignals:
    def test_pre_save_fires(self, patch_collection):
        calls = []

        def handler(sender, instance, created, **kwargs):
            calls.append(("pre_save", sender.__name__, instance.name, created))

        pre_save.connect(handler, sender=SignalDoc)
        try:
            doc = SignalDoc(name="test")
            doc.save()
            assert len(calls) == 1
            assert calls[0] == ("pre_save", "SignalDoc", "test", True)
        finally:
            pre_save.disconnect(handler, sender=SignalDoc)

    def test_post_save_fires(self, patch_collection):
        calls = []

        def handler(sender, instance, created, **kwargs):
            calls.append(("post_save", created))

        post_save.connect(handler, sender=SignalDoc)
        try:
            doc = SignalDoc(name="test")
            doc.save()
            assert len(calls) == 1
            assert calls[0] == ("post_save", True)
        finally:
            post_save.disconnect(handler, sender=SignalDoc)

    def test_save_created_flag(self, patch_collection):
        """First save should have created=True, second should have created=False."""
        calls = []

        def handler(sender, instance, created, **kwargs):
            calls.append(created)

        post_save.connect(handler, sender=SignalDoc)
        try:
            doc = SignalDoc(name="test")
            doc.save()
            doc.save()
            assert calls == [True, False]
        finally:
            post_save.disconnect(handler, sender=SignalDoc)

    def test_pre_delete_fires(self, patch_collection):
        calls = []

        def handler(sender, instance, **kwargs):
            calls.append(("pre_delete", instance.name))

        pre_delete.connect(handler, sender=SignalDoc)
        try:
            doc = SignalDoc(name="goodbye")
            doc.save()
            doc.delete()
            assert len(calls) == 1
            assert calls[0] == ("pre_delete", "goodbye")
        finally:
            pre_delete.disconnect(handler, sender=SignalDoc)

    def test_post_delete_fires(self, patch_collection):
        calls = []

        def handler(sender, instance, **kwargs):
            calls.append(("post_delete", instance.pk))

        post_delete.connect(handler, sender=SignalDoc)
        try:
            doc = SignalDoc(_id="del-1", name="bye")
            doc.save()
            doc.delete()
            assert len(calls) == 1
            assert calls[0] == ("post_delete", "del-1")
        finally:
            post_delete.disconnect(handler, sender=SignalDoc)

    def test_signal_only_for_sender(self, patch_collection):
        """Signals registered for one class shouldn't fire for another."""

        class OtherDoc(Document):
            name = StringField()

        calls = []

        def handler(sender, **kwargs):
            calls.append(sender.__name__)

        post_save.connect(handler, sender=SignalDoc)
        try:
            other = OtherDoc(name="other")
            other.save()
            assert calls == []  # Should not fire for OtherDoc

            sig = SignalDoc(name="sig")
            sig.save()
            assert calls == ["SignalDoc"]
        finally:
            post_save.disconnect(handler, sender=SignalDoc)

    def test_pre_save_can_modify(self, patch_collection):
        """pre_save handler can modify the instance before it's saved."""

        def auto_counter(sender, instance, **kwargs):
            instance._data["counter"] = (instance._data.get("counter") or 0) + 1

        pre_save.connect(auto_counter, sender=SignalDoc)
        try:
            doc = SignalDoc(name="counted")
            doc.save()
            assert doc.counter == 1
            doc.save()
            assert doc.counter == 2
        finally:
            pre_save.disconnect(auto_counter, sender=SignalDoc)

    def test_signal_order(self, patch_collection):
        """pre_save fires before post_save."""
        order = []

        def pre_handler(sender, **kwargs):
            order.append("pre")

        def post_handler(sender, **kwargs):
            order.append("post")

        pre_save.connect(pre_handler, sender=SignalDoc)
        post_save.connect(post_handler, sender=SignalDoc)
        try:
            SignalDoc(name="order").save()
            assert order == ["pre", "post"]
        finally:
            pre_save.disconnect(pre_handler, sender=SignalDoc)
            post_save.disconnect(post_handler, sender=SignalDoc)
