"""Couchbase document models for the beer-sample bucket."""

from django_cb import Document, StringField, FloatField, IntegerField


class Brewery(Document):
    name = StringField(required=True)
    description = StringField()
    address = StringField()
    city = StringField()
    state = StringField()
    code = StringField()
    country = StringField()
    phone = StringField()
    website = StringField()
    updated = StringField()

    class Meta:
        collection_name = "_default"
        doc_type_field = "type"


class Beer(Document):
    name = StringField(required=True)
    description = StringField()
    abv = FloatField()
    ibu = FloatField()
    srm = FloatField()
    upc = IntegerField()
    brewery_id = StringField()
    category = StringField()
    style = StringField()
    updated = StringField()

    class Meta:
        collection_name = "_default"
        doc_type_field = "type"
