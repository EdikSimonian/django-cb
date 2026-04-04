from django.apps import AppConfig


class HomeConfig(AppConfig):
    default_auto_field = "django_couchbase_orm.db.backends.couchbase.fields.CouchbaseAutoField"
    name = "home"

    def ready(self):
        from wagtail.signals import page_published
        from home.signals import embed_blog_title
        page_published.connect(embed_blog_title)
