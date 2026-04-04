"""Embed title and slug into BlogPage documents for mobile sync.

Wagtail stores page title in wagtailcore_page (base table) and blog
fields in home_blogpage. Mobile app needs title in the blogpage doc
to avoid joining two collections.
"""
from django.db import connection


def embed_blog_title(sender, instance, **kwargs):
    """After a BlogPage is published, copy title+slug into home_blogpage doc."""
    from home.models import BlogPage
    if not isinstance(instance.specific, BlogPage):
        return
    try:
        cursor = connection.cursor()
        cursor.execute(
            "UPDATE `beer-sample`.`_default`.`home_blogpage` "
            "SET title = %s, slug = %s "
            "WHERE page_ptr_id = %s",
            [instance.title, instance.slug, instance.pk],
        )
    except Exception as e:
        print(f"[Signal] Failed to embed blog title: {e}")
