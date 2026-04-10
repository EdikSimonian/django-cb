"""Wagtail CRUD integration tests — create, publish, update, unpublish, delete.

Tests the full Wagtail page lifecycle against Couchbase, including admin
form submissions with DraftJS body, slug changes (SUBSTR), revisions,
site history logging, and page explorer.

Requires a running Couchbase instance with Wagtail migrations applied.
"""

import json
import uuid

import pytest

from tests.conftest import couchbase_available

# Skip the entire module if Wagtail is not installed.
wagtail = pytest.importorskip("wagtail")

pytestmark = [
    pytest.mark.wagtail,
    pytest.mark.skipif(not couchbase_available, reason="Local Couchbase not available"),
    pytest.mark.django_db(transaction=True),
]


def _draftjs(text):
    """Generate DraftJS contentstate JSON for a RichTextField."""
    return json.dumps({
        "blocks": [{
            "key": uuid.uuid4().hex[:5],
            "text": text,
            "type": "unstyled",
            "depth": 0,
            "inlineStyleRanges": [],
            "entityRanges": [],
            "data": {},
        }],
        "entityMap": {},
    })


@pytest.fixture()
def admin_user():
    from django.contrib.auth.models import User

    user, _ = User.objects.get_or_create(
        username="wagtail_crud_admin",
        defaults={"is_superuser": True, "is_staff": True, "email": "wt@test.com"},
    )
    user.set_password("testpass")
    user.save()
    return user


@pytest.fixture()
def admin_client(admin_user):
    from django.test import Client

    client = Client()
    client.login(username="wagtail_crud_admin", password="testpass")
    return client


@pytest.fixture()
def parent_page():
    from wagtail.models import Locale, Page, Site

    # Ensure locale exists (Page requires it)
    if not Locale.objects.exists():
        Locale.objects.create(language_code="en")

    # Ensure root page exists
    root = Page.objects.filter(depth=1).first()
    if root is None:
        root = Page.add_root(title="Root", slug="root")

    parent = Page.objects.filter(depth=2).first()
    if parent is None:
        parent = root.add_child(instance=Page(title="Home", slug="home"))

    # Ensure default site exists
    if not Site.objects.exists():
        Site.objects.create(
            hostname="localhost",
            root_page=parent,
            is_default_site=True,
        )

    return parent


class TestPageCreate:
    """Test page creation via API and admin."""

    def test_create_via_add_child(self, admin_user, parent_page):
        from tests.wagtailapp.models import SimplePage

        page = SimplePage(
            title=f"API Create {uuid.uuid4().hex[:6]}",
            slug=f"api-create-{uuid.uuid4().hex[:6]}",
            body="<p>Created via API</p>",
        )
        parent_page.add_child(instance=page)
        assert page.pk is not None
        assert page.depth == parent_page.depth + 1
        page.delete()

    def test_create_and_publish(self, admin_user, parent_page):
        from tests.wagtailapp.models import SimplePage

        page = SimplePage(
            title=f"Pub Create {uuid.uuid4().hex[:6]}",
            slug=f"pub-create-{uuid.uuid4().hex[:6]}",
            body="<p>Published</p>",
        )
        parent_page.add_child(instance=page)
        page.save_revision(user=admin_user).publish(user=admin_user)
        page.refresh_from_db()
        assert page.live
        assert page.first_published_at is not None
        page.delete()

    def test_create_via_admin(self, admin_client, parent_page):
        from tests.wagtailapp.models import SimplePage

        slug = f"admin-create-{uuid.uuid4().hex[:6]}"
        response = admin_client.post(
            f"/admin/pages/add/wagtailapp/simplepage/{parent_page.pk}/",
            {
                "title": f"Admin Create {slug}",
                "slug": slug,
                "body": _draftjs("Created via admin"),
                "action-publish": "action-publish",
            },
            follow=True,
        )
        assert response.status_code == 200
        page = SimplePage.objects.filter(slug=slug).first()
        assert page is not None
        assert page.live
        page.delete()


class TestPageUpdate:
    """Test page updates via API and admin, including slug changes."""

    def _create_page(self, admin_user, parent_page):
        from tests.wagtailapp.models import SimplePage

        page = SimplePage(
            title=f"Update Test {uuid.uuid4().hex[:6]}",
            slug=f"update-{uuid.uuid4().hex[:6]}",
            body="<p>Original</p>",
        )
        parent_page.add_child(instance=page)
        page.save_revision(user=admin_user).publish(user=admin_user)
        page.refresh_from_db()
        return page

    def test_update_via_api(self, admin_user, parent_page):
        page = self._create_page(admin_user, parent_page)
        page.body = "<p>Updated via API</p>"
        page.title = "Updated Title"
        page.save_revision(user=admin_user).publish(user=admin_user)
        page.refresh_from_db()
        assert page.title == "Updated Title"
        page.delete()

    def test_update_via_admin(self, admin_client, admin_user, parent_page):
        page = self._create_page(admin_user, parent_page)
        response = admin_client.post(
            f"/admin/pages/{page.pk}/edit/",
            {
                "title": page.title + " v2",
                "slug": page.slug,
                "body": _draftjs("Updated via admin"),
                "action-publish": "action-publish",
            },
            follow=True,
        )
        assert response.status_code == 200
        page.refresh_from_db()
        assert "v2" in page.title
        page.delete()

    def test_slug_change_updates_url_path(self, admin_client, admin_user, parent_page):
        """Slug change triggers SUBSTR in N1QL for descendant URL updates."""
        page = self._create_page(admin_user, parent_page)
        new_slug = f"new-slug-{uuid.uuid4().hex[:6]}"
        response = admin_client.post(
            f"/admin/pages/{page.pk}/edit/",
            {
                "title": page.title,
                "slug": new_slug,
                "body": _draftjs("Slug changed"),
                "action-publish": "action-publish",
            },
            follow=True,
        )
        assert response.status_code == 200
        page.refresh_from_db()
        assert page.slug == new_slug
        assert new_slug in page.url_path
        page.delete()


class TestPagePublishUnpublish:
    """Test publish and unpublish lifecycle."""

    def _create_published(self, admin_user, parent_page):
        from tests.wagtailapp.models import SimplePage

        page = SimplePage(
            title=f"PubUnpub {uuid.uuid4().hex[:6]}",
            slug=f"pubunpub-{uuid.uuid4().hex[:6]}",
            body="<p>test</p>",
        )
        parent_page.add_child(instance=page)
        page.save_revision(user=admin_user).publish(user=admin_user)
        page.refresh_from_db()
        return page

    def test_unpublish(self, admin_user, parent_page):
        page = self._create_published(admin_user, parent_page)
        page.unpublish(user=admin_user)
        page.refresh_from_db()
        assert not page.live
        page.delete()

    def test_republish(self, admin_user, parent_page):
        page = self._create_published(admin_user, parent_page)
        page.unpublish(user=admin_user)
        page.save_revision(user=admin_user).publish(user=admin_user)
        page.refresh_from_db()
        assert page.live
        page.delete()

    def test_frontend_renders_published(self, admin_client, admin_user, parent_page):
        page = self._create_published(admin_user, parent_page)
        response = admin_client.get(page.url)
        assert response.status_code == 200
        page.delete()

    def test_frontend_404_unpublished(self, admin_client, admin_user, parent_page):
        page = self._create_published(admin_user, parent_page)
        page.unpublish(user=admin_user)
        response = admin_client.get(page.url)
        assert response.status_code in (302, 404)  # Redirect to login or 404
        page.delete()


class TestRevisions:
    """Test that revisions are tracked."""

    def test_revisions_created(self, admin_user, parent_page):
        from tests.wagtailapp.models import SimplePage
        from wagtail.models import Revision

        page = SimplePage(
            title=f"Rev Test {uuid.uuid4().hex[:6]}",
            slug=f"rev-{uuid.uuid4().hex[:6]}",
            body="<p>v1</p>",
        )
        parent_page.add_child(instance=page)
        page.save_revision(user=admin_user)
        page.body = "<p>v2</p>"
        page.save_revision(user=admin_user)
        page.body = "<p>v3</p>"
        page.save_revision(user=admin_user).publish(user=admin_user)

        revisions = Revision.objects.filter(object_id=str(page.pk))
        assert revisions.count() >= 3
        page.delete()


class TestSiteHistory:
    """Test that PageLogEntry tracks changes."""

    def test_publish_logged(self, admin_user, parent_page):
        from tests.wagtailapp.models import SimplePage
        from wagtail.models import PageLogEntry

        page = SimplePage(
            title=f"Log Test {uuid.uuid4().hex[:6]}",
            slug=f"log-{uuid.uuid4().hex[:6]}",
            body="<p>test</p>",
        )
        parent_page.add_child(instance=page)
        page.save_revision(user=admin_user).publish(user=admin_user)

        logs = PageLogEntry.objects.filter(page_id=page.pk)
        actions = set(log.action for log in logs)
        assert "wagtail.publish" in actions
        page.delete()

    def test_unpublish_logged(self, admin_user, parent_page):
        from tests.wagtailapp.models import SimplePage
        from wagtail.models import PageLogEntry

        page = SimplePage(
            title=f"Unpub Log {uuid.uuid4().hex[:6]}",
            slug=f"unpublog-{uuid.uuid4().hex[:6]}",
            body="<p>test</p>",
        )
        parent_page.add_child(instance=page)
        page.save_revision(user=admin_user).publish(user=admin_user)
        page.unpublish(user=admin_user)

        logs = PageLogEntry.objects.filter(page_id=page.pk)
        actions = set(log.action for log in logs)
        assert "wagtail.unpublish" in actions
        page.delete()

    def test_rename_logged(self, admin_client, admin_user, parent_page):
        from tests.wagtailapp.models import SimplePage
        from wagtail.models import PageLogEntry

        page = SimplePage(
            title=f"Rename Log {uuid.uuid4().hex[:6]}",
            slug=f"renamelog-{uuid.uuid4().hex[:6]}",
            body="<p>test</p>",
        )
        parent_page.add_child(instance=page)
        page.save_revision(user=admin_user).publish(user=admin_user)

        # Rename via admin
        admin_client.post(
            f"/admin/pages/{page.pk}/edit/",
            {
                "title": "Renamed Page",
                "slug": page.slug,
                "body": _draftjs("renamed"),
                "action-publish": "action-publish",
            },
            follow=True,
        )

        logs = PageLogEntry.objects.filter(page_id=page.pk)
        actions = set(log.action for log in logs)
        assert "wagtail.rename" in actions
        page.refresh_from_db()
        page.delete()


class TestPageDelete:
    """Test page deletion."""

    def test_delete_via_api(self, admin_user, parent_page):
        from tests.wagtailapp.models import SimplePage
        from wagtail.models import Page

        page = SimplePage(
            title=f"Del Test {uuid.uuid4().hex[:6]}",
            slug=f"del-{uuid.uuid4().hex[:6]}",
            body="<p>delete me</p>",
        )
        parent_page.add_child(instance=page)
        pk = page.pk
        page.delete()
        assert not Page.objects.filter(pk=pk).exists()

    def test_delete_published_page(self, admin_user, parent_page):
        from tests.wagtailapp.models import SimplePage
        from wagtail.models import Page

        page = SimplePage(
            title=f"Del Pub {uuid.uuid4().hex[:6]}",
            slug=f"delpub-{uuid.uuid4().hex[:6]}",
            body="<p>published then deleted</p>",
        )
        parent_page.add_child(instance=page)
        page.save_revision(user=admin_user).publish(user=admin_user)
        pk = page.pk
        page.delete()
        assert not Page.objects.filter(pk=pk).exists()


class TestPageTree:
    """Test page tree operations."""

    def test_children(self, parent_page):
        from wagtail.models import Page

        root = Page.objects.get(depth=1)
        assert root.get_children().count() >= 1

    def test_ancestors(self, parent_page):
        assert parent_page.get_ancestors().count() >= 1

    def test_specific_pages(self, parent_page):
        from wagtail.models import Page

        pages = list(Page.objects.filter(depth__gte=1).specific())
        assert len(pages) >= 1


class TestAdminPages:
    """Test admin pages load without errors."""

    def test_dashboard(self, admin_client, parent_page):
        r = admin_client.get("/admin/", follow=True)
        assert r.status_code == 200

    def test_page_explorer_root(self, admin_client, parent_page):
        from wagtail.models import Page

        root = Page.objects.filter(depth=1).first()
        r = admin_client.get(f"/admin/pages/{root.pk}/", follow=True)
        assert r.status_code == 200

    def test_page_explorer_welcome(self, admin_client, parent_page):
        assert admin_client.get(f"/admin/pages/{parent_page.pk}/", follow=True).status_code == 200

    def test_images(self, admin_client):
        assert admin_client.get("/admin/images/", follow=True).status_code == 200

    def test_documents(self, admin_client):
        assert admin_client.get("/admin/documents/", follow=True).status_code == 200

    def test_site_history(self, admin_client, parent_page):
        r = admin_client.get("/admin/reports/site-history/", follow=True)
        assert r.status_code == 200

    def test_aging_pages(self, admin_client):
        assert admin_client.get("/admin/reports/aging-pages/", follow=True).status_code == 200

    def test_edit_page(self, admin_client, parent_page):
        assert admin_client.get(f"/admin/pages/{parent_page.pk}/edit/", follow=True).status_code == 200

    def test_add_page_form(self, admin_client, parent_page):
        r = admin_client.get(f"/admin/pages/add/wagtailapp/simplepage/{parent_page.pk}/")
        assert r.status_code == 200
