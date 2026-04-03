"""Create BlogIndexPage and reparent existing BlogPages under it."""
from django.core.management.base import BaseCommand
from wagtail.models import Page
from home.models import BlogIndexPage, BlogPage


class Command(BaseCommand):
    help = "Create BlogIndexPage and move BlogPages under it"

    def handle(self, *args, **options):
        root = Page.objects.filter(depth=2).first()
        if not root:
            self.stderr.write("No root page found")
            return

        # Delete old plain Blog page (HomePage type) if it exists
        old_blog = Page.objects.filter(slug="blog", depth=3).exclude(
            pk__in=BlogIndexPage.objects.values_list("pk", flat=True)
        ).first()

        # Create BlogIndexPage if it doesn't exist
        blog_index = BlogIndexPage.objects.first()
        if not blog_index:
            blog_index = BlogIndexPage(
                title="Blog",
                slug="blog-index",
                intro="<p>Tales from the tap room — craft beer stories, homebrew disasters, and brewery adventures.</p>",
            )
            root.add_child(instance=blog_index)
            blog_index.save_revision().publish()
            self.stdout.write(f"Created BlogIndexPage (pk={blog_index.pk})")
        else:
            self.stdout.write(f"BlogIndexPage already exists (pk={blog_index.pk})")

        # Move all BlogPages that aren't already under the index
        moved = 0
        for post in BlogPage.objects.all():
            if post.get_parent().pk != blog_index.pk:
                post.move(blog_index, pos="last-child")
                moved += 1
                self.stdout.write(f"  Moved: {post.title}")

        # Now safe to delete old blog page
        if old_blog:
            old_blog.delete()
            self.stdout.write(f"Deleted old Blog page (pk={old_blog.pk})")

        # Update slug to /blog/ if the old one is gone
        if blog_index.slug != "blog":
            if not Page.objects.filter(slug="blog", depth=3).exists():
                blog_index.slug = "blog"
                blog_index.save()
                blog_index.save_revision().publish()
                self.stdout.write("Updated slug to 'blog'")

        self.stdout.write(self.style.SUCCESS(
            f"Done! {moved} posts moved. Blog index at: /{blog_index.slug}/"
        ))
