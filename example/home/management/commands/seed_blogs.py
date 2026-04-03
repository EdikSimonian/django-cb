"""Seed funny blog posts under the Blog page."""
import datetime
from django.core.management.base import BaseCommand
from wagtail.models import Page
from home.models import BlogPage


POSTS = [
    {
        "title": "I Tried Every IPA So You Don't Have To (You're Welcome)",
        "slug": "tried-every-ipa",
        "date": datetime.date(2026, 3, 28),
        "intro": "47 IPAs. 3 days. 1 very confused liver.",
        "body": (
            "<p>It started innocently enough. 'I'll just do a quick IPA comparison,' I told myself, "
            "standing in the beer aisle like a man with a plan. Three days later, I woke up on my couch "
            "wearing a shirt that said 'HOPS ARE A VEGETABLE' that I definitely did not own before.</p>"
            "<p>Here's what I learned: after IPA number 12, they all taste like a pine tree punched you "
            "in the mouth. After number 25, you start arguing with your dog about terroir. After number 40, "
            "you achieve a state of hoppy enlightenment where bitterness is just a social construct.</p>"
            "<p>My top pick? Honestly, whichever one is closest to you right now. They're all good. "
            "I'm going to go lie down for a week.</p>"
        ),
    },
    {
        "title": "The Homebrew That Gained Sentience",
        "slug": "homebrew-gained-sentience",
        "date": datetime.date(2026, 3, 30),
        "intro": "When your garage experiment starts making demands.",
        "body": (
            "<p>Look, I'm not saying my latest homebrew is alive. But it did knock over the airlock "
            "at 3 AM, and when I checked on it, the yeast had arranged itself into what I can only "
            "describe as a tiny middle finger.</p>"
            "<p>By day three, the fermenter was vibrating. By day five, it had developed what I assume "
            "is a personality — angry, with hints of citrus. My wife said it growled at her when she "
            "walked past. I told her that's just off-gassing. She moved to her mother's.</p>"
            "<p>I finally bottled it last Tuesday. Each bottle cap popped itself back off within "
            "minutes. I've sealed them with duct tape and holy water. The garage now smells like a "
            "Belgian abbey that's also haunted.</p>"
            "<p>Tasting notes: surprisingly smooth, with a finish that whispers 'release me' if you "
            "listen closely. 4 out of 5 stars.</p>"
        ),
    },
    {
        "title": "A Field Guide to People Who Say 'Actually' at Breweries",
        "slug": "actually-people-breweries",
        "date": datetime.date(2026, 4, 1),
        "intro": "Identifying and surviving the most dangerous predator in craft beer.",
        "body": (
            "<p>You know the type. You're standing at the bar, peacefully enjoying a perfectly fine "
            "lager, when someone materializes beside you like a ghost made of beard oil and opinions.</p>"
            "<p><strong>Species 1: The Hop Scholar.</strong> 'Actually, this isn't a West Coast IPA, "
            "it's more of a Pacific Northwest Interpretive Pale Ale.' They can name 47 hop varieties "
            "but not their own children.</p>"
            "<p><strong>Species 2: The Temperature Truther.</strong> 'Actually, you should let that "
            "warm up to exactly 48.7 degrees.' They brought their own thermometer. It has a name.</p>"
            "<p><strong>Species 3: The Glassware Guru.</strong> 'Actually, a tulip glass would unlock "
            "the aromatics.' Sir, I'm drinking from a plastic cup at a music festival. Please leave me alone.</p>"
            "<p><strong>Survival tips:</strong> Maintain eye contact. Nod slowly. Say 'oh wow, I had no idea' "
            "in a flat tone. They'll either feel validated and leave, or adopt you as their protégé. "
            "Either way, you're going to need another beer.</p>"
        ),
    },
    {
        "title": "My Dog Rates Local Breweries (He Has Strong Opinions)",
        "slug": "dog-rates-breweries",
        "date": datetime.date(2026, 4, 3),
        "intro": "Bark Brewster reviews the city's finest establishments.",
        "body": (
            "<p>I take my golden retriever, Captain Barley, to every dog-friendly brewery in town. "
            "He has developed a rating system that I believe is more honest than any beer blog.</p>"
            "<p><strong>Hop Valley Brewing — 5/5 paws.</strong> Gave him a free biscuit AND let him "
            "behind the bar. He tried to eat a bag of grain. They called him 'employee of the month.' "
            "He has never been happier.</p>"
            "<p><strong>Ironside Taproom — 2/5 paws.</strong> No water bowl. Captain Barley stared "
            "at the bartender for 11 straight minutes until one appeared. He then refused to drink from it "
            "on principle. We left in protest.</p>"
            "<p><strong>The Rusty Keg — 4/5 paws.</strong> Another dog was there. Captain Barley "
            "spent the entire visit trying to assert dominance by sitting on increasingly tall things. "
            "He ended up on a barstool looking deeply uncomfortable but refusing to get down.</p>"
            "<p><strong>Golden Grain Co. — 3/5 paws.</strong> Great patio, but a child tried to "
            "ride him. He allowed it briefly, then walked away with the quiet dignity of a creature "
            "who knows his worth. Points deducted for tiny humans.</p>"
        ),
    },
]


class Command(BaseCommand):
    help = "Seed 4 more funny blog posts under the Blog page"

    def handle(self, *args, **options):
        blog = Page.objects.filter(slug="blog").first()
        if not blog:
            self.stderr.write("No 'blog' page found. Create one in Wagtail admin first.")
            return

        for post_data in POSTS:
            if BlogPage.objects.filter(slug=post_data["slug"]).exists():
                self.stdout.write(f"  Skipping '{post_data['title']}' (already exists)")
                continue

            page = BlogPage(
                title=post_data["title"],
                slug=post_data["slug"],
                date=post_data["date"],
                intro=post_data["intro"],
                body=post_data["body"],
            )
            blog.add_child(instance=page)
            page.save_revision().publish()
            self.stdout.write(f"  Created: {post_data['title']}")

        self.stdout.write(self.style.SUCCESS("Done! 4 new blog posts added."))
