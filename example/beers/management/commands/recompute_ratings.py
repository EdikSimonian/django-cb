"""Recompute avg_rating and rating_count on all beers from Rating documents."""
from django.core.management.base import BaseCommand
from django.db import connection


class Command(BaseCommand):
    help = "Recompute avg_rating and rating_count on all beers from ratings"

    def handle(self, *args, **options):
        cursor = connection.cursor()

        # Get avg and count per beer_id using raw N1QL
        cursor.execute(
            'SELECT r.beer_id, AVG(r.score) AS avg, COUNT(*) AS cnt '
            'FROM `beer-sample`.`_default`.`beers_rating` r '
            'WHERE r.doc_type = "rating" AND r.beer_id IS VALUED '
            'GROUP BY r.beer_id'
        )
        rows = cursor.fetchall()

        updated = 0
        for row in rows:
            beer_id = row[0]
            avg_rating = round(row[1] or 0, 1)
            count = row[2] or 0

            cursor.execute(
                'UPDATE `beer-sample`.`_default`.`beers_beer` '
                'SET avg_rating = %s, rating_count = %s '
                'WHERE META().id = %s',
                [avg_rating, count, str(beer_id)],
            )
            updated += 1
            self.stdout.write(f"  Beer {beer_id}: avg={avg_rating}, count={count}")

        self.stdout.write(self.style.SUCCESS(f"Recomputed ratings for {updated} beers."))
