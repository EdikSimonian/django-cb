# Initial migration for the beer-sample app.
# Demonstrates django-couchbase-orm's migration framework.

from django_couchbase_orm.migrations import Migration as BaseMigration
from django_couchbase_orm.migrations.operations import CreateIndex


class Migration(BaseMigration):
    app_label = 'beers'
    name = '0001_initial'

    dependencies = []

    operations = [
        CreateIndex(
            index_name='idx_brewery_name',
            fields=['name'],
            where='`type` = "brewery"',
        ),
        CreateIndex(
            index_name='idx_beer_name',
            fields=['name'],
            where='`type` = "beer"',
        ),
        CreateIndex(
            index_name='idx_beer_brewery_id',
            fields=['brewery_id'],
            where='`type` = "beer"',
        ),
        CreateIndex(
            index_name='idx_beer_style',
            fields=['style'],
            where='`type` = "beer"',
        ),
        CreateIndex(
            index_name='idx_beer_abv',
            fields=['abv'],
            where='`type` = "beer"',
        ),
    ]
