"""Management command to ensure Couchbase scopes and collections exist for all registered Documents."""

from django.core.management.base import BaseCommand

from django_couchbase_orm.connection import get_bucket
from django_couchbase_orm.document import get_document_registry


class Command(BaseCommand):
    help = "Create Couchbase scopes and collections for all registered Document classes"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Print what would be created without executing.",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        registry = get_document_registry()

        if not registry:
            self.stdout.write(self.style.WARNING("No Document classes found in registry."))
            return

        created_scopes = set()
        created_collections = set()

        for name, doc_class in sorted(registry.items()):
            meta = doc_class._meta
            scope_name = meta.scope_name
            collection_name = meta.collection_name
            bucket_alias = meta.bucket_alias

            key = (bucket_alias, scope_name, collection_name)
            if key in created_collections:
                continue

            bucket = get_bucket(bucket_alias)
            cm = bucket.collections()

            # Ensure scope exists
            scope_key = (bucket_alias, scope_name)
            if scope_name != "_default" and scope_key not in created_scopes:
                if dry_run:
                    self.stdout.write(f"  [DRY RUN] Create scope '{scope_name}'")
                else:
                    try:
                        cm.create_scope(scope_name)
                        self.stdout.write(self.style.SUCCESS(f"  Created scope '{scope_name}'"))
                    except Exception as e:
                        if "already exists" in str(e).lower():
                            self.stdout.write(f"  Scope '{scope_name}' already exists")
                        else:
                            raise
                created_scopes.add(scope_key)

            # Ensure collection exists
            if collection_name != "_default":
                if dry_run:
                    self.stdout.write(f"  [DRY RUN] Create collection '{scope_name}.{collection_name}'")
                else:
                    try:
                        from couchbase.management.collections import CollectionSpec

                        cm.create_collection(CollectionSpec(collection_name, scope_name))
                        self.stdout.write(self.style.SUCCESS(f"  Created collection '{scope_name}.{collection_name}'"))
                    except Exception as e:
                        if "already exists" in str(e).lower():
                            self.stdout.write(f"  Collection '{scope_name}.{collection_name}' already exists")
                        else:
                            raise

            created_collections.add(key)

        if dry_run:
            self.stdout.write(self.style.WARNING("\nDry run — nothing created."))
        else:
            self.stdout.write(self.style.SUCCESS("\nDone."))
