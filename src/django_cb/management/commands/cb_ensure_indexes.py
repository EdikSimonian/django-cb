"""Management command to create N1QL indexes declared on Document classes."""

from django.core.management.base import BaseCommand

from django_cb.connection import get_cluster
from django_cb.document import get_document_registry


class Command(BaseCommand):
    help = "Create N1QL indexes declared in Document Meta.indexes"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Print the index statements without executing them.",
        )
        parser.add_argument(
            "--primary",
            action="store_true",
            help="Also create a primary index on each collection (required for ad-hoc queries).",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        create_primary = options["primary"]
        registry = get_document_registry()

        if not registry:
            self.stdout.write(self.style.WARNING("No Document classes found in registry."))
            return

        from django_cb.connection import _get_config

        created = 0
        for name, doc_class in sorted(registry.items()):
            meta = doc_class._meta
            config = _get_config(meta.bucket_alias)
            bucket = config["BUCKET"]
            scope = meta.scope_name
            collection = meta.collection_name
            keyspace = f"`{bucket}`.`{scope}`.`{collection}`"

            # Primary index
            if create_primary:
                stmt = f"CREATE PRIMARY INDEX IF NOT EXISTS ON {keyspace}"
                if dry_run:
                    self.stdout.write(f"  [DRY RUN] {stmt}")
                else:
                    self._execute(stmt, meta.bucket_alias)
                    created += 1
                    self.stdout.write(self.style.SUCCESS(f"  Primary index on {keyspace}"))

            # Secondary indexes from Meta.indexes
            for idx in meta.indexes:
                idx_name = idx.get("name", f"idx_{collection}_{'_'.join(idx['fields'])}")
                fields = ", ".join(f"d.`{f}`" for f in idx["fields"])
                where = ""
                if "where" in idx:
                    where = f" WHERE {idx['where']}"

                stmt = f"CREATE INDEX `{idx_name}` IF NOT EXISTS ON {keyspace}({fields}){where}"
                if dry_run:
                    self.stdout.write(f"  [DRY RUN] {stmt}")
                else:
                    self._execute(stmt, meta.bucket_alias)
                    created += 1
                    self.stdout.write(self.style.SUCCESS(f"  Index `{idx_name}` on {keyspace}"))

        if dry_run:
            self.stdout.write(self.style.WARNING("Dry run — no indexes created."))
        else:
            self.stdout.write(self.style.SUCCESS(f"\nDone. {created} index(es) created/verified."))

    def _execute(self, statement, alias):
        cluster = get_cluster(alias)
        cluster.query(statement).execute()
