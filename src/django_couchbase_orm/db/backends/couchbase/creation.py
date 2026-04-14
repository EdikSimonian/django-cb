"""Couchbase test database creation and destruction."""

from __future__ import annotations

import logging

from django.db.backends.base.creation import BaseDatabaseCreation

logger = logging.getLogger("django.db.backends.couchbase.creation")


class DatabaseCreation(BaseDatabaseCreation):
    def _get_test_db_name(self):
        """Return the test database (bucket) name.

        For Couchbase, we always reuse the same bucket. TEST NAME can override
        it, but defaults to the configured bucket name.
        """
        test_name = self.connection.settings_dict.get("TEST", {}).get("NAME")
        if test_name:
            return test_name
        return self.connection.settings_dict["NAME"]

    def create_test_db(self, verbosity=1, autoclobber=False, serialize=True, keepdb=False):
        """Create a test database (reuses the existing Couchbase bucket).

        Override the full create_test_db to skip the connection.close() call
        that Django's base implementation does between _create_test_db and
        migrations. Closing the Couchbase SDK cluster while background
        threads (logging_meter, threshold_logging) are still running causes
        a segfault in the C++ layer on macOS.
        """
        from django.conf import settings

        test_db_name = self._get_test_db_name()
        if verbosity >= 1:
            self.log(
                f"Using existing test database for alias {self._get_database_display_str(verbosity, test_db_name)}..."
            )

        self._create_test_db(verbosity, autoclobber, keepdb)

        # Update settings WITHOUT closing the connection.
        settings.DATABASES[self.connection.alias]["NAME"] = test_db_name
        self.connection.settings_dict["NAME"] = test_db_name

        return test_db_name

    def _create_test_db(self, verbosity, autoclobber, keepdb=False):
        """For Couchbase, we reuse the existing bucket.

        Collections are created by the migration framework when migrate runs.
        No need to create a separate database.
        """
        test_db_name = self._get_test_db_name()
        if verbosity >= 1:
            logger.info("Using Couchbase bucket '%s' for testing", test_db_name)
        return test_db_name

    def serialize_db_to_string(self):
        """Skip serialization — Couchbase test isolation uses flush, not rollback."""
        return ""

    def deserialize_db_from_string(self, data):
        """Skip deserialization — Couchbase test isolation uses flush, not rollback."""
        pass

    def destroy_test_db(self, old_database_name=None, verbosity=1, keepdb=False, suffix=None):
        """Destroy test database — skip connection.close() to avoid SDK segfault."""
        from django.conf import settings

        if old_database_name is not None:
            settings.DATABASES[self.connection.alias]["NAME"] = old_database_name
            self.connection.settings_dict["NAME"] = old_database_name

    def _destroy_test_db(self, test_database_name, verbosity):
        """Clean up test data but don't destroy the bucket."""
        if verbosity >= 1:
            logger.info("Cleaning up test data from bucket '%s'", test_database_name)

    def _clone_test_db(self, suffix, verbosity, keepdb=False):
        """Parallel test DBs not supported."""
        raise NotImplementedError("Couchbase backend does not support parallel test databases.")
