"""Couchbase authentication backend for Django.

Usage in settings.py:
    AUTHENTICATION_BACKENDS = [
        "django_cb.contrib.auth.backend.CouchbaseAuthBackend",
    ]
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class CouchbaseAuthBackend:
    """Authenticates users against Couchbase-backed User documents.

    Supports authentication by username or email.
    Does NOT implement permissions (has_perm always returns False).
    """

    def authenticate(self, request, username=None, password=None, **kwargs):
        """Authenticate a user by username/email and password.

        Returns the User instance if credentials are valid, None otherwise.
        """
        from django_cb.contrib.auth.models import User

        if username is None or password is None:
            return None

        # Try username first, then email
        user = None
        try:
            user = User.get_by_username(username)
        except User.DoesNotExist:
            try:
                user = User.get_by_email(username)
            except User.DoesNotExist:
                # Run the default password hasher to prevent timing attacks
                User().check_password(password)
                return None

        if user and user.check_password(password) and user.is_active:
            return user

        return None

    def get_user(self, user_id):
        """Retrieve a user by their primary key (document ID)."""
        from django_cb.contrib.auth.models import User

        try:
            return User.objects.get(pk=user_id)
        except User.DoesNotExist:
            return None

    # ---- Permissions stubs (always False — no permission system) ----

    def has_perm(self, user_obj, perm, obj=None):
        return False

    def has_module_perms(self, user_obj, app_label):
        return False
