"""Minimal Django settings for running tests."""

SECRET_KEY = "test-secret-key-not-for-production"

INSTALLED_APPS = [
    "django_couchbase_orm",
]

COUCHBASE = {
    "default": {
        "CONNECTION_STRING": "couchbase://localhost",
        "USERNAME": "admin",
        "PASSWORD": "password",
        "BUCKET": "test_bucket",
        "SCOPE": "_default",
    }
}
