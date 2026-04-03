#!/bin/bash
set -e

pip install -r requirements.txt
python manage.py collectstatic --noinput
python manage.py migrate --noinput || true

# Create superuser if DJANGO_SUPERUSER_* env vars are set
if [ -n "$DJANGO_SUPERUSER_USERNAME" ]; then
    python manage.py createsuperuser --noinput 2>/dev/null || true
fi
