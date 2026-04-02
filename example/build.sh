#!/bin/bash
set -e

# Install the django-cb package from the repo root
pip install -e /app/

# Install example app dependencies
pip install -r /app/example/requirements.txt

# Collect static files
cd /app/example
python manage.py collectstatic --noinput
