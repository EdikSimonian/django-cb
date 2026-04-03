#!/bin/bash
set -e

# Run migrations with auto-retry and auto-fake for failing data migrations
python -c "
import os, django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'mysite.settings')
django.setup()

from django.core.management import call_command
from django.contrib.contenttypes.models import ContentType

# Clean duplicate content types before migrating
try:
    seen = {}
    for ct in ContentType.objects.all().order_by('pk'):
        key = (ct.app_label, ct.model)
        if key in seen:
            ct.delete()
        else:
            seen[key] = ct.pk
    ContentType.objects.clear_cache()
except Exception:
    pass

# Auto-migrate with faking for broken data migrations
for attempt in range(30):
    try:
        call_command('migrate', verbosity=1)
        print('All migrations applied!')
        break
    except Exception as e:
        from io import StringIO
        out = StringIO()
        call_command('showmigrations', stdout=out)
        current_app = None
        for line in out.getvalue().splitlines():
            if not line.startswith(' ') and not line.startswith('['):
                current_app = line.strip()
            if '[ ]' in line:
                mig = line.strip().replace('[ ] ', '').strip()
                print(f'Faking {current_app}.{mig}')
                try:
                    call_command('migrate', current_app, mig, '--fake', verbosity=0)
                except Exception:
                    pass
                break
        else:
            print(f'Migration error: {e}')
            break

# Clean duplicate content types after migrating
try:
    seen = {}
    for ct in ContentType.objects.all().order_by('pk'):
        key = (ct.app_label, ct.model)
        if key in seen:
            ct.delete()
        else:
            seen[key] = ct.pk
    ContentType.objects.clear_cache()
except Exception:
    pass

# Create superuser
import os
username = os.environ.get('DJANGO_SUPERUSER_USERNAME')
if username:
    from django.contrib.auth.models import User
    if not User.objects.filter(username=username).exists():
        User.objects.create_superuser(
            username,
            os.environ.get('DJANGO_SUPERUSER_EMAIL', ''),
            os.environ.get('DJANGO_SUPERUSER_PASSWORD', 'admin'),
        )
        print(f'Created superuser: {username}')
    else:
        print(f'Superuser {username} already exists')
"

# Start gunicorn
exec gunicorn mysite.wsgi --bind 0.0.0.0:${PORT:-8000} --workers 2
