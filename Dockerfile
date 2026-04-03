FROM python:3.13-slim

WORKDIR /app

# Install system dependencies for Couchbase SDK
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake libssl-dev ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies first (cached unless requirements change)
COPY pyproject.toml ./
COPY src/django_couchbase_orm/__init__.py src/django_couchbase_orm/__init__.py
RUN pip install --no-cache-dir -e . 2>/dev/null || true

COPY example/requirements.txt example/requirements.txt
RUN pip install --no-cache-dir -r example/requirements.txt

# Now copy the rest (only this layer rebuilds on code changes)
COPY . .
RUN pip install --no-cache-dir -e .

# Collect static files
RUN cd example && python manage.py collectstatic --noinput 2>/dev/null || true

WORKDIR /app/example

EXPOSE ${PORT:-8000}

CMD ["bash", "start.sh"]
