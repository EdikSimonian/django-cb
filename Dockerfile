FROM python:3.13-slim

WORKDIR /app

# Install system dependencies for Couchbase SDK
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake libssl-dev ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install example requirements first (cached unless requirements.txt changes)
COPY example/requirements.txt example/requirements.txt
RUN pip install --no-cache-dir -r example/requirements.txt

# Copy everything and install the ORM package
COPY . .
RUN pip install --no-cache-dir -e .

# Collect static files
RUN cd example && python manage.py collectstatic --noinput 2>/dev/null || true

WORKDIR /app/example

EXPOSE ${PORT:-8000}

CMD ["bash", "start.sh"]
