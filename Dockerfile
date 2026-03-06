FROM python:3.12-slim

WORKDIR /app

# Install system dependencies for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

# Install Python package
COPY pyproject.toml .
COPY ep_parity/ ep_parity/
RUN pip install --no-cache-dir .

# Config, output, and SQL query directories mounted at runtime
VOLUME ["/app/config", "/app/output", "/app/queries"]

ENTRYPOINT ["python", "-m", "ep_parity"]
