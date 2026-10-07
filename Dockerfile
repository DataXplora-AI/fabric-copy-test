FROM python:3.12-slim-bookworm

# Microsoft ODBC Driver 18 (TDS to the Fabric Warehouse SQL endpoint) + socat/netcat for the
# Satellite Connector tunnel.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl gnupg ca-certificates socat netcat-openbsd \
 && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
      | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
 && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list \
      -o /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 unixodbc \
 && apt-get purge -y gnupg && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY fabric_copy_test.py make_sample_parquet.py entrypoint.sh ./
RUN chmod +x entrypoint.sh \
 && python make_sample_parquet.py /app/sample/sample.parquet

# Default source: the sample baked into the image. Override SOURCE_PARQUET with a path on a
# mounted data store or an http(s) URL (e.g. a COS pre-signed URL) to test your own file.
ENV SOURCE_PARQUET=/app/sample/sample.parquet \
    PYTHONUNBUFFERED=1

# Intentionally no USER: the entrypoint must append to /etc/hosts and bind 443/1433 on loopback.
ENTRYPOINT ["/app/entrypoint.sh"]
