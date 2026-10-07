#!/usr/bin/env python3
"""
End-to-end test: Parquet file -> Fabric Lakehouse (OneLake) -> Fabric Warehouse table.

Steps
  1. Load the given Parquet file (local path or http(s) URL) and read its schema / row count.
  2. Preflight: resolve + TCP-connect the OneLake and Warehouse SQL endpoints (these go
     through the IBM Satellite Connector when the entrypoint has set up the tunnel).
  3. Upload the file to <lakehouse>/Files/<LAKEHOUSE_TARGET_DIR>/<run-id>/<file>.parquet via the
     OneLake DFS API.
  4. Connect to the Warehouse SQL endpoint (TDS/1433, Entra token auth), create the target
     table from the Parquet schema if it does not exist, and load it (LOAD_METHOD):
       copy_into        COPY INTO from the OneLake URL of the uploaded file.
       lakehouse_table  Write the data as a staging Delta table under the lakehouse's Tables/
                        (OneLake DFS, same path as the upload), then
                        INSERT INTO <target> SELECT ... FROM <lakehouse>.dbo.<staging>.
                        Use this when COPY INTO cannot read OneLake as the SPN (error 13840).
  5. Verify the number of rows added equals the Parquet row count. Exit code 0 = pass.

All configuration comes from environment variables (see .env.example) so it runs unchanged
as an IBM Code Engine job.
"""

from __future__ import annotations

import logging
import os
import socket
import struct
import sys
import tempfile
import io
import json
import re
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.parquet as pq
import pyodbc
from azure.core.credentials import TokenCredential
from azure.identity import AzureCliCredential, ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

log = logging.getLogger("fabric-copy-test")

SQL_SCOPE = "https://database.windows.net/.default"
FABRIC_API_SCOPE = "https://api.fabric.microsoft.com/.default"
# "Access token couldn't be fetched for storage path ..." - raised by COPY INTO when the SPN has no
# Fabric control-plane token yet (or it is still propagating).
COPY_TOKEN_ERROR = "13840"
SQL_COPT_SS_ACCESS_TOKEN = 1256  # msodbcsql pre-connect attribute for Entra access tokens
ODBC_DRIVER = "ODBC Driver 18 for SQL Server"


# --------------------------------------------------------------------------- config


def env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value or ""


def env_bool(name: str, default: bool) -> bool:
    return env(name, str(default)).strip().lower() in ("1", "true", "yes", "y")


@dataclass(frozen=True)
class Config:
    source_parquet: str
    workspace_id: str
    lakehouse_id: str
    onelake_host: str
    copy_source_host: str
    sql_host: str
    sql_port: int
    warehouse: str
    target_table: str
    lakehouse_target_dir: str
    cleanup_lakehouse_file: bool
    connect_timeout: int
    api_host: str
    copy_retries: int
    copy_retry_wait: int
    copy_credential: str
    load_method: str
    operation_timeout: int
    sql_sync_timeout: int

    @classmethod
    def from_env(cls) -> "Config":
        onelake_host = env("FABRIC_ONELAKE_HOST", "onelake.dfs.fabric.microsoft.com")
        return cls(
            source_parquet=env("SOURCE_PARQUET", required=True),
            workspace_id=env("FABRIC_WORKSPACE_ID", required=True),
            lakehouse_id=env("FABRIC_LAKEHOUSE_ID", required=True),
            onelake_host=onelake_host,
            # Host used *inside* the COPY INTO statement. The warehouse engine reads the file
            # server-side within Fabric, so this is normally the global OneLake host even when
            # the client itself talks to a workspace-specific private-link FQDN.
            copy_source_host=env("FABRIC_COPY_SOURCE_HOST", "onelake.dfs.fabric.microsoft.com"),
            sql_host=env("FABRIC_SQL_HOST", required=True),
            sql_port=int(env("FABRIC_SQL_PORT", "1433")),
            warehouse=env("FABRIC_WAREHOUSE_NAME", required=True),
            target_table=env("TARGET_TABLE", "dbo.copy_into_test"),
            lakehouse_target_dir=env("LAKEHOUSE_TARGET_DIR", "copy-into-test").strip("/"),
            cleanup_lakehouse_file=env_bool("CLEANUP_LAKEHOUSE_FILE", False),
            connect_timeout=int(env("CONNECT_TIMEOUT_SECONDS", "30")),
            api_host=env("FABRIC_API_HOST", "api.fabric.microsoft.com"),
            copy_retries=int(env("COPY_RETRIES", "3")),
            copy_retry_wait=int(env("COPY_RETRY_WAIT_SECONDS", "30")),
            # How the warehouse authorizes reading the OneLake file:
            #   entra              - the executing identity (this SPN); the default in Fabric
            #   workspace_identity - CREDENTIAL = (IDENTITY = 'Workspace Identity')
            copy_credential=env("COPY_CREDENTIAL", "entra").strip().lower(),
            load_method=env("LOAD_METHOD", "copy_into").strip().lower(),
            operation_timeout=int(env("FABRIC_OPERATION_TIMEOUT_SECONDS", "600")),
            # The lakehouse SQL analytics endpoint picks up new Delta tables asynchronously.
            sql_sync_timeout=int(env("SQL_ENDPOINT_SYNC_TIMEOUT_SECONDS", "300")),
        )


def build_credential() -> TokenCredential:
    tenant, client, secret = (
        env("AZURE_TENANT_ID"),
        env("AZURE_CLIENT_ID"),
        env("AZURE_CLIENT_SECRET"),
    )
    if tenant and client and secret:
        log.info("Auth: service principal %s (tenant %s)", client, tenant)
        return ClientSecretCredential(tenant, client, secret)
    log.info("Auth: AZURE_* not set, falling back to Azure CLI login (local runs only)")
    return AzureCliCredential()


# --------------------------------------------------------------------------- steps


def fetch_source(source: str, workdir: str) -> str:
    rows = env("GENERATE_ROWS")
    if rows:
        # Volume testing: generate the sample at runtime instead of using SOURCE_PARQUET.
        from make_sample_parquet import write_sample

        local = os.path.join(workdir, "generated.parquet")
        start = time.monotonic()
        write_sample(local, int(rows))
        log.info("Generated %s rows in %.1fs (%.1f MB)", rows, time.monotonic() - start,
                 os.path.getsize(local) / 1e6)
        return local
    if source.startswith(("http://", "https://")):
        local = os.path.join(workdir, "source.parquet")
        log.info("Downloading source parquet from %s", source.split("?")[0])
        urllib.request.urlretrieve(source, local)  # noqa: S310 - URL comes from operator config
        return local
    if not os.path.isfile(source):
        raise SystemExit(f"SOURCE_PARQUET not found: {source}")
    return source


def preflight(host: str, port: int, timeout: int) -> None:
    addrs = sorted({ai[4][0] for ai in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)})
    log.info("Preflight %s:%d resolves to %s", host, port, ", ".join(addrs))
    start = time.monotonic()
    with socket.create_connection((host, port), timeout=timeout):
        pass
    log.info("Preflight %s:%d TCP OK (%.0f ms)", host, port, (time.monotonic() - start) * 1000)


def upload_to_lakehouse(cfg: Config, cred: TokenCredential, local_file: str, run_id: str) -> str:
    """Upload to OneLake and return the path relative to the lakehouse item."""
    file_name = os.path.basename(local_file) if local_file.endswith(".parquet") else "data.parquet"
    rel_path = f"Files/{cfg.lakehouse_target_dir}/{run_id}/{file_name}"

    service = DataLakeServiceClient(
        account_url=f"https://{cfg.onelake_host}",
        credential=cred,
        connection_timeout=cfg.connect_timeout,
    )
    # In OneLake the "filesystem" is the workspace and the first path segment the item.
    fs = service.get_file_system_client(cfg.workspace_id)
    file_client = fs.get_file_client(f"{cfg.lakehouse_id}/{rel_path}")

    size = os.path.getsize(local_file)
    log.info("Uploading %s (%d bytes) to onelake://%s/%s/%s",
             local_file, size, cfg.workspace_id, cfg.lakehouse_id, rel_path)
    start = time.monotonic()
    with open(local_file, "rb") as fh:
        file_client.upload_data(fh, overwrite=True, length=size)
    remote_size = file_client.get_file_properties().size
    if remote_size != size:
        raise RuntimeError(f"Upload size mismatch: local={size} remote={remote_size}")
    log.info("Upload OK in %.1fs", time.monotonic() - start)
    return rel_path


def delete_from_lakehouse(cfg: Config, cred: TokenCredential, rel_path: str) -> None:
    service = DataLakeServiceClient(account_url=f"https://{cfg.onelake_host}", credential=cred)
    service.get_file_system_client(cfg.workspace_id).get_file_client(
        f"{cfg.lakehouse_id}/{rel_path}"
    ).delete_file()
    log.info("Deleted lakehouse file %s", rel_path)


def fabric_api(cfg: Config, cred: TokenCredential, method: str, path_or_url: str,
               body: dict | None = None) -> tuple[int, dict, dict | None]:
    """Call the Fabric REST API; raises urllib.error.HTTPError on 4xx/5xx."""
    url = path_or_url if path_or_url.startswith("https://") else f"https://{cfg.api_host}{path_or_url}"
    headers = {"Authorization": f"Bearer {cred.get_token(FABRIC_API_SCOPE).token}"}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        resp = urllib.request.urlopen(req, timeout=cfg.connect_timeout)  # noqa: S310
    except urllib.error.HTTPError as exc:
        # Fabric puts the actual reason (errorCode/message) in the body; keep it in the error.
        detail = exc.read().decode(errors="replace")[:2000]
        log.error("Fabric API %s %s -> %s: %s", method, url, exc.code, detail)
        raise urllib.error.HTTPError(exc.url, exc.code, f"{exc.reason}: {detail}", exc.headers,
                                     None) from None
    with resp:
        raw = resp.read()
        log.debug("Fabric API %s %s -> %s", method, url, resp.status)
        return resp.status, dict(resp.headers), (json.loads(raw) if raw else None)


def fabric_api_get(cfg: Config, cred: TokenCredential, path: str) -> dict | None:
    """GET that logs instead of raising; used for diagnostics."""
    url = f"https://{cfg.api_host}{path}"
    try:
        status, _, body = fabric_api(cfg, cred, "GET", url)
        log.info("Fabric API GET %s -> %s", url, status)
        return body or {}
    except urllib.error.HTTPError:
        pass  # already logged with the response body by fabric_api
    except OSError as exc:
        log.warning("Fabric API GET %s not reachable: %s", url, exc)
    return None


def wait_for_operation(cfg: Config, cred: TokenCredential, status: int, headers: dict,
                       what: str) -> None:
    """Follow a Fabric long-running operation (202 + Location) until it finishes."""
    if status != 202:
        return
    url = headers.get("Location")
    if not url:
        raise RuntimeError(f"{what}: 202 without Location header")
    deadline = time.monotonic() + cfg.operation_timeout
    while True:
        time.sleep(min(int(headers.get("Retry-After", 5)), 30))
        _, headers, body = fabric_api(cfg, cred, "GET", url)
        state = (body or {}).get("status")
        log.info("%s: %s", what, state)
        if state == "Succeeded":
            return
        if state in ("Failed", "Cancelled"):
            raise RuntimeError(f"{what} {state}: {json.dumps((body or {}).get('error'))}")
        if time.monotonic() > deadline:
            raise TimeoutError(f"{what} still {state} after {cfg.operation_timeout}s")


def init_fabric_token(cfg: Config, cred: TokenCredential) -> None:
    """Call the Fabric REST API so the SPN gets a Fabric control-plane token.

    Logging in over TDS does not create one, and without it COPY INTO / OPENROWSET cannot read
    OneLake or external storage on the SPN's behalf (error 13840). The token lasts 30 days;
    calling on every run keeps it fresh. Besides the tenant-level workspace list, this makes
    the workspace-scoped call from Microsoft's docs, which with workspace inbound protection
    must be allowed over the network path used for FABRIC_API_HOST.
    See https://learn.microsoft.com/fabric/data-warehouse/service-principals
    """
    workspaces = fabric_api_get(cfg, cred, "/v1/workspaces")
    if workspaces is not None:
        ids = {w.get("id", "").lower() for w in workspaces.get("value", [])}
        if cfg.workspace_id.lower() not in ids and not workspaces.get("continuationToken"):
            log.warning("SPN does not see workspace %s - check workspace access", cfg.workspace_id)

    items = fabric_api_get(cfg, cred, f"/v1/workspaces/{cfg.workspace_id}/items")
    if items is None:
        log.warning("Workspace-scoped Fabric API call failed - COPY INTO will likely fail with "
                    "error %s. With workspace inbound protection, route the workspace API FQDN "
                    "through the connector (FABRIC_API_HOST + CONNECTOR_API_ENDPOINT).",
                    COPY_TOKEN_ERROR)
        return
    kinds = sorted({f"{i.get('type')}:{i.get('displayName')}" for i in items.get("value", [])})
    log.info("SPN sees %d item(s) in the workspace: %s", len(kinds), ", ".join(kinds))


def sql_connect(cfg: Config, cred: TokenCredential) -> pyodbc.Connection:
    token = cred.get_token(SQL_SCOPE).token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token)}s", len(token), token)
    conn_str = (
        f"Driver={{{ODBC_DRIVER}}};"
        f"Server=tcp:{cfg.sql_host},{cfg.sql_port};"
        f"Database={cfg.warehouse};"
        "Encrypt=yes;TrustServerCertificate=no;"
        f"Connection Timeout={cfg.connect_timeout};"
    )
    log.info("Connecting to warehouse %s on %s:%d", cfg.warehouse, cfg.sql_host, cfg.sql_port)
    conn = pyodbc.connect(
        conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct}, autocommit=True
    )
    row = conn.cursor().execute("SELECT @@VERSION, DB_NAME(), SUSER_SNAME()").fetchone()
    log.info("Connected: db=%s user=%s", row[1], row[2])
    return conn


def quote_ident(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def split_table(name: str) -> tuple[str, str]:
    schema, _, table = name.rpartition(".")
    return (schema or "dbo"), table


def arrow_to_fabric_type(t: pa.DataType) -> str:
    """Map an Arrow type to a type supported by Fabric Warehouse (no tinyint/nvarchar/datetime)."""
    if pa.types.is_boolean(t):
        return "bit"
    if pa.types.is_int8(t) or pa.types.is_int16(t) or pa.types.is_uint8(t):
        return "smallint"
    if pa.types.is_int32(t) or pa.types.is_uint16(t):
        return "int"
    if pa.types.is_int64(t) or pa.types.is_uint32(t):
        return "bigint"
    if pa.types.is_uint64(t):
        return "decimal(20,0)"
    if pa.types.is_float16(t) or pa.types.is_float32(t):
        return "real"
    if pa.types.is_float64(t):
        return "float"
    if pa.types.is_decimal(t):
        return f"decimal({min(t.precision, 38)},{t.scale})"
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "varchar(8000)"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t):
        return "varbinary(8000)"
    if pa.types.is_date(t):
        return "date"
    if pa.types.is_timestamp(t):
        return "datetime2(6)"
    if pa.types.is_time(t):
        return "time(6)"
    raise ValueError(f"Unsupported Parquet/Arrow type for Fabric Warehouse: {t}")


def ensure_table(cur: pyodbc.Cursor, table: str, schema: pa.Schema) -> None:
    sch, tbl = split_table(table)
    cols = ",\n  ".join(
        f"{quote_ident(f.name)} {arrow_to_fabric_type(f.type)} NULL" for f in schema
    )
    ddl = (
        f"IF OBJECT_ID(N'{sch}.{tbl}', N'U') IS NULL\n"
        f"CREATE TABLE {quote_ident(sch)}.{quote_ident(tbl)} (\n  {cols}\n);"
    )
    log.info("Ensuring target table:\n%s", ddl)
    cur.execute(ddl)


def row_count(cur: pyodbc.Cursor, table: str) -> int:
    sch, tbl = split_table(table)
    return cur.execute(f"SELECT COUNT_BIG(*) FROM {quote_ident(sch)}.{quote_ident(tbl)}").fetchone()[0]


def copy_into(cur: pyodbc.Cursor, cfg: Config, rel_path: str) -> None:
    sch, tbl = split_table(cfg.target_table)
    source_url = f"https://{cfg.copy_source_host}/{cfg.workspace_id}/{cfg.lakehouse_id}/{rel_path}"
    if cfg.copy_credential == "workspace_identity":
        options = "FILE_TYPE = 'PARQUET',\n  CREDENTIAL = (IDENTITY = 'Workspace Identity')"
    elif cfg.copy_credential == "entra":
        options = "FILE_TYPE = 'PARQUET'"
    else:
        raise SystemExit(f"COPY_CREDENTIAL must be 'entra' or 'workspace_identity', "
                         f"got {cfg.copy_credential!r}")
    sql = (
        f"COPY INTO {quote_ident(sch)}.{quote_ident(tbl)}\n"
        f"FROM '{source_url}'\n"
        f"WITH (\n  {options}\n);"
    )
    log.info("Running:\n%s", sql)
    for attempt in range(1, cfg.copy_retries + 1):
        start = time.monotonic()
        try:
            cur.execute(sql)
        except pyodbc.Error as exc:
            if COPY_TOKEN_ERROR not in str(exc):
                raise
            if attempt == cfg.copy_retries:
                if cfg.copy_credential == "entra":
                    log.error("The warehouse could not get a OneLake token for this SPN. "
                              "Try LOAD_METHOD=lakehouse_table (see README).")
                raise
            log.warning("COPY INTO attempt %d/%d hit error %s (SPN token not ready yet?), "
                        "retrying in %ds", attempt, cfg.copy_retries, COPY_TOKEN_ERROR,
                        cfg.copy_retry_wait)
            time.sleep(cfg.copy_retry_wait)
            continue
        log.info("COPY INTO finished in %.1fs", time.monotonic() - start)
        return


def to_delta_compatible(table: pa.Table) -> tuple[pa.Table, str]:
    """Cast Arrow columns to types Delta/Spark can represent and return the Delta schemaString."""
    columns, fields = [], []
    for field, column in zip(table.schema, table.columns):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", field.name):
            # Other names need Delta column mapping, which this minimal writer doesn't do.
            raise ValueError(f"Column name {field.name!r} not supported for the staging table")
        t = field.type
        if pa.types.is_boolean(t):
            target, spark = t, "boolean"
        elif pa.types.is_int8(t):
            target, spark = t, "byte"
        elif pa.types.is_int16(t) or pa.types.is_uint8(t):
            target, spark = pa.int16(), "short"
        elif pa.types.is_int32(t) or pa.types.is_uint16(t):
            target, spark = pa.int32(), "integer"
        elif pa.types.is_int64(t) or pa.types.is_uint32(t):
            target, spark = pa.int64(), "long"
        elif pa.types.is_uint64(t):
            target, spark = pa.decimal128(20, 0), "decimal(20,0)"
        elif pa.types.is_float16(t) or pa.types.is_float32(t):
            target, spark = pa.float32(), "float"
        elif pa.types.is_float64(t):
            target, spark = t, "double"
        elif pa.types.is_decimal(t):
            target, spark = t, f"decimal({t.precision},{t.scale})"
        elif pa.types.is_string(t) or pa.types.is_large_string(t):
            target, spark = pa.string(), "string"
        elif pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t):
            target, spark = pa.binary(), "binary"
        elif pa.types.is_date(t):
            target, spark = pa.date32(), "date"
        elif pa.types.is_timestamp(t):
            # Delta "timestamp" is UTC-adjusted microseconds; naive values are taken as UTC.
            target, spark = pa.timestamp("us", tz="UTC"), "timestamp"
        else:
            raise ValueError(f"Unsupported type for the staging table: {field.name} {t}")
        columns.append(column.cast(target) if target != t else column)
        fields.append({"name": field.name, "type": spark, "nullable": True, "metadata": {}})
    schema_string = json.dumps({"type": "struct", "fields": fields})
    return pa.table(columns, names=table.column_names), schema_string


def write_staging_delta(cfg: Config, cred: TokenCredential, table: pa.Table,
                        table_dir: str) -> None:
    """Write `table` as a single-file Delta table at <lakehouse>/<table_dir> in OneLake.

    Minimal Delta writer (protocol 1/2, one data file, one commit) so it needs nothing beyond
    the OneLake DFS connection already used for the upload. The data file is written first;
    the table only exists once the _delta_log commit is in place.
    """
    delta_table, schema_string = to_delta_compatible(table)
    buf = io.BytesIO()
    pq.write_table(delta_table, buf, compression="snappy")
    data = buf.getvalue()

    now_ms = int(time.time() * 1000)
    data_name = f"part-00000-{uuid.uuid4()}-c000.snappy.parquet"
    actions = [
        {"commitInfo": {"timestamp": now_ms, "operation": "WRITE",
                        "operationParameters": {"mode": "ErrorIfExists"},
                        "engineInfo": "fabric-copy-test"}},
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {"metaData": {"id": str(uuid.uuid4()), "format": {"provider": "parquet", "options": {}},
                      "schemaString": schema_string, "partitionColumns": [],
                      "configuration": {}, "createdTime": now_ms}},
        {"add": {"path": data_name, "partitionValues": {}, "size": len(data),
                 "modificationTime": now_ms, "dataChange": True,
                 "stats": json.dumps({"numRecords": delta_table.num_rows})}},
    ]
    commit = "\n".join(json.dumps(a) for a in actions).encode()

    service = DataLakeServiceClient(
        account_url=f"https://{cfg.onelake_host}", credential=cred,
        connection_timeout=cfg.connect_timeout,
    )
    fs = service.get_file_system_client(cfg.workspace_id)
    base = f"{cfg.lakehouse_id}/{table_dir}"
    # Create the folders explicitly and write unconditionally, like the Files/ upload: OneLake
    # answers conditional creates under Tables/ with PathNotFound. The staging name is unique
    # per run, so nothing existing gets overwritten.
    fs.get_directory_client(f"{base}/_delta_log").create_directory()
    fs.get_file_client(f"{base}/{data_name}").upload_data(data, overwrite=True)
    fs.get_file_client(f"{base}/_delta_log/00000000000000000000.json").upload_data(
        commit, overwrite=True
    )
    log.info("Wrote Delta table %s (%d rows, %d bytes)", table_dir, delta_table.num_rows, len(data))


def load_via_lakehouse_table(cur: pyodbc.Cursor, cfg: Config, cred: TokenCredential,
                             table: pa.Table, run_id: str) -> str:
    """Data -> lakehouse staging Delta table -> INSERT ... SELECT into the warehouse.

    Everything runs as the SPN over connections it already uses (OneLake DFS writes plus a
    cross-database query), so the warehouse never has to fetch a OneLake token itself, and it
    works for lakehouses with and without schemas. Returns the staging table directory."""
    expected_rows = table.num_rows
    _, _, lakehouse = fabric_api(
        cfg, cred, "GET", f"/v1/workspaces/{cfg.workspace_id}/lakehouses/{cfg.lakehouse_id}"
    )
    lh_name = lakehouse["displayName"]
    log.info("Lakehouse %s properties: %s", lh_name, json.dumps(lakehouse.get("properties")))
    sql_endpoint_id = ((lakehouse.get("properties") or {}).get("sqlEndpointProperties") or {}).get("id")
    schemas_enabled = bool((lakehouse.get("properties") or {}).get("defaultSchema"))
    staging = "stg_" + re.sub(r"[^A-Za-z0-9_]", "_", run_id)
    # Schema-enabled lakehouse: Tables/<schema>/<table>; otherwise Tables/<table> (shows as dbo).
    table_dir = f"Tables/dbo/{staging}" if schemas_enabled else f"Tables/{staging}"

    write_staging_delta(cfg, cred, table, table_dir)

    # Ask the SQL analytics endpoint to pick up the new table now instead of on its own schedule.
    if sql_endpoint_id:
        try:
            status, headers, _ = fabric_api(
                cfg, cred, "POST",
                f"/v1/workspaces/{cfg.workspace_id}/sqlEndpoints/{sql_endpoint_id}/refreshMetadata",
                {},
            )
            wait_for_operation(cfg, cred, status, headers, "SQL endpoint metadata refresh")
        except (urllib.error.HTTPError, RuntimeError, TimeoutError) as exc:
            log.warning("Metadata refresh failed (%s); waiting for the automatic sync", exc)

    source = f"{quote_ident(lh_name)}.[dbo].{quote_ident(staging)}"
    deadline = time.monotonic() + cfg.sql_sync_timeout
    while True:
        try:
            staged = cur.execute(f"SELECT COUNT_BIG(*) FROM {source}").fetchone()[0]
        except pyodbc.ProgrammingError as exc:
            if "42S02" not in str(exc) or time.monotonic() > deadline:  # invalid object name
                raise
            log.info("Waiting for %s to appear in the SQL analytics endpoint...", source)
            time.sleep(10)
            continue
        if staged == expected_rows:
            break
        if time.monotonic() > deadline:
            raise RuntimeError(f"{source} has {staged} rows, expected {expected_rows}")
        time.sleep(10)
    log.info("Staging table %s visible with %d rows", source, staged)

    sch, tbl = split_table(cfg.target_table)
    cols = ", ".join(quote_ident(f.name) for f in table.schema)
    sql = (f"INSERT INTO {quote_ident(sch)}.{quote_ident(tbl)} ({cols})\n"
           f"SELECT {cols} FROM {source};")
    log.info("Running:\n%s", sql)
    start = time.monotonic()
    cur.execute(sql)
    log.info("INSERT ... SELECT finished in %.1fs", time.monotonic() - start)
    return table_dir


def delete_staging_table(cfg: Config, cred: TokenCredential, table_dir: str) -> None:
    service = DataLakeServiceClient(account_url=f"https://{cfg.onelake_host}", credential=cred)
    service.get_file_system_client(cfg.workspace_id).get_directory_client(
        f"{cfg.lakehouse_id}/{table_dir}"
    ).delete_directory()
    log.info("Deleted lakehouse staging table %s", table_dir)


# --------------------------------------------------------------------------- main


def main() -> int:
    logging.basicConfig(
        level=env("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-7s %(message)s",
    )
    # The Azure SDK HTTP logger is very chatty at INFO.
    logging.getLogger("azure").setLevel(logging.WARNING)

    cfg = Config.from_env()
    run_id = time.strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
    log.info("Run %s", run_id)
    run_start = time.monotonic()

    with tempfile.TemporaryDirectory() as workdir:
        local_file = fetch_source(cfg.source_parquet, workdir)
        pf = pq.ParquetFile(local_file)
        expected_rows = pf.metadata.num_rows
        arrow_schema = pf.schema_arrow
        data = pq.read_table(local_file) if cfg.load_method == "lakehouse_table" else None
        log.info("Source parquet: %d rows, %d columns: %s", expected_rows,
                 len(arrow_schema), ", ".join(f"{f.name}:{f.type}" for f in arrow_schema))

        preflight(cfg.onelake_host, 443, cfg.connect_timeout)
        preflight(cfg.sql_host, cfg.sql_port, cfg.connect_timeout)

        cred = build_credential()
        init_fabric_token(cfg, cred)
        rel_path = upload_to_lakehouse(cfg, cred, local_file, run_id)

    conn = sql_connect(cfg, cred)
    try:
        cur = conn.cursor()
        ensure_table(cur, cfg.target_table, arrow_schema)
        before = row_count(cur, cfg.target_table)
        staging = None
        if cfg.load_method == "copy_into":
            copy_into(cur, cfg, rel_path)
        elif cfg.load_method == "lakehouse_table":
            staging = load_via_lakehouse_table(cur, cfg, cred, data, run_id)
        else:
            raise SystemExit(f"LOAD_METHOD must be 'copy_into' or 'lakehouse_table', "
                             f"got {cfg.load_method!r}")
        after = row_count(cur, cfg.target_table)
    finally:
        conn.close()

    if cfg.cleanup_lakehouse_file:
        delete_from_lakehouse(cfg, cred, rel_path)
        if staging:
            delete_staging_table(cfg, cred, staging)

    loaded = after - before
    if loaded != expected_rows:
        log.error("FAIL: expected %d new rows in %s, got %d (before=%d after=%d)",
                  expected_rows, cfg.target_table, loaded, before, after)
        return 1
    log.info("PASS: %d rows loaded into %s (total now %d) in %.1fs", loaded, cfg.target_table,
             after, time.monotonic() - run_start)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:  # noqa: BLE001 - surface any failure as a non-zero job exit
        log.exception("FAIL: unhandled error")
        sys.exit(1)
