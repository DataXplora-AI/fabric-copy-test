# fabric-copy-test

End-to-end connectivity and ingestion test, run as an **IBM Code Engine job**:

1. Uploads a Parquet file to a **Fabric Lakehouse** (`Files/…`) through the OneLake DFS API.
2. Runs `COPY INTO <warehouse table> FROM 'https://onelake.dfs.fabric.microsoft.com/<ws>/<lakehouse>/Files/…'` on the **Fabric Warehouse**.
3. Checks that the table gained exactly as many rows as the file has. The job **succeeds only if they match**.

## Network path

```
Code Engine job (IBM Cloud)
  python ──► 127.0.0.1:443 / :1433   (/etc/hosts pins the Fabric FQDNs to loopback)
  socat  ──► Satellite Connector endpoint  c-0X.private.<region>.link.satellite.cloud.ibm.com:3xxxx
                │  (IBM Cloud private network)
                ▼
  Satellite Connector agent (AKS) ──► Fabric private endpoint (VNet) ──► OneLake / Warehouse
```

TLS runs end to end. The clients keep the real Fabric hostname, which is used for SNI, the certificate check and the TDS login packet. The Fabric front ends need that name to route the request, so you can't just point the clients at the connector hostname. Entra ID tokens (`login.microsoftonline.com`) go over Code Engine's normal public egress.

## Prerequisites

**Fabric / Entra**
- A service principal with **Contributor** on the workspace. Turn on the tenant setting *Service principals can use Fabric APIs*.
- A Lakehouse and a Warehouse in the workspace. Get the IDs from the item URLs (`/groups/<workspaceId>/lakehouses/<lakehouseId>`).

**Satellite Connector** (agent already running in AKS)
Create two **location endpoints** of type *TCP*. Don't use TLS/HTTPS termination, because that would break end-to-end TLS:

| Endpoint | Destination FQDN | Port |
|---|---|---|
| fabric-onelake | OneLake DFS FQDN (workspace-specific with workspace private link) | 443 |
| fabric-sql | Warehouse SQL endpoint FQDN | 1433 |

The connector agent pods must resolve those FQDNs to the **private endpoint IPs**. Link the `privatelink.*fabric*` private DNS zones to the AKS VNet. To check from AKS:
`kubectl run -it --rm dbg --image=nicolaka/netshoot -- nslookup <fqdn>`

Use the `host:port` that each endpoint shows as `CONNECTOR_ONELAKE_ENDPOINT` / `CONNECTOR_SQL_ENDPOINT`.

## Run on Code Engine

```bash
cp .env.example .env    # fill in
```

```bash
./deploy-codeengine.sh <code-engine-project>
```

The script stores the `AZURE_*` values as a secret and everything else as a configmap. It then builds the image from this folder with Code Engine's Dockerfile build, submits a job run and prints its logs.

To test **your own Parquet file**, set `SOURCE_PARQUET` to an `https://` URL (for example an IBM COS pre-signed URL), or mount a data store with `ibmcloud ce job update --name fabric-copy-test --mount-data-store /mnt/data=<store>` and set `SOURCE_PARQUET=/mnt/data/file.parquet`. If `SOURCE_PARQUET` is unset, the job uses a 1,000-row sample built into the image.

## Run locally (no tunnel)

Leave `CONNECTOR_*` empty. Locally this only works where Fabric is reachable directly. You need msodbcsql18 installed, and `az login` works when no SP is set.

```bash
pip install -r requirements.txt && python make_sample_parquet.py sample.parquet
```

```bash
set -a; . ./.env; set +a; SOURCE_PARQUET=sample.parquet python fabric_copy_test.py
```

## Notes / troubleshooting

- **Target table**: if it's missing, the job creates it from the Parquet schema using Fabric Warehouse types (`varchar(8000)`, `datetime2(6)`, …). It's never truncated. The check uses the row-count delta.
- **COPY INTO from OneLake** reads the file inside Fabric using the caller's identity, so the SP needs read access to the Lakehouse. If your tenant needs a different host in the `FROM` URL, set `FABRIC_COPY_SOURCE_HOST`.
- **`/etc/hosts is not writable`**: the image has to run as root (the Dockerfile has no `USER`). Code Engine uses the image's user.
- **Preflight TCP fails**: the connector endpoint or agent isn't reachable. Check the endpoint status in the Satellite console and the agent pod logs in AKS.
- **TLS/handshake or login errors after TCP OK**: the agent is probably resolving the *public* Fabric IP, or the endpoint is set to TLS instead of TCP.
- **COPY INTO error 13840 "Access token couldn't be fetched for storage path"**: the SPN has no Fabric control-plane token yet. Logging in over SQL doesn't create one. The job now calls `GET https://api.fabric.microsoft.com/v1/workspaces` before COPY INTO to create or refresh it (it lasts 30 days), and retries COPY INTO a few times while the token propagates. See [Service principals in Fabric Data Warehouse](https://learn.microsoft.com/fabric/data-warehouse/service-principals#token-renewal-and-initialization-requirements).
- **13840 still failing after the token call** (while the same COPY INTO works for you in the portal): the warehouse can't get a OneLake token for the SPN. Let COPY INTO read the file as the **workspace identity** instead:
  1. Workspace → **Workspace settings → Workspace identity → + Workspace identity**.
  2. In **Manage access**, check that the identity (named after the workspace) has at least **Contributor**. Add it if it's missing.
  3. Set `COPY_CREDENTIAL=workspace_identity`. The statement gets `CREDENTIAL = (IDENTITY = 'Workspace Identity')`. The SPN then only needs Viewer on the workspace and `INSERT` on the table.
- **`LOAD_METHOD=lakehouse_table`** (use this when the SPN keeps getting 13840): the job writes the data as a staging Delta table `stg_<run-id>` under the Lakehouse's `Tables/` folder. It uses `Tables/dbo/` when the Lakehouse has schemas enabled. The write goes over the same OneLake connection as the upload, using a minimal built-in Delta writer: one Parquet file plus one `_delta_log` commit. The job then asks the Lakehouse SQL endpoint to refresh its metadata and waits until the table is visible with all rows. Finally it runs `INSERT INTO <target> SELECT … FROM <lakehouse>.dbo.stg_<run-id>` on the Warehouse connection. Everything runs as the SPN, so the Warehouse never reads OneLake files itself. It works with and without Lakehouse schemas; the Load Table API isn't used because it rejects schema-enabled Lakehouses. Column names must be plain identifiers (`[A-Za-z_][A-Za-z0-9_]*`). Naive timestamps are stored as UTC. `CLEANUP_LAKEHOUSE_FILE=true` also removes the staging table.
- Each run uploads to `Files/<LAKEHOUSE_TARGET_DIR>/<run-id>/`. Set `CLEANUP_LAKEHOUSE_FILE=true` to delete the file after the run.
