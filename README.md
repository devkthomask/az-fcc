# Azure Durable Functions — **FCC Pipeline**

End-to-end ingestion suite built on **Azure Durable Functions (Python)** with **Snowflake key-pair authentication** and **per-API-call logging**.

* **FCC Public Files**
  Facilities (TV/FM/AM/Cable) and Political Folders/Files

Every outbound HTTP call is logged to `<SNOWFLAKE_DB>.LOGGING.API_CALL_LOG` (traceability for ADF, cost/latency, error triage).

> **Note**: TTD (The Trade Desk) functions have been archived. See the `archive/` folder for legacy TTD implementations.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Folder & File Layout](#folder--file-layout)
3. [Prerequisites](#prerequisites)
4. [Configuration (Environment Variables)](#configuration-environment-variables)
5. [Deployment](#deployment)
6. [Local Development](#local-development)
7. [Function Endpoints & Payloads](#function-endpoints--payloads)
8. [Snowflake Setup](#snowflake-setup)
9. [Snowflake Objects & Upsert Strategy](#snowflake-objects--upsert-strategy)
10. [Slack Notifications](#slack-notifications)
11. [Operational Behavior](#operational-behavior)
12. [Logs & Observability](#logs--observability)
13. [Error Handling & Retries](#error-handling--retries)
14. [Security Notes](#security-notes)
15. [Troubleshooting](#troubleshooting)
16. [Appendix: API Call Log Schema](#appendix-api-call-log-schema)

---

## Architecture

* **Durable Functions fan-out/fan-in**:
  An **HTTP starter** accepts parameters (including ADF context). An **orchestrator** fans out to activities (e.g., per service type or per job). Activities do the heavy work and write to Snowflake.

* **Key-pair auth to Snowflake**:
  Uses **RSA private key** (PEM/PKCS8) via file path or base64 env var (no passwords).

* **Per-API-call logging**:
  A thin HTTP wrapper logs method, URL, status, latency, headers (safely masked), request body, and ADF correlation metadata to Snowflake (`API_CALL_LOG`).

* **Resilience**:
  HTTP retries with backoff; paging with cursors; adaptive throttling in AdGroups; staged upserts via Snowflake `MERGE`.

* **FCC snapshots**:
  Interval-buffered `.ndjson.gz` files in `OUT_DIR/YYYY-MM-DD/` prior to Snowflake merge (then **emptied at end**).

---

## Folder & File Layout

```
/fcc_files_run/              # FCC activity (one service type per call)
/start_fcc_files/            # FCC starter (route: fcc-ingest/start)
/fcc_files_orchestrator/     # FCC orchestrator (fans out tv/fm/am/cable)
fcc_files_lib.py             # FCC ingestion core
slack_notifier.py            # Slack notification handler
host.json
requirements.txt
/archive/                    # Archived TTD functions
```

> **Azure Function name = folder name**. Orchestrators call activities by folder name (e.g., `"fcc_files_run"`). A mismatch will leave the orchestration “Running” forever.

---

## Prerequisites

* **Python** 3.12
* **Azure Functions Core Tools v4**
* **Azure Storage** (Azurite locally; Azure Storage account in cloud) for Durable state
* **Snowflake** account with user public key registered

---

## Configuration (Environment Variables)

> Set in Azure App Settings or a local `.env`.

### Global

| Key         | Purpose           | Example        |
| ----------- | ----------------- | -------------- |
| `ENV_NAME`  | Environment tag   | `prod`,`dev`   |
| `LOG_LEVEL` | Logging verbosity | `INFO`,`DEBUG` |

### FCC Runtime Settings

These can be set as environment variables (defaults) or overridden via the request payload:

| Key                   | Purpose                             | Default                             |
| --------------------- | ----------------------------------- | ----------------------------------- |
| `OUT_DIR`             | Snapshot dir (ephemeral)            | `/tmp/out` (Azure Linux) or `./out` |
| `MAX_WORKERS_SEARCH`  | Thread pool for facility search     | 4                                   |
| `REQUESTS_PER_SECOND` | Rate-limit for FCC API              | 10                                  |
| `HTTP_TIMEOUT`        | HTTP timeout (seconds)              | 60                                  |
| `FLUSH_EVERY_SECONDS` | Snapshot → Snowflake flush interval | 60                                  |
| `BATCH_LOG_EVERY`     | Progress log interval               | 1000                                |

> **Note**: Snowflake connection parameters are now passed via the request payload, not environment variables. See [Function Endpoints & Payloads](#function-endpoints--payloads) for details.

---

## Deployment

1. **Create Function App** (Linux, Python 3.12, Premium or Dedicated recommended for long runs).
2. **Configure App Settings** (all env vars above + Az Storage connection).
3. **Publish**:

   ```bash
   func azure functionapp publish <your-func-app-name>
   ```
4. **Restart** Function App after config changes.

> Durable extension bundle is defined in `host.json`. Ensure it includes:

```json
{
  "version": "2.0",
  "extensionBundle": {
    "id": "Microsoft.Azure.Functions.ExtensionBundle",
    "version": "[3.*, 4.0.0)"
  },
  "extensions": {
    "durableTask": { "hubName": "DurableFunctionsHub" }
  }
}
```

---

## Local Development

1. **Install deps**

   ```bash
   python -m venv .venv
   . .venv/Scripts/activate    # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```
2. **Azurite** (for Durable local state):

   ```bash
   # VS Code Azurite extension OR
   npx azurite
   ```

   Set `local.settings.json`:

   ```json
   {
     "IsEncrypted": false,
     "Values": {
       "AzureWebJobsStorage": "UseDevelopmentStorage=true",
       "FUNCTIONS_WORKER_RUNTIME": "python"
     }
   }
   ```
3. **Run**

   ```bash
   func start
   ```

---

## Function Endpoints & Payloads

All starters accept **ADF metadata**, **Snowflake connection parameters**, and **runtime overrides**:

### Common Metadata (logged with every API call)

```json
{
  "run_id": "string",
  "activity_run_id": "string",
  "pipeline_name": "string",
  "activity_name": "string",
  "correlation_id": "string",
  "env_name": "prod"
}
```

### Snowflake Connection Parameters (required)

Snowflake credentials are passed via the request payload for security and flexibility:

| Parameter                     | Description                                      | Required |
| ----------------------------- | ------------------------------------------------ | -------- |
| `sf_account`                  | Snowflake account identifier                     | Yes      |
| `sf_user`                     | Snowflake username                               | Yes      |
| `sf_role`                     | Snowflake role (e.g., `ACCOUNTADMIN`)            | Yes      |
| `sf_warehouse`                | Snowflake warehouse                              | Yes      |
| `sf_database`                 | Snowflake database (e.g., `<SNOWFLAKE_DB>`)       | Yes      |
| `sf_schema`                   | Snowflake schema (e.g., `FCC`)                   | Yes      |
| `snowflake_private_key`       | Path to PEM private key file (PKCS#8)            | *        |
| `snowflake_private_key_text`  | PEM-encoded private key string                   | *        |
| `snowflake_private_key_pass`  | Optional passphrase for encrypted private key    | No       |

> **\*** Provide either `snowflake_private_key` (file path) **or** `snowflake_private_key_text` (PEM string), not both.

### FCC — Public Files

* **Endpoint**: `POST /api/fcc-ingest/start`
* **Full Request Body Example**:

```json
{
  "run_id": "demo-run-001",
  "pipeline_name": "FCC_PublicFiles_Pipeline",
  "activity_name": "Call_Function_Ingest",
  "correlation_id": "corr-12345",
  "env_name": "prod",
  
  "sf_account": "xxxxxx-xxxxx",
  "sf_user": "YOUR_USER",
  "sf_role": "ACCOUNTADMIN",
  "sf_warehouse": "YOUR_WH",
  "sf_database": "<SNOWFLAKE_DB>",
  "sf_schema": "FCC",
  "snowflake_private_key_text": "<REDACTED_PRIVATE_KEY>",
  "snowflake_private_key_pass": "optional-passphrase",
  
  "service_types": ["tv", "fm", "am", "cable"],
  "requests_per_second": 10,
  "flush_every_seconds": 60,
  "http_timeout": 60,
  "batch_log_every": 1000,
  "year": 2025,
  
  "slack_failure_channel": "https://hooks.slack.com/services/YOUR/WEBHOOK/URL_FAILURE",
  "slack_success_channel": "https://hooks.slack.com/services/YOUR/WEBHOOK/URL_SUCCESS",
  "slack_notification_channel": "https://hooks.slack.com/services/YOUR/WEBHOOK/URL_INFO",
  "send_failure": true,
  "send_success": false,
  "send_info": false
}
```

### Runtime Override Parameters

| Parameter              | Description                              | Default |
| ---------------------- | ---------------------------------------- | ------- |
| `service_types`        | Subset of `["tv","fm","am","cable"]`     | All     |
| `requests_per_second`  | FCC API rate limit                       | 10      |
| `flush_every_seconds`  | Snowflake flush interval                 | 60      |
| `http_timeout`         | HTTP request timeout (seconds)           | 60      |
| `batch_log_every`      | Progress logging interval                | 1000    |
| `year`                 | Force a specific year for queries        | Current |

### Status Polling

Every starter returns `202 Accepted` with a full Durable **status payload** (statusQueryGetUri, terminatePostUri, etc.).
Poll `statusQueryGetUri` from ADF (or a loop) until `runtimeStatus` is `Completed`/`Failed`.

---

## Snowflake Setup

### 1) User Key-Pair Authentication

Generate a private key (PKCS#8) and public key on your workstation, then register the public key in Snowflake:

```sql
ALTER USER YOUR_USER SET RSA_PUBLIC_KEY='MIIBIjANBgkqh...'; -- your public key
```

For key rotation, use `RSA_PUBLIC_KEY_2`.

### 2) Target Tables & Permissions

Grant your user role the necessary privileges to create tables, temp tables, and merge into targets.

**API call log table** (must exist before running):

```sql
CREATE OR REPLACE TABLE <SNOWFLAKE_DB>.LOGGING.API_CALL_LOG (
  api_call_id STRING DEFAULT UUID_STRING() NOT NULL,
  correlation_id STRING,
  run_id STRING,
  activity_run_id STRING,
  pipeline_name STRING,
  activity_name STRING,
  ts_utc TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  http_method STRING,
  endpoint_host STRING,
  endpoint_path STRING,
  query_params VARIANT,
  request_headers VARIANT,
  request_body VARIANT,
  response_status NUMBER,
  response_time_ms NUMBER,
  success_flag BOOLEAN,
  error_summary STRING,
  error_details VARIANT,
  env_name STRING,
  CREATED_AT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_API_CALL_LOG PRIMARY KEY (api_call_id),
  CONSTRAINT FK_PIPELINE_RUN FOREIGN KEY (run_id) REFERENCES <SNOWFLAKE_DB>.LOGGING.ADF_PIPELINE_RUN(run_id),
  CONSTRAINT FK_ACTIVITY_RUN FOREIGN KEY (activity_run_id) REFERENCES <SNOWFLAKE_DB>.LOGGING.ADF_ACTIVITY_RUN(activity_run_id)
);
```

> The ingestion code will **auto-create** `FCC_FACILITIES`, `FCC_POLITICAL_FOLDERS`, `FCC_POLITICAL_FILES`, and their `*_STAGE` temp tables if missing.

---

## Snowflake Objects & Upsert Strategy

* **Tables** are created if not exists by the FCC library (`ensure_objects`).
* **Stage tables** (TEMP) receive batch data via `write_pandas`.
* **Upserts** use `MERGE` by a natural key:

  * FCC → `FCC_FACILITIES`, `FCC_POLITICAL_FOLDERS`, `FCC_POLITICAL_FILES`
* **De-dup**: Merges use `QUALIFY ROW_NUMBER()` or DataFrame de-dup to prevent duplicate source rows.

> All SQL DDL/MERGE live alongside each function’s library (review them there if you need to modify schemas).

---

## Operational Behavior

### FCC Pipeline Flow

1. **HTTP Starter** (`POST /api/fcc-ingest/start`): returns immediately with an orchestration `instanceId` and `statusQueryGetUri`
2. **Orchestrator**: fans out to one **activity** per service type (`tv`, `fm`, `am`, `cable`), then aggregates results
3. **Activity** (`fcc_files_run`): does the heavy lifting for a single service type:
   - Fetch facilities → fetch political files/folders → flush to Snowflake with `MERGE`s
   - Writes NDJSON.gz snapshots and logs **every outbound API call** to Snowflake

This pattern avoids HTTP timeouts and is ADF-friendly.

### Runtime Details

* Fetches per service type; heavy calls run in thread pool
* **Snapshots**: `.ndjson.gz` written into `OUT_DIR/YYYY-MM-DD/` at flush intervals
* **Flush**: reads snapshots into stage tables and `MERGE`s into targets
* **Cleanup**: on completion, **empties `OUT_DIR` recursively** (keeps parent dir)

---

## Slack Notifications

The pipeline includes built-in **Slack notification support** that can alert your team on success, failure, or informational events during ingestion runs.

### Configuration

Notifications are configured via the HTTP starter payload. You can specify different webhook URLs for different notification types:

| Parameter                    | Description                                        |
| ---------------------------- | -------------------------------------------------- |
| `slack_success_channel`      | Webhook URL for success notifications (optional)   |
| `slack_failure_channel`      | Webhook URL for failure notifications (optional)   |
| `slack_notification_channel` | Webhook URL for info/fallback notifications        |

### Notification Toggles

| Parameter      | Description                    | Default |
| -------------- | ------------------------------ | ------- |
| `send_failure` | Send failure notifications     | `true`  |
| `send_success` | Send success notifications     | `false` |
| `send_info`    | Send informational updates     | `false` |

### Channel Routing

The notifier uses a fallback mechanism:
* Success notifications → `slack_success_channel` or fallback to `slack_notification_channel`
* Failure notifications → `slack_failure_channel` or fallback to `slack_notification_channel`
* Info notifications → `slack_notification_channel` only

### Notification Content

**Success notifications** include:
* Client name (SMS)
* ETL type (FCC Public Files List)
* Service type processed
* Run ID
* Runtime duration
* Status indicator (✅)

**Failure notifications** include:
* All success fields plus:
* Error message/summary (truncated to 500 chars)
* Failed files list (up to 10 shown)
* Status indicator (❌)

**Info notifications** include:
* General status updates
* Progress information
* Status indicator (ℹ️)

### Azure Deployment Notes for Slack

Store your Slack webhook URLs securely:
* Use **Key Vault references** for sensitive webhook URLs
* Configure as App Settings: `SLACK_SUCCESS_WEBHOOK`, `SLACK_FAILURE_WEBHOOK`, etc.
* Reference them in your ADF pipeline parameters

---

## Logs & Observability

* **Azure Functions logs**: runtime + your app logs
* **Durable status**: Orchestration history & status endpoints
* **Snowflake audit**:
  `<SNOWFLAKE_DB>.LOGGING.API_CALL_LOG`

  * `correlation_id`, `run_id`, `pipeline_name`, `activity_name`
  * HTTP method, host, path, request/response metadata, latency, success
  * `env_name`

Example:

```sql
SELECT ts_utc, http_method, endpoint_host, response_status, response_time_ms,
       run_id, pipeline_name, activity_name, success_flag
FROM <SNOWFLAKE_DB>.LOGGING.API_CALL_LOG
ORDER BY ts_utc DESC
LIMIT 100;
```

---

## Error Handling & Retries

* **HTTP**:
  Requests are wrapped with retry/backoff; errors recorded to API\_CALL\_LOG with summaries/details.
* **Snowflake**:
  Connector exceptions bubble; stage truncation is guarded; reconnection retries on heartbeat failures.
* **FCC**:
  Throttled session enforces RPS across threads; failures log and continue (best-effort per facility).
* **Slack Notifications**:
  Failure notifications are sent by default; success and info notifications are opt-in.

---

## Security Notes

* **Do not log secrets**. Sensitive headers are masked before logging.
* Use **Key Vault references** for Snowflake keys and Slack webhook URLs in Azure App Settings.
* Lock down starters with `authLevel:"function"` or APIM; prefer Private Endpoints if possible.

---

## Troubleshooting

### Orchestrator shows **Running** forever

* The orchestrator activity name likely doesn’t match the activity **folder** name.
  Example fix (FCC): call `"fcc_files_run"` (not `"do_slice"`).


### `TypeError: coroutine is not JSON serializable` in starters

* Starters must be `async` and **`await client.start_new(...)`**.
* Use `client.create_check_status_response(req, instance_id)`.

### FCC snapshots not cleared

* The FCC library now empties `OUT_DIR` at the **end** of `run_slice`.
  If you want to **delete** the directory itself, replace the walk/delete with `shutil.rmtree(OUT_DIR)` and re-create when needed.

### Snowflake auth failures

* Verify `RSA_PUBLIC_KEY` is set on the Snowflake user
* Ensure the private key and passphrase (if any) match
* Check that either `snowflake_private_key` (path) or `snowflake_private_key_text` (PEM string) is provided in the payload
* For PEM strings, ensure escaped newlines (`\n`) are handled correctly

### 429/5xx from FCC

* Lower `requests_per_second` in your payload; the client has retries + backoff

### Long runs

* Fan-out is per service type. If needed, further shard by facility ranges (open an issue / ask and we'll add it)

### Durable bindings not registered

* Ensure `host.json` includes `extensionBundle` block (v3 bundle).

### Snowflake DDL/MERGE errors

* Check column name typos (e.g., `CPC_ADVERTISERCURRENCY` vs `CPC ADVERTISERCURRENCY`).
* Ensure schema/table names match your payload parameters.

---

## Appendix: API Call Log Schema

Table: `<SNOWFLAKE_DB>.LOGGING.API_CALL_LOG`

```
api_call_id STRING DEFAULT UUID_STRING() PRIMARY KEY
correlation_id STRING
run_id STRING
activity_run_id STRING
pipeline_name STRING
activity_name STRING
ts_utc TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
http_method STRING
endpoint_host STRING
endpoint_path STRING
query_params VARIANT
request_headers VARIANT
request_body VARIANT
response_status NUMBER
response_time_ms NUMBER
success_flag BOOLEAN
error_summary STRING
error_details VARIANT
env_name STRING
CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
-- FKs to ADF run tables (if present):
-- FK_PIPELINE_RUN(run_id) → <SNOWFLAKE_DB>.LOGGING.ADF_PIPELINE_RUN
-- FK_ACTIVITY_RUN(activity_run_id) → <SNOWFLAKE_DB>.LOGGING.ADF_ACTIVITY_RUN
```

> All libraries' HTTP wrappers insert a row per outbound call. Sensitive header values are masked before logging.

---

### Need more?

* Want to shard FCC by facility ID ranges?
* Push FCC snapshots to Blob for archival?
* Add SQL statement logging to Snowflake as “API” rows?
* Customize Slack notification templates or add additional channels?
