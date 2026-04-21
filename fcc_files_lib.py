# fcc_files_lib.py

import os
import json
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from datetime import datetime, timezone
from typing import Dict, List, Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from cryptography.hazmat.primitives.serialization import load_pem_private_key, Encoding, PrivateFormat, NoEncryption
from dataclasses import dataclass
from urllib.parse import urlparse
import time as _time

# ---------------------------- Config & Logger --------------------------------
BASE_URL = "https://publicfiles.fcc.gov"

# Defaults; raise after stabilization if needed
MAX_WORKERS_SEARCH = int(os.getenv("MAX_WORKERS_SEARCH", "4"))
REQUESTS_PER_SECOND = int(os.getenv("REQUESTS_PER_SECOND", "10"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "60"))
BATCH_LOG_EVERY = int(os.getenv("BATCH_LOG_EVERY", "1000"))

FLUSH_EVERY_SECONDS = int(os.getenv("FLUSH_EVERY_SECONDS", "60"))

# Bounded memory safety
FLUSH_MAX_ROWS = int(os.getenv("FLUSH_MAX_ROWS", "10000"))
STAGE_LOAD_CHUNK_ROWS = int(os.getenv("STAGE_LOAD_CHUNK_ROWS", "100000"))
FACILITIES_EMIT_BATCH = int(os.getenv("FACILITIES_EMIT_BATCH", "10000"))

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("ingest_lib")

# ---------------------------- Invocation context -----------------------------
@dataclass
class InvokeCtx:
    run_id: str | None = None
    activity_run_id: str | None = None
    pipeline_name: str | None = None
    activity_name: str | None = None
    correlation_id: str | None = None
    env_name: str | None = os.getenv("ENV_NAME", "prod")

_invoke_ctx = InvokeCtx()     # set per-activity call
_conn_for_logs = None         # Snowflake connection for logging

# Year override (instead of monkey-patching datetime)
OVERRIDE_YEAR: int | None = None

# ---------------------------- HTTP client w/ throttle ------------------------
class ThrottledSession(requests.Session):
    _lock = threading.Lock()
    _last_call = 0.0

    def __init__(self, rps: float):
        super().__init__()
        self.min_interval = 1.0 / float(rps)
        retries = Retry(
            total=6, backoff_factor=0.6,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"], raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=100, pool_maxsize=100)
        self.mount("https://", adapter)
        self.mount("http://", adapter)

    def get(self, *args, **kwargs):
        with ThrottledSession._lock:
            now = time.monotonic()
            wait_time = self.min_interval - (now - ThrottledSession._last_call)
            if wait_time > 0:
                time.sleep(wait_time)
            ThrottledSession._last_call = time.monotonic()
        kwargs.setdefault("timeout", HTTP_TIMEOUT)
        return super().get(*args, **kwargs)

session = ThrottledSession(REQUESTS_PER_SECOND)

def _apply_overrides_local(overrides: dict | None):
    """
    Apply per-run overrides safely (no monkey-patching).
    """
    global REQUESTS_PER_SECOND, FLUSH_EVERY_SECONDS, HTTP_TIMEOUT, BATCH_LOG_EVERY, session, OVERRIDE_YEAR
    if not overrides:
        return
    rps = overrides.get("requests_per_second")
    if isinstance(rps, (int, float)) and rps > 0:
        REQUESTS_PER_SECOND = float(rps)
        session = ThrottledSession(REQUESTS_PER_SECOND)
    fe = overrides.get("flush_every_seconds")
    if isinstance(fe, int) and fe > 0:
        FLUSH_EVERY_SECONDS = fe
    to = overrides.get("http_timeout")
    if isinstance(to, int) and to > 0:
        HTTP_TIMEOUT = to
    be = overrides.get("batch_log_every")
    if isinstance(be, int) and be > 0:
        BATCH_LOG_EVERY = be
    yr = overrides.get("year")
    if isinstance(yr, int) and 2000 <= yr <= 2100:
        OVERRIDE_YEAR = yr

# ---------------------------- Snowflake Config & Connection ------------------
@dataclass
class SFConfig:
    """Snowflake connection configuration passed via payload."""
    account: str
    user: str
    role: str
    warehouse: str
    database: str
    schema: str
    private_key_path: str | None = None
    private_key_text: str | None = None  # PEM string
    private_key_pass: str | None = None


def _load_private_key_bytes_from_text(key_text: str, passphrase: str | None) -> bytes:
    """Load private key bytes from a PEM-encoded string."""
    # Handle escaped newlines from command line (convert \n literals to actual newlines)
    if isinstance(key_text, str) and '\\n' in key_text:
        key_text = key_text.replace('\\n', '\n')
    key_data = key_text.encode() if isinstance(key_text, str) else key_text
    password = None if not passphrase or passphrase.strip("'\"") == "" else passphrase.encode()
    key = load_pem_private_key(key_data, password=password)
    return key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())


def _load_private_key_bytes_from_path(path: str, passphrase: str | None) -> bytes:
    """Load private key bytes from a file path."""
    with open(path, "rb") as f:
        key_data = f.read()
    password = None if not passphrase or passphrase.strip("'\"") == "" else passphrase.encode()
    key = load_pem_private_key(key_data, password=password)
    return key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())


def sf_connect(cfg: SFConfig):
    """Create a Snowflake connection using the provided configuration."""
    if cfg.private_key_text:
        pkb = _load_private_key_bytes_from_text(cfg.private_key_text, cfg.private_key_pass)
    elif cfg.private_key_path:
        pkb = _load_private_key_bytes_from_path(cfg.private_key_path, cfg.private_key_pass)
    else:
        raise ValueError("Either private_key_text or private_key_path must be provided for Snowflake auth.")
    
    logger.info("Connecting to Snowflake as %s…", cfg.user)
    return snowflake.connector.connect(
        account=cfg.account,
        user=cfg.user,
        role=cfg.role,
        warehouse=cfg.warehouse,
        database=cfg.database,
        schema=cfg.schema,
        private_key=pkb,
        client_session_keep_alive=True,
        autocommit=True,
    )

# ---------------------------- API-call logging -------------------------------
# Database name for logging - set dynamically from SFConfig
_log_database: str | None = None

def _get_api_log_sql(database: str) -> str:
    return f"""
INSERT INTO {database}.LOGGING.API_CALL_LOG
( correlation_id, run_id, activity_run_id, pipeline_name, activity_name,
  http_method, endpoint_host, endpoint_path, query_params, request_headers, request_body,
  response_status, response_time_ms, success_flag, error_summary, error_details, env_name )
SELECT
  %s, %s, %s, %s, %s,
  %s, %s, %s,
  PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s),
  %s, %s, %s, %s, PARSE_JSON(%s), %s
"""

# --- ADF Pipeline Run bookkeeping -------------------------------------------
def _get_adf_run_upsert_start_sql(database: str) -> str:
    return f"""
MERGE INTO {database}.LOGGING.ADF_PIPELINE_RUN t
USING (
  SELECT
    %s::string            AS RUN_ID,
    %s::string            AS PIPELINE_NAME,
    'Running'::string     AS RUN_STATUS,
    CURRENT_TIMESTAMP()   AS START_TIME_UTC,
    NULL::timestamp_tz    AS END_TIME_UTC,
    NULL::number          AS DURATION_MS,
    PARSE_JSON(%s)        AS PARAMETERS,
    NULL::variant         AS RUN_METRICS,
    NULL::string          AS ERROR_SUMMARY,
    NULL::variant         AS ERROR_DETAILS,
    %s::string            AS ENV_NAME
) s
ON t.RUN_ID = s.RUN_ID
WHEN MATCHED THEN UPDATE SET
  t.PIPELINE_NAME   = s.PIPELINE_NAME,
  t.RUN_STATUS      = s.RUN_STATUS,
  t.START_TIME_UTC  = COALESCE(t.START_TIME_UTC, s.START_TIME_UTC),
  t.PARAMETERS      = s.PARAMETERS,
  t.ENV_NAME        = s.ENV_NAME,
  t.UPDATED_AT      = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  RUN_ID, PIPELINE_NAME, RUN_STATUS, START_TIME_UTC, END_TIME_UTC, DURATION_MS,
  PARAMETERS, RUN_METRICS, ERROR_SUMMARY, ERROR_DETAILS, ENV_NAME, CREATED_AT, UPDATED_AT
) VALUES (
  s.RUN_ID, s.PIPELINE_NAME, s.RUN_STATUS, s.START_TIME_UTC, s.END_TIME_UTC, s.DURATION_MS,
  s.PARAMETERS, s.RUN_METRICS, s.ERROR_SUMMARY, s.ERROR_DETAILS, s.ENV_NAME, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);
"""

def _get_adf_run_update_end_sql(database: str) -> str:
    return f"""
UPDATE {database}.LOGGING.ADF_PIPELINE_RUN
SET
  RUN_STATUS   = %s,
  END_TIME_UTC = CURRENT_TIMESTAMP(),
  DURATION_MS  = DATEDIFF('millisecond', START_TIME_UTC, CURRENT_TIMESTAMP()),
  ERROR_SUMMARY = %s,
  ERROR_DETAILS = PARSE_JSON(%s),
  UPDATED_AT    = CURRENT_TIMESTAMP()
WHERE RUN_ID = %s;
"""

def _adf_run_start(conn, ctx: InvokeCtx, parameters: dict | None):
    """Upsert a Running row so API_CALL_LOG FK inserts won't fail."""
    if not _log_database:
        logger.warning("Log database not set, skipping ADF run start logging")
        return
    try:
        params_json = json.dumps(parameters or {}, ensure_ascii=False)
        with conn.cursor() as cur:
            cur.execute(
                _get_adf_run_upsert_start_sql(_log_database),
                (
                    ctx.run_id or "",                # RUN_ID
                    ctx.pipeline_name or "",         # PIPELINE_NAME
                    params_json,                     # PARAMETERS (VARIANT)
                    ctx.env_name or "prod",          # ENV_NAME
                ),
            )
    except Exception as e:
        logger.warning("Failed to upsert ADF pipeline run start: %s", e)

def _adf_run_end(conn, ctx: InvokeCtx, status: str, err_summary: str | None = None, err_details: dict | None = None):
    """Update END_TIME_UTC, DURATION_MS, and RUN_STATUS at the end."""
    if not _log_database:
        logger.warning("Log database not set, skipping ADF run end logging")
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                _get_adf_run_update_end_sql(_log_database),
                (
                    status,                           # RUN_STATUS
                    err_summary,                      # ERROR_SUMMARY
                    json.dumps(err_details or {}),    # ERROR_DETAILS
                    ctx.run_id or "",                 # WHERE RUN_ID
                ),
            )
    except Exception as e:
        logger.warning("Failed to update ADF pipeline run end: %s", e)

def _sf_log_api_call(conn,
                     method: str,
                     url: str,
                     *,
                     query_params: dict | None,
                     request_headers: dict | None,
                     request_body: dict | None,
                     response_status: int | None,
                     elapsed_ms: int,
                     success_flag: bool,
                     error_summary: str | None,
                     error_details: dict | None):
    if not _log_database:
        return  # Skip logging if database not set
    try:
        from urllib.parse import urlparse as _urlparse
        u = _urlparse(url)
        host = u.netloc
        path = u.path or "/"
        qp_json = json.dumps(query_params or {}, ensure_ascii=False)
        hdrs_json = json.dumps(request_headers or {}, ensure_ascii=False)
        body_json = json.dumps(request_body or {}, ensure_ascii=False)
        err_json = json.dumps(error_details or {}, ensure_ascii=False)
        corr_id = _invoke_ctx.correlation_id or _invoke_ctx.run_id
        with conn.cursor() as cur:
            cur.execute(
                _get_api_log_sql(_log_database),
                (
                    corr_id, _invoke_ctx.run_id, _invoke_ctx.activity_run_id,
                    _invoke_ctx.pipeline_name, _invoke_ctx.activity_name,
                    method, host, path, qp_json, hdrs_json, body_json,
                    response_status, elapsed_ms, success_flag,
                    (error_summary or None), err_json, (_invoke_ctx.env_name or "prod"),
                )
            )
    except Exception as e:
        logger.warning("API call log insert failed: %s", e)

def http_get(url: str, *, params: dict | None = None, headers: dict | None = None):
    headers = headers or {}
    start = _time.perf_counter()
    status = None
    err_summary = None
    err_details = None
    success = False
    try:
        resp = session.get(url, params=params, headers=headers)
        status = resp.status_code
        success = (status is not None and status < 400)
        return resp
    except Exception as ex:
        err_summary = str(ex)[:512]
        err_details = {"exception": repr(ex)}
        raise
    finally:
        elapsed_ms = int((_time.perf_counter() - start) * 1000)
        if _conn_for_logs is not None:
            _sf_log_api_call(
                _conn_for_logs,
                "GET",
                url,
                query_params=params,
                request_headers={k: headers.get(k) for k in ["User-Agent", "Accept", "Content-Type"] if k in headers},
                request_body=None,
                response_status=status,
                elapsed_ms=elapsed_ms,
                success_flag=success,
                error_summary=err_summary,
                error_details=err_details,
            )

# ---------------------------- DDL & MERGE SQL --------------------------------
CABLE_FACILITY_COLS = [
    "CABLE_SYSTEM_ID","HEAD_END_ID","OPERATOR_ID","LEGAL_NAME","APPLICATION_ID",
    "STATUS","STATUS_DATE","FRN","PSID","FACILITY_TYPE","OPERATOR_ADDRESS_LINE1",
    "OPERATOR_NAME","OPERATOR_ADDRESS_LINE2","OPERATOR_PO_BOX","OPERATOR_CITY",
    "OPERATOR_ZIP_CODE","OPERATOR_ZIP_CODE_SUFFIX","OPERATOR_STATE","OPERATOR_EMAIL",
    "OPERATOR_WEBSITE","OPERATOR_PHONE","OPERATOR_FAX","CORES_USER",
    "PRINCIPAL_HEADEND_NAME","PRINCIPAL_ADDRESS_LINE1","PRINCIPAL_ADDRESS_LINE2",
    "PRINCIPAL_PO_BOX","PRINCIPAL_CITY","PRINCIPAL_STATE","PRINCIPAL_ZIP_CODE",
    "PRINCIPAL_ZIP_CODE_SUFFIX","PRINCIPAL_FAX","PRINCIPAL_PHONE","PRINCIPAL_EMAIL",
    "LOCAL_FILE_CONTACT_NAME","LOCAL_FILE_ADDRESS_LINE1","LOCAL_FILE_ADDRESS_LINE2",
    "LOCAL_FILE_PO_BOX","LOCAL_FILE_CITY","LOCAL_FILE_STATE","LOCAL_FILE_ZIP_CODE",
    "LOCAL_FILE_ZIP_CODE_SUFFIX","LOCAL_FILE_CONTACT_FAX","LOCAL_FILE_CONTACT_PHONE",
    "LOCAL_FILE_CONTACT_EMAIL","ACCESS_TOKEN","PRINCIPAL_ADDRESS_IN_LOCAL_FILES",
    "CABLE_SERVICE_ZIP_CODES","CABLE_SERVICE_EMP_UNITS","CABLE_COMMUNITIES",
    "CENEMAIL","CENPHONE"
]
FACILITIES_COLS = (
    ["ID","CALL_SIGN","FREQUENCY","ACTIVE_IND","SERVICE_TYPE"] + CABLE_FACILITY_COLS + ["RAW"]
)

DDL_TARGET = """
CREATE TABLE IF NOT EXISTS FCC_FACILITIES (
  ID VARCHAR PRIMARY KEY,
  CALL_SIGN VARCHAR,
  FREQUENCY VARCHAR,
  ACTIVE_IND VARCHAR,
  SERVICE_TYPE VARCHAR,
  CABLE_SYSTEM_ID VARCHAR,
  HEAD_END_ID VARCHAR,
  OPERATOR_ID VARCHAR,
  LEGAL_NAME VARCHAR,
  APPLICATION_ID VARCHAR,
  STATUS VARCHAR,
  STATUS_DATE TIMESTAMP_TZ,
  FRN VARCHAR,
  PSID VARCHAR,
  FACILITY_TYPE VARCHAR,
  OPERATOR_ADDRESS_LINE1 VARCHAR,
  OPERATOR_NAME VARCHAR,
  OPERATOR_ADDRESS_LINE2 VARCHAR,
  OPERATOR_PO_BOX VARCHAR,
  OPERATOR_CITY VARCHAR,
  OPERATOR_ZIP_CODE VARCHAR,
  OPERATOR_ZIP_CODE_SUFFIX VARCHAR,
  OPERATOR_STATE VARCHAR,
  OPERATOR_EMAIL VARCHAR,
  OPERATOR_WEBSITE VARCHAR,
  OPERATOR_PHONE VARCHAR,
  OPERATOR_FAX VARCHAR,
  CORES_USER VARCHAR,
  PRINCIPAL_HEADEND_NAME VARCHAR,
  PRINCIPAL_ADDRESS_LINE1 VARCHAR,
  PRINCIPAL_ADDRESS_LINE2 VARCHAR,
  PRINCIPAL_PO_BOX VARCHAR,
  PRINCIPAL_CITY VARCHAR,
  PRINCIPAL_STATE VARCHAR,
  PRINCIPAL_ZIP_CODE VARCHAR,
  PRINCIPAL_ZIP_CODE_SUFFIX VARCHAR,
  PRINCIPAL_FAX VARCHAR,
  PRINCIPAL_PHONE VARCHAR,
  PRINCIPAL_EMAIL VARCHAR,
  LOCAL_FILE_CONTACT_NAME VARCHAR,
  LOCAL_FILE_ADDRESS_LINE1 VARCHAR,
  LOCAL_FILE_ADDRESS_LINE2 VARCHAR,
  LOCAL_FILE_PO_BOX VARCHAR,
  LOCAL_FILE_CITY VARCHAR,
  LOCAL_FILE_STATE VARCHAR,
  LOCAL_FILE_ZIP_CODE VARCHAR,
  LOCAL_FILE_ZIP_CODE_SUFFIX VARCHAR,
  LOCAL_FILE_CONTACT_FAX VARCHAR,
  LOCAL_FILE_CONTACT_PHONE VARCHAR,
  LOCAL_FILE_CONTACT_EMAIL VARCHAR,
  ACCESS_TOKEN VARCHAR,
  PRINCIPAL_ADDRESS_IN_LOCAL_FILES VARCHAR,
  CABLE_SERVICE_ZIP_CODES VARIANT,
  CABLE_SERVICE_EMP_UNITS VARIANT,
  CABLE_COMMUNITIES VARIANT,
  CENEMAIL VARCHAR,
  CENPHONE VARCHAR,
  RAW VARIANT,
  LOADED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS FCC_POLITICAL_FOLDERS (
  ENTITY_FOLDER_ID VARCHAR PRIMARY KEY,
  ENTITY_ID VARCHAR,
  FOLDER_NAME VARCHAR,
  FOLDER_PATH VARCHAR,
  PARENT_FOLDER_ID VARCHAR,
  CREATE_TS TIMESTAMP_TZ,
  LAST_UPDATE_TS TIMESTAMP_TZ,
  SERVICE_TYPE VARCHAR,
  RAW VARIANT,
  LOADED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT FK_FOLDERS_FACILITY FOREIGN KEY (ENTITY_ID) REFERENCES FCC_FACILITIES(ID)
);

CREATE TABLE IF NOT EXISTS FCC_POLITICAL_FILES (
  FILE_ID VARCHAR PRIMARY KEY,
  ENTITY_ID VARCHAR,
  FOLDER_ID VARCHAR,
  FILE_NAME VARCHAR,
  FILE_EXTENSION VARCHAR,
  FILE_SIZE NUMBER,
  FILE_STATUS VARCHAR,
  FILE_FOLDER_PATH VARCHAR,
  FILE_MANAGER_ID VARCHAR,
  CREATE_TS TIMESTAMP_TZ,
  LAST_UPDATE_TS TIMESTAMP_TZ,
  SERVICE_TYPE VARCHAR,
  RAW VARIANT,
  LOADED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT FK_FILES_FACILITY FOREIGN KEY (ENTITY_ID) REFERENCES FCC_FACILITIES(ID),
  CONSTRAINT FK_FILES_FOLDER FOREIGN KEY (FOLDER_ID) REFERENCES FCC_POLITICAL_FOLDERS(ENTITY_FOLDER_ID)
);
"""

DDL_STAGE = """
CREATE OR REPLACE TEMP TABLE FCC_FACILITIES_STAGE (ID VARCHAR, CALL_SIGN VARCHAR, FREQUENCY VARCHAR, ACTIVE_IND VARCHAR,
  SERVICE_TYPE VARCHAR, CABLE_SYSTEM_ID VARCHAR, HEAD_END_ID VARCHAR, OPERATOR_ID VARCHAR, LEGAL_NAME VARCHAR,
  APPLICATION_ID VARCHAR, STATUS VARCHAR, STATUS_DATE VARCHAR, FRN VARCHAR, PSID VARCHAR, FACILITY_TYPE VARCHAR,
  OPERATOR_ADDRESS_LINE1 VARCHAR, OPERATOR_NAME VARCHAR, OPERATOR_ADDRESS_LINE2 VARCHAR, OPERATOR_PO_BOX VARCHAR,
  OPERATOR_CITY VARCHAR, OPERATOR_ZIP_CODE VARCHAR, OPERATOR_ZIP_CODE_SUFFIX VARCHAR, OPERATOR_STATE VARCHAR,
  OPERATOR_EMAIL VARCHAR, OPERATOR_WEBSITE VARCHAR, OPERATOR_PHONE VARCHAR, OPERATOR_FAX VARCHAR, CORES_USER VARCHAR,
  PRINCIPAL_HEADEND_NAME VARCHAR, PRINCIPAL_ADDRESS_LINE1 VARCHAR, PRINCIPAL_ADDRESS_LINE2 VARCHAR, PRINCIPAL_PO_BOX VARCHAR,
  PRINCIPAL_CITY VARCHAR, PRINCIPAL_STATE VARCHAR, PRINCIPAL_ZIP_CODE VARCHAR, PRINCIPAL_ZIP_CODE_SUFFIX VARCHAR,
  PRINCIPAL_FAX VARCHAR, PRINCIPAL_PHONE VARCHAR, PRINCIPAL_EMAIL VARCHAR, LOCAL_FILE_CONTACT_NAME VARCHAR,
  LOCAL_FILE_ADDRESS_LINE1 VARCHAR, LOCAL_FILE_ADDRESS_LINE2 VARCHAR, LOCAL_FILE_PO_BOX VARCHAR, LOCAL_FILE_CITY VARCHAR,
  LOCAL_FILE_STATE VARCHAR, LOCAL_FILE_ZIP_CODE VARCHAR, LOCAL_FILE_ZIP_CODE_SUFFIX VARCHAR, LOCAL_FILE_CONTACT_FAX VARCHAR,
  LOCAL_FILE_CONTACT_PHONE VARCHAR, LOCAL_FILE_CONTACT_EMAIL VARCHAR, ACCESS_TOKEN VARCHAR, PRINCIPAL_ADDRESS_IN_LOCAL_FILES VARCHAR,
  CABLE_SERVICE_ZIP_CODES VARCHAR, CABLE_SERVICE_EMP_UNITS VARCHAR, CABLE_COMMUNITIES VARCHAR, CENEMAIL VARCHAR, CENPHONE VARCHAR, RAW VARCHAR
);

CREATE OR REPLACE TEMP TABLE FCC_POLITICAL_FOLDERS_STAGE (
  ENTITY_FOLDER_ID VARCHAR, ENTITY_ID VARCHAR, FOLDER_NAME VARCHAR, FOLDER_PATH VARCHAR, PARENT_FOLDER_ID VARCHAR,
  CREATE_TS VARCHAR, LAST_UPDATE_TS VARCHAR, SERVICE_TYPE VARCHAR, RAW VARCHAR
);

CREATE OR REPLACE TEMP TABLE FCC_POLITICAL_FILES_STAGE (
  FILE_ID VARCHAR, ENTITY_ID VARCHAR, FOLDER_ID VARCHAR, FILE_NAME VARCHAR, FILE_EXTENSION VARCHAR, FILE_SIZE VARCHAR,
  FILE_STATUS VARCHAR, FILE_FOLDER_PATH VARCHAR, FILE_MANAGER_ID VARCHAR, CREATE_TS VARCHAR, LAST_UPDATE_TS VARCHAR,
  SERVICE_TYPE VARCHAR, RAW VARCHAR
);
"""

MERGE_FACILITIES = """
MERGE INTO FCC_FACILITIES t
USING (
  SELECT
    ID, CALL_SIGN, FREQUENCY, ACTIVE_IND, SERVICE_TYPE,
    CABLE_SYSTEM_ID, HEAD_END_ID, OPERATOR_ID, LEGAL_NAME, APPLICATION_ID,
    STATUS, TRY_TO_TIMESTAMP_TZ(STATUS_DATE) AS STATUS_DATE, FRN, PSID, FACILITY_TYPE,
    OPERATOR_ADDRESS_LINE1, OPERATOR_NAME, OPERATOR_ADDRESS_LINE2, OPERATOR_PO_BOX,
    OPERATOR_CITY, OPERATOR_ZIP_CODE, OPERATOR_ZIP_CODE_SUFFIX, OPERATOR_STATE,
    OPERATOR_EMAIL, OPERATOR_WEBSITE, OPERATOR_PHONE, OPERATOR_FAX, CORES_USER,
    PRINCIPAL_HEADEND_NAME, PRINCIPAL_ADDRESS_LINE1, PRINCIPAL_ADDRESS_LINE2,
    PRINCIPAL_PO_BOX, PRINCIPAL_CITY, PRINCIPAL_STATE, PRINCIPAL_ZIP_CODE,
    PRINCIPAL_ZIP_CODE_SUFFIX, PRINCIPAL_FAX, PRINCIPAL_PHONE, PRINCIPAL_EMAIL,
    LOCAL_FILE_CONTACT_NAME, LOCAL_FILE_ADDRESS_LINE1, LOCAL_FILE_ADDRESS_LINE2,
    LOCAL_FILE_PO_BOX, LOCAL_FILE_CITY, LOCAL_FILE_STATE, LOCAL_FILE_ZIP_CODE,
    LOCAL_FILE_ZIP_CODE_SUFFIX, LOCAL_FILE_CONTACT_FAX, LOCAL_FILE_CONTACT_PHONE,
    LOCAL_FILE_CONTACT_EMAIL, ACCESS_TOKEN, PRINCIPAL_ADDRESS_IN_LOCAL_FILES,
    TRY_PARSE_JSON(CABLE_SERVICE_ZIP_CODES) AS CABLE_SERVICE_ZIP_CODES,
    TRY_PARSE_JSON(CABLE_SERVICE_EMP_UNITS) AS CABLE_SERVICE_EMP_UNITS,
    TRY_PARSE_JSON(CABLE_COMMUNITIES) AS CABLE_COMMUNITIES,
    CENEMAIL, CENPHONE,
    TRY_PARSE_JSON(RAW) AS RAW
  FROM FCC_FACILITIES_STAGE
) s
ON t.ID = s.ID
WHEN MATCHED THEN UPDATE SET
  CALL_SIGN = s.CALL_SIGN,
  FREQUENCY = s.FREQUENCY,
  ACTIVE_IND = s.ACTIVE_IND,
  SERVICE_TYPE = s.SERVICE_TYPE,
  CABLE_SYSTEM_ID = s.CABLE_SYSTEM_ID,
  HEAD_END_ID = s.HEAD_END_ID,
  OPERATOR_ID = s.OPERATOR_ID,
  LEGAL_NAME = s.LEGAL_NAME,
  APPLICATION_ID = s.APPLICATION_ID,
  STATUS = s.STATUS,
  STATUS_DATE = s.STATUS_DATE,
  FRN = s.FRN,
  PSID = s.PSID,
  FACILITY_TYPE = s.FACILITY_TYPE,
  OPERATOR_ADDRESS_LINE1 = s.OPERATOR_ADDRESS_LINE1,
  OPERATOR_NAME = s.OPERATOR_NAME,
  OPERATOR_ADDRESS_LINE2 = s.OPERATOR_ADDRESS_LINE2,
  OPERATOR_PO_BOX = s.OPERATOR_PO_BOX,
  OPERATOR_CITY = s.OPERATOR_CITY,
  OPERATOR_ZIP_CODE = s.OPERATOR_ZIP_CODE,
  OPERATOR_ZIP_CODE_SUFFIX = s.OPERATOR_ZIP_CODE_SUFFIX,
  OPERATOR_STATE = s.OPERATOR_STATE,
  OPERATOR_EMAIL = s.OPERATOR_EMAIL,
  OPERATOR_WEBSITE = s.OPERATOR_WEBSITE,
  OPERATOR_PHONE = s.OPERATOR_PHONE,
  OPERATOR_FAX = s.OPERATOR_FAX,
  CORES_USER = s.CORES_USER,
  PRINCIPAL_HEADEND_NAME = s.PRINCIPAL_HEADEND_NAME,
  PRINCIPAL_ADDRESS_LINE1 = s.PRINCIPAL_ADDRESS_LINE1,
  PRINCIPAL_ADDRESS_LINE2 = s.PRINCIPAL_ADDRESS_LINE2,
  PRINCIPAL_PO_BOX = s.PRINCIPAL_PO_BOX,
  PRINCIPAL_CITY = s.PRINCIPAL_CITY,
  PRINCIPAL_STATE = s.PRINCIPAL_STATE,
  PRINCIPAL_ZIP_CODE = s.PRINCIPAL_ZIP_CODE,
  PRINCIPAL_ZIP_CODE_SUFFIX = s.PRINCIPAL_ZIP_CODE_SUFFIX,
  PRINCIPAL_FAX = s.PRINCIPAL_FAX,
  PRINCIPAL_PHONE = s.PRINCIPAL_PHONE,
  PRINCIPAL_EMAIL = s.PRINCIPAL_EMAIL,
  LOCAL_FILE_CONTACT_NAME = s.LOCAL_FILE_CONTACT_NAME,
  LOCAL_FILE_ADDRESS_LINE1 = s.LOCAL_FILE_ADDRESS_LINE1,
  LOCAL_FILE_ADDRESS_LINE2 = s.LOCAL_FILE_ADDRESS_LINE2,
  LOCAL_FILE_PO_BOX = s.LOCAL_FILE_PO_BOX,
  LOCAL_FILE_CITY = s.LOCAL_FILE_CITY,
  LOCAL_FILE_STATE = s.LOCAL_FILE_STATE,
  LOCAL_FILE_ZIP_CODE = s.LOCAL_FILE_ZIP_CODE,
  LOCAL_FILE_ZIP_CODE_SUFFIX = s.LOCAL_FILE_ZIP_CODE_SUFFIX,
  LOCAL_FILE_CONTACT_FAX = s.LOCAL_FILE_CONTACT_FAX,
  LOCAL_FILE_CONTACT_PHONE = s.LOCAL_FILE_CONTACT_PHONE,
  LOCAL_FILE_CONTACT_EMAIL = s.LOCAL_FILE_CONTACT_EMAIL,
  ACCESS_TOKEN = s.ACCESS_TOKEN,
  PRINCIPAL_ADDRESS_IN_LOCAL_FILES = s.PRINCIPAL_ADDRESS_IN_LOCAL_FILES,
  CABLE_SERVICE_ZIP_CODES = s.CABLE_SERVICE_ZIP_CODES,
  CABLE_SERVICE_EMP_UNITS = s.CABLE_SERVICE_EMP_UNITS,
  CABLE_COMMUNITIES = s.CABLE_COMMUNITIES,
  CENEMAIL = s.CENEMAIL,
  CENPHONE = s.CENPHONE,
  RAW = s.RAW,
  LOADED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  ID, CALL_SIGN, FREQUENCY, ACTIVE_IND, SERVICE_TYPE,
  CABLE_SYSTEM_ID, HEAD_END_ID, OPERATOR_ID, LEGAL_NAME, APPLICATION_ID,
  STATUS, STATUS_DATE, FRN, PSID, FACILITY_TYPE, OPERATOR_ADDRESS_LINE1,
  OPERATOR_NAME, OPERATOR_ADDRESS_LINE2, OPERATOR_PO_BOX, OPERATOR_CITY,
  OPERATOR_ZIP_CODE, OPERATOR_ZIP_CODE_SUFFIX, OPERATOR_STATE, OPERATOR_EMAIL,
  OPERATOR_WEBSITE, OPERATOR_PHONE, OPERATOR_FAX, CORES_USER,
  PRINCIPAL_HEADEND_NAME, PRINCIPAL_ADDRESS_LINE1, PRINCIPAL_ADDRESS_LINE2,
  PRINCIPAL_PO_BOX, PRINCIPAL_CITY, PRINCIPAL_STATE, PRINCIPAL_ZIP_CODE,
  PRINCIPAL_ZIP_CODE_SUFFIX, PRINCIPAL_FAX, PRINCIPAL_PHONE, PRINCIPAL_EMAIL,
  LOCAL_FILE_CONTACT_NAME, LOCAL_FILE_ADDRESS_LINE1, LOCAL_FILE_ADDRESS_LINE2,
  LOCAL_FILE_PO_BOX, LOCAL_FILE_CITY, LOCAL_FILE_STATE, LOCAL_FILE_ZIP_CODE,
  LOCAL_FILE_ZIP_CODE_SUFFIX, LOCAL_FILE_CONTACT_FAX, LOCAL_FILE_CONTACT_PHONE,
  LOCAL_FILE_CONTACT_EMAIL, ACCESS_TOKEN, PRINCIPAL_ADDRESS_IN_LOCAL_FILES,
  CABLE_SERVICE_ZIP_CODES, CABLE_SERVICE_EMP_UNITS, CABLE_COMMUNITIES,
  CENEMAIL, CENPHONE, RAW
) VALUES (
  s.ID, s.CALL_SIGN, s.FREQUENCY, s.ACTIVE_IND, s.SERVICE_TYPE,
  s.CABLE_SYSTEM_ID, s.HEAD_END_ID, s.OPERATOR_ID, s.LEGAL_NAME, s.APPLICATION_ID,
  s.STATUS, s.STATUS_DATE, s.FRN, s.PSID, s.FACILITY_TYPE, s.OPERATOR_ADDRESS_LINE1,
  s.OPERATOR_NAME, s.OPERATOR_ADDRESS_LINE2, s.OPERATOR_PO_BOX, s.OPERATOR_CITY,
  s.OPERATOR_ZIP_CODE, s.OPERATOR_ZIP_CODE_SUFFIX, s.OPERATOR_STATE, s.OPERATOR_EMAIL,
  s.OPERATOR_WEBSITE, s.OPERATOR_PHONE, s.OPERATOR_FAX, s.CORES_USER,
  s.PRINCIPAL_HEADEND_NAME, s.PRINCIPAL_ADDRESS_LINE1, s.PRINCIPAL_ADDRESS_LINE2,
  s.PRINCIPAL_PO_BOX, s.PRINCIPAL_CITY, s.PRINCIPAL_STATE, s.PRINCIPAL_ZIP_CODE,
  s.PRINCIPAL_ZIP_CODE_SUFFIX, s.PRINCIPAL_FAX, s.PRINCIPAL_PHONE, s.PRINCIPAL_EMAIL,
  s.LOCAL_FILE_CONTACT_NAME, s.LOCAL_FILE_ADDRESS_LINE1, s.LOCAL_FILE_ADDRESS_LINE2,
  s.LOCAL_FILE_PO_BOX, s.LOCAL_FILE_CITY, s.LOCAL_FILE_STATE, s.LOCAL_FILE_ZIP_CODE,
  s.LOCAL_FILE_ZIP_CODE_SUFFIX, s.LOCAL_FILE_CONTACT_FAX, s.LOCAL_FILE_CONTACT_PHONE,
  s.LOCAL_FILE_CONTACT_EMAIL, s.ACCESS_TOKEN, s.PRINCIPAL_ADDRESS_IN_LOCAL_FILES,
  s.CABLE_SERVICE_ZIP_CODES, s.CABLE_SERVICE_EMP_UNITS, s.CABLE_COMMUNITIES,
  s.CENEMAIL, s.CENPHONE, s.RAW
);
"""

MERGE_FILES = """
MERGE INTO FCC_POLITICAL_FILES t
USING (
  SELECT
    FILE_ID, ENTITY_ID, FOLDER_ID, FILE_NAME, FILE_EXTENSION,
    TRY_TO_NUMBER(FILE_SIZE) AS FILE_SIZE, FILE_STATUS, FILE_FOLDER_PATH, FILE_MANAGER_ID,
    TRY_TO_TIMESTAMP_TZ(CREATE_TS) AS CREATE_TS, TRY_TO_TIMESTAMP_TZ(LAST_UPDATE_TS) AS LAST_UPDATE_TS,
    SERVICE_TYPE, TRY_PARSE_JSON(RAW) AS RAW
  FROM FCC_POLITICAL_FILES_STAGE
) s
ON t.FILE_ID = s.FILE_ID
WHEN MATCHED THEN UPDATE SET
  ENTITY_ID = s.ENTITY_ID, FOLDER_ID = s.FOLDER_ID, FILE_NAME = s.FILE_NAME,
  FILE_EXTENSION = s.FILE_EXTENSION, FILE_SIZE = s.FILE_SIZE, FILE_STATUS = s.FILE_STATUS,
  FILE_FOLDER_PATH = s.FILE_FOLDER_PATH, FILE_MANAGER_ID = s.FILE_MANAGER_ID,
  CREATE_TS = s.CREATE_TS, LAST_UPDATE_TS = s.LAST_UPDATE_TS, SERVICE_TYPE = s.SERVICE_TYPE,
  RAW = s.RAW, LOADED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  FILE_ID, ENTITY_ID, FOLDER_ID, FILE_NAME, FILE_EXTENSION, FILE_SIZE, FILE_STATUS,
  FILE_FOLDER_PATH, FILE_MANAGER_ID, CREATE_TS, LAST_UPDATE_TS, SERVICE_TYPE, RAW
) VALUES (
  s.FILE_ID, s.ENTITY_ID, s.FOLDER_ID, s.FILE_NAME, s.FILE_EXTENSION, s.FILE_SIZE,
  s.FILE_STATUS, s.FILE_FOLDER_PATH, s.FILE_MANAGER_ID, s.CREATE_TS, s.LAST_UPDATE_TS, s.SERVICE_TYPE, s.RAW
);
"""

MERGE_FOLDERS = """
MERGE INTO FCC_POLITICAL_FOLDERS t
USING (
  SELECT
    ENTITY_FOLDER_ID, ENTITY_ID, FOLDER_NAME, FOLDER_PATH, PARENT_FOLDER_ID,
    TRY_TO_TIMESTAMP_TZ(CREATE_TS) AS CREATE_TS, TRY_TO_TIMESTAMP_TZ(LAST_UPDATE_TS) AS LAST_UPDATE_TS,
    SERVICE_TYPE, TRY_PARSE_JSON(RAW) AS RAW
  FROM FCC_POLITICAL_FOLDERS_STAGE
) s
ON t.ENTITY_FOLDER_ID = s.ENTITY_FOLDER_ID
WHEN MATCHED THEN UPDATE SET
  ENTITY_ID = s.ENTITY_ID, FOLDER_NAME = s.FOLDER_NAME, FOLDER_PATH = s.FOLDER_PATH,
  PARENT_FOLDER_ID = s.PARENT_FOLDER_ID, CREATE_TS = s.CREATE_TS, LAST_UPDATE_TS = s.LAST_UPDATE_TS,
  SERVICE_TYPE = s.SERVICE_TYPE, RAW = s.RAW, LOADED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  ENTITY_FOLDER_ID, ENTITY_ID, FOLDER_NAME, FOLDER_PATH, PARENT_FOLDER_ID, CREATE_TS, LAST_UPDATE_TS, SERVICE_TYPE, RAW
) VALUES (
  s.ENTITY_FOLDER_ID, s.ENTITY_ID, s.FOLDER_NAME, s.FOLDER_PATH, s.PARENT_FOLDER_ID,
  s.CREATE_TS, s.LAST_UPDATE_TS, s.SERVICE_TYPE, s.RAW
);
"""

def sf_prepare(conn):
    with conn.cursor() as cur:
        for stmt in DDL_TARGET.strip().split(";\n\n"):
            s = stmt.strip()
            if s:
                cur.execute(s)
        for stmt in DDL_STAGE.strip().split(";\n\n"):
            s = stmt.strip()
            if s:
                cur.execute(s)

# ---------------------------- Flusher & helpers ------------------------------
class BufferFlusher:
    """
    Bounded, chunked flusher to keep memory flat and avoid OOM.

    - Buffers are capped by FLUSH_MAX_ROWS; exceeding it triggers an immediate flush.
    - Periodic flush still runs every FLUSH_EVERY_SECONDS.
    - Snowflake loads are chunked with STAGE_LOAD_CHUNK_ROWS.
    """
    def __init__(self, conn: snowflake.connector.SnowflakeConnection):
        self.conn = conn

        # Locks
        self.lock_fac = threading.Lock()
        self.lock_fol = threading.Lock()
        self.lock_fil = threading.Lock()
        self.flush_lock = threading.Lock()

        self.facilities: List[Dict[str, Any]] = []
        self.folders: List[Dict[str, Any]] = []
        self.files: List[Dict[str, Any]] = []

        self.stop_evt = threading.Event()
        self.flush_now_evt = threading.Event()

        self.thread = threading.Thread(target=self._run, name="flusher", daemon=True)

    def emit_facilities(self, rows):
        if not rows: return
        with self.lock_fac:
            self.facilities.extend(rows)
            if len(self.facilities) >= FLUSH_MAX_ROWS:
                self.flush_now_evt.set()

    def emit_folders(self, rows):
        if not rows: return
        with self.lock_fol:
            self.folders.extend(rows)
            if len(self.folders) >= FLUSH_MAX_ROWS:
                self.flush_now_evt.set()

    def emit_files(self, rows):
        if not rows: return
        with self.lock_fil:
            self.files.extend(rows)
            if len(self.files) >= FLUSH_MAX_ROWS:
                self.flush_now_evt.set()

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_evt.set()
        self.flush_now_evt.set()
        self.thread.join()
        self._flush_once()

    def _run(self):
        while not self.stop_evt.is_set():
            fired = self.flush_now_evt.wait(timeout=FLUSH_EVERY_SECONDS)
            try:
                self._flush_once()
            finally:
                if fired:
                    self.flush_now_evt.clear()

    def _pop_buffers(self):
        with self.lock_fac:
            fac = self.facilities; self.facilities = []
        with self.lock_fol:
            fol = self.folders; self.folders = []
        with self.lock_fil:
            fil = self.files; self.files = []
        return fac, fol, fil

    def _flush_once(self):
        if not self.flush_lock.acquire(blocking=False):
            return
        try:
            fac, fol, fil = self._pop_buffers()
            if not (fac or fol or fil):
                return

            # Keep Snowflake connection alive
            try:
                with self.conn.cursor() as c:
                    c.execute("SELECT 1")
            except Exception:
                logger.warning("Reconnecting Snowflake...")
                try:
                    self.conn.close()
                except Exception:
                    pass
                self.conn = sf_connect()
                sf_prepare(self.conn)

            def df_from(rows, cols):
                if not rows:
                    return pd.DataFrame(columns=cols)
                return pd.DataFrame.from_records(rows, columns=cols)

            def write_stage_chunks(df: pd.DataFrame, stage_table: str):
                if df is None or df.empty:
                    return
                with self.conn.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE {stage_table}")
                total = len(df)
                start = 0
                while start < total:
                    end = min(start + STAGE_LOAD_CHUNK_ROWS, total)
                    chunk = df.iloc[start:end]
                    ok, nchunks, nrows, _ = write_pandas(
                        self.conn,
                        chunk.reset_index(drop=True),   # reset index
                        table_name=stage_table,
                        quote_identifiers=False
                    )
                    logger.info("Loaded %s rows (%s:%s) into %s (ok=%s)",
                                nrows, start, end, stage_table, ok)
                    start = end

            def merge_into(merge_sql: str):
                with self.conn.cursor() as cur:
                    cur.execute(merge_sql)

            # Facilities
            df_fac = df_from(fac, FACILITIES_COLS)
            write_stage_chunks(df_fac, "FCC_FACILITIES_STAGE")
            if not df_fac.empty:
                merge_into(MERGE_FACILITIES)

            # Folders
            df_fol = df_from(fol, [
                "ENTITY_FOLDER_ID","ENTITY_ID","FOLDER_NAME","FOLDER_PATH",
                "PARENT_FOLDER_ID","CREATE_TS","LAST_UPDATE_TS","SERVICE_TYPE","RAW"
            ])
            write_stage_chunks(df_fol, "FCC_POLITICAL_FOLDERS_STAGE")
            if not df_fol.empty:
                merge_into(MERGE_FOLDERS)

            # Files
            df_fil = df_from(fil, [
                "FILE_ID","ENTITY_ID","FOLDER_ID","FILE_NAME","FILE_EXTENSION","FILE_SIZE",
                "FILE_STATUS","FILE_FOLDER_PATH","FILE_MANAGER_ID","CREATE_TS","LAST_UPDATE_TS",
                "SERVICE_TYPE","RAW"
            ])
            write_stage_chunks(df_fil, "FCC_POLITICAL_FILES_STAGE")
            if not df_fil.empty:
                merge_into(MERGE_FILES)

            logger.info("Flushed to Snowflake (fac=%d fol=%d fil=%d)", len(fac), len(fol), len(fil))

        except Exception as e:
            logger.exception("Flush failed: %s", e)
        finally:
            try:
                self.flush_lock.release()
            except Exception:
                pass

# ---------------------------- API callers & ingest ----------------------------
def _str(v): return "" if v is None else str(v).strip()

def iter_facilities_for_service(service_type: str):
    """
    Streaming version of facilities fetcher.
    Yields one facility dict at a time to avoid holding the entire set in RAM.
    """
    url = f"{BASE_URL}/api/service/{service_type}/facility/getall"
    r = http_get(url)
    r.raise_for_status()
    data = r.json() or {}

    if service_type == "cable":
        items = (data.get("results", {}) or {}).get("cableSystemsList", []) or []
        for c in items:
            yield {
                "ID": _str(c.get("psid")),
                "CALL_SIGN": "", "FREQUENCY": "", "ACTIVE_IND": _str(c.get("activeInd")),
                "SERVICE_TYPE": "cable",
                "CABLE_SYSTEM_ID": _str(c.get("cableSystemId")),
                "HEAD_END_ID": _str(c.get("headEndId")),
                "OPERATOR_ID": _str(c.get("operatorId")),
                "LEGAL_NAME": _str(c.get("legalName")),
                "APPLICATION_ID": _str(c.get("applicationId")),
                "STATUS": _str(c.get("status")),
                "STATUS_DATE": _str(c.get("statusDate")),
                "FRN": _str(c.get("frn")),
                "PSID": _str(c.get("psid")),
                "FACILITY_TYPE": _str(c.get("facilityType")),
                "OPERATOR_ADDRESS_LINE1": _str(c.get("operatorAddressLine1")),
                "OPERATOR_NAME": _str(c.get("operatorName")),
                "OPERATOR_ADDRESS_LINE2": _str(c.get("operatorAddressLine2")),
                "OPERATOR_PO_BOX": _str(c.get("operatorPoBox")),
                "OPERATOR_CITY": _str(c.get("operatorCity")),
                "OPERATOR_ZIP_CODE": _str(c.get("operatorZipCode")),
                "OPERATOR_ZIP_CODE_SUFFIX": _str(c.get("operatorZipCodeSuffix")),
                "OPERATOR_STATE": _str(c.get("operatorState")),
                "OPERATOR_EMAIL": _str(c.get("operatorEmail")),
                "OPERATOR_WEBSITE": _str(c.get("operatorWebsite")),
                "OPERATOR_PHONE": _str(c.get("operatorPhone")),
                "OPERATOR_FAX": _str(c.get("operatorFax")),
                "CORES_USER": _str(c.get("coresUser")),
                "PRINCIPAL_HEADEND_NAME": _str(c.get("principalHeadendName")),
                "PRINCIPAL_ADDRESS_LINE1": _str(c.get("principalAddressLine1")),
                "PRINCIPAL_ADDRESS_LINE2": _str(c.get("principalAddressLine2")),
                "PRINCIPAL_PO_BOX": _str(c.get("principalPoBox")),
                "PRINCIPAL_CITY": _str(c.get("principalCity")),
                "PRINCIPAL_STATE": _str(c.get("principalState")),
                "PRINCIPAL_ZIP_CODE": _str(c.get("principalZipCode")),
                "PRINCIPAL_ZIP_CODE_SUFFIX": _str(c.get("principalZipCodeSuffix")),
                "PRINCIPAL_FAX": _str(c.get("principalFax")),
                "PRINCIPAL_PHONE": _str(c.get("principalPhone")),
                "PRINCIPAL_EMAIL": _str(c.get("principalEmail")),
                "LOCAL_FILE_CONTACT_NAME": _str(c.get("localFileContactName")),
                "LOCAL_FILE_ADDRESS_LINE1": _str(c.get("localFileAddressLine1")),
                "LOCAL_FILE_ADDRESS_LINE2": _str(c.get("localFileAddressLine2")),
                "LOCAL_FILE_PO_BOX": _str(c.get("localFilePoBox")),
                "LOCAL_FILE_CITY": _str(c.get("localFileCity")),
                "LOCAL_FILE_STATE": _str(c.get("localFileState")),
                "LOCAL_FILE_ZIP_CODE": _str(c.get("localFileZipCode")),
                "LOCAL_FILE_ZIP_CODE_SUFFIX": _str(c.get("localFileZipCodeSuffix")),
                "LOCAL_FILE_CONTACT_FAX": _str(c.get("localFileContactFax")),
                "LOCAL_FILE_CONTACT_PHONE": _str(c.get("localFileContactPhone")),
                "LOCAL_FILE_CONTACT_EMAIL": _str(c.get("localFileContactEmail")),
                "ACCESS_TOKEN": _str(c.get("accessToken")),
                "PRINCIPAL_ADDRESS_IN_LOCAL_FILES": _str(c.get("principalAddressInLocalFiles")),
                "CABLE_SERVICE_ZIP_CODES": json.dumps(c.get("cableServiceZipCodes")),
                "CABLE_SERVICE_EMP_UNITS": json.dumps(c.get("cableServiceEmpUnits")),
                "CABLE_COMMUNITIES": json.dumps(c.get("cableCommunities")),
                "CENEMAIL": _str(c.get("cenemail")),
                "CENPHONE": _str(c.get("cenphone")),
                "RAW": json.dumps(c, ensure_ascii=False),
            }
    else:
        items = (data.get("results", {}) or {}).get("facilityList", []) or []
        for f in items:
            yield {
                "ID": _str(f.get("id")),
                "CALL_SIGN": _str(f.get("callSign")),
                "FREQUENCY": _str(f.get("frequency")),
                "ACTIVE_IND": _str(f.get("activeInd")),
                "SERVICE_TYPE": service_type,
                **{k: "" for k in CABLE_FACILITY_COLS},
                "CABLE_SERVICE_ZIP_CODES": json.dumps(None),
                "CABLE_SERVICE_EMP_UNITS": json.dumps(None),
                "CABLE_COMMUNITIES": json.dumps(None),
                "RAW": json.dumps(f, ensure_ascii=False),
            }

def fetch_political_search_for_facility(facility: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    fac_id = facility["ID"]
    service_type = facility["SERVICE_TYPE"]
    year = OVERRIDE_YEAR if isinstance(OVERRIDE_YEAR, int) else datetime.now(timezone.utc).year

    folders: List[Dict[str, Any]] = []
    files: List[Dict[str, Any]] = []
    seen_folder_ids: set[str] = set()
    seen_file_ids: set[str] = set()

    for y in (year,):
        url = f"{BASE_URL}/api/manager/search/key/Political%20Files.json"
        r = http_get(url, params={"entityId": fac_id})
        if r.status_code >= 400 or not r.content:
            continue
        data = r.json() or {}
        sr = (data or {}).get("searchResult", {}) or {}
        for fo in (sr.get("folders") or []):
            efid = _str(fo.get("entity_folder_id"))
            if efid and efid not in seen_folder_ids:
                seen_folder_ids.add(efid)
                folders.append({
                    "ENTITY_FOLDER_ID": efid,
                    "ENTITY_ID": _str(fo.get("entity_id")),
                    "FOLDER_NAME": _str(fo.get("folder_name")),
                    "FOLDER_PATH": _str(fo.get("folder_path")),
                    "PARENT_FOLDER_ID": _str(fo.get("parent_folder_id")),
                    "CREATE_TS": _str(fo.get("create_ts")),
                    "LAST_UPDATE_TS": _str(fo.get("last_update_ts")),
                    "SERVICE_TYPE": service_type,
                    "RAW": json.dumps(fo, ensure_ascii=False),
                })
        for fi in (sr.get("files") or []):
            fid = _str(fi.get("file_id"))
            if fid and fid not in seen_file_ids:
                seen_file_ids.add(fid)
                files.append({
                    "FILE_ID": fid,
                    "ENTITY_ID": fac_id,
                    "FOLDER_ID": _str(fi.get("folder_id")),
                    "FILE_NAME": _str(fi.get("file_name")),
                    "FILE_EXTENSION": _str(fi.get("file_extension")),
                    "FILE_SIZE": _str(fi.get("file_size")),
                    "FILE_STATUS": _str(fi.get("file_status")),
                    "FILE_FOLDER_PATH": _str(fi.get("file_folder_path")),
                    "FILE_MANAGER_ID": _str(fi.get("file_manager_id")),
                    "CREATE_TS": _str(fi.get("create_ts")),
                    "LAST_UPDATE_TS": _str(fi.get("last_update_ts")),
                    "SERVICE_TYPE": service_type,
                    "RAW": json.dumps(fi, ensure_ascii=False),
                })
    return {"folders": folders, "files": files}

# ---------------------------- Public entry: run_slice -------------------------
def run_slice(service_type: str, ctx: InvokeCtx, sf_cfg: SFConfig, overrides: dict | None = None) -> dict:
    """
    Process exactly ONE service type (tv/fm/am/cable).
    Returns a small summary dict.
    
    Args:
        service_type: One of "tv", "fm", "am", "cable"
        ctx: Invocation context for logging
        sf_cfg: Snowflake connection configuration
        overrides: Optional runtime overrides
    """
    if service_type not in {"tv", "fm", "am", "cable"}:
        return {"ok": False, "error": f"invalid service_type: {service_type}"}

    # Apply per-call overrides (safe)
    _apply_overrides_local(overrides)

    # Set invocation context for API logging
    global _invoke_ctx
    _invoke_ctx = ctx

    # Set logging database from config
    global _log_database
    _log_database = sf_cfg.database

    # Connect Snowflake & prep
    conn = sf_connect(sf_cfg)
    global _conn_for_logs
    _conn_for_logs = conn

    # 1) Ensure DDL and START row for the ADF pipeline run
    sf_prepare(conn)
    _adf_run_start(conn, ctx, parameters={
        "service_type": service_type,
        "overrides": overrides or {}
    })

    flusher = BufferFlusher(conn)
    flusher.start()

    stats = {"facilities": 0, "folders": 0, "files": 0}
    run_status = "Succeeded"
    err_summary = None
    err_details = None

    try:
        # Stream facilities; keep in-flight futures bounded
        in_flight_limit = max(1, MAX_WORKERS_SEARCH * 2)
        futs = set()
        count_fac = 0
        emit_batch: List[Dict[str, Any]] = []

        def _handle_future(fut):
            nonlocal stats
            try:
                res = fut.result()
                fos = res.get("folders") or []
                fis = res.get("files") or []
                if fos:
                    flusher.emit_folders(fos)
                    stats["folders"] += len(fos)
                if fis:
                    flusher.emit_files(fis)
                    stats["files"] += len(fis)
            except Exception as e:
                logger.warning("Search fetch error: %s", e)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS_SEARCH) as ex:
            for fac in iter_facilities_for_service(service_type):
                count_fac += 1
                emit_batch.append(fac)
                if len(emit_batch) >= FACILITIES_EMIT_BATCH:
                    flusher.emit_facilities(emit_batch)
                    emit_batch = []

                # submit and bound in-flight
                futs.add(ex.submit(fetch_political_search_for_facility, fac))
                if len(futs) >= in_flight_limit:
                    done, _ = wait(futs, return_when=FIRST_COMPLETED)
                    for fut in done:
                        _handle_future(fut)
                        futs.remove(fut)

            # emit remaining facilities
            if emit_batch:
                flusher.emit_facilities(emit_batch)
                emit_batch = []

            # drain remaining futures
            if futs:
                for fut in as_completed(futs):
                    _handle_future(fut)

        stats["facilities"] = count_fac

    except Exception as e:
        run_status = "Failed"
        err_summary = str(e)[:1024]
        err_details = {"type": type(e).__name__}
        logger.exception("run_slice failure: %s", e)
        raise

    finally:
        flusher.stop()
        try:
            _adf_run_end(conn, ctx, status=run_status, err_summary=err_summary, err_details=err_details)
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    return {"ok": True}
