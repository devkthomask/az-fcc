# start_fcc_files/__init__.py

import json
import logging
from urllib.parse import urlsplit, urlunsplit

import azure.functions as func
import azure.durable_functions as df

logger = logging.getLogger("start_ingest")

DEFAULT_SERVICE_TYPES = ["tv", "fm", "am", "cable"]

def _base_origin(req: func.HttpRequest) -> str:
    """
    Build scheme://host from the incoming request in a way that works locally and in Azure.
    """
    parts = urlsplit(req.url)
    host = req.headers.get("X-Forwarded-Host") or req.headers.get("Host") or parts.netloc
    scheme = req.headers.get("X-Forwarded-Proto") or parts.scheme or "https"
    return urlunsplit((scheme, host, "", "", ""))  # e.g. https://myapp.azurewebsites.net

async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    """
    HTTP starter. ADF calls this endpoint (POST).
    Body can include:
      - run_id, activity_run_id, pipeline_name, activity_name, correlation_id, env_name
      - service_types (subset of ["tv","fm","am","cable"])
      - requests_per_second, flush_every_seconds, http_timeout, batch_log_every, year
      
    Slack notification parameters:
      - slack_success_channel: Webhook URL for success notifications
      - slack_failure_channel: Webhook URL for failure notifications  
      - slack_notification_channel: Webhook URL for all notifications (fallback)
      - send_failure: Send failure notifications (default: true)
      - send_success: Send success notifications (default: false)
      - send_info: Send info notifications (default: false)
    """
    client = df.DurableOrchestrationClient(starter)

    try:
        payload = req.get_json() if req.get_body() else {}
    except Exception:
        payload = {}

    st = payload.get("service_types")
    if not isinstance(st, list) or not all(s in {"tv", "fm", "am", "cable"} for s in st):
        payload["service_types"] = DEFAULT_SERVICE_TYPES

    # IMPORTANT: await start_new so instance_id is a string, not a coroutine
    instance_id = await client.start_new("fcc_files_orchestrator", None, payload)

    # Try public helper first (works on current durable versions)
    try:
        return client.create_check_status_response(req, instance_id)
    except Exception as e:
        logger.warning("create_check_status_response failed; returning manual payload. %s", e)

    # Fallback: construct standard management URLs without touching private members
    origin = _base_origin(req)
    status_url = f"{origin}/runtime/webhooks/durabletask/instances/{instance_id}"
    payload_out = {
        "id": instance_id,
        "instanceId": instance_id,
        "runtimeStatusUri": status_url,
        "statusQueryGetUri": status_url,
        "sendEventPostUri": f"{status_url}/raiseEvent/{{eventName}}",
        "terminatePostUri": f"{status_url}/terminate",
        "purgeHistoryDeleteUri": f"{status_url}"
    }
    return func.HttpResponse(
        body=json.dumps(payload_out),
        status_code=202,
        mimetype="application/json"
    )
