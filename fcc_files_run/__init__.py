# fcc_files_run/__init__.py

import os
import logging
from fcc_files_lib import run_slice, InvokeCtx, SFConfig
from slack_notifier import SlackConfig, create_fcc_notifier

logger = logging.getLogger("do_slice")

def main(input: dict) -> dict:
    """
    Activity: process exactly ONE service type (tv/fm/am/cable).
    
    Payload can include:
      run_id, pipeline_name, activity_run_id, activity_name, correlation_id, env_name,
      requests_per_second, flush_every_seconds, http_timeout, batch_log_every, year,
      slack_success_channel, slack_failure_channel, slack_notification_channel,
      send_failure, send_success, send_info,
      sf_account, sf_user, sf_role, sf_warehouse, sf_database, sf_schema,
      sf_private_key_path, sf_private_key_text, sf_private_key_pass
    
    Snowflake config falls back to environment variables:
      SF_ACCOUNT, SF_USER, SF_ROLE, SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA,
      SF_PRIVATE_KEY_PATH, SF_PRIVATE_KEY_TEXT, SF_PRIVATE_KEY_PASS
    """
    service_type = (input or {}).get("service_type")
    payload = (input or {}).get("payload") or {}

    ctx = InvokeCtx(
        run_id=payload.get("run_id"),
        activity_run_id=payload.get("activity_run_id"),
        pipeline_name=payload.get("pipeline_name"),
        activity_name=payload.get("activity_name"),
        correlation_id=payload.get("correlation_id") or payload.get("run_id"),
        env_name=payload.get("env_name") or None
    )

    # Build Snowflake configuration from payload, falling back to environment variables
    # Support both 'sf_*' and 'snowflake_*' naming conventions for private key fields
    sf_cfg = SFConfig(
        account=payload.get("sf_account") or os.getenv("SF_ACCOUNT"),
        user=payload.get("sf_user") or os.getenv("SF_USER"),
        role=payload.get("sf_role") or os.getenv("SF_ROLE"),
        warehouse=payload.get("sf_warehouse") or os.getenv("SF_WAREHOUSE"),
        database=payload.get("sf_database") or os.getenv("SF_DATABASE"),
        schema=payload.get("sf_schema") or os.getenv("SF_SCHEMA"),
        private_key_path=payload.get("sf_private_key_path") or payload.get("snowflake_private_key_path") or os.getenv("SF_PRIVATE_KEY_PATH"),
        private_key_text=payload.get("sf_private_key_text") or payload.get("snowflake_private_key_text") or os.getenv("SF_PRIVATE_KEY_TEXT"),
        private_key_pass=payload.get("sf_private_key_pass") or payload.get("snowflake_private_key_pass") or os.getenv("SF_PRIVATE_KEY_PASS")
    )

    overrides = {
        "requests_per_second": payload.get("requests_per_second"),
        "flush_every_seconds": payload.get("flush_every_seconds"),
        "http_timeout": payload.get("http_timeout"),
        "batch_log_every": payload.get("batch_log_every"),
        "year": payload.get("year")
    }

    # Set up Slack notifications
    slack_config = SlackConfig.from_payload(payload)
    notifier = create_fcc_notifier(slack_config)
    notifier.start_timer()
    run_id = payload.get("run_id")

    logger.info("Starting slice for service_type=%s", service_type)
    
    try:
        res = run_slice(service_type, ctx, sf_cfg, overrides)
        logger.info("Completed slice for service_type=%s", service_type)
        
        notifier.send_success(
            message=f"FCC Public Files List ingestion completed successfully for {service_type}.",
            run_id=run_id,
            details={
                "Service Type": service_type,
                "Runtime": notifier.get_runtime_formatted()
            }
        )
        return {"service_type": service_type, "result": res}
    except Exception as e:
        logger.exception("FCC slice failed for service_type=%s: %s", service_type, e)
        notifier.send_failure(
            message=f"FCC Public Files List ingestion failed for {service_type}.",
            run_id=run_id,
            error=str(e),
            details={
                "Service Type": service_type,
                "Runtime": notifier.get_runtime_formatted()
            }
        )
        raise
