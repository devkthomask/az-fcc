# slack_notifier.py

"""
Slack notification handler for SMS Azure Functions.
Supports separate channels for success, failure, and info notifications.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Optional, Any
from dataclasses import dataclass
import requests

logger = logging.getLogger("slack_notifier")


@dataclass
class SlackConfig:
    """Configuration for Slack notifications"""
    # Webhook URLs for different notification types
    slack_success_channel: Optional[str] = None
    slack_failure_channel: Optional[str] = None
    slack_notification_channel: Optional[str] = None
    
    # Notification toggles
    send_failure: bool = True    # Enabled by default
    send_success: bool = False   # Disabled by default
    send_info: bool = False      # Disabled by default
    
    @classmethod
    def from_payload(cls, payload: dict) -> "SlackConfig":
        """Create SlackConfig from a request payload dict"""
        return cls(
            slack_success_channel=payload.get("slack_success_channel"),
            slack_failure_channel=payload.get("slack_failure_channel"),
            slack_notification_channel=payload.get("slack_notification_channel"),
            send_failure=payload.get("send_failure", True),
            send_success=payload.get("send_success", False),
            send_info=payload.get("send_info", False),
        )


class SlackNotifier:
    """Handles Slack notifications with client and ETL context"""
    
    CLIENT_NAME = "SMS"
    
    def __init__(self, config: SlackConfig, etl_type: str, object_type: Optional[str] = None):
        """
        Initialize the Slack notifier.
        
        Args:
            config: SlackConfig with webhook URLs and notification toggles
            etl_type: The ETL type (e.g., "The Trade Desk", "FCC Public Files List")
            object_type: Optional object type for TTD (e.g., "Ad Groups", "Advertisers", etc.)
        """
        self.config = config
        self.etl_type = etl_type
        self.object_type = object_type
        self._start_time: Optional[datetime] = None
    
    def start_timer(self):
        """Start the runtime timer"""
        self._start_time = datetime.now(timezone.utc)
    
    def get_runtime_seconds(self) -> Optional[float]:
        """Get the runtime in seconds since start_timer was called"""
        if self._start_time:
            return (datetime.now(timezone.utc) - self._start_time).total_seconds()
        return None
    
    def get_runtime_formatted(self) -> str:
        """Get a human-readable runtime string"""
        seconds = self.get_runtime_seconds()
        if seconds is None:
            return "N/A"
        
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes}m {secs}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"
    
    def _get_webhook_url(self, notification_type: str) -> Optional[str]:
        """
        Get the appropriate webhook URL based on notification type.
        
        Args:
            notification_type: One of 'success', 'failure', or 'info'
        
        Returns:
            The webhook URL or None if not configured
        """
        if notification_type == "success":
            # Try success channel first, fall back to general
            return self.config.slack_success_channel or self.config.slack_notification_channel
        elif notification_type == "failure":
            # Try failure channel first, fall back to general
            return self.config.slack_failure_channel or self.config.slack_notification_channel
        elif notification_type == "info":
            return self.config.slack_notification_channel
        return None
    
    def _should_send(self, notification_type: str) -> bool:
        """Check if we should send this type of notification"""
        if notification_type == "success":
            return self.config.send_success
        elif notification_type == "failure":
            return self.config.send_failure
        elif notification_type == "info":
            return self.config.send_info
        return False
    
    def _build_etl_display(self) -> str:
        """Build the ETL type display string"""
        if self.object_type:
            return f"{self.etl_type} - {self.object_type}"
        return self.etl_type
    
    def _send_notification(self, webhook_url: str, payload: dict) -> bool:
        """
        Send a notification to Slack.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            response = requests.post(webhook_url, json=payload, timeout=30)
            if response.status_code != 200:
                logger.warning("Slack notification failed: %s - %s", response.status_code, response.text)
                return False
            return True
        except Exception as e:
            logger.warning("Failed to send Slack notification: %s", e)
            return False
    
    def send_success(self, message: str, run_id: Optional[str] = None,
                     details: Optional[Dict[str, Any]] = None):
        """
        Send a success notification.
        
        Args:
            message: The main message to display
            run_id: Optional run identifier
            details: Optional additional details to include
        """
        if not self._should_send("success"):
            return
        
        webhook_url = self._get_webhook_url("success")
        if not webhook_url:
            return
        
        fields = [
            {"title": "Client", "value": self.CLIENT_NAME, "short": True},
            {"title": "Status", "value": ":white_check_mark: Success", "short": True},
            {"title": "ETL Type", "value": self._build_etl_display(), "short": True},
        ]
        
        runtime = self.get_runtime_formatted()
        if runtime != "N/A":
            fields.append({"title": "Runtime", "value": runtime, "short": True})
        
        if run_id:
            fields.append({"title": "Run ID", "value": run_id, "short": True})
        
        if details:
            for key, value in details.items():
                fields.append({"title": key, "value": str(value), "short": True})
        
        payload = {
            "attachments": [{
                "color": "good",
                "title": f"{self.CLIENT_NAME} ETL Notification",
                "text": message,
                "fields": fields,
                "footer": f"{self._build_etl_display()} ETL",
                "ts": int(datetime.now(timezone.utc).timestamp())
            }]
        }
        
        self._send_notification(webhook_url, payload)
    
    def send_failure(self, message: str, run_id: Optional[str] = None,
                     error: Optional[str] = None,
                     failed_files: Optional[list] = None,
                     details: Optional[Dict[str, Any]] = None):
        """
        Send a failure notification.
        
        Args:
            message: The main message to display
            run_id: Optional run identifier
            error: Optional error message/summary
            failed_files: Optional list of files that failed
            details: Optional additional details to include
        """
        if not self._should_send("failure"):
            return
        
        webhook_url = self._get_webhook_url("failure")
        if not webhook_url:
            return
        
        fields = [
            {"title": "Client", "value": self.CLIENT_NAME, "short": True},
            {"title": "Status", "value": ":x: Failed", "short": True},
            {"title": "ETL Type", "value": self._build_etl_display(), "short": True},
        ]
        
        runtime = self.get_runtime_formatted()
        if runtime != "N/A":
            fields.append({"title": "Runtime", "value": runtime, "short": True})
        
        if run_id:
            fields.append({"title": "Run ID", "value": run_id, "short": True})
        
        if error:
            # Truncate error if too long
            error_display = error[:500] + "..." if len(error) > 500 else error
            fields.append({"title": "Error", "value": error_display, "short": False})
        
        if failed_files:
            # Show up to 10 failed files
            files_display = failed_files[:10]
            if len(failed_files) > 10:
                files_display.append(f"... and {len(failed_files) - 10} more")
            fields.append({
                "title": f"Failed Files ({len(failed_files)})",
                "value": "\n".join(str(f) for f in files_display),
                "short": False
            })
        
        if details:
            for key, value in details.items():
                fields.append({"title": key, "value": str(value), "short": True})
        
        payload = {
            "attachments": [{
                "color": "danger",
                "title": f"{self.CLIENT_NAME} ETL Notification",
                "text": message,
                "fields": fields,
                "footer": f"{self._build_etl_display()} ETL",
                "ts": int(datetime.now(timezone.utc).timestamp())
            }]
        }
        
        self._send_notification(webhook_url, payload)
    
    def send_info(self, message: str, run_id: Optional[str] = None,
                  details: Optional[Dict[str, Any]] = None):
        """
        Send an info notification.
        
        Args:
            message: The main message to display
            run_id: Optional run identifier
            details: Optional additional details to include
        """
        if not self._should_send("info"):
            return
        
        webhook_url = self._get_webhook_url("info")
        if not webhook_url:
            return
        
        fields = [
            {"title": "Client", "value": self.CLIENT_NAME, "short": True},
            {"title": "Status", "value": ":information_source: Info", "short": True},
            {"title": "ETL Type", "value": self._build_etl_display(), "short": True},
        ]
        
        runtime = self.get_runtime_formatted()
        if runtime != "N/A":
            fields.append({"title": "Runtime", "value": runtime, "short": True})
        
        if run_id:
            fields.append({"title": "Run ID", "value": run_id, "short": True})
        
        if details:
            for key, value in details.items():
                fields.append({"title": key, "value": str(value), "short": True})
        
        payload = {
            "attachments": [{
                "color": "#439FE0",  # Blue for info
                "title": f"{self.CLIENT_NAME} ETL Notification",
                "text": message,
                "fields": fields,
                "footer": f"{self._build_etl_display()} ETL",
                "ts": int(datetime.now(timezone.utc).timestamp())
            }]
        }
        
        self._send_notification(webhook_url, payload)


# Convenience factory functions for specific ETL types

def create_ttd_notifier(config: SlackConfig, object_type: str) -> SlackNotifier:
    """
    Create a Slack notifier for The Trade Desk ETL.
    
    Args:
        config: SlackConfig with webhook URLs and notification toggles
        object_type: The TTD object type (e.g., "Ad Groups", "Advertisers", 
                     "Campaigns", "Creatives", "Creatives Daily Reporting", "Partners")
    """
    return SlackNotifier(config, "The Trade Desk", object_type)


def create_fcc_notifier(config: SlackConfig) -> SlackNotifier:
    """
    Create a Slack notifier for FCC Public Files List ETL.
    
    Args:
        config: SlackConfig with webhook URLs and notification toggles
    """
    return SlackNotifier(config, "FCC Public Files List")
