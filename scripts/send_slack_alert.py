#!/usr/bin/env python3
"""
Send Slack notifications for pipeline status
Usage: python scripts/send_slack_alert.py "Pipeline completed successfully"
"""

import sys
import os
import requests
import json
from typing import Optional

def send_slack_alert(
    message: str,
    webhook_url: Optional[str] = None,
    status: str = "info"
) -> bool:
    """
    Send a Slack notification
    
    Args:
        message: Message to send
        webhook_url: Slack webhook URL (or set SLACK_WEBHOOK_URL env var)
        status: Status type (success, failure, warning, info)
    
    Returns:
        True if successful, False otherwise
    """
    webhook_url = webhook_url or os.getenv('SLACK_WEBHOOK_URL')
    
    if not webhook_url:
        print("⚠️  No Slack webhook URL provided. Set SLACK_WEBHOOK_URL env var or pass webhook_url parameter.")
        return False
    
    # Status emoji mapping
    emoji_map = {
        "success": ":white_check_mark:",
        "failure": ":x:",
        "warning": ":warning:",
        "info": ":information_source:"
    }
    
    emoji = emoji_map.get(status, ":information_source:")
    
    payload = {
        "text": f"{emoji} {message}",
        "username": "Healthcare Analytics Pipeline",
        "icon_emoji": ":hospital:",
        "attachments": [
            {
                "color": "good" if status == "success" else "danger" if status == "failure" else "warning",
                "text": message,
                "footer": "Healthcare Analytics Platform",
                "ts": int(os.path.getmtime(__file__)) if os.path.exists(__file__) else None
            }
        ]
    }
    
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        print(f"✅ Slack notification sent successfully")
        return True
    except Exception as e:
        print(f"❌ Failed to send Slack notification: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python send_slack_alert.py 'message' [status]")
        print("Status options: success, failure, warning, info")
        sys.exit(1)
    
    message = sys.argv[1]
    status = sys.argv[2] if len(sys.argv) > 2 else "info"
    
    success = send_slack_alert(message, status=status)
    sys.exit(0 if success else 1)

