import json
import logging
import os
import urllib.request

log = logging.getLogger(__name__)

def notify_failure(context):
    # on_failure_callback: POST a short message to the webhook in ALERT_WEBHOOK_URL.
    # no-op if the var is unset so local runs without a webhook don't error.
    url = os.getenv("ALERT_WEBHOOK_URL")
    if not url:
        return
    ti = context.get("task_instance")
    msg = f"Airflow task failed: {ti.dag_id}.{ti.task_id} (run {context.get('ds')})"
    # send both keys so a Slack ("text") or Discord ("content") webhook both work
    payload = json.dumps({"text": msg, "content": msg}).encode()
    req = urllib.request.Request(
        url, data=payload, headers={"Content-Type": "application/json"}
    )
    try:
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:  # network/webhook errors shouldn't mask the real task failure
        log.warning("could not post failure alert: %s", e)
