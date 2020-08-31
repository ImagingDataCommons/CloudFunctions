
import base64
import json
import os
from google.cloud import storage

def sink_budget_alert_to_logs(event, context):

    STATE_BUCKET = os.environ["STATE_BUCKET"]
    STATE_BLOB = os.environ["STATE_BLOB"]
    MAX_MSG = int(os.environ["MAX_MSG"])

    """Logs a pubsub message, but only the first MAX_MSG times we see it per month (used for budget alerts)"""

    data_str = base64.b64decode(event['data']).decode('utf-8') if 'data' in event else "none"
    if data_str == "none":
        print('PubSub messageId {} published at {} with data: {}'.format(context.event_id, context.timestamp, data_str))
        return

    json_data = json.loads(data_str)
    cis = json_data["costIntervalStart"]
    thresh = str(json_data["alertThresholdExceeded"])

    storage_client = storage.Client()

    bucket = storage_client.get_bucket(STATE_BUCKET)
    blob = bucket.blob(STATE_BLOB)
    blob_str = blob.download_as_string()
    last_record = None if (blob_str == b'') else json.loads(blob.download_as_string())

    new_alerts = None
    if last_record is None:
        new_alerts = {
            cis: {thresh: 1}
        }
    else:
        alerts = last_record["alerts"]
        if cis in alerts:
            thresh_map = alerts[cis]
            if thresh not in thresh_map:
                new_alerts = alerts.copy()
                new_thresh_map = thresh_map.copy()
                new_thresh_map[thresh] = 1
                new_alerts[cis] = new_thresh_map
            elif thresh_map[thresh] < MAX_MSG:
                new_alerts = alerts.copy()
                new_thresh_map = thresh_map.copy()
                new_thresh_map[thresh] = thresh_map[thresh] + 1
                new_alerts[cis] = new_thresh_map
        else:
            new_alerts = {
                cis: {thresh: 1}
            }
    if new_alerts is not None:
        print('PubSub messageId {} published at {} with data: {}'.format(context.event_id, context.timestamp, data_str))
        new_record = {
            "alerts": new_alerts
        }
        # Seems we get a checksum complaint if we don't reinitialize the blob:
        blob = bucket.blob(STATE_BLOB)
        blob.upload_from_string(json.dumps(new_record))

    return
