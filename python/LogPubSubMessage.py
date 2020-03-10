

def sink_pubsub_to_logs(event, context):
    """Logs a pubsub message"""
    import base64
    data = base64.b64decode(event['data']).decode('utf-8') if 'data' in event else "none"
    print('PubSub messageId {} published at {} with data: {}'.format(context.event_id, context.timestamp, data))