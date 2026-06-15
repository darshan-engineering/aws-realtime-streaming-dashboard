import boto3
import json
import base64
import os
from datetime import datetime, timezone

dynamodb = boto3.resource('dynamodb')
state_table = dynamodb.Table('stream-state')
connections_table = dynamodb.Table('connections')

def lambda_handler(event, context):
    latest = None

    # Process all records in the batch — keep only the last state per entity
    for record in event['Records']:
        payload = json.loads(
            base64.b64decode(record['kinesis']['data']).decode('utf-8')
        )
        latest = payload

    if not latest:
        return

    # Write latest state to DynamoDB (overwrites previous state for this entity_id)
    latest['last_updated'] = datetime.now(timezone.utc).isoformat()
    state_table.put_item(Item=latest)

    # Push update to all connected WebSocket clients
    api_id = os.environ['WEBSOCKET_API_ID']
    stage = os.environ['WEBSOCKET_STAGE']
    region = os.environ['AWS_REGION']

    apigw = boto3.client(
        'apigatewaymanagementapi',
        endpoint_url=f"https://{api_id}.execute-api.{region}.amazonaws.com/{stage}"
    )

    connections = connections_table.scan().get('Items', [])
    message = json.dumps(latest).encode('utf-8')

    for conn in connections:
        conn_id = conn['connection_id']
        try:
            apigw.post_to_connection(ConnectionId=conn_id, Data=message)
        except apigw.exceptions.GoneException:
            # Client disconnected without $disconnect firing — clean up stale entry
            connections_table.delete_item(Key={'connection_id': conn_id})
