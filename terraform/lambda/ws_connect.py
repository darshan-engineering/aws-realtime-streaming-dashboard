import boto3
from datetime import datetime, timezone

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('connections')

def lambda_handler(event, context):
    connection_id = event['requestContext']['connectionId']
    table.put_item(Item={
        'connection_id': connection_id,
        'connected_at': datetime.now(timezone.utc).isoformat()
    })
    return {'statusCode': 200}
