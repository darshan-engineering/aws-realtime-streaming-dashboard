import boto3
import json

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('stream-state')

def lambda_handler(event, context):
    items = table.scan().get('Items', [])
    return {
        'statusCode': 200,
        'headers': {'Access-Control-Allow-Origin': '*'},
        'body': json.dumps(items, default=str)
    }
