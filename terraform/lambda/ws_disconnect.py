import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('connections')

def lambda_handler(event, context):
    connection_id = event['requestContext']['connectionId']
    table.delete_item(Key={'connection_id': connection_id})
    return {'statusCode': 200}
