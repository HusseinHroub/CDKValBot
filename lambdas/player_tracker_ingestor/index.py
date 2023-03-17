import json

import boto3
from boto3.dynamodb.conditions import Key
import os

table = boto3.resource('dynamodb').Table(os.environ['dynamoTableName'])
queue = boto3.resource('sqs').Queue(os.environ['queueURL'])


def lambda_handler(event, context):
    items = get_items()
    send_to_sqs(items)


def get_items():
    response = table.query(
        KeyConditionExpression=Key('partn').eq('0')
    )
    items = response['Items']
    while response.get('LastEvaluatedKey') is not None:
        response = table.query(
            KeyConditionExpression=Key('partn').eq('0'),
            ExclusiveStartKey=response.get('LastEvaluatedKey'),
        )
        items.extend(response['Items'])
    return items


def send_to_sqs(items):
    batch_size = 10
    for i in range(0, len(items), batch_size):
        items_batch_entries = [{
            'Id': item.get('puuid'),
            'MessageBody': json.dumps({
                'puuid': item.get('puuid'),
                'partn': item.get('partn')
            })
        } for item in items[i:i + batch_size]]
        queue.send_messages(Entries=items_batch_entries)
