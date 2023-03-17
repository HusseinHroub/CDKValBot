import boto3
import os
import random
import requests
import time
from datetime import datetime

dynamodb = boto3.client('dynamodb')

playerDataTrackingTableName = os.environ['playerDataTrackingTableName']
DISCORD_HOOK_URI = ""
DISCORD_BOT_AUTH_VALUE = ""


def lambda_handler(event, context):
    date = datetime.utcnow()
    month = date.month
    postToDiscordAPI({
        "title": "Month Result",
        "description": f"Summoner stats of this month",
        "color": 0x27966b,
        "fields": [getTopPlayerField('kills', month), getTopPlayerField('deaths', month),
                   getTopPlayerField('assists', month), getTopPlayerField('kda', month),
                   getTopPlayerField('winrate', month)]
    })


def getTopPlayerBasedOnMetric(metric, month):
    response = dynamodb.query(
        TableName=playerDataTrackingTableName,
        IndexName=f'month-{metric}-index',
        Limit=1,
        ScanIndexForward=False,
        ReturnConsumedCapacity='NONE',
        KeyConditionExpression='#month = :month',
        ExpressionAttributeNames={
            '#month': 'month'
        },
        ExpressionAttributeValues={
            ':month': {
                'S': month
            }
        }
    )
    return response['Items'][0]


def getTopPlayerField(metric, month):
    player_data = getTopPlayerBasedOnMetric(metric, month)
    return {'name': f'Top {metric}',
            'value': f'{player_data["pname"]["S"]} with {player_data[metric]["N"]} {metric.lower()}', 'inline': False}


def postToDiscordAPI(embeds):
    retry_attempts = 3
    for i in range(retry_attempts):
        response = requests.post(DISCORD_HOOK_URI, headers={
            "Authorization": DISCORD_BOT_AUTH_VALUE
        }, json={
            "embeds": embeds
        })
        if int(response.status_code / 100) != 2:
            if response.status_code == 429:
                print(f'discord sleeping {response.json().get("retry_after")} seconds')
                time.sleep(response.json().get('retry_after') + random.random() + 2)
            else:
                print('discord sleeping 3 seconds')
                time.sleep(3)
        else:
            return
    raise Exception(f"Cant invoke discord notification endpoint!")
