import json
import time

import boto3
import requests
import os

API_KEY = ''
ROOT = 'https://eu.api.riotgames.com/val/match/v1/matchlists/by-puuid'
queue = boto3.resource('sqs').Queue(os.environ['playerMatchQueueURL'])
table = boto3.resource('dynamodb').Table(os.environ['dynamoTableName'])


def lambda_handler(event, context):
    if len(event.get('Records')) > 1:
        raise Exception("Ask hussein.")
    record = event.get('Records')[0]
    body = json.loads(record.get('body'))
    puuid = body.get('puuid')
    partn = body.get('partn')
    limit_time = get_limit_time(partn, puuid)
    match_history = get_latest_match_history(limit_time, puuid)
    if len(match_history) > 0:
        send_to_sqs(match_history, partn, puuid)
        update_last_played_time(match_history[len(match_history) - 1].get('gameStartTimeMillis') + 1, partn, puuid)


def update_last_played_time(last_game_time_stamp, partn, puuid):
    response = table.update_item(
        Key={
            'partn': partn,
            'puuid': puuid
        },
        UpdateExpression='SET lgts=:lgts',
        ExpressionAttributeValues={
            ":lgts": last_game_time_stamp
        },
        ReturnValues='NONE',
        ReturnConsumedCapacity='NONE'
    )


def get_latest_match_history(limit_time, puuid):
    match_list_uri = f'{ROOT}/{puuid}?api_key={API_KEY}'
    match_list = getHTTPJsonResponse(match_list_uri)
    match_history = match_list.get('history')
    result = []
    if len(match_history) == 0 or match_history[0].get('gameStartTimeMillis') < limit_time:
        return result
    for i, x in enumerate(match_history):
        if x.get('gameStartTimeMillis') < limit_time:
            result = match_history[0:i]
            break
    if len(result) == 0:
        result = match_history
    result.reverse()
    return [
        {
            'puuid': puuid,
            **match
        } for match in result
    ]


def get_limit_time(partn, puuid):
    response = table.get_item(Key={
        'partn': partn,
        'puuid': puuid
    })
    limit_time = int(response.get('Item').get('lgts'))
    return limit_time


def send_to_sqs(items, partn, puuid):
    batch_size = 10
    for i in range(0, len(items), batch_size):
        items_batch_entries = [{
            'Id': item.get('matchId'),
            'MessageBody': json.dumps({
                **item,
                'partn': partn
            }),
            'MessageDeduplicationId': item.get('matchId'),
            'MessageGroupId': puuid
        } for item in items[i:i + batch_size]]
        queue.send_messages(Entries=items_batch_entries)


def getHTTPJsonResponse(url):
    retry_attempts = 3
    for i in range(retry_attempts):
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        if response.status_code == 400:
            return None
        if response.headers.get('Retry-After') is not None:
            print(f'retrying after: {response.headers.get("Retry-After")}')
            time.sleep(int(response.headers.get('Retry-After')))
        else:
            print('retrying after 3 seconds')
            time.sleep(3)
    raise Exception(f"Can't trigger this URL: {url}")
