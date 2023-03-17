import json
import time

import boto3
import requests
import os

API_KEY = ''
ROOT = 'https://eu.api.riotgames.com/val/match/v1/matches'
queue = boto3.resource('sqs').Queue(os.environ['playerMatchInfoQueueUrl'])


def lambda_handler(event, context):
    if len(event.get('Records')) > 1:
        raise Exception("Ask hussein.")
    record = event.get('Records')[0]
    body = json.loads(record.get('body'))
    puuid = body.get('puuid')
    match_id = body.get('matchId')
    player_data = get_match_info(match_id, puuid)
    player_data['partn'] = body.get('partn')
    send_to_sqs(player_data)


def get_match_info(match_id, puuid):
    match_url = f'{ROOT}/{match_id}?api_key={API_KEY}'
    player_data = None
    match_info = getHTTPJsonResponse(match_url)
    for player in match_info.get('players'):
        if player.get('puuid') == puuid:
            player_data = player
            break
    if player_data is None:
        raise Exception(f"Couldn't find {puuid} in match_info")
    for team in match_info.get('teams'):
        if player_data.get('teamId') == team.get('teamId'):
            player_data['won'] = team.get('won')
            break
    if player_data.get('won') is None:
        raise Exception(f"Couldn't find if this player lost or won for puuid: {puuid}")
    player_data['matchId'] = match_info.get('matchInfo').get('matchId')
    player_data['gameStartMillis'] = match_info.get('matchInfo').get('gameStartMillis')
    return player_data


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


def send_to_sqs(player_data):
    queue.send_message(
        MessageBody=json.dumps(player_data),
        MessageDeduplicationId=player_data.get('matchId'),
        MessageGroupId=player_data.get('puuid')
    )
