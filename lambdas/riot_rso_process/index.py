import json
import time
import traceback

import boto3
import botocore
import requests
from requests.auth import HTTPBasicAuth
import os

AUTH_PROVIDER_ROOT = 'https://auth.riotgames.com'
TOKEN_URL = AUTH_PROVIDER_ROOT + '/token'
AUTH = HTTPBasicAuth('hevalorant', 'IaxIudkUs7UvFgwOOI7llNA6u-2Z94BWz_k-3EhwddA')
REDIRECT_URI = 'https://riot.hroub-entertainment.com/val/oauth-callback'
ACCOUNT_ME_URI = 'https://europe.api.riotgames.com/riot/account/v1/accounts/me'
playerRefTable = boto3.resource('dynamodb').Table(os.environ['playerRefTableName'])
API_KEY = ''
MATCH_LIST_ROOT_URI = 'https://eu.api.riotgames.com/val/match/v1/matchlists/by-puuid'
MATCH_INFO_ROOT_URI = 'https://eu.api.riotgames.com/val/match/v1/matches'


def lambda_handler(event, context):
    try:
        access_code = event['queryStringParameters']['code']
        tokens = getPlayerTokens(access_code)
        access_token = tokens['access_token']
        player_tracking_details = getPlayerTrackingDetails(access_token)
        match_list_uri = f'{MATCH_LIST_ROOT_URI}/{player_tracking_details["puuid"]}?api_key={API_KEY}'
        games_history = getHTTPJsonResponse(match_list_uri).get('history')
        player_name = None
        if len(games_history) > 0:
            player_name = get_player_name(games_history, player_tracking_details['puuid'])
        registerPlayerInDB({
            'puuid': player_tracking_details['puuid'],
            'pname': player_name
        })
        body = None
        if player_name is not None:
            body = f'You are now registered with the name: {player_name}, bot will start tracking your stats, you can now close this window'
        else:
            body = f"You are now registered and bot will start tracking your stats, we couldn't know your name as it seems you don't play Valorant that much, however once you start any new game we will hunt your name, you can now close this window"
        return {
            'statusCode': 200,
            'body': json.dumps(body)
        }
    except PlayerExistException:
        return {
            'statusCode': 500,
            'body': json.dumps(
                'You are already registered..')
        }
    except Exception:
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps(
                'Sorry, something wrong has happened :(')
        }


def get_player_name(games_history, puuid):
    latest_game = games_history[0]
    match_info = getHTTPJsonResponse(f'{MATCH_INFO_ROOT_URI}/{latest_game["matchId"]}?api_key={API_KEY}')
    for player in match_info.get('players'):
        if player.get('puuid') == puuid:
            return player.get('gameName')


def getPlayerTokens(access_code):
    retry_attempts = 3
    for i in range(retry_attempts):
        response = requests.post(TOKEN_URL, data={
            'grant_type': "authorization_code",
            'code': access_code,
            'redirect_uri': REDIRECT_URI
        }, auth=AUTH)
        if int(response.status_code / 100) == 2:
            return response.json()
        if response.headers.get('Retry-After') is not None:
            print(f'retrying after: {response.headers.get("Retry-After")}')
            time.sleep(int(response.headers.get('Retry-After')))
        else:
            print('retrying after 3 seconds')
            time.sleep(3)
    raise Exception(
        f"Can't trigger this URL: {TOKEN_URL}, status_code: {response.status_code}, response body: {response.text}")


def getPlayerTrackingDetails(access_token):
    retry_attempts = 3
    for i in range(retry_attempts):
        response = requests.get(ACCOUNT_ME_URI, headers={
            'Authorization': f'Bearer {access_token}'
        })
        if int(response.status_code / 100) == 2:
            return response.json()
        if response.headers.get('Retry-After') is not None:
            print(f'retrying after: {response.headers.get("Retry-After")}')
            time.sleep(int(response.headers.get('Retry-After')))
        else:
            print('retrying after 3 seconds')
            time.sleep(3)
    raise Exception(f"Can't trigger this URL: {ACCOUNT_ME_URI}")


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
    raise Exception(
        f"Can't trigger this URL: {url}, status_code: {response.status_code}, response body: {response.text}")


class PlayerExistException(Exception):
    pass


def registerPlayerInDB(player_tracking_details):
    item = {
        'puuid': player_tracking_details['puuid'],
        'partn': '0'  # TODO on high scale this need to be determined using something like zoo keeper
    }
    if player_tracking_details['pname'] is not None:
        item['pname'] = player_tracking_details['pname']
    try:
        playerRefTable.put_item(
            Item={
                **item,
                'lgts': int(time.time() * 1000)
            },
            ConditionExpression='attribute_not_exists(puuid)'
        )
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            raise PlayerExistException()
        raise
