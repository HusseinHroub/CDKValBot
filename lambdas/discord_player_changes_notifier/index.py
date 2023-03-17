import json
import random
import time

import boto3
import requests

dynamodb = boto3.resource('dynamodb')
#TODO THIS IS SECRET NOT HERE
DISCORD_HOOK_URI = ""
DISCORD_BOT_AUTH_VALUE = ""


def lambda_handler(event, context):
    for record in event.get('Records'):
        body = json.loads(record.get('body'))
        notifiyIfIntersetingStats(body['newPlayerGamesGroupedByPUUID'], body['oldPlayerStats'])


def notifiyIfIntersetingStats(new_player_games_grouped_by_puuid, old_player_stats):
    special_messages = []
    for puuid in new_player_games_grouped_by_puuid.keys():
        last_game = new_player_games_grouped_by_puuid[puuid][len(new_player_games_grouped_by_puuid[puuid]) - 1]
        if last_game.get('gameName') is not None and old_player_stats[puuid]['pname'] is not None and last_game.get(
                'gameName') != \
                old_player_stats[puuid]['pname']:
            postToDiscordAPI([{
                "title": "News!",
                "description": f"{old_player_stats[puuid]['pname']} changed his name to: {last_game.get('gameName')}",
                "color": 0x27966b
            }])
        #TODO add if to handle incase pnname was NNone to announce new discovered player
        for player_data in new_player_games_grouped_by_puuid[puuid]:
            player_stats = player_data.get('stats')
            fields = [{'name': 'Kills', 'value': player_stats["kills"], 'inline': True}]
            if player_stats['deaths'] > 14:
                fields.append({'name': 'Deaths (waaw!)', 'value': player_stats["deaths"], 'inline': True})
            if player_stats['assists'] > 19:
                fields.append({'name': 'Assists', 'value': player_stats["assists"], 'inline': True})
            if len(fields) > 0:
                special_messages.append({
                    "title": "matchId: " + player_data.get('matchId'),
                    "description": f"Following highlights from one of {last_game.get('gameName')} games",
                    "color": 0x27966b,
                    "fields": fields
                })
    if len(special_messages) > 0:
        postToDiscordAPI(special_messages)


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
