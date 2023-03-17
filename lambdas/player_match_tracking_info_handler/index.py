import copy
import json
import time
from datetime import datetime
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key
import os

dynamodb = boto3.resource('dynamodb')
queue = boto3.resource('sqs').Queue(os.environ['playerChangesQueueURL'])
PLAYER_TRACKING_TABLE_NAME = os.environ['playerDataTrackTableName']
playerRefTable = dynamodb.Table(os.environ['playerRefTableName'])
playerTrackingTable = dynamodb.Table(os.environ['playerDataTrackTableName'])



def lambda_handler(event, context):
    if len(event.get('Records')) > 10:
        raise Exception("I can't handle more than 10 messages at a single time :(, ask Hussein why")
    new_player_games_grouped_by_puuid = get_latest_player_games_grouped_by_puuid(event)
    if len(new_player_games_grouped_by_puuid.keys()) > 0:
        existing_players_stats = get_existing_players_stats(new_player_games_grouped_by_puuid)
        update_players_stats(new_player_games_grouped_by_puuid, existing_players_stats)
        sendToSQS(new_player_games_grouped_by_puuid, existing_players_stats)


def get_latest_player_games_grouped_by_puuid(event):
    latest_players_games_stats = []
    for record in event.get('Records'):
        latest_players_games_stats.append(json.loads(record.get('body')))
    result = {}
    for data in latest_players_games_stats:
        key = data["puuid"]
        if key not in result:
            result[key] = {
                'games': {},
                'partn': data['partn']
            }
        games = result[key]['games']
        game_month = datetime.utcfromtimestamp(data["gameStartMillis"] / 1000).month
        if game_month not in games:
            games[game_month] = []
        games[game_month].append(data)
    return result


def get_existing_players_stats(new_player_games_grouped_by_puuid):
    results = {}
    dynamo_keys_input = []
    for puuid in new_player_games_grouped_by_puuid.keys():
        for month in new_player_games_grouped_by_puuid[puuid]['games'].keys():
            dynamo_keys_input.append({'puuid': puuid, 'month': month})
    players_stats = read_keys(dynamo_keys_input)
    for player_stats in players_stats:
        initNumberIfNone(player_stats, 'assists')
        initNumberIfNone(player_stats, 'deaths')
        initNumberIfNone(player_stats, 'kda')
        initNumberIfNone(player_stats, 'kills')
        initNumberIfNone(player_stats, 'ngames')
        initNumberIfNone(player_stats, 'winrate')
        initNumberIfNone(player_stats, 'wins')
        puuid = player_stats["puuid"]
        month = player_stats['month']
        if puuid not in results:
            results[puuid] = {}
        results[puuid][month] = player_stats
    for puuid in new_player_games_grouped_by_puuid.keys():
        game_months = new_player_games_grouped_by_puuid[puuid]['games'].keys()
        partn = new_player_games_grouped_by_puuid[puuid]["partn"]
        if puuid not in results:
            results[puuid] = {}
            #get pname onnce please
            for month in game_months:
                results[puuid][month] = get_player_init_data(month, partn, puuid)
        else:
            for month in game_months:
                if month not in results[puuid]:
                    results[puuid][month] = get_player_init_data(month, partn, puuid)

    return results


def get_player_init_data(month, partn, puuid):
    player_data_init = {
        'puuid': puuid,
        'month': month,
        'kills': 0,
        'deaths': 0,
        'assists': 0,
        'ngames': 0,
        'wins': 0,
        'winrate': 0,
        'kda': 0
    }
    pname = getPnameFromPlayerRef(partn, puuid)
    if pname is not None:
        player_data_init['pname'] = pname
    #What happenn if Pnname is nnone
    return player_data_init


def initNumberIfNone(player_stats, stat_value):
    if player_stats.get(stat_value) is None:
        player_stats[stat_value] = 0


def getPnameFromPlayerRef(partn, puuid):
    response = playerRefTable.get_item(Key={
        'partn': partn,
        'puuid': puuid
    })
    if response is None or response.get('Item') is None:
        raise Exception(f"Player with partn: {partn}, puuid: {puuid} wasn't found")
    return response['Item'].get('pname')


def get_players_who_changed_name_per_puuid(new_player_games_grouped_by_puuid, existing_players_stats):
    players_who_changed_name_per_puuid = {}
    last_known_new_game_per_puuid = get_last_known_new_game_per_puuid(new_player_games_grouped_by_puuid)
    last_known_existing_game_per_puuid = get_last_known_existing_game_per_puuid(existing_players_stats)

    for puuid in last_known_new_game_per_puuid.keys():
        if last_known_new_game_per_puuid[puuid].get('gameName') != last_known_existing_game_per_puuid[puuid].get(
                'pname'):
            players_who_changed_name_per_puuid[puuid] = {
                'pname': last_known_new_game_per_puuid[puuid].get('gameName'),
                'partn': new_player_games_grouped_by_puuid[puuid]['partn']
            }
    return players_who_changed_name_per_puuid


def get_last_known_new_game_per_puuid(new_player_games_grouped_by_puuid):
    print(new_player_games_grouped_by_puuid)
    last_known_game_per_puuid = {}
    for puuid in new_player_games_grouped_by_puuid.keys():
        for games in new_player_games_grouped_by_puuid[puuid]['games'].values():
            for game in games:
                if puuid not in last_known_game_per_puuid:
                    last_known_game_per_puuid[puuid] = game
                elif game['gameStartMillis'] > last_known_game_per_puuid[puuid]['gameStartMillis']:
                    last_known_game_per_puuid[puuid] = game
    return last_known_game_per_puuid


def get_last_known_existing_game_per_puuid(existing_players_stats):
    last_known_game_per_puuid = {}
    for puuid in existing_players_stats.keys():
        for month in existing_players_stats[puuid].keys():
            if puuid not in last_known_game_per_puuid:
                last_known_game_per_puuid[puuid] = existing_players_stats[puuid][month]
            elif month > last_known_game_per_puuid[puuid]['month']:
                last_known_game_per_puuid[puuid] = existing_players_stats[puuid][month]
    return last_known_game_per_puuid


def update_players_stats(new_player_games_grouped_by_puuid, existing_players_stats):
    new_players_stats = get_new_player_stats(new_player_games_grouped_by_puuid, existing_players_stats)
    write_items([{'PutRequest': {
        'Item': new_player_stats
    }} for new_player_stats in new_players_stats])

    players_who_changed_name = get_players_who_changed_name_per_puuid(new_player_games_grouped_by_puuid,
                                                                      existing_players_stats)
    if len(players_who_changed_name.keys()) > 0:
        update_all_with_new_pname(players_who_changed_name)


def update_all_with_new_pname(players_who_changed_name):
    for puuid in players_who_changed_name.keys():
        pname = players_who_changed_name[puuid]['pname']
        response = playerTrackingTable.query(KeyConditionExpression=Key('puuid').eq(puuid))
        report_items = response['Items']
        while response.get('LastEvaluatedKey') is not None:
            response = playerTrackingTable.query(KeyConditionExpression=Key('puuid').eq(puuid),
                                                 Select='SPECIFIC_ATTRIBUTES',
                                                 ProjectionExpression='month')
            report_items.extend(response['Items'])
        for report_item in report_items:
            playerTrackingTable.update_item(
                Key={
                    'puuid': puuid,
                    'month': report_item['month']
                },
                UpdateExpression='SET pname = :pname',
                ExpressionAttributeValues={
                    ':pname': pname
                },
                ReturnValues='NONE',
                ReturnConsumedCapacity='NONE'
            )
        playerRefTable.update_item(
            Key={
                'partn': players_who_changed_name[puuid]['partn'],
                'puuid': puuid
            },
            UpdateExpression='SET pname = :pname',
            ExpressionAttributeValues={
                ':pname': pname
            },
            ReturnValues='NONE',
            ReturnConsumedCapacity='NONE'
        )


def get_new_player_stats(new_player_games_grouped_by_puuid, existing_players_stats):
    new_players_stats = []
    for puuid in new_player_games_grouped_by_puuid.keys():
        games = new_player_games_grouped_by_puuid[puuid]['games']
        for month in games.keys():
            player_games = games[month]
            existing_stats = existing_players_stats[puuid][month]
            total_kills = 0
            total_deaths = 0
            total_assists = 0
            kda = 0.0
            number_of_games = 0
            number_of_wins = 0
            for player_data in player_games:
                print('Processing match_id: ' + player_data.get('matchId'))
                player_stats = player_data.get('stats')
                if player_stats != None:
                    total_kills += player_stats['kills']
                    total_deaths += player_stats['deaths']
                    total_assists += player_stats['assists']
                    number_of_games += 1
                    kda += (player_stats['kills'] + player_stats['assists']) / (
                        player_stats['deaths'] if player_stats['deaths'] > 0 else 1)
                    number_of_wins += 1 if player_data['won'] else 0
            avg_kda = 0.0
            if number_of_games != 0:
                avg_kda = kda / number_of_games
            new_winrate = (existing_stats['wins'] + number_of_wins) / (
                    existing_stats['ngames'] + number_of_games) * 100
            new_winrate = Decimal(str(round(new_winrate, 3)))
            new_avg_kda = getKda({
                'newCount': number_of_wins,
                'oldCount': existing_stats.get('ngames'),
                'newKDA': avg_kda,
                'oldKda': existing_stats.get('kda')
            })
            new_player_stats = copy.deepcopy(existing_stats)
            new_player_stats.update({
                'kills': total_kills + existing_stats['kills'],
                'deaths': total_deaths + existing_stats['deaths'],
                'assists': total_assists + existing_stats['assists'],
                'ngames': number_of_games + existing_stats['ngames'],
                'wins': number_of_wins + existing_stats['wins'],
                'winrate': new_winrate,
                'kda': new_avg_kda,
            })
            new_players_stats.append(new_player_stats)
    return new_players_stats


def getKda(kda_meta_data):
    totalCount = kda_meta_data['newCount'] + kda_meta_data['oldCount']
    new_kda = kda_meta_data['newKDA'] if kda_meta_data['oldCount'] == 0 else (
            kda_meta_data['oldKda'] + (Decimal(str(kda_meta_data['newKDA'])) - kda_meta_data['oldKda']) / totalCount)
    return Decimal(str(round(new_kda, 3)))


# recommended max 10 keys only for now till I optmize...
def read_keys(keys):
    max_retry_attempts = 3
    backoff_in_seconds = 1
    for retry_attempts in range(max_retry_attempts):
        try:
            response = dynamodb.batch_get_item(RequestItems={
                PLAYER_TRACKING_TABLE_NAME: {
                    'Keys': keys
                }
            })
            items = response['Responses'][PLAYER_TRACKING_TABLE_NAME]
            if response['UnprocessedKeys']:
                print('getting items from unprocessed keys read operation')
                items_from_unprocessed_keys = read_keys(response['UnprocessedKeys'][PLAYER_TRACKING_TABLE_NAME]['Keys'])
                items += items_from_unprocessed_keys
            return items
        except Exception as e:
            print(f'got exception: {e}')
            sleep_time = (backoff_in_seconds * 2 ** retry_attempts + 1)
            print(f'retry_attempts: {retry_attempts}')
            print(f'sleep time: {sleep_time}')
            time.sleep(sleep_time)
    raise Exception('I FAILED TO RETRY AAAAAAAAAH')


# recommended max 10 keys only for now till I optmize...
def write_items(items_put_requests):
    max_retry_attempts = 3
    backoff_in_seconds = 1
    for retry_attempts in range(max_retry_attempts):
        try:
            response = dynamodb.batch_write_item(RequestItems={
                PLAYER_TRACKING_TABLE_NAME: items_put_requests
            })
            if response['UnprocessedItems']:
                print('getting items from unprocessed items write opreation')
                write_items(response['UnprocessedItems'][PLAYER_TRACKING_TABLE_NAME])
            return
        except Exception as e:
            print(f'got exception: {e}')
            sleep_time = (backoff_in_seconds * 2 ** retry_attempts + 1)
            print(f'retry_attempts: {retry_attempts}')
            print(f'sleep time: {sleep_time}')
            time.sleep(sleep_time)
    raise Exception('I FAILED TO RETRY AAAAAAAAAH')


def sendToSQS(new_player_games_grouped_by_puuid, existing_players_stats):
    queue.send_message(
        MessageBody=json.dumps({
            'newPlayerGamesGroupedByPUUID': new_player_games_grouped_by_puuid,
            'oldPlayerStatsByPUUID': existing_players_stats
        })
    )
