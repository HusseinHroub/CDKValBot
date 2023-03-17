import json
import os
import boto3
from nacl.signing import VerifyKey
from tabulate import tabulate
from datetime import datetime

dynamodb = boto3.client('dynamodb')

playerDataTrackingTableName = os.environ['playerDataTrackingTableName']
class RegisterCommand:
    def execute(self, commandBody):
        return json.dumps({
            "type": 4,
            "data": {"content": "https://auth.riotgames.com/authorize?redirect_uri=https://riot.hroub-entertainment.com/val/oauth-callback&client_id=hevalorant&response_type=code&scope=openid"}
        })


class RankCommand:
    def execute(self, commandBody):
        options = commandBody.get('data').get('options')
        pname = options[0].get('value')
        rankMetricType = len(options) > 1 and options[1].get('value') or 'kda'
        month = options is not None and len(options) > 2 and options[2].get('value') or datetime.utcfromtimestamp().month
        playerDataResponse = dynamodb.query(
            TableName=playerDataTrackingTableName,
            IndexName=f'pname-month-index',
            ScanIndexForward=False,
            ReturnConsumedCapacity='NONE',
            KeyConditionExpression='#pname = :pname AND #month = :month',
            ExpressionAttributeNames={
                '#pname': 'pname'
            },
            ExpressionAttributeValues={
                ':pname': {
                    'S': pname
                },
                ':month': {
                    'S': month
                }
            }
        )
        if len(playerDataResponse['Items']) == 0:
            return json.dumps({
                "type": 4,
                "data": {
                    "content": f"Player with name: {pname} wasn't found"}
            })
        response = dynamodb.query(
            TableName=playerDataTrackingTableName,
            IndexName=f'month-{rankMetricType}-index',
            Limit=20,
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
        rankValue = playerDataResponse['Items'][0][rankMetricType]['N']
        rankCount = 0
        found = False
        for item in response['Items']:
            rankCount += 1
            if item['pname']['S'] == pname:
                found = True
                break
        if not found:
            rankCount = '+20'

        return json.dumps({
            "type": 4,
            "data": {
                "content": f"{pname} rank in {rankMetricType} is {rankCount}, {rankMetricType} value is {rankValue + ('' if rankMetricType != 'winrate' else '%')}"}
        })


class PlayerStatsCommand:
    def execute(self, commandBody):
        options = commandBody.get('data').get('options')
        pname = options[0].get('value')
        month = datetime.utcfromtimestamp().month
        if len(options) > 1:
            month = options[1].get('value')
        playerDataResponse = dynamodb.query(
            TableName=playerDataTrackingTableName,
            IndexName=f'pname-month-index',
            ScanIndexForward=False,
            ReturnConsumedCapacity='NONE',
            KeyConditionExpression='#pname = :pname AND #month = :month',
            ExpressionAttributeNames={
                '#pname': 'pname'
            },
            ExpressionAttributeValues={
                ':pname': {
                    'S': pname
                },
                ':month': {
                    'S': month
                }
            }
        )
        if len(playerDataResponse['Items']) == 0:
            return json.dumps({
                "type": 4,
                "data": {
                    "content": f"Player with name: {pname} wasn't found"
                }})
        player_data = playerDataResponse['Items'][0]
        return json.dumps({
            "type": 4,
            "data": {
                "embeds": [
                    {
                        "title": f"{pname} Stats",
                        "color": 0x27966b,
                        "fields": [{'name': 'Kills', 'value': player_data["kills"]["N"], 'inline': True},
                                   {'name': 'Deaths', 'value': player_data["deaths"]["N"], 'inline': True},
                                   {'name': 'Assists', 'value': player_data["assists"]["N"], 'inline': True},
                                   {'name': 'KDA', 'value': player_data["kda"]["N"], 'inline': True},
                                   {'name': '#Games', 'value': player_data["ngames"]["N"], 'inline': True},
                                   {'name': 'WinRate', 'value': player_data["winrate"]["N"], 'inline': True}]
                    }
                ]
            }
        })


class TopCommand:
    def execute(self, commandBody):
        options = commandBody.get('data').get('options')
        commandMetricType = options is not None and options[0].get('value') or 'kda'
        month = options is not None and len(options) > 1 and options[1].get('value') or datetime.utcfromtimestamp().month
        response = dynamodb.query(
            TableName=playerDataTrackingTableName,
            IndexName=f'month-{commandMetricType}-index',
            Limit=20,
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
        itemsHeader = ['name', commandMetricType]
        items = []
        for item in response['Items']:
            items.append([item['pname']['S'], item[commandMetricType]['N']])
        self.processItems(commandMetricType, items)
        return json.dumps({
            "type": 4,
            "data": {"content": '```' + tabulate(items,
                                                 headers=itemsHeader,
                                                 tablefmt="fancy_grid",
                                                 numalign="left",
                                                 stralign="left") + '```'}
        })

    def processItems(self, commandMetricType, items):
        if commandMetricType == 'winrate':
            for item in items:
                item[1] = f'{item[1]}%'


class NotFoundCommand:
    def execute(self, commandBody):
        return json.dumps({
            "type": 4,
            "data": {"content": "idk the command :("}
        })


def getCommand(commandName):
    if commandName == 'rank':
        return RankCommand()
    if commandName == 'top':
        return TopCommand()
    if commandName == 'register':
        return RegisterCommand()
    if commandName == 'stats':
        return PlayerStatsCommand()
    return NotFoundCommand()


def lambda_handler(request, context):
    # Your public key can be found on your application in the Developer Portal
    PUBLIC_KEY = 'df947dabfb2654096006b1baceee7e49cee0e4cce82b5ff1599e1713f4dba104'

    verify_key = VerifyKey(bytes.fromhex(PUBLIC_KEY))

    signature = request.get('headers').get('x-signature-ed25519')
    timestamp = request.get('headers').get('x-signature-timestamp')
    body = request.get('body')

    try:
        verify_key.verify(f'{timestamp}{body}'.encode(), bytes.fromhex(signature))
    except:
        return {
            'statusCode': 401,
            'body': json.dumps('invalid request signature')
        }

    bodyJson = json.loads(body)
    if bodyJson.get('type') == 1:
        return {
            'statusCode': 200,
            'body': json.dumps({"type": 1})
        }
    return getCommand(bodyJson.get('data').get('name')).execute(bodyJson)
