import requests
import json

url = "https://discord.com/api/v10/applications/804661459703169045/guilds/642348923712700417/commands"

# This is an example CHAT_INPUT or Slash Command, with a type of 1
# command = {
#
#     "name": "asdasd",
#     "type": 1,
#     "description": "replies with bar ;/",
# }
commands = [
    {
        "name": "register",
        "description": "Registration link, to authorize me to monitor your wonderful stats",
    },
    {
        "name": "stats",
        "description": "Get player stats",
        "options": [
            {
                "name": "name",
                "description": "Player name as in valorant",
                "type": 3,
                "required": True
            }
        ]
    },
    {
        "name": "rank",
        "description": "Get player rank",
        "options": [
            {
                "name": "name",
                "description": "Player name as in valorant",
                "type": 3,
                "required": True
            },
            {
                "name": "metric",
                "description": "Get rank based on selected metric, default is KDA",
                "type": 3,
                "choices": [
                    {
                        "name": "Kills",
                        "value": "kills"
                    },
                    {
                        "name": "Assists",
                        "value": "assists"
                    },
                    {
                        "name": "KDA",
                        "value": "kda"
                    },
                    {
                        "name": "Deaths",
                        "value": "deaths"
                    },
                    {
                        "name": "WinRate",
                        "value": "winrate"
                    }
                ]
            }
        ]
    },
    {
        "name": "top",
        "description": "Get player rank",
        "options": [
            {
                "name": "metric",
                "description": "Get sorted list of selected metric, default is kda",
                "type": 3,
                "choices": [
                    {
                        "name": "Kills",
                        "value": "kills"
                    },
                    {
                        "name": "Assists",
                        "value": "assists"
                    },
                    {
                        "name": "KDA",
                        "value": "kda"
                    },
                    {
                        "name": "Deaths",
                        "value": "deaths"
                    },
                    {
                        "name": "WinRate",
                        "value": "winrate"
                    }
                ]
            }
        ]
    }
]

# For authorization, you can use either your bot token
headers = {
    "Authorization": ""
}

for command in commands:
    r = requests.post(url, headers=headers, json=command)
    print(r.json())
