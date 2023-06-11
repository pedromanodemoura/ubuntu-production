#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
import lxml.html as LH
import json
import datetime
import time
from google.cloud import bigquery


# In[2]:

def games_day():
    url = 'https://footballapi.pulselive.com/football/fixtures?&pageSize=100&page=0&competition=1&sort=desc&statuses=C'
            
    response = requests.request("GET", url, headers=headers, data = payload)

    df = pd.json_normalize(response.json()['content'])

    col_list = ['teams', 'status', 'outcome', 'goals', 'id', 'attendance',
                'gameweek.compSeason.id', 'gameweek.gameweek', 'ground.id', 'ground.name']

    game_df = df[(df["gameweek.compSeason.competition.id"] == 1) &
                (pd.to_datetime(df['kickoff.label']).dt.date == yesterday)][col_list]

    return game_df

# In[1]:

## events
game_id = 75224

def game_events(game_id):
    url = 'https://footballapi.pulselive.com/football/fixtures/' + str(game_id) + '/textstream/EN?pageSize=1000&sort=desc'

    response = requests.request("GET", url, headers=headers, data = payload)

    event_df = pd.json_normalize(response.json()['events']['content'])

    return event_df


# In[1]:

## players
def game_players(game_id):
    url = 'https://www.premierleague.com/match/' + str(game_id)

    response = requests.request("GET", url, headers=headers, data = payload)

    tables = LH.fromstring(response.text)

    df_list = []

    for i in range(0,2):
        elem = tables.find_class('teamList')[i]

        team = elem.find_class('squadHeader')[0].find_class('position')[0].text.strip()
        team_id = elem.find_class('matchTeamFormation')[0].attrib['data-team-id']
        team_url = elem.find_class('squadHeader')[0].find('a').attrib['href']
        team_formation = elem.find_class('matchTeamFormation')[0].text
        home_away = 'Home' if i == 0 else "Away"
        player_id = [i.find('a').attrib['href'].split('/')[2] for i in elem.find_class('player')]
        player_pid = [i.attrib['data-player'] for i in elem.find_class('img')]
        number = [i.text for i in elem.find_class('number')]
        position = [i.text_content().replace('C\n', '').strip() for i in elem.find_class('position')[1::]]
        captain = ['' if re.search(r"C\n",i.text_content()) == None else "C" for i in elem.find_class('position')[1::]]
        name = [i.text.strip() for i in elem.find_class('name')]
        player_url = [i.find('a').attrib['href'] for i in elem.find_class('player')]
        image = [i.attrib['src'] for i in elem.find_class('img')]

        df_list.append(
            pd.DataFrame({'team_name': team,
                        'team_id': team_id,
                        'team_url': team_url,
                        'team_formation': team_formation,
                        'home_away': home_away,
                        'player_id': player_id,
                        'player_pid': player_pid,
                        'number': number, 
                        'position': position,
                        'captain': captain,
                        'name': name,
                        'url': url,
                        'image': image}))

    player_df = pd.concat(df_list)

    return player_df

# In[2]:
def game_pitch(game_id):
    url = 'https://www.premierleague.com/match/' + str(game_id)

    response = requests.request("GET", url, headers=headers, data = payload)

    tables = LH.fromstring(response.text)

    df_list = []

    pitch = tables.find_class('pitch')[0]

    for i in range(0,2):
        team = pitch.find_class("team")[i]

        rows = team.find_class("row")
        home_away = 'Home' if i == 0 else "Away"

        for j in range(0,len(rows)):
            number = [i.text for i in rows[j].find_class("pos")]
            position = "GoalKeeper" if j == 0 else "Defender" if j == 1 else "Forward" if j == len(rows) - 1 else "Midfielder"

            df_list.append(
                pd.DataFrame({'home_away': home_away,
                            'number': number, 
                            'position': position}))

    position_df = pd.concat(df_list)

    return position_df


# In[2]:

pid = 'p49262'

def player_game_stats(pid, game_id):
    url = f"https://footballapi.pulselive.com/football/stats/player/{pid}?fixtures={game_id}&sys=opta&altIds=false&compCodeForActivePlayer=EN_PR"

    response = requests.request("GET", url, headers=headers, data = payload)

    player_json = pd.json_normalize(response.json()['entity'])[[
        'age', 'birth.date.millis', 'birth.country.country', 'info.loan', 'nationalTeam.country'
    ]]
    player_json.columns = ['age', 'birthDate', 'birthCountry', 'loan', 'nationalTeam']
    player_json['birthDate'] = pd.to_datetime(player_json['birthDate']).dt.date
    player_json['pid'] = pid

    player_stats = pd.json_normalize(response.json()['stats'])[[
        'name', 'value'
    ]]
    player_stats['pid'] = pid

    return player_json, player_stats


# In[2]:

