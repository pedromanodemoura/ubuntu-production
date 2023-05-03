# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 16:49:45 2022

@author: c20460
"""

import requests
import pandas as pd
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import math
import numpy as np
from tqdm import tqdm

key_path = r"C:\Users\c20460\Desktop\Projects\Google Cloud\soccer-341717-4072959238a4.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
# client = bigquery.Client()

job_config = bigquery.LoadJobConfig()
# The source format defaults to CSV, so the line below is optional.
job_config.source_format = bigquery.SourceFormat.CSV
        
dataset_id = credentials.project_id + '.premier_league'

def bq_load(table, table_name, load_type, df_schema):
    if load_type == 'append':    
        job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_APPEND
    elif load_type == 'overwrite':
        job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
    job_config.schema = df_schema
    job = client.load_table_from_dataframe(table, dataset_id + table_name, job_config=job_config)  # Make an API request.
    job.result()  # Wait for the job to complete.

def seasons():
    url = 'https://footballapi.pulselive.com/football/competitions/1/compseasons?page=0&pageSize=100'
     
    payload = {}
    headers= {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36',
              'account': 'premierleague',
              'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
              'origin': 'https://www.premierleague.com',
              'referer': 'https://www.premierleague.com/'}
     
    response = requests.request("GET", url, headers=headers, data = payload)
     
    df = pd.json_normalize(response.json()['content'], max_level=0)
    df['id'] = df.id.astype(int).astype(object)
    
    return df

# season_df = seasons() ### Get from competitions

def competitions():
    url = 'https://footballapi.pulselive.com/football/competitions?page=0&pageSize=1000&detail=2'
     
    payload = {}
    headers= {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36',
              'account': 'premierleague',
              'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
              'origin': 'https://www.premierleague.com',
              'referer': 'https://www.premierleague.com/'}
     
    response = requests.request("GET", url, headers=headers, data = payload)
     
    df = pd.json_normalize(response.json()['content'], record_path=['compSeasons'], meta=['abbreviation', 'description', 'level', 'source', 'id'], record_prefix='season_', errors='ignore')
    df['id'] = df.id.astype(int).astype(object)
    df['season_id'] = df.season_id.astype(int).astype(object)
    
    try:
        df.pop('source')
    except:
        pass
    
    return df

comp_df = competitions()
bq_load(comp_df, '.competitions', 'overwrite')

pl_seasons = list(comp_df[comp_df['id'] == 1]['season_id'])

def players(pl_seasons):
    pdf = pd.DataFrame()

    for season_id in pl_seasons:
        print(str(season_id))
    
        url = 'https://footballapi.pulselive.com/football/players?pageSize=100&compSeasons=' + str(season_id) + '&altIds=true&page=0&type=player&id=-1&compSeasonId=' + str(season_id)
         
        payload = {}
        
        headers= {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36',
                  'account': 'premierleague',
                  'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                  'origin': 'https://www.premierleague.com',
                  'referer': 'https://www.premierleague.com/'}
         
        response = requests.request("GET", url, headers=headers, data = payload)
         
        df = pd.json_normalize(response.json()['content'])
        df['playerId'] = df.playerId.astype(int).astype(object)
        df['id'] = df.id.astype(int).astype(object)
        
        pdf = pdf.append(df)
        
        page = 1
        
        while len(df) == 100:
            print(str(page))

            url = 'https://footballapi.pulselive.com/football/players?pageSize=100&compSeasons=' + str(season_id) + '&altIds=true&page=' + str(page) + '&type=player&id=-1&compSeasonId=' + str(season_id)
            
            response = requests.request("GET", url, headers=headers, data = payload)
             
            df = pd.json_normalize(response.json()['content'])
            df['playerId'] = df.playerId.astype(int).astype(object)
            df['id'] = df.id.astype(int).astype(object)
            
            pdf = pdf.append(df)
            
            page += 1
    
    pdf = pdf[['playerId', 'id', 'info.position', 'info.shirtNum', 'info.positionInfo', 'nationalTeam.isoCode', 'nationalTeam.country', 
               'nationalTeam.demonym', 'currentTeam.club.name', 'currentTeam.club.abbr', 'currentTeam.club.id', 'birth.date.label',
               'birth.country.isoCode', 'birth.country.country', 'birth.country.demonym', 'name.display', 'name.first', 'name.last',
               'altIds.opta', 'birth.place', 'name.middle']]
    
    pdf = pdf.drop_duplicates(subset='playerId', keep = 'first')
    
    return pdf

player_df = players(pl_seasons)
player_df.columns = player_df.columns.str.replace('.','_')
bq_load(player_df, '.players', 'append')


def teams(pl_seasons):
    pdf = pd.DataFrame()

    for season_id in pl_seasons:
        print(str(season_id))
    
        url = 'https://footballapi.pulselive.com/football/teams?pageSize=100&compSeasons=' + str(season_id) + '&altIds=true&page=0'
         
        payload = {}
        
        headers= {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36',
                  'account': 'premierleague',
                  'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                  'origin': 'https://www.premierleague.com',
                  'referer': 'https://www.premierleague.com/'}
         
        response = requests.request("GET", url, headers=headers, data = payload)
         
        col_list = list(pd.json_normalize(response.json()['content']).columns)
        col_list.remove('grounds')
        df = pd.json_normalize(response.json()['content'], record_path=['grounds'], meta=col_list, record_prefix='stadium_', errors='ignore')
        df['stadium_id'] = df.stadium_id.astype(int).astype(object)
        df['stadium_capacity'] = df.stadium_capacity.astype(int).astype(object)
        df['id'] = df.id.astype(int).astype(object)
        
        pdf = pdf.append(df)

    
    pdf = pdf.drop_duplicates(subset=['id','stadium_id'], keep = 'first')
    
    return pdf

teams_df = teams(pl_seasons)
teams_df.columns = teams_df.columns.str.replace('.','_')
bq_load(player_df, '.teams', 'append')


def player_stats(pl_players):
    pdf = pd.DataFrame()

    for player_id in pl_players:
        print(str(player_id))
    
        url = 'https://footballapi.pulselive.com/football/stats/player/' + str(player_id) + '?comps=1'
         
        payload = {}
        
        headers= {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36',
                  'account': 'premierleague',
                  'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                  'origin': 'https://www.premierleague.com',
                  'referer': 'https://www.premierleague.com/'}
         
        response = requests.request("GET", url, headers=headers, data = payload)
         
        df = pd.json_normalize(response.json()['stats'])[['name', 'value']]
        df['value'] = df.value.astype(int).astype(object)
        df.index = df.name
        df.pop('name')
        df = df.T
        df['id'] = player_id
        
        pdf = pdf.append(df)
        
    pdf = pdf.reset_index()
    pdf.pop('index')

        
    return pdf

pl_players = list(player_df['playerId'])
player_stats_df = player_stats(pl_players)
bq_load(player_stats_df, '.teams', 'append')






def match_info(page):
    print(str(page))

    url = 'https://footballapi.pulselive.com/football/fixtures?&pageSize=100&page=' + str(page) + '&sort=desc&statuses=C'
     
    payload = {}
    
    headers= {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36',
              'account': 'premierleague',
              'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
              'origin': 'https://www.premierleague.com',
              'referer': 'https://www.premierleague.com/'}
     
    response = requests.request("GET", url, headers=headers, data = payload)
    
    col_list = ['teams', 'replay', 'neutralGround', 'status', 'phase', 'outcome', 'group', 'goals', 'penaltyShootouts', 'id', 'gameweek.id',
                'gameweek.compSeason.label', 'gameweek.compSeason.competition.abbreviation', 'gameweek.compSeason.competition.id', 
                'gameweek.compSeason.id', 'gameweek.gameweek', 'ground.id', 'attendance']
     
    df = pd.json_normalize(response.json()['content'])
    
    if len(col_list) != len(df.columns):
        for col in col_list:
            if col not in df.columns:
                df[col] = None
    
    df = df[col_list]
    df = df[df.status == "C"]
    
    if len(df) > 0:
        score = df.apply(lambda x: pd.json_normalize(x['teams'])['score'], axis = 1).astype(int).astype(object)
        score.columns = ['score_1', 'score_2']
        team_name = df['teams'].apply(lambda x:pd.json_normalize(x)['team.name'])
        team_name.columns = ['team_1', 'team_2']
        team_id = df['teams'].apply(lambda x:pd.json_normalize(x)['team.id']).astype(int).astype(object)
        team_id.columns = ['id_1', 'id_2']
        
        df = pd.concat([df, score, team_name, team_id], axis = 1)
        df.pop('teams')
        df.pop('goals')
        
        df['id'] = df['id'].astype(int).astype(object)
        df['gameweek.id'] = df['gameweek.id'].astype(int).astype(object)
        df['gameweek.compSeason.competition.id'] = df['gameweek.compSeason.competition.id'].astype(int).astype(object)
        df['gameweek.compSeason.id'] = df['gameweek.compSeason.id'].astype(int).astype(object)
        df['gameweek.gameweek'] = df['gameweek.gameweek'].fillna(0).astype(int).astype(object)
        df['ground.id'] = df['ground.id'].astype(int).astype(object)
        df['attendance'] = df['attendance'].fillna(0).astype(int).astype(object)
        
    num_games = response.json()['pageInfo']['numEntries']
        
    return df, num_games

def matches():
    pdf = pd.DataFrame()
    
    page = 0
    num_games = 1
    
    while page*100 < num_games:
        df, num_games = match_info(page)
    
        pdf = pdf.append(df)
        
        page += 1
        
    pdf = pdf.reset_index()
    pdf.pop('index')
    
    return pdf

game_info = matches()

game_info.pop('teams')
game_info.pop('goals')

game_info.columns = game_info.columns.str.replace('.','_')

df_schema=[
            bigquery.SchemaField("replay", "BOOL"), 
            bigquery.SchemaField("neutralGround", "STRING"), 
            bigquery.SchemaField("status", "STRING"), 
            bigquery.SchemaField("phase", "STRING"), 
            bigquery.SchemaField("outcome", "STRING"), 
            bigquery.SchemaField("group", "STRING"),
            bigquery.SchemaField("penaltyShootouts", "STRING"), 
            bigquery.SchemaField("id", "STRING"), 
            bigquery.SchemaField("gameweek_id", "STRING"), 
            bigquery.SchemaField("gameweek_compSeason_label", "STRING"),
            bigquery.SchemaField("gameweek_compSeason_competition_abbreviation", "STRING"),
            bigquery.SchemaField("gameweek_compSeason_competition_id", "STRING"),
            bigquery.SchemaField("gameweek_compSeason_id", "STRING"),
            bigquery.SchemaField("gameweek_gameweek", "STRING"), 
            bigquery.SchemaField("ground_id", "STRING"), 
            bigquery.SchemaField("attendance", "INT64"), 
            bigquery.SchemaField("score_1", "INT64"), 
            bigquery.SchemaField("score_2", "INT64"),
            bigquery.SchemaField("team_1", "STRING"), 
            bigquery.SchemaField("team_2", "STRING"), 
            bigquery.SchemaField("id_1", "STRING"), 
            bigquery.SchemaField("id_2", "STRING"),
          ]

bq_load(game_info, '.game_info', 'append', df_schema) #### up to 2/23/2022



# Match
def match(game_id_list):    
    
        # print(str(game_id))
        
    url = 'https://footballapi.pulselive.com/football/fixtures/' + str(game_id) + '/textstream/EN?pageSize=1000&sort=desc'
    
    payload = {}
    
    headers= {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36',
              'account': 'premierleague',
              'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
              'origin': 'https://www.premierleague.com',
              'referer': 'https://www.premierleague.com/'}
    
    response = requests.request("GET", url, headers=headers, data = payload)
    
    df = pd.json_normalize(response.json()['events']['content'])
    
    return df

game_events = pd.DataFrame()

game_id_list = list(game_info.id.drop_duplicates())
game_id_list.sort()

for game_id in tqdm(game_id_list):
    df = match(game_id)
    game_events = game_events.append(df)

game_events.columns = game_events.columns.str.replace('.','_')

df_schema=[
            bigquery.SchemaField("id", "STRING"), 
            bigquery.SchemaField("type", "STRING"), 
            bigquery.SchemaField("text", "STRING"), 
            bigquery.SchemaField("time_secs", "STRING"), 
            bigquery.SchemaField("time_label", "STRING"), 
            bigquery.SchemaField("playerIds", "STRING"),
          ]

bq_load(game_events, '.game_events', 'append', df_schema) #### up to 2/23/2022