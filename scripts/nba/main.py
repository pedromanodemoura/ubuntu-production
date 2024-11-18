#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
import lxml.html as LH
import json
from datetime import datetime, date, timedelta
import time
from google.cloud import bigquery
import itertools

# In[2]:


class NBA:
    def __init__(self, game_date = None, game_set = {}):
        self.VAR_NAMING_GUIDE = {
            'GAMES': {
                'description': 'Table with details on each game like: date, attendance, team names',
                'bigquery_table': 'game_details'
            }, 
            'ARENAS': {
                'description': 'Arenas where each game happened, including arena name and geographical location',
                'bigquery_table': 'game_arenas'
            }, 
            'BS_TRAD': {
                'description': 'Traditional option on the box-score. Included data like: points, rebounds, assists, fouls...',
                'bigquery_table': 'boxscore_trad'
            }, 
            'BS_ADV': {
                'description': 'Advanced option on the box-score. Included data like: offensive rating, defensive rating, net rating, assist to turnover ratio...',
                'bigquery_table': 'boxscore_adv'
            },  
            'BS_MISC': {
                'description': 'Miscellaneous option on the box-score. Included data like: contested shots, charges drawned, box outs...',
                'bigquery_table': 'boxscore_misc'
            }, 
            'BS_SCORE': {
                'description': 'Scoring option on the box-score. Included data like: % FG2A, % FG3A, % points on fast break, % assisted 3PT, % unassisted 3PT...',
                'bigquery_table': 'boxscore_scoring'
            }, 
            'BS_USAGE': {
                'description': 'Usage option on the box-score. Included data like: usage percentage, % FGM, % FGA, % fouls drawned...',
                'bigquery_table': 'boxscore_usage'
            }, 
            'BS_FOUR': {
                'description': 'Four Factors option on the box-score. Included data like: effective FGP, FTA, offensive rebound percentage...',
                'bigquery_table': 'boxscore_fourfac'
            }, 
            'BS_TRACK': {
                'description': 'Tracking option on the box-score. Included data like: distancem touches, secondary and free throw assists...',
                'bigquery_table': 'boxscore_tracking'
            }, 
            'BS_HUSTLE': {
                'description': 'Hustle option on the box-score. Included data like: contested shots, charges drawned, box outs...',
                'bigquery_table': 'boxscore_hustle'
            }, 
            'BS_MATCH': {
                'description': 'Matchup option on the box-score. Not every game has this data available. Included data like: % of total offensive time, minutes matched up, help blocks...',
                'bigquery_table': 'boxscore_matchup'
            }, 
            'BS_DEF': {
                'description': 'Defense option on the box-score. Not every game has this data available. Included data like: partial possessions, matchup assists, matchup turnovers...',
                'bigquery_table': 'boxscore_defense'
            }, 
            'PBP': {
                'description': 'Table with play-by-play data for each game',
                'bigquery_table': 'playbyplay'
            }
        }

                

        if game_date is None:
            today = date.today()
            game_date = today - timedelta(days=1)
            self.GAME_DATE_SAME_SEASON = datetime.strftime(game_date, '%m/%d/%Y')
            self.GAME_DATE = datetime.strftime(game_date, '%Y-%m-%d')
        elif isinstance(game_date, date):
            self.GAME_DATE_SAME_SEASON = datetime.strftime(game_date, '%m/%d/%Y')
            self.GAME_DATE = datetime.strftime(game_date, '%Y-%m-%d')
        else:
            raise Exception("Please enter a date following the format of datetime.date(2024, 11, 9)")


        if game_set == {}:
            self.GAME_SET = set(self.func_get_games())
        elif isinstance(game_set, set):
            self.GAME_SET = game_set
        else:
            raise Exception("Please enter a SET {} of game IDs")
        
        # self.GAMES, self.ARENAS, self.BS_TRAD, self.BS_ADV, self.BS_MISC, self.BS_SCORE, self.BS_USAGE, self.BS_FOUR, self.BS_TRACK, self.BS_HUSTLE, self.BS_DEF, self.BS_MATCH, self.PBP = self.func_run_proc()

        # self.func_load_tables()

    def func_clean_cols(self, col_name):
        new_col_name = col_name.split('.')[-1]

        return new_col_name

    def func_get_req(self, url):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0'}

        html_content = requests.get(url, headers = headers).text
        tables = LH.fromstring(html_content)

        table_json = tables.xpath('/html/body/script[1]')[0].text

        json_object = json.loads(table_json)

        df = pd.json_normalize(json_object)    

        time.sleep(1)

        return df

    def func_get_games_same_season(self):
        url = 'https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json'
        r = requests.request("GET", url)

        game_dates = r.json()['leagueSchedule']['gameDates']

        games_list = list(itertools.chain(*[
            [i['gameId'] for i in date['games']]
            for date in game_dates if 
                date['gameDate'][:10] == self.GAME_DATE_SAME_SEASON
            ]
        ))

        return games_list
    
    def func_get_games(self):
        url = f"https://www.nba.com/games?date={self.GAME_DATE}"

        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0'}

        html_content = requests.get(url, headers = headers).text
        tables = LH.fromstring(html_content)

        table_json = tables.xpath('//script[@id="__NEXT_DATA__"]')[0].text
        
        json_object = json.loads(table_json)

        games_list = [card['cardData']['gameId'] for card in json_object['props']['pageProps']['gameCardFeed']['modules'][0]['cards']]
        
        return games_list
        
    def func_game_details(self, game_id):

        url = f"https://www.nba.com/game/{game_id}"
        
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0'}

        html_content = requests.get(url, headers = headers).text
        tables = LH.fromstring(html_content)

        table_json = tables.xpath('//script[@id="__NEXT_DATA__"]')[0].text

        json_object = json.loads(table_json)

        df = pd.json_normalize(json_object) 
        
        df_game = df[['props.pageProps.game.gameId', 'props.pageProps.game.period', 'props.pageProps.game.gameEt', 'props.pageProps.game.attendance', 
                      'props.pageProps.game.homeTeam.teamId', 'props.pageProps.game.homeTeam.teamName', 
                      'props.pageProps.game.homeTeam.teamCity', 'props.pageProps.game.homeTeam.teamTricode',
                      'props.pageProps.game.awayTeam.teamId', 'props.pageProps.game.awayTeam.teamName', 
                      'props.pageProps.game.awayTeam.teamCity', 'props.pageProps.game.awayTeam.teamTricode']]
        df_game.columns = ['gameId', 'period', 'gameEt', 'attendance', 'homeTeamId', 'homeTeamName', 'homeTeamCity', 
                           'homeTeamTricode','awayTeamId', 'awayTeamName', 'awayTeamCity', 'awayTeamTricode']
        
        df_arena = df[['props.pageProps.game.gameId', 'props.pageProps.game.arena.arenaId', 'props.pageProps.game.arena.arenaName', 
                       'props.pageProps.game.arena.arenaCity', 'props.pageProps.game.arena.arenaState', 'props.pageProps.game.arena.arenaCountry']]
        df_arena.columns = list(map(self.func_clean_cols, df_arena.columns))     

        return df_game, df_arena
    
    def func_game_set_details(self):            
            games = pd.concat([self.func_game_details(game)[0] for game in self.GAME_SET])
            arenas = pd.concat([self.func_game_details(game)[1] for game in self.GAME_SET])

            return games, arenas

    def func_box_score(self, game_id, bs_type):
        bs_list = {'traditional', 'advanced', 'misc', 'scoring', 'usage', 'fourfactors', 'tracking', 'hustle', 'defense', 'matchups'}

        if bs_type not in bs_list:
            raise ValueError("results: box score type must be one of %r." % bs_list)

        url = f"https://www.nba.com/game/{game_id}/box-score?type={bs_type}"

        test = 0
        complete = 0

        while (complete == 0) & (test < 3):
            try:
                df = self.func_get_req(url)
                complete = 1
            except:
                test += 1

        if bs_type == 'matchups':
            home_team = pd.json_normalize(df['props.pageProps.game.homeTeam.players'][0], record_path=['matchups'])
            away_team = pd.json_normalize(df['props.pageProps.game.awayTeam.players'][0], record_path=['matchups'])
        else:
            home_team = pd.json_normalize(df['props.pageProps.game.homeTeam.players'][0])
            away_team = pd.json_normalize(df['props.pageProps.game.awayTeam.players'][0])

        home_team['teamId'] = df['props.pageProps.game.homeTeam.teamId'][0]
        away_team['teamId'] = df['props.pageProps.game.awayTeam.teamId'][0]

        full_game = pd.concat([home_team, away_team]).reset_index(drop=True)

        full_game['gameId'] = game_id

        full_game.columns = list(map(self.func_clean_cols, full_game.columns))

        # print(f"{game_id} - {bs_type}")

        return full_game
    
    def func_game_set_box_scores(self, bs_type):
        box_scores = pd.concat([self.func_box_score(game, bs_type) for game in self.GAME_SET])

        return box_scores

    def func_play_by_play(self, game_id):    
        url = f"https://www.nba.com/game/{game_id}/play-by-play"

        df = self.func_get_req(url)

        pbp = pd.json_normalize(df['props.pageProps.playByPlay.actions'][0])

        pbp['gameId'] = game_id

        print(f"{game_id} - playbyplay")

        return pbp

    def func_load_data(self, data, table):
        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_APPEND
        # The source format defaults to CSV, so the line below is optional.
        job_config.source_format = bigquery.SourceFormat.CSV

        project = client.project
        dataset_id = bigquery.DatasetReference(project, 'nba')
        table_id = dataset_id.table(table)

        job = client.load_table_from_dataframe(data, table_id, job_config=job_config)  # Make an API request.
        job.result()  # Wait for the job to complete.

        print(f'Loaded table {table}')

    def func_run_proc(self):

        games = pd.DataFrame()
        arenas = pd.DataFrame()
        bs_trad = pd.DataFrame()
        bs_adv = pd.DataFrame()
        bs_misc = pd.DataFrame()
        bs_score = pd.DataFrame()
        bs_usage = pd.DataFrame()
        bs_four = pd.DataFrame()
        bs_track = pd.DataFrame()
        bs_hustle = pd.DataFrame()
        bs_def = pd.DataFrame()
        bs_match = pd.DataFrame()
        pbp = pd.DataFrame()

        for game in self.GAME_SET:

            print(game)

            games = pd.concat([games, self.func_game_details(game)[0]])
            arenas = pd.concat([arenas, self.func_game_details(game)[1]])
            bs_trad = pd.concat([bs_trad, self.func_box_score(game, 'traditional')])
            bs_adv = pd.concat([bs_adv, self.func_box_score(game, 'advanced')])
            bs_misc = pd.concat([bs_misc, self.func_box_score(game, 'misc')])
            bs_score = pd.concat([bs_score, self.func_box_score(game, 'scoring')])
            bs_usage = pd.concat([bs_usage, self.func_box_score(game, 'usage')])
            bs_four = pd.concat([bs_four, self.func_box_score(game, 'fourfactors')])
            bs_track = pd.concat([bs_track, self.func_box_score(game, 'tracking')])
            bs_hustle = pd.concat([bs_hustle, self.func_box_score(game, 'hustle')])
            bs_def = pd.concat([bs_def, self.func_box_score(game, 'defense')])
            bs_match = pd.concat([bs_match, self.func_box_score(game, 'matchups')])

            pbp = pd.concat([pbp, self.func_play_by_play(game)])
 
        return games, arenas, bs_trad, bs_adv, bs_misc, bs_score, bs_usage, bs_four, bs_track, bs_hustle, bs_def, bs_match, pbp

    def func_load_tables(self):
        self.func_load_data(self.GAMES, 'game_details')
        self.func_load_data(self.ARENAS, 'game_arenas')
        self.func_load_data(self.BS_TRAD, 'boxscore_trad')
        self.func_load_data(self.BS_ADV, 'boxscore_adv')
        self.func_load_data(self.BS_MISC, 'boxscore_misc')
        self.func_load_data(self.BS_SCORE, 'boxscore_scoring')
        self.func_load_data(self.BS_USAGE, 'boxscore_usage')
        self.func_load_data(self.BS_FOUR, 'boxscore_fourfac')
        self.func_load_data(self.BS_TRACK, 'boxscore_tracking')
        self.func_load_data(self.BS_HUSTLE, 'boxscore_hustle')
        self.func_load_data(self.PBP, 'playbyplay')

        test = 0
        complete = 0

        while (complete == 0) & (test < 3):
            try:
                self.func_load_data(self.BS_DEF, 'boxscore_defense')
                complete = 1
            except:
                test += 1    

        test = 0
        complete = 0

        while (complete == 0) & (test < 3):
            try:
                self.func_load_data(self.BS_MATCH, 'boxscore_matchup')
                complete = 1
            except:
                test += 1
                


# In[3]:


nba = NBA()

nba.GAMES, nba.ARENAS = nba.func_game_set_details()


# %%
#################################################################
# If not running all the process and tables at once, run the below instead of func_load_tables
# func_load_tables will expect all tables to be gathered, while process beow can be adjusted

nba.VAR_NAMING_GUIDE # to get table names and their respective BigQuery table name

# create your own table name to BigQuery table name. Pick and choose at your leasure.
tables = {
    'GAMES': {'bigquery_table': 'game_details'}, 
    'ARENAS': {'bigquery_table': 'game_arenas'}, 
    'BS_TRAD': {'bigquery_table': 'boxscore_trad'}, 
    'BS_ADV': {'bigquery_table': 'boxscore_adv'},  
    'BS_MISC': {'bigquery_table': 'boxscore_misc'}, 
    'BS_SCORE': {'bigquery_table': 'boxscore_scoring'}, 
    'BS_USAGE': {'bigquery_table': 'boxscore_usage'}, 
    'BS_FOUR': {'bigquery_table': 'boxscore_fourfac'}, 
    'BS_TRACK': {'bigquery_table': 'boxscore_tracking'}, 
    'BS_HUSTLE': {'bigquery_table': 'boxscore_hustle'}, 
    'BS_MATCH': {'bigquery_table': 'boxscore_matchup'}, 
    'BS_DEF': {'bigquery_table': 'boxscore_defense'}, 
    'PBP': {'bigquery_table': 'playbyplay'}
}

# Load each table defined above to BigQuery
# [nba.func_load_data(vars(nba)[table], tables[table]['bigquery_table']) for table in tables]

# In[4]:


#nba_test_2 = NBA(game_set = {'0022201073', '0022400231'})

# In[5]:


#nba_test_3 = NBA(game_date = date(2022,2,1))

# %%
