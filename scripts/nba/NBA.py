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


class NBA:
    def __init__(self, game_date = None, game_set = {}):
        if game_date is None:
            today = datetime.date.today()
            yesterday = today - datetime.timedelta(days=1)
            game_date = datetime.datetime.strftime(yesterday, '%Y-%m-%d')
        self.game_date = game_date
        
        if game_set == {}:
            game_set = set(self.get_games(self.game_date))
        self.game_set = game_set
        
        self.games, self.arenas, self.bs_trad, self.bs_adv, self.bs_misc, self.bs_score, self.bs_usage, self.bs_four, self.bs_track, self.bs_hustle, self.bs_def, self.bs_match, self.pbp = self.run_proc()

        # self.load_tables()

    def clean_cols(self, col_name):
        new_col_name = col_name.split('.')[-1]

        return new_col_name

    def get_req(self, url):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0'}

        html_content = requests.get(url, headers = headers).text
        tables = LH.fromstring(html_content)

        table_json = tables.xpath('/html/body/script[1]')[0].text

        json_object = json.loads(table_json)

        df = pd.json_normalize(json_object)    

        time.sleep(1)

        return df

    def get_games(self, game_date):
        url = f"https://www.nba.com/games?date={game_date}"

        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0'}

        html_content = requests.get(url, headers = headers).text
        tables = LH.fromstring(html_content)

        table_json = tables.xpath('//a[contains(@class, "GameCard_gcm__SKtfh")]')
        
        games_list = [i.attrib['href'].split('-')[-1] for i in table_json]
        
        return games_list
        
    def game_details(self, game_id):

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
        df_game.columns = list(map(self.clean_cols, df_game.columns))

        df_arena = df[['props.pageProps.game.gameId', 'props.pageProps.game.arena.arenaId', 'props.pageProps.game.arena.arenaName', 
                       'props.pageProps.game.arena.arenaCity', 'props.pageProps.game.arena.arenaState', 'props.pageProps.game.arena.arenaCountry']]
        df_arena.columns = list(map(self.clean_cols, df_arena.columns))     

        return df_game, df_arena

    def box_score(self, game_id, bs_type):
        bs_list = {'traditional', 'advanced', 'misc', 'scoring', 'usage', 'fourfactors', 'tracking', 'hustle', 'defense', 'matchups'}

        if bs_type not in bs_list:
            raise ValueError("results: box score type must be one of %r." % bs_list)

        url = f"https://www.nba.com/game/{game_id}/box-score?type={bs_type}"

        test = 0
        complete = 0

        while (complete == 0) & (test < 3):
            try:
                df = self.get_req(url)
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

        full_game.columns = list(map(self.clean_cols, full_game.columns))

        # print(f"{game_id} - {bs_type}")

        return full_game

    def play_by_play(self, game_id):    
        url = f"https://www.nba.com/game/{game_id}/play-by-play"

        df = self.get_req(url)

        pbp = pd.json_normalize(df['props.pageProps.playByPlay.actions'][0])

        pbp['gameId'] = game_id

        print(f"{game_id} - playbyplay")

        return pbp

    def load_data(self, data, table):
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

    def run_proc(self):

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

        for game in self.game_set:

            print(game)

            games = pd.concat([games, self.game_details(game)[0]])
            arenas = pd.concat([arenas, self.game_details(game)[1]])
            bs_trad = pd.concat([bs_trad, self.box_score(game, 'traditional')])
            bs_adv = pd.concat([bs_adv, self.box_score(game, 'advanced')])
            bs_misc = pd.concat([bs_misc, self.box_score(game, 'misc')])
            bs_score = pd.concat([bs_score, self.box_score(game, 'scoring')])
            bs_usage = pd.concat([bs_usage, self.box_score(game, 'usage')])
            bs_four = pd.concat([bs_four, self.box_score(game, 'fourfactors')])
            bs_track = pd.concat([bs_track, self.box_score(game, 'tracking')])
            bs_hustle = pd.concat([bs_hustle, self.box_score(game, 'hustle')])
            bs_def = pd.concat([bs_def, self.box_score(game, 'defense')])
            bs_match = pd.concat([bs_match, self.box_score(game, 'matchups')])

            pbp = pd.concat([pbp, self.play_by_play(game)])
 
        return games, arenas, bs_trad, bs_adv, bs_misc, bs_score, bs_usage, bs_four, bs_track, bs_hustle, bs_def, bs_match, pbp

    def load_tables(self):
        self.load_data(self.games, 'game_details')
        self.load_data(self.arenas, 'game_arenas')
        self.load_data(self.bs_trad, 'boxscore_trad')
        self.load_data(self.bs_adv, 'boxscore_adv')
        self.load_data(self.bs_misc, 'boxscore_misc')
        self.load_data(self.bs_score, 'boxscore_scoring')
        self.load_data(self.bs_usage, 'boxscore_usage')
        self.load_data(self.bs_four, 'boxscore_fourfac')
        self.load_data(self.bs_track, 'boxscore_tracking')
        self.load_data(self.bs_hustle, 'boxscore_hustle')
        self.load_data(self.pbp, 'playbyplay')

        test = 0
        complete = 0

        while (complete == 0) & (test < 3):
            try:
                self.load_data(self.bs_def, 'boxscore_defense')
                complete = 1
            except:
                test += 1    

        test = 0
        complete = 0

        while (complete == 0) & (test < 3):
            try:
                self.load_data(self.bs_match, 'boxscore_matchup')
                complete = 1
            except:
                test += 1
                


# In[3]:


nba_test = NBA()


# In[4]:


# nba_test = NBA(game_set = {'0022201073'})


# In[5]:


# nba_test_2 = NBA(game_date = '1974-02-01')

