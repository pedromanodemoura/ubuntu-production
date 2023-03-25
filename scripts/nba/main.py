import requests
import pandas as pd
import lxml.html as LH
import json
import datetime
import time
from google.cloud import bigquery


def get_date():
    today = datetime.date.today()
    
    yesterday = today - datetime.timedelta(days=1)
    
    yesterday_str = datetime.datetime.strftime(yesterday, '%Y-%m-%d')
    
    return yesterday_str

def clean_cols(col_name):
    new_col_name = col_name.split('.')[-1]
    
    return new_col_name
    
def get_req(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0'}

    html_content = requests.get(url, headers = headers).text
    tables = LH.fromstring(html_content)
        
    table_json = tables.xpath('/html/body/script[1]')[0].text
    
    json_object = json.loads(table_json)
    
    df = pd.json_normalize(json_object)    
    
    time.sleep(1)
    
    return df

def get_games():
    url = "https://www.nba.com"

    df = get_req(url)

    games_slides = pd.DataFrame(df['props.pageProps.rollingschedule'][0])

    home_games = pd.DataFrame(games_slides[games_slides['gameDate'] == get_date()].reset_index(drop=True)['games'][0])[
        ['gameId','period','gameDateTimeEst','day','arenaName','arenaState','arenaCity','homeTeam']]
    away_games = pd.DataFrame(games_slides[games_slides['gameDate'] == get_date()].reset_index(drop=True)['games'][0])[
        ['gameId','period','gameDateTimeEst','day','arenaName','arenaState','arenaCity','visitorTeam']]
    
    home_games['homeaway'] = 'home'
    away_games['homeaway'] = 'away'
    
    games = pd.concat([
        pd.concat([home_games.drop(['homeTeam'], axis=1), home_games['homeTeam'].apply(pd.Series)], axis=1),
        pd.concat([away_games.drop(['visitorTeam'], axis=1), away_games['visitorTeam'].apply(pd.Series)], axis=1)
        ]).reset_index(drop=True)
    
    return games

def box_score(game_id, bs_type):
    bs_list = {'traditional', 'advanced', 'misc', 'scoring', 'usage', 'fourfactors', 'tracking', 'hustle', 'defense', 'matchups'}
    
    if bs_type not in bs_list:
        raise ValueError("results: box score type must be one of %r." % bs_list)
    
    url = f"https://www.nba.com/game/{game_id}/box-score?type={bs_type}"
    
    test = 0
    complete = 0
    
    while (complete == 0) & (test < 3):
        try:
            df = get_req(url)
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
    
    full_game.columns = list(map(clean_cols, full_game.columns))
    
    print(f"{game_id} - {bs_type}")
    
    return full_game

def play_by_play(game_id):    
    url = f"https://www.nba.com/game/{game_id}/play-by-play"

    df = get_req(url)

    pbp = pd.json_normalize(df['props.pageProps.playByPlay.actions'][0])
    
    pbp['gameId'] = game_id
    
    print(f"{game_id} - playbyplay")
        
    return pbp

def load_data(data, table):
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_APPEND
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    
    data['dt'] = get_date()
            
    project = client.project
    dataset_id = bigquery.DatasetReference(project, 'nba')
    table_id = dataset_id.table(table)

    job = client.load_table_from_dataframe(data, table_id, job_config=job_config)  # Make an API request.
    job.result()  # Wait for the job to complete.
    
    print(f'Loaded table {table}')

def run_proc():
    games = get_games()
    
    games_set = set(games['gameId'])
    
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
    
    for game in games_set:
        
        print(game)
    
        bs_trad = pd.concat([bs_trad, box_score(game, 'traditional')])
        bs_adv = pd.concat([bs_adv, box_score(game, 'advanced')])
        bs_misc = pd.concat([bs_misc, box_score(game, 'misc')])
        bs_score = pd.concat([bs_score, box_score(game, 'scoring')])
        bs_usage = pd.concat([bs_usage, box_score(game, 'usage')])
        bs_four = pd.concat([bs_four, box_score(game, 'fourfactors')])
        bs_track = pd.concat([bs_track, box_score(game, 'tracking')])
        bs_hustle = pd.concat([bs_hustle, box_score(game, 'hustle')])
        bs_def = pd.concat([bs_def, box_score(game, 'defense')])
        bs_match = pd.concat([bs_match, box_score(game, 'matchups')])
        
        pbp = pd.concat([pbp, play_by_play(game)])
        
    load_data(games, 'game_details')
    load_data(bs_trad, 'boxscore_trad')
    load_data(bs_adv, 'boxscore_adv')
    load_data(bs_misc, 'boxscore_misc')
    load_data(bs_score, 'boxscore_scoring')
    load_data(bs_usage, 'boxscore_usage')
    load_data(bs_four, 'boxscore_fourfac')
    load_data(bs_track, 'boxscore_tracking')
    load_data(bs_hustle, 'boxscore_hustle')
    load_data(pbp, 'playbyplay')

    test = 0
    complete = 0
    
    while (complete == 0) & (test < 3):
        try:
            load_data(bs_def, 'boxscore_defense')
            complete = 1
        except:
            test += 1    
            
    test = 0
    complete = 0
    
    while (complete == 0) & (test < 3):
        try:
            load_data(bs_match, 'boxscore_matchup')
            complete = 1
        except:
            test += 1


    
