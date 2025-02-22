# %% imports
from riotwatcher import LolWatcher, ApiError
from datetime import date
import pandas as pd, numpy as np
from tqdm import tqdm
from numpy import nan 
import os
import requests
import json
import time

# %% get current folder
cwd = os.getcwd()
path_data = f'{cwd}/data/'

# %% get key
df_key = pd.read_csv(f'{path_data}key.csv')
key = df_key._get_value(0, 'key')

# %% def funcions
lol_watcher = LolWatcher(key)
extract_date = date.today().strftime("%Y-%m-%d")

def get_challengers(region, ext_date):
    df_rank = pd.DataFrame(columns=['extract_date', 'summonerId', 'puuid', 'pdl', 'wins', 'losses', 'winrate'])
    queue = 'RANKED_SOLO_5x5'
    entries = lol_watcher.league.challenger_by_queue('br1', queue)['entries']
    for item in entries:
        summoner_id = item['summonerId']
        puuid = item['puuid']
        pdl = item['leaguePoints']
        wins = item['wins']
        losses = item['losses']
        winrate = item['wins'] / (item['wins'] + item['losses'])
        #
        df_rank.loc[df_rank.shape[0]] = [
        ext_date,
        summoner_id,
        puuid,
        pdl,
        wins,
        losses,
        winrate,
        ]

    df_rank = df_rank.sort_values(by='pdl', ascending=False)
    return df_rank

def get_summoner(puuid, key):
    try:
        resp_code = 0
        while resp_code != 200:
            resp = requests.get(f'https://americas.api.riotgames.com/riot/account/v1/accounts/by-puuid/{puuid}?api_key={key}')
            resp_code = resp.status_code
            time.sleep(1)

        resp_json = json.loads(resp.text)
        sumonner_name = resp_json['gameName']

    except:
        sumonner_name = np.nan
    return sumonner_name

# %%    get challengers list and summoner name
df_challengers = get_challengers('br1', extract_date)
df_name = pd.DataFrame(columns=['puuid', 'summoner_name'])

for puuid in tqdm(df_challengers['puuid']):
    summoner_name = get_summoner(puuid, key)

    df_name.loc[df_name.shape[0]] = [
        puuid
        ,summoner_name
    ]

df_players = df_challengers.merge(df_name, on= 'puuid')
# %%
df_players.to_csv(f'{path_data}dim_players.csv', index= False)
# %%
