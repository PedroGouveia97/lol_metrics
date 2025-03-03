# %% imports
from riotwatcher import LolWatcher, ApiError
from datetime import datetime
import pandas as pd, numpy as np
from tqdm import tqdm
import os
import requests
import json
import time

# %% get current folder
print('-----   GET MATCH DETAIL     -----')

cwd = os.getcwd()
path_data = f'{cwd}/data/'

# %% get key
df_key = pd.read_csv(f'{path_data}key.csv')
key = df_key._get_value(0, 'key')
lol_watcher = LolWatcher(key)

# %% get all matches
df_matches = pd.DataFrame()

for dir, subs, files in os.walk(f'{path_data}/bronze/matches_id'):
    for file in files:
        if file.endswith('.csv'):
            #print(file)
            df_matches_date = pd.read_csv(f'{path_data}/bronze/matches_id/{file}')
            df_matches = pd.concat([df_matches, df_matches_date])

all_matches = df_matches.drop_duplicates(subset='match_id')['match_id']

# filter matches processed
df_matches_detail = pd.DataFrame()

for dir, subs, files in os.walk(f'{path_data}/bronze/matches_detail'):
    for file in files:
        if file.endswith('.csv'):
            #print(file)
            df_matches_date = pd.read_csv(f'{path_data}/bronze/matches_detail/{file}')

            df_matches_detail = pd.concat([df_matches_detail, df_matches_date])

processed_matches = df_matches_detail.drop_duplicates(subset='match_id')['match_id']

# %%
new_games = list(set(all_matches) - set(processed_matches))

# %%
df_metadata = pd.DataFrame()
df_info = pd.DataFrame()

for match in tqdm(new_games):
    match_all = lol_watcher.match.by_id('br1', match)

    match_metadata = match_all['metadata']
    df_metadata_match = pd.json_normalize(match_metadata)
    df_metadata = pd.concat([df_metadata_match, df_metadata])

    match_info = match_all['info']
    df_info_match = pd.json_normalize(match_info)
    df_info_match = df_info_match.rename(columns={'participants': 'ingameData'})
    df_info_match['matchId'] = match
    df_info = pd.concat([df_info_match, df_info])

# %%
df_match_detail = df_info.merge(df_metadata, on='matchId', how='left')
df_match_detail = df_match_detail.rename(columns={'matchId': 'match_id'})

# %%
extract_date = datetime.today().strftime("%Y-%m-%d__%H_%M_%S")

df_match_detail.to_csv(f'{path_data}/bronze/matches_detail/matches_{extract_date}.csv', index= False)
# %%

# %%
