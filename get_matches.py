# %% imports
from riotwatcher import LolWatcher, ApiError
from datetime import date
import pandas as pd
from tqdm import tqdm
import os

# %% get current folder
print('-----   GET MATCHES     -----')

cwd = os.getcwd()
path_data = f'{cwd}/data/'

# %% get key
df_key = pd.read_csv(f'{path_data}key.csv')
key = df_key._get_value(0, 'key')
lol_watcher = LolWatcher(key)

# %% get all players
df_players = pd.DataFrame()

for dir, subs, files in os.walk(f'{path_data}/bronze/players'):
    for file in files:
        if file.endswith('.csv'):
            df_players_date = pd.read_csv(f'{path_data}/bronze/players/{file}')

            df_players = pd.concat([df_players, df_players_date])

all_players = df_players['puuid'].unique()

# %% get players matches id
matches_list_all = []
for player_puuid in tqdm(all_players):
    player_hist = lol_watcher.match.matchlist_by_puuid('br1', player_puuid)
    for match in player_hist:
        matches_list_all.append(match)

matches_list = list(set(matches_list_all))
print(f'Match Qty:    {len(matches_list)}')

# %%
extract_date = date.today().strftime("%Y-%m-%d")

df_matches = pd.DataFrame(matches_list, columns=['match_id'])
df_matches['extract_date'] = extract_date

df_matches.to_csv(f'{path_data}/bronze/matches_id/dim_matches_{extract_date}.csv', index= False)
# %%
