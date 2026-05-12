from Investing.core.division import calculate_division
from Investing.core.define import *
from Investing.core.data_load import fetch_alpha_prices, append_rows_to_csv
from Investing.core.const import *
import pandas as pd
import numpy as np
import requests
import time, json, os
import sqlalchemy as sql
from Investing.core.simst import cd

def fetchda(asset):
    db = f'{cd}/db/daily{asset or ""}.db'
    conn = sql.create_engine(f'sqlite:///{db}').connect()
    if not os.path.getsize(db):
        pd.read_csv(f'{db[:-3]}.00').to_sql('bndaily', conn, index=False)
    bn = pd.read_sql('SELECT * FROM bndaily', conn)
    last = bn['date'].iat[-1] if len(bn) > 1 else FIRST_DATE
    late = [7200, 28800]
    if last >= time.strftime('%F', time.gmtime(time.time() - 86400 - late[0])):
        return None

    print(f'Fetching {db.split("/")[-1]}, begin {last}...', end='', flush=True)
    try: r, e = json.loads(requests.get(f'{API_ROOT}/daily{asset or ""}/{last}').json()), 'done'
    except: r, e = [], 'error'
    df = pd.DataFrame(r, None, bn.columns).astype(bn.dtypes)
    if len(bn) > 1: df = df[df['date'] > last]
    bn = pd.concat([bn, df])
    print(f" {e}, end {bn['date'].iat[-1] if len(bn) > 1 else last}.")
    df.to_sql('bndaily', conn, if_exists='append', index=False)
    conn.commit()

def calculate_compare_score(past_score, score):
    if isinstance(past_score, str):
        past_score = np.fromstring(past_score.strip("[]"), sep=" ")
    past_non_zero = [v for v in past_score if v != 0]
    past_sorted = sorted(past_non_zero, reverse=True)
    past_72value = past_sorted[71]
    past_netuid = []
    for netuid, value in enumerate(past_score):
        if value != 0 and value >= past_72value:
            past_netuid.append(netuid)

    current_non_zero = [v for v in score if v != 0]
    current_sorted = sorted(current_non_zero, reverse=True)
    current_72value = current_sorted[71]
    current_netuid = []
    for netuid, value in enumerate(score):
        if value != 0 and value >= current_72value:
            current_netuid.append(netuid)
    matching_count = len(set(past_netuid) & set(current_netuid))
    compare_score = matching_count / 72

    return compare_score

def generate_strat(start_time, asset, raw_db = None):
    if raw_db is None:
        fetchda(ASSET)
    strat, score = calculate_division(start_time, asset, raw_db)
    if not os.path.isfile(STAKING_STRATEGY_PATH):
        with open(STAKING_STRATEGY_PATH, 'w') as file:
            file.write(strat)
        with open(SCORE_PATH, 'w') as file:
            file.write(str(score))
        return strat
    else:
        with open(SCORE_PATH, 'r') as file:
            past_score = file.read()
        compare_score = calculate_compare_score(past_score, score)
        if compare_score <= 0.65:
            with open(STAKING_STRATEGY_PATH, 'w') as file:
                file.write(strat)
            with open(SCORE_PATH, 'w') as file:
                file.write(str(score))
            return strat

    return None

def fetch_data():
    rows = fetch_alpha_prices()
    append_rows_to_csv(rows, DATA_NAME)

    df = pd.read_csv(DATA_NAME)
    df['date'] = pd.to_datetime(df['date'])
    df = df[df['date'] > (pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=7))]
    df.to_csv(DATA_NAME, index=False)
