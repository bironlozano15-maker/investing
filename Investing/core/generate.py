from Investing.core.division import calculate_division
from Investing.core.define import *
from Investing.core.data_load import fetch_alpha_prices, append_rows_to_csv
from Investing.core.const import *
import pandas as pd
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

def calculate_compare_score(past_strat, strat):
    import ast
    # Convert string to dict if needed
    if isinstance(past_strat, str):
        past_strat = ast.literal_eval(past_strat)
    if isinstance(strat, str):
        strat = ast.literal_eval(strat)
    # Get netuid sets
    past_keys = set(past_strat.keys())
    current_keys = set(strat.keys()) 
    # Count matching netuids
    matching = len(past_keys & current_keys)
    # Total netuids in past strategy
    total = len(past_keys)
    # Avoid division by zero
    if total == 0:
        return 0
    # Calculate score
    score = matching / total
    return score

def generate_strat(start_time, asset, raw_db = None):
    if raw_db is None:
        fetchda(ASSET)
    strat = calculate_division(start_time, asset, raw_db)
    if not os.path.isfile(STAKING_STRATEGY_PATH):
        with open(STAKING_STRATEGY_PATH, 'w') as file:
            file.write(strat)
        return strat
    else:
        with open(STAKING_STRATEGY_PATH, 'r') as file:
            past_strat = file.read()
        compare_strat = calculate_compare_score(past_strat, strat)
        if compare_strat <= 0.6:
            with open(STAKING_STRATEGY_PATH, 'w') as file:
                file.write(strat)
            return strat

    return None

def fetch_data():
    rows = fetch_alpha_prices()
    append_rows_to_csv(rows, DATA_NAME)

    df = pd.read_csv(DATA_NAME)
    df['date'] = pd.to_datetime(df['date'])
    df = df[df['date'] > (pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=7))]
    df.to_csv(DATA_NAME, index=False)
