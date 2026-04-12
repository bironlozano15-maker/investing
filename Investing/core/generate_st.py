from Investing.core.division import calculate_division
from Investing.core.define import *
from Investing.core.data_load import fetch_alpha_prices, append_rows_to_csv
from Investing.core.const import *
import pandas as pd
import requests
import time, json, os
import sqlalchemy as sql
from Investing.core.simst import cd
from datetime import datetime, timezone, timedelta

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

def generate_strat(start_time, asset, flag):
    fetchda(ASSET)

    strat = calculate_division(start_time, asset, flag)
    
    if asset == 0:
        with open(STAKING_STRATEGY_PATH, 'w') as file:
            file.write(strat)
    else:
        with open(STOCKS_STRATEGY_PATH, 'w') as file:
            file.write(str(strat))

    print("Generated new strategy at", datetime.now(timezone.utc).replace(microsecond=0), "asset=", asset, "flag =", flag)

    return strat

def fetch_data():
    rows = fetch_alpha_prices()
    append_rows_to_csv(rows, DATA_NAME)
