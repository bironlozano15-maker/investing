from Investing.core.division import calculate_division
from Investing.core.define import *
from Investing.core.data_load import fetch_alpha_prices, append_rows_to_csv
from Investing.core.const import *
import pandas as pd
import requests
import time, json, os
import sqlalchemy as sql
from Investing.core.simst import cd

def fetchda(a):
    date = "2026-03-20"
    db = f'{cd}/db/daily{a or ""}.db'
    conn = sql.create_engine(f'sqlite:///{db}').connect()
    if not os.path.getsize(db):
        pd.read_csv(f'{db[:-3]}.00').to_sql('bndaily', conn, index=False)
    bn = pd.read_sql('SELECT * FROM bndaily', conn)
    last = bn['date'].iat[-1] if len(bn) > 1 else FIRST_DATE
    late = [7200, 28800]
    if last >= time.strftime('%F', time.gmtime(time.time() - 86400 - late[a])):
        return bn[bn['date'] >= date]

    print(f'Fetching {db.split("/")[-1]}, begin {last}...', end='', flush=True)
    try: r, e = json.loads(requests.get(f'{API_ROOT}/daily{a or ""}/{last}').json()), 'done'
    except: r, e = [], 'error'
    df = pd.DataFrame(r, None, bn.columns).astype(bn.dtypes)
    if len(bn) > 1: df = df[df['date'] > last]
    bn = pd.concat([bn, df])
    print(f" {e}, end {bn['date'].iat[-1] if len(bn) > 1 else last}.")
    df.to_sql('bndaily', conn, if_exists='append', index=False)
    conn.commit()
    return bn[bn['date'] >= date]

def fetchdb(asset):
    fd = [fetchda(asset) for a in range(2)]
    if len(fd[1]): fd[1].insert(3, 'price', fd[1]['open'])
    for da in ['split', 'dividend'] if len(fd[1]) else []:
        db = f'{cd}/db/{da}.db'
        conn = sql.create_engine(f'sqlite:///{db}').connect()
        try: r = json.loads(requests.get(f'{API_ROOT}/{da}').json())
        except: r = []
        df = pd.DataFrame(r, None, pd.read_csv(f'{db[:-3]}.col').columns)
        if len(df): df.to_sql(da, conn, if_exists='replace', index=False)
        conn.commit()
        try: df = pd.read_sql(f'SELECT * FROM {da}', conn)
        except: pass
        fd.append(df)
    return fd

def generate_strat(start_time, asset):
    if asset == 1:
        fetchdb(asset)

    strat = calculate_division(start_time, asset)
    
    if asset == 0:
        with open(STAKING_STRATEGY_PATH, 'w') as file:
            file.write(strat)
    else:
        with open(STOCKS_STRATEGY_PATH, 'w') as file:
            file.write(str(strat))

    return strat

def fetch_data():
    rows = fetch_alpha_prices()
    append_rows_to_csv(rows, DATA_NAME)
