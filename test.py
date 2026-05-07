from Investing.core.const import *
from Investing.core.derived_simst import *
from Investing.core.define import *
from Investing.core.generate import generate_strat
from datetime import datetime, timedelta
import pandas as pd
import sqlite3, math

def fetch_from_top_strategy(start, end):
    strategy = pd.read_csv(TOP_STRATEGY)
    strategy['datetime'] = pd.to_datetime(strategy['date'] + ' ' + strategy['time'])
    strategy['datetime'] = strategy['datetime'].dt.tz_localize(None)
    largest_before_start = strategy[strategy['datetime'] <= start]['datetime'].max()
    largest_before_end = strategy[strategy['datetime'] < end]['datetime'].max()

    strat_times = strategy[
        (strategy['datetime'] >= largest_before_start) & 
        (strategy['datetime'] <= largest_before_end)
    ]['datetime'].tolist()
    strats = strategy[
        (strategy['datetime'] >= largest_before_start) & 
        (strategy['datetime'] <= largest_before_end)
    ]['strat'].tolist()
    return strat_times, strats

def load_data(asset):
    if asset == 1:
        conn = sqlite3.connect(r'Investing/core/db/daily1.db')
    else:
        conn = sqlite3.connect(r'Investing/core/db/daily.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM bndaily")
    rows = cursor.fetchall()
    conn.close()
    # Reorganize data: column_name -> list of all values in that column
    db = {}
    if rows:
        # Get all column names
        columns = rows[0].keys()
        # For each column, collect all values
        for col in columns:
            db[col] = [row[col] for row in rows]
    return db

def datetime_to_blocks(time, db):
    base_time = time.replace(hour=0, minute=0, second=0, microsecond=0)
    db['date'] = pd.to_datetime(db['date'], format='%Y-%m-%d', errors='coerce')
    db = db.dropna(subset=['date'])
    yesterday = pd.Timestamp(time.date() - timedelta(days=1))
    filtered = db[(db['date'].dt.date == yesterday.date()) & (db['netuid'] == 0)]
    base_block = filtered['block'].iat[-1]
    delta_time = (time - base_time).total_seconds()
    block = base_block + delta_time / 12
    block = math.ceil(block)
    return block

def main():
    db = load_data(ASSET)
    db = pd.DataFrame(db)
    #set the start_time, end_time
    start = datetime(2026, 4, 10, 2, 35, 20)
    end = datetime(2026, 4, 29, 0, 21, 17)
    end_block = datetime_to_blocks(end, db)
    #delete the past strat
    if os.path.exists(STAKING_STRATEGY_PATH):
        os.remove(STAKING_STRATEGY_PATH)
    #calculate the strat_times and strats
    #if signal=1, this is top miner and if signal=0, this is my strategy
    if SIGNAL == 1:
        strat_times, strats = fetch_from_top_strategy(start, end)
    else:
        check_point = start
        check_points = []
        while check_point < end:
            check_points.append(check_point)
            check_point += timedelta(hours=24)
        strats = []
        strat_times = []
        for check_point in check_points:
            checkpoint_strat = generate_strat(check_point, ASSET)
            if checkpoint_strat is not None:
                print(check_point)
                strats.append(checkpoint_strat)
                strat_times.append(check_point)
    #if ASSET = 0, this is staking. if ASSET = 1, this is stocks.
    if ASSET == 0:
        fund = STAKING_FUND
        hotkey = STAKING_HOTKEY
        uid = STAKING_UID
        save_directory = STAKING_STRATEGY_PATH_CSV
    else:
        fund = STOCKS_FUND
        hotkey = STOCKS_HOTKEY
        uid = STOCKS_UID
        save_directory = STOCKS_STRATEGY_PATH_CSV
    
    csv, fund, end, clip, win = (
        os.path.join(save_directory),
        fund,
        end,
        CLIP_OUTLIERS,
        WIN_SIZE_DTAO
    )
    #generate the strategy csv file.
    file_exists = os.path.isfile(save_directory)
    for i, (strat_time, strat) in enumerate(zip(strat_times, strats)):
        strat_time_block = datetime_to_blocks(strat_time, db)
        new_row = pd.DataFrame([[uid, hotkey, strat_time.date(), strat_time_block, fund, strat]], 
                    columns=['uid', 'hotkey', 'date', 'block', 'fund', 'strat'])
        new_row.to_csv(save_directory, mode='a', header=(not file_exists and i == 0), index=False)
    #calculate the score
    sim = SimSt(pd.read_csv(csv))
    if fund: sim.fi['fund'] = fund
    if clip >= 0: sim.clip_outliers = clip
    if win: sim.win_size = [win] * an
    if end:
        sim.db[:an] = [bn[bn['block'] <= end_block] if len(bn) else bn for bn in sim.db[:an]]
    dates = sorted(set([d for bn in sim.db[:an] if len(bn) for d in bn['date'].values]))
    for date in dates:
        sim.pldaily(date)
        sim.pldaily1(date)
        sim.plfinal()
    sim.pl2sc()
    if sim.pnl_dir:
        os.makedirs(sim.pnl_dir, exist_ok=True)
        output_path = os.path.join(sim.pnl_dir, f'PnL_{os.path.basename(csv)}')
        sim.pl.to_csv(output_path)
    if not len(sim.sc): return
    print(sim.sc2pct().to_string(index=False))
    os.remove(save_directory)

if __name__ == "__main__": main()