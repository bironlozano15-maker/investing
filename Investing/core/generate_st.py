from datetime import datetime, timezone
from simst import SimSt
from division import calculate_division
from const import *
import pandas as pd
import os
import time
from data_load import fetch_alpha_prices, append_rows_to_csv
from define import *

def datetime_to_blocks(close_time) -> int:
    base_dt = datetime.strptime(BASE_TIME_STR, '%Y-%m-%d %H:%M:%S')
    delta_sec = (close_time - base_dt).total_seconds()
    return int(BASE_BLOCK + delta_sec // BLOCK_SECONDS)

def generate_strat(start_time):
    strat = calculate_division(start_time)
    strat_dict = {n: float(strat[n-1]) for n in range(1, 129)}
    strat_dict = {k: v for k, v in strat_dict.items() if v != 0}
    strat_string = str(strat_dict)
    
    new_row = pd.DataFrame([[uid, hotkey, start_time.date(), start_time.time(), 
                           datetime_to_blocks(start_time), 1000, strat_string]], 
                          columns=['uid', 'hotkey', 'date', 'time', 'block', 'fund', 'strat'])
    
    try:
        df = pd.read_csv(PATH)
        new_row.to_csv(PATH, index=False)
    except FileNotFoundError:
        new_row.to_csv(PATH, index=False)

def test():
    start_time = [
        datetime(2026, 3, 13, 13, 5, 0),
    ]

    end_time = [
        datetime(2026, 3, 14, 13, 5, 0),
    ]

    for i, (s_time, e_time) in enumerate(zip(start_time, end_time)):
        generate_strat(s_time)
        csv, fund, end, clip, win = (
            os.path.join(SCRIPT_DIR, '..', strat_direct, '{}.csv'.format(hotkey)),
            1000,
            e_time,
            2,
            30
        )

        sim = SimSt(pd.read_csv(csv))
        if fund: sim.fi['fund'] = fund
        if clip >= 0: sim.clip_outliers = clip
        if win: sim.win_size = [win] * an
        if end:
            sim.db[:an] = [bn[pd.to_datetime(bn['date']) <= end] if len(bn) else bn for bn in sim.db[:an]]
        dates = sorted(set([d for bn in sim.db[:an] if len(bn) for d in bn['date'].values]))
        for date in dates:
            sim.pldaily(date, end)
            sim.pldaily1(date, end)
            sim.plfinal()
        sim.pl2sc()
        if sim.pnl_dir:
            os.makedirs(sim.pnl_dir, exist_ok=True)
            sim.pl.to_csv(os.path.join(sim.pnl_dir, f'PnL_{os.path.basename(csv)}'))
        if not len(sim.sc): return
        print(f'clip outlier days: {sim.clip_outliers}, rolling window days: {sim.win_size}')
        print(sim.sc2pct().to_string(index=False))

def live():
    while True:
        if datetime.now().hour == 13 and datetime.now().minute == 5:
            generate_strat(datetime.now())
            time.sleep(300)
        else:
            time.sleep(30)

def fetch_data():
    while True:
        rows = fetch_alpha_prices()
        append_rows_to_csv(rows, data_name)
        print(f"Fetched new data at", datetime.now(timezone.utc).replace(microsecond=0))
        time.sleep(300)

if __name__ == "__main__": test()