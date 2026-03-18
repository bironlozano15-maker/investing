from datetime import datetime
import os
import pandas as pd
from Investing.core.define import *
from Investing.core.generate_st import generate_strat
from Investing.core.simst import SimSt

def datetime_to_blocks(close_time) -> int:
    base_dt = datetime.strptime(BASE_TIME_STR, '%Y-%m-%d %H:%M:%S')
    delta_sec = (close_time - base_dt).total_seconds()
    return int(BASE_BLOCK + delta_sec // BLOCK_SECONDS)

def test():
    start_time = [
        datetime(2026, 3, 13, 18, 5, 0),
    ]

    end_time = [
        datetime(2026, 3, 14, 18, 5, 0),
    ]

    for i, (s_time, e_time) in enumerate(zip(start_time, end_time)):
        strat = generate_strat(s_time)
        new_row = pd.DataFrame([[uid, hotkey, s_time.date(), s_time.time(), 
                        datetime_to_blocks(s_time), 1000, strat]], 
                        columns=['uid', 'hotkey', 'date', 'time', 'block', 'fund', 'strat'])
                              
        new_row.to_csv(STRATEGY_PATH_CSV, index=False)

        csv, fund, end, clip, win = (
            os.path.join(STRATEGY_PATH_CSV),
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
        os.remove(STRATEGY_PATH_CSV)

if __name__ == "__main__": test()