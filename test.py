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
        datetime(2026, 3, 23, 7, 30, 0),
    ]

    end_time = [
        datetime(2026, 3, 24, 7, 30, 0), 
    ]

    strategy_times = [
        (datetime(2026, 3, 23, 7, 30, 0)),
    ]

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

    for s_time, e_time, strat_times in zip(start_time, end_time, strategy_times):
        strat_times = strat_times if isinstance(strat_times, (list, tuple)) else [strat_times]
        for i, strat_time in enumerate(strat_times):
            strat = generate_strat(strat_time, ASSET)
            if isinstance(strat_time, tuple):
                strat_time = strat_time[0]
            start = max(s_time, strat_time)
            new_row = pd.DataFrame([[uid, hotkey, start.date(), start.time(), 
                            datetime_to_blocks(start), fund, strat]], 
                            columns=['uid', 'hotkey', 'date', 'time', 'block', 'fund', 'strat'])
                                
            new_row.to_csv(save_directory, index=False)

            if i + 1 < len(strat_times):
                end = min(strat_times[i+1], e_time)
            else:
                end = e_time
            csv, fund, end, clip, win = (
                os.path.join(save_directory),
                fund,
                end,
                2,
                30
            )

            sim = SimSt(pd.read_csv(csv))
            if fund: sim.fi['fund'] = fund
            if clip >= 0: sim.clip_outliers = clip
            if win: sim.win_size = [win] * AN
            if end:
                sim.db[:AN] = [bn[pd.to_datetime(bn['date']) <= end] if len(bn) else bn for bn in sim.db[:AN]]
            dates = sorted(set([d for bn in sim.db[:AN] if len(bn) for d in bn['date'].values]))
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
            swap = (sim.sc2pct().swap.values[0])
            fund = float(swap)
            os.remove(save_directory)

if __name__ == "__main__": test()