from datetime import datetime, timezone
import os
import pandas as pd
from Investing.core.define import *
from Investing.core.generate_st import generate_strat
from Investing.core.simst import SimSt
from Investing.core.thread_strat import calculate_flag


def datetime_to_blocks(close_time) -> int:
    base_dt = datetime.strptime(BASE_TIME_STR, '%Y-%m-%d %H:%M:%S')
    delta_sec = (close_time - base_dt).total_seconds()
    return int(BASE_BLOCK + delta_sec // BLOCK_SECONDS)

def test():
    start_time = [
        datetime(2026, 3, 27, 0, 0, 0),
        datetime(2026, 3, 28, 0, 0, 0),
        datetime(2026, 3, 29, 0, 0, 0),
    ]

    end_time = [
        datetime(2026, 3, 27, 23, 59, 0),
        datetime(2026, 3, 28, 23, 59, 0),
        datetime(2026, 3, 29, 23, 59, 0),
    ] 

    # strategy_times = [
    #     (datetime(2026, 3, 13, 13, 5, 0)),
    # ]

    strategy_times = []
    for s_time, e_time in zip(start_time, end_time):
        strategy = pd.read_csv("last10.csv")
        strategy['datetime'] = pd.to_datetime(strategy['date'] + ' ' + strategy['time'])
        strategy['datetime'] = strategy['datetime'].dt.tz_localize(None)

        times_before_start = strategy[strategy['datetime'] < s_time]['datetime']
        times_before_end = strategy[strategy['datetime'] < e_time]['datetime']
        largest_before_start = times_before_start.max()
        largest_before_end = times_before_end.max()

        strat_times = strategy[
            (strategy['datetime'] >= largest_before_start) & 
            (strategy['datetime'] <= largest_before_end)
        ]['datetime'].tolist()
        strategy_times.append(strat_times)

    for s_time, e_time, strat_times in zip(start_time, end_time, strategy_times):
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

        strat_times = strat_times if isinstance(strat_times, (list, tuple)) else [strat_times]
        for i, strat_time in enumerate(strat_times):
            start = max(s_time, strat_time)
            # strat = generate_strat(strat_time, ASSET, 0)
            strat = strategy[strategy['datetime'] == strat_time]['strat'].values[0]
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