from datetime import datetime, timedelta
import os
import pandas as pd
from Investing.core.define import *
from Investing.core.generate_st import generate_strat
from Investing.core.simst import SimSt
from Investing.core.compare_strategies import calculate_difference_score

def datetime_to_blocks(close_time) -> int:
    base_dt = datetime.strptime(BASE_TIME_STR, '%Y-%m-%d %H:%M:%S')
    delta_sec = (close_time - base_dt).total_seconds()
    return int(BASE_BLOCK + delta_sec // BLOCK_SECONDS)

def calculate_score(save_directory, end, fund):
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
        pl = sim.plfinal()
        for i in range(1, len(pl)):
            value_close = pl.iloc[i]['value_close']
            swap_close = pl.iloc[i]['swap_close']
            if value_close - swap_close > 50:
                prev_swap = pl.iloc[i-1]['swap_close']
                prev_value = pl.iloc[i-1]['value_close']
                current_swap = pl.iloc[i]['swap_close']
                new_value = current_swap + (prev_value - prev_swap)
                pl.iloc[i, pl.columns.get_loc('value_close')] = new_value
        sim.pl = pl
    sim.pl2sc()
    if sim.pnl_dir:
        os.makedirs(sim.pnl_dir, exist_ok=True)
        sim.pl.to_csv(os.path.join(sim.pnl_dir, f'PnL_{os.path.basename(csv)}'))
    if not len(sim.sc): return
    print(f'clip outlier days: {sim.clip_outliers}, rolling window days: {sim.win_size}')
    print(sim.sc2pct().to_string(index=False))
    swap = (sim.sc2pct().swap.values[0])
    value = (sim.sc2pct().value.values[0])
    if float(value) - float(swap) > 50:
        pl = sim.pl
        prev_row = pl.iloc[-2]
        prev_diff = prev_row['value_close'] - prev_row['swap_close']
        value = float(swap) + prev_diff
    fund = float(value)
    if not os.path.exists('result.csv'):
        pl.to_csv('result.csv', index=False, header=True)  # Write headers first time
    else:
        pl.to_csv('result.csv', mode='a', index=False, header=False)  # No headers when appending
    return value

def test():
    signal = 0
    start_time = datetime(2026, 3, 11, 0, 12, 23)
    end_time = datetime(2026, 4, 7, 7, 23, 8)
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

    if signal == 0:
        current_strategy = generate_strat(start_time, ASSET)
        # NEW: Generate 1-hour interval checkpoints
        checkpoints = []
        current = start_time
        past_generate_time = start_time
        while current <= end_time:
            checkpoints.append(current)
            current += timedelta(hours=12)

        strat_times = []
        for checkpoint in checkpoints:
            if checkpoint == start_time:
                strat_times.append(checkpoint)
            else:
                checkpoint_strategy = generate_strat(checkpoint, ASSET)
                difference_score = calculate_difference_score(past_generate_time, checkpoint, current_strategy, checkpoint_strategy)
                if difference_score >= 0.55:
                    current_strategy = checkpoint_strategy
                    past_generate_time = checkpoint
                    strat_times.append(checkpoint)
        print(strat_times)
    else:
        strategy = pd.read_csv("last10.csv")
        strategy['datetime'] = pd.to_datetime(strategy['date'] + ' ' + strategy['time'])
        strategy['datetime'] = strategy['datetime'].dt.tz_localize(None)

        times_before_start = strategy[strategy['datetime'] <= start_time]['datetime']
        times_before_end = strategy[strategy['datetime'] < end_time]['datetime']
        largest_before_start = times_before_start.max()
        largest_before_end = times_before_end.max()

        strat_times = strategy[
            (strategy['datetime'] >= largest_before_start) & 
            (strategy['datetime'] <= largest_before_end)
        ]['datetime'].tolist()

    for i, strat_time in enumerate(strat_times):
        start = max(start_time, strat_time)
        if signal == 0:
            strat = generate_strat(strat_time, ASSET)
        else:
            strat = strategy[strategy['datetime'] == strat_time]['strat'].values[0]
        new_row = pd.DataFrame([[uid, hotkey, start.date(), start.time(), 
                        datetime_to_blocks(start), fund, strat]], 
                        columns=['uid', 'hotkey', 'date', 'time', 'block', 'fund', 'strat'])
                            
        new_row.to_csv(save_directory, index=False)

        if i + 1 < len(strat_times):
            end = min(strat_times[i+1], end_time)
            value = calculate_score(save_directory, end, fund)
        else:
            end = end_time
            value = calculate_score(save_directory, end, fund)
            df = pd.read_csv('result.csv')
            # Convert date to datetime and sort
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            # Get first value_open for each day (remove duplicate dates)
            daily_open = df.groupby('date')['value_open'].first().reset_index()
            daily_open.columns = ['date', 'open']
            value = float(value)
            # Get open values for next day as close
            daily_open['close'] = daily_open['open'].shift(-1)
            # Fill the last empty close value with swap
            daily_open.loc[daily_open.index[-1], 'close'] = value
            # Save to result1.csv
            daily_open.to_csv('result1.csv', index=False)
            os.remove(save_directory)
        fund = float(value)

if __name__ == "__main__": test()