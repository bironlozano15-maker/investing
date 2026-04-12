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
        datetime(2026, 3, 18, 13, 5, 0),
    ]

    end_time = [
        datetime(2026, 3, 19, 13, 5, 0), 
    ]

    strategy_times = [
        (datetime(2026, 3, 18, 13, 5, 0)),
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
            start = max(s_time, strat_time)
            strat = generate_strat(start, ASSET, 0)
            # strat = {1:0.012876876868,2:0.012876876868,3:0.003029853381,4:0.003029853381,5:0.002642631930,6:0.009847023487,7:0.002642631930,8:0.012876876868,9:0.003029853381,10:0.009847023487,11:0.009847023487,12:0.009847023487,13:0.002642631930,14:0.002642631930,15:0.002642631930,16:0.012876876868,17:0.003029853381,18:0.012876876868,19:0.002642631930,20:0.002642631930,21:0.009847023487,22:0.002642631930,23:0.012876876868,24:0.002642631930,25:0.012876876868,26:0.012876876868,27:0.002642631930,28:0.002642631930,29:0.002642631930,30:0.002642631930,31:0.002642631930,32:0.002642631930,33:0.012876876868,34:0.012876876868,35:0.002642631930,36:0.012876876868,37:0.012876876868,38:0.002642631930,39:0.012876876868,40:0.002642631930,41:0.009847023487,42:0.002642631930,43:0.012876876868,44:0.012876876868,45:0.009847023487,46:0.012876876868,48:0.009847023487,49:0.002642631930,50:0.009847023487,51:0.012876876868,52:0.012876876868,53:0.012876876868,54:0.009847023487,55:0.002642631930,56:0.002642631930,57:0.012876876868,58:0.009847023487,59:0.002642631930,60:0.002642631930,61:0.012876876868,62:0.009847023487,63:0.002642631930,64:0.012876876868,65:0.012876876868,66:0.012876876868,68:0.002642631930,69:0.009847023487,71:0.002642631930,72:0.012876876868,73:0.012876876868,74:0.002642631930,75:0.009847023487,77:0.012876876868,78:0.012876876868,79:0.012876876868,80:0.002642631930,81:0.012876876868,82:0.009847023487,83:0.009847023487,84:0.009847023487,85:0.012876876868,86:0.002642631930,87:0.002642631930,88:0.012876876868,89:0.012876876868,90:0.012876876868,92:0.002642631930,93:0.002642631930,94:0.002642631930,95:0.012876876868,98:0.009847023487,100:0.012876876868,101:0.002642631930,103:0.009847023487,104:0.012876876868,105:0.012876876868,106:0.009847023487,107:0.002642631930,108:0.009847023487,109:0.005637242411,110:0.012876876868,111:0.012876876868,112:0.012876876868,113:0.002642631930,114:0.002642631930,115:0.012876876868,116:0.012876876868,117:0.012876876868,118:0.012876876868,119:0.012876876868,120:0.003029853381,121:0.012876876868,122:0.012876876868,123:0.002642631930,124:0.012876876868,125:0.012876876868,127:0.002642631930,128:0.012876876868}
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