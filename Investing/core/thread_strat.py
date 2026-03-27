from Investing.core.generate_st import generate_strat
import time
from datetime import datetime, timezone, timedelta
from Investing.core.define import *
import pandas as pd
import ast

def calculate_flag(db, close_time):
    # Convert time column, handling bad data
    db['time'] = pd.to_datetime(db['time'], errors='coerce')
    
    # Remove timezone (convert to naive)
    db['time'] = db['time'].dt.tz_localize(None)
    
    # Drop rows with invalid times
    db = db.dropna(subset=['time'])
    
    if db.empty:
        return []
    
    # Make close_time naive too
    close_time = pd.to_datetime(close_time).tz_localize(None)
    start_time = close_time - pd.Timedelta(hours=12)
    
    # Filter for the 30-minute window
    mask = (db['time'] > start_time) & (db['time'] < close_time)
    db_12h = db[mask].copy()
    
    if db_12h.empty:
        return []
    
    # Remove netuid 0
    df = db_12h[db_12h['netuid'] != 0]
    
    if df.empty:
        return []
    
    # Sort and get first alpha_in for each netuid
    df = df.sort_values(['netuid', 'time'])

    max_idx = df['tao_price'].idxmax()
    min_idx = df['tao_price'].idxmin()

    # Get the corresponding rows
    max_row = df.loc[max_idx]
    min_row = df.loc[min_idx]

    # Extract values and times
    max_tao_price = max_row['tao_price']
    max_time = max_row['time']
    min_tao_price = min_row['tao_price']
    min_time = min_row['time']

    if max_tao_price - min_tao_price >= min_tao_price * 0.1 and max_time > min_time:
        flag = 1
    else:
        flag = 0

    return flag

if __name__ == "__main__":
    while True:
        db = pd.read_csv(DATA_NAME)
        db = pd.DataFrame(db)
        current_time = datetime.utcnow()
        flag = calculate_flag(db, current_time)
        with open(STAKING_STRATEGY_PATH, 'r') as file:
            file_content = file.read()
            # Convert string representation of dict to actual dict
            last_strat = ast.literal_eval(file_content)
        sum_values = sum(last_strat.values())
        if flag == 1:
            if 0 < sum_values <= 0.1:
                time.sleep(30)
                continue
            else:
                if current_time - lastupdate_time >= timedelta(hours=6):
                    current_time = datetime.utcnow()
                    if current_time.hour >= 13:
                        time = current_time.replace(hour=13, minute=5, second=0, microsecond=0)
                        generate_strat(time, 0, flag)
                    else:
                        time = time = current_time.replace(hour=13, minute=5, second=0, microsecond=0) - timedelta(days=1)
                        generate_strat(time, 0, flag)
                else:
                    time.sleep(30)
                    continue
        else:
            if 0.95 <= sum_values <= 1:
                time.sleep(30)
                continue
            else:
                if current_time - lastupdate_time >= timedelta(hours=6):
                    if current_time.hour >= 13:
                        time = current_time.replace(hour=13, minute=5, second=0, microsecond=0)
                        generate_strat(time, 0, flag)
                    else:
                        time = current_time.replace(hour=13, minute=5, second=0, microsecond=0) - timedelta(days=1)
                        generate_strat(time, 0, flag)
                else:
                    time.sleep(30)
                    continue
   
        if datetime.utcnow().hour == 13 and datetime.utcnow().minute == 5:
            generate_strat(datetime.utcnow(), 0, flag)
            print("Generated new staking strategy at", datetime.now(timezone.utc).replace(microsecond=0))
            time.sleep(300)
        elif datetime.utcnow().hour == 7 and datetime.utcnow().minute == 30:
            generate_strat(datetime.utcnow(), 1, flag)
            print("Generated new stocks strategy at", datetime.now(timezone.utc).replace(microsecond=0))
            time.sleep(300)
        else:
            time.sleep(30)