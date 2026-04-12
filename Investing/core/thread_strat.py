from Investing.core.generate_st import generate_strat
import time
from datetime import datetime, timedelta
from Investing.core.define import *
import pandas as pd
import ast
import math

def calculate_flag(db, close_time, sign):
    # Convert time column, handling bad data
    db['time'] = pd.to_datetime(db['time'], errors='coerce')
    
    # Remove timezone (convert to naive)
    db['time'] = db['time'].dt.tz_localize(None)
    
    # Drop rows with invalid times
    db = db.dropna(subset=['time'])
    
    if db.empty:
        return 0
    
    # Make close_time naive too
    close_time = pd.to_datetime(close_time).tz_localize(None)
    start_time = close_time - pd.Timedelta(hours=20)
    
    # Filter for the 30-minute window
    mask = (db['time'] > start_time) & (db['time'] < close_time)
    db_20h = db[mask].copy()
    
    if db_20h.empty:
        return 0
    
    # Remove netuid 0
    df = db_20h[db_20h['netuid'] != 0]
    
    if df.empty:
        return 0
    
    # Sort and get first alpha_in for each netuid
    df = df.sort_values(['netuid', 'time'])

    max_idx = df['price'].idxmax()
    min_idx = df['price'].idxmin()

    # Get the corresponding rows
    max_row = df.loc[max_idx]
    min_row = df.loc[min_idx]

    # Extract values and times
    max_tao_price = max_row['price']
    max_time = max_row['time']
    min_tao_price = min_row['price']
    min_time = min_row['time']

    time_delta = (max_time - min_time).total_seconds() / 3600
    price_delta = (max_tao_price - min_tao_price) / min_tao_price

    time = min(time_delta / FLUCT_TIME, 1.0)
    price = min(price_delta / FLUCT_RATE, 1.0)
    value = max(math.sqrt(price * time), 0) if price * time >= 0 else 0
    flag = (value >= STANDARD_PROB) if sign == 0 else (not ((max_tao_price - min_tao_price) / min_tao_price < STABLE_RATE or max_time < min_time))

    return flag

def check_flag(sign):
    db = pd.read_csv(TAO_DATA_NAME)
    db = pd.DataFrame(db)
    current_time = datetime.utcnow()
    flag = calculate_flag(db, current_time, sign)
    with open(STAKING_STRATEGY_PATH, 'r') as file:
        file_content = file.read()
        # Convert string representation of dict to actual dict
        last_strat = ast.literal_eval(file_content)
    sum_values = sum(last_strat.values())
    if flag == 1 and sum_values > 0.1:
        if current_time.hour >= 13:
            time = current_time.replace(hour=13, minute=5, second=0, microsecond=0)
            generate_strat(time, 0, flag)
        else:
            time = current_time.replace(hour=13, minute=5, second=0, microsecond=0) - timedelta(days=1)
            generate_strat(time, 0, flag)
    elif flag == 0 and sum_values < 0.1:
        if current_time.hour >= 13:
            time = current_time.replace(hour=13, minute=5, second=0, microsecond=0)
            generate_strat(time, 0, flag)
        else:
            time = current_time.replace(hour=13, minute=5, second=0, microsecond=0) - timedelta(days=1)
            next_update_time = time + pd.Timedelta(days=1)
            if next_update_time.hour - current_time.hour >= 3:
                generate_strat(time, 0, flag)

    return flag

if __name__ == "__main__":
    sign = 0
    while True:
        try:
            flag = check_flag(sign)
            sign = flag
        except Exception as e:
            time.sleep(1)  # Wait 1 second before retrying

        if datetime.utcnow().hour == 13 and datetime.utcnow().minute == 5:
            generate_strat(datetime.utcnow(), 0, flag)
            time.sleep(300)
        elif datetime.utcnow().hour == 8 and datetime.utcnow().minute == 30:
            generate_strat(datetime.utcnow(), 1, 0)
            time.sleep(300)
        else:
            time.sleep(30)