import pandas as pd
import numpy as np
from Investing.core.define import *
import sqlite3
import json
import math
from datetime import datetime

def datetime_to_blocks(close_time) -> int:
    base_dt = datetime.strptime(BASE_TIME_STR, '%Y-%m-%d %H:%M:%S')
    delta_sec = (close_time - base_dt).total_seconds()
    return int(BASE_BLOCK + delta_sec // BLOCK_SECONDS)

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

def calculate_ma(db, close_time):
    db = db[db['date'] != '0000-00-00'].copy()
    db['date'] = pd.to_datetime(db['date'], errors='coerce')
    db = db.dropna(subset=['date'])

    if close_time.weekday() == 0:
        delta = 3
    else:
        delta = 1
    target_day = (close_time - pd.Timedelta(days=delta)).date()
    db = db[(db['date'].dt.date == target_day) & (db['ochl'] == 'day')]

    if db.empty:
        return 0

    # --- compute mid price vectorized ---
    db_transformed = db[['date', 'netuid', 'open', 'close']].copy()
    db_transformed['open'] = (db_transformed['open'] + db_transformed['close']) / 2
    db_transformed = db_transformed[['date', 'netuid', 'open']]

    return db_transformed

def calculate_rsi(db, close_time):
    db = db[db['date'] != '0000-00-00'].copy()
    db['date'] = pd.to_datetime(db['date'], errors='coerce')
    db = db.dropna(subset=['date'])

    # --- filter range once ---
    start_time = close_time - pd.Timedelta(days=25)

    mask = (
        (db['date'].dt.date > start_time.date()) &
        (db['date'].dt.date < close_time.date()) &
        (db['ochl'] == 'day')
    )

    db = db.loc[mask]

    if db.empty:
        return 0
    
    db_transformed = db[['date', 'netuid', 'open', 'close']].copy()
    db_transformed['open'] = (db_transformed['open'] + db_transformed['close']) / 2
    db_transformed = db_transformed[['date', 'netuid', 'open']]

    result = (db_transformed
          .sort_values(['netuid', 'date'])  # Sort by netuid then date
          .groupby('netuid')                 # Group by netuid
          .tail(14)                          # Keep last 14 per group
          .reset_index(drop=True))    

    rsi_results = {}
    
    # Group by netuid
    for netuid, group in result.groupby('netuid'):
        # Sort by date just to be sure
        group = group.sort_values('date')
        
        # Get the open prices
        prices = group['open'].values
        
        # Need at least 14 prices for RSI
        if len(prices) < 14:
            continue
            
        # Calculate price differences
        differences = []
        for i in range(1, len(prices)):
            differences.append(prices[i] - prices[i-1])
        
        # Take only the last 13 differences (for 14-period RSI)
        differences = differences[-13:]
        
        # Calculate average gain and loss
        gains = [d for d in differences if d > 0]
        losses = [abs(d) for d in differences if d < 0]
        
        avg_gain = sum(gains) / 13 if gains else 0
        avg_loss = sum(losses) / 13 if losses else 0
        
        # Calculate RSI
        if avg_loss == 0:
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
        
        # Store result
        rsi_results[netuid] = rsi

    return rsi_results

def calculate_ema_fast(db, close_time):
    db = db[db['date'] != '0000-00-00'].copy()
    db['date'] = pd.to_datetime(db['date'], errors='coerce')
    db = db.dropna(subset=['date'])

    # --- filter range once ---
    start_time = close_time - pd.Timedelta(days=20)

    mask = (
        (db['date'].dt.date > start_time.date()) &
        (db['date'].dt.date < close_time.date()) &
        (db['ochl'] == 'day')
    )

    db = db.loc[mask]

    if db.empty:
        return 0
    
    db_transformed = db[['date', 'netuid', 'open', 'close']].copy()
    db_transformed = db_transformed[['date', 'netuid', 'close']]

    result = (db_transformed
          .sort_values(['netuid', 'date'])  # Sort by netuid then date
          .groupby('netuid')                 # Group by netuid
          .tail(11)                        
          .reset_index(drop=True))    
    
    alpha = 2 / (10 + 1)

    ema_dict = {}

    for netuid, group in result.groupby('netuid'):
        group = group.sort_values('date')

        closes = group['close'].values

        # skip if not enough data
        if len(closes) < 11:
            continue

        # Step 1: initial EMA = average of first 10 days
        initial_avg = closes[:10].mean()

        # Step 2: calculate EMA using 11th day
        ema = (closes[10] * alpha) + (initial_avg * (1 - alpha))

        ema_dict[netuid] = ema

    return ema_dict

def calculate_ema_slow(db, close_time):
    db = db[db['date'] != '0000-00-00'].copy()
    db['date'] = pd.to_datetime(db['date'], errors='coerce')
    db = db.dropna(subset=['date'])

    # --- filter range once ---
    start_time = close_time - pd.Timedelta(days=50)

    mask = (
        (db['date'].dt.date > start_time.date()) &
        (db['date'].dt.date < close_time.date()) &
        (db['ochl'] == 'day')
    )

    db = db.loc[mask]

    if db.empty:
        return 0
    
    db_transformed = db[['date', 'netuid', 'open', 'close']].copy()
    db_transformed = db_transformed[['date', 'netuid', 'close']]

    result = (db_transformed
          .sort_values(['netuid', 'date'])  # Sort by netuid then date
          .groupby('netuid')                 # Group by netuid
          .tail(31)                       
          .reset_index(drop=True))    
    
    alpha = 2 / (30 + 1)

    ema_dict = {}

    for netuid, group in result.groupby('netuid'):
        group = group.sort_values('date')

        closes = group['close'].values

        # skip if not enough data
        if len(closes) < 31:
            continue

        # Step 1: initial EMA = average of first 10 days
        initial_avg = closes[:30].mean()

        # Step 2: calculate EMA using 11th day
        ema = (closes[30] * alpha) + (initial_avg * (1 - alpha))

        ema_dict[netuid] = ema

    return ema_dict

def calculate_volume(db, close_time):
    db = db[db['date'] != '0000-00-00'].copy()
    db['date'] = pd.to_datetime(db['date'], errors='coerce')
    db = db.dropna(subset=['date'])

    # --- filter range once ---
    start_time = close_time - pd.Timedelta(days=36)

    mask = (
        (db['date'].dt.date > start_time.date()) &
        (db['date'].dt.date < close_time.date()) &
        (db['ochl'] == 'day')
    )

    db = db.loc[mask]

    if db.empty:
        return 0
    
    db_transformed = db[['date', 'netuid', 'open', 'close', 'volume']].copy()
    db_transformed = db_transformed[['date', 'netuid', 'volume']]

    result = (db_transformed
          .sort_values(['netuid', 'date'])  # Sort by netuid then date
          .groupby('netuid')                 # Group by netuid
          .tail(21)                       
          .reset_index(drop=True))    
    
    volume_dict = {}
    close_dict = {}

    for netuid, group in result.groupby('netuid'):
        group = group.sort_values('date')

        volumes = group['volume'].values

        # skip if not enough data
        if len(volumes) < 21:
            continue

        # Step 1: initial EMA = average of first 20 days
        avg = volumes[:20].mean()
        close_volume = volumes[20]

        volume_dict[netuid] = avg
        close_dict[netuid] = close_volume

    return volume_dict, close_dict

def calculate_resistance(db, close_time):
    db = db[db['date'] != '0000-00-00'].copy()
    db['date'] = pd.to_datetime(db['date'], errors='coerce')
    db = db.dropna(subset=['date'])

    # --- filter range once ---
    start_time = close_time - pd.Timedelta(days=36)

    mask = (
        (db['date'].dt.date > start_time.date()) &
        (db['date'].dt.date < close_time.date()) &
        (db['ochl'] == 'day')
    )

    db = db.loc[mask]

    if db.empty:
        return 0
    
    db_transformed = db[['date', 'netuid', 'open', 'close']].copy()
    db_transformed = db_transformed[['date', 'netuid', 'close']]

    result = (db_transformed
          .sort_values(['netuid', 'date'])  # Sort by netuid then date
          .groupby('netuid')                 # Group by netuid
          .tail(21)                       
          .reset_index(drop=True))    
    
    resistance_max_dict = {}
    resistance_min_dict = {}
    close_dict = {}

    for netuid, group in result.groupby('netuid'):
        group = group.sort_values('date')

        closes = group['close'].values

        # skip if not enough data
        if len(closes) < 21:
            continue

        # Step 1: initial EMA = average of first 20 days
        resistance_max = closes[:20].max()
        resistance_min = closes[:20].min()
        close_price = closes[20]

        resistance_max_dict[netuid] = resistance_max
        resistance_min_dict[netuid] = resistance_min
        close_dict[netuid] = close_price

    return resistance_max_dict, resistance_min_dict, close_dict

def truncate_to_12(value):
    if value >= 0:
        return math.floor(value * 10**12) / 10**12
    else:
        return math.ceil(value * 10**12) / 10**12

def generate_stocks_strat_by_score(investing):
    result = {'_': 1}
    
    if not investing:
        return result
    
    # Check if investing contains tuples (score, score_1) or just single scores
    first_value = next(iter(investing.values()))
    is_tuple = isinstance(first_value, tuple)
    
    if is_tuple:
        # Handle dual scores (score and score_1)
        score_stocks = []
        score_1_stocks = []
        
        for netuid, (score, score_1) in investing.items():
            score_stocks.append((netuid, score))
            score_1_stocks.append((netuid, score_1))
        
        # Sort and assign positive weights
        score_stocks.sort(key=lambda x: x[1], reverse=True)
        positive_groups = [(28, 0.35), (13, 0.11), (13, 0.04)]
        
        start_idx = 0
        for group_size, total_weight in positive_groups:
            group_stocks = score_stocks[start_idx:start_idx + group_size]
            if group_stocks:
                weight_per_stock = total_weight / len(group_stocks)
                # Truncate to 12 decimal places (no rounding)
                weight_per_stock = truncate_to_12(weight_per_stock)
                for stock, _ in group_stocks:
                    result[stock] = weight_per_stock
            start_idx += group_size
        
        # Sort and assign negative weights
        score_1_stocks.sort(key=lambda x: x[1], reverse=True)
        negative_groups = [(28, -0.35), (13, -0.11), (13, -0.04)]
        
        start_idx = 0
        for group_size, total_weight in negative_groups:
            group_stocks = score_1_stocks[start_idx:start_idx + group_size]
            if group_stocks:
                weight_per_stock = total_weight / len(group_stocks)
                # Truncate to 12 decimal places (no rounding)
                weight_per_stock = truncate_to_12(weight_per_stock)
                for stock, _ in group_stocks:
                    if stock in result:
                        result[stock] = truncate_to_12(result[stock] + weight_per_stock)
                    else:
                        result[stock] = weight_per_stock
            start_idx += group_size
    
    else:
        # Handle single score values
        score_stocks = [(netuid, score) for netuid, score in investing.items()]
        score_stocks.sort(key=lambda x: x[1], reverse=True)
        
        # Groups: (number_of_stocks, total_weight)
        groups = [(56, 0.7), (26, 0.22), (26, 0.08)]
        
        start_idx = 0
        for group_size, total_weight in groups:
            group_stocks = score_stocks[start_idx:start_idx + group_size]
            if group_stocks:
                weight_per_stock = total_weight / len(group_stocks)
                # Truncate to 12 decimal places (no rounding)
                weight_per_stock = truncate_to_12(weight_per_stock)
                for stock, _ in group_stocks:
                    result[stock] = weight_per_stock
            start_idx += group_size
    
    return result

def calculate_flow_amount(db, close_time):
    df = db.copy()
    df = df[df['netuid'] != 0]
    df['date'] = pd.to_datetime(df['date'])

    start_time = close_time - pd.Timedelta(hours=1)
    start_block = datetime_to_blocks(start_time)
    close_block = datetime_to_blocks(close_time)

    df = df[(df['block'] >= start_block) & (df['block'] <= close_block)]
    df = df.sort_values(['netuid', 'block'])
    alpha_in_start = df.groupby('netuid')['alpha_in'].first().values
    alpha_in_close = df.groupby('netuid')['alpha_in'].last().values

    tao_in_start = df.groupby('netuid')['tao_in'].first().values
    tao_in_close = df.groupby('netuid')['tao_in'].last().values

    flow_amounts = []
    for i in range(len(alpha_in_start)):
        flow_amount = (alpha_in_start[i] + alpha_in_close[i]) * (tao_in_start[i] + tao_in_close[i]) / 4
        flow_amounts.append(flow_amount)
    
    return flow_amounts

def normalize_flow(flow_amount):
    # Find non-zero values
    non_zero = [x for x in flow_amount if x != 0]
    
    # If all are zero, return as is
    if not non_zero:
        return flow_amount
    
    # Find min and max of non-zero values
    min_val = min(non_zero)
    max_val = max(non_zero)
    
    # Transform each value
    result = []
    for value in flow_amount:
        if value == 0:
            result.append(0)
        else:
            # Scale between 0 and 1
            scaled = (value - min_val) / (max_val - min_val)
            result.append(scaled)
    
    return result

def calculate_momentum(db, close_time):
    df = db.copy()
    df = df[df['netuid'] != 0]
    df['date'] = pd.to_datetime(df['date']).dt.date

    first_time = close_time - pd.Timedelta(days=30)
    last_time = close_time - pd.Timedelta(days=1)
    start_time = close_time - pd.Timedelta(hours=2)
    start_block = datetime_to_blocks(start_time)
    close_block = datetime_to_blocks(close_time)
    
    df_30d = df[(df['date'] >= first_time.date()) & (df['date'] <= last_time.date())]
    df_1h = df[(df['block'] >= start_block) & (df['block'] <= close_block)]
    df_1h = df_1h.sort_values(['netuid', 'block'])
    price_start = df_1h.groupby('netuid')['price'].first().values
    price_close = df_1h.groupby('netuid')['price'].last().values
    average_price = df_30d.groupby('netuid')['price'].mean().values

    momentum_scores = []
    for i in range(len(price_start)):
        momentum_raw = (price_start[i] + price_close[i]) / average_price[i] / 2
        momentum_normalized = (momentum_raw - 0.8) / (1.2 - 0.8)
        momentum_score = max(0, min(1, momentum_normalized))
        momentum_scores.append(momentum_score)
                               
    return momentum_scores

def calculate_tao_flow(db, close_time):
    df = db.copy()
    df = df[df['netuid'] != 0]
    df['date'] = pd.to_datetime(df['date']).dt.date

    first_time = close_time - pd.Timedelta(days=30)
    last_time = close_time - pd.Timedelta(days=1)
    start_time = close_time - pd.Timedelta(hours=2)
    start_block = datetime_to_blocks(start_time)
    close_block = datetime_to_blocks(close_time)
    
    df_30d = df[(df['date'] >= first_time.date()) & (df['date'] <= last_time.date())]
    df_1h = df[(df['block'] >= start_block) & (df['block'] <= close_block)]
    df_1h = df_1h.sort_values(['netuid', 'block'])
    tao_in_start = df_1h.groupby('netuid')['tao_in'].first().values
    tao_in_close = df_1h.groupby('netuid')['tao_in'].last().values
    tao_in_30days_ago = df_30d.groupby('netuid')['tao_in'].mean().values

    tao_flow_scores = []
    for i in range(len(tao_in_start)):
        tao_flow_score = (((tao_in_start[i] + tao_in_close[i]) / 2) - tao_in_30days_ago[i]) / tao_in_30days_ago[i]
        tao_flow_scores.append(tao_flow_score)
                               
    return tao_flow_scores

def normalize_tao_flow(tao_flow_rate):
    flow = np.array(tao_flow_rate)
    
    # Find min and max
    min_val = flow.min()
    max_val = flow.max()
    
    # If all values are the same, return all 0.5
    if min_val == max_val:
        return np.full_like(flow, 0.5, dtype=float)
    
    normalized = (flow - min_val) / (max_val - min_val)
    
    return normalized

def generate_staking_strat_by_score(score):
    score = np.array(score)
    
    # Get indices that would sort the scores in descending order
    sorted_indices = np.argsort(score)[::-1]
    
    # Select top 75 scores and set others to 0
    strat = np.zeros_like(score, dtype=float)
    top_indices = sorted_indices[:75]
    
    # Get the top 75 scores
    top_scores = score[top_indices]
    n = len(top_scores)  # n = 75
    
    # We need: min_value = 0.009, max_value = 0.017, sum = 1
    # For n values with min = a, max = b, and sum = S
    # Values are linearly spaced based on scores
    
    if n == 1:
        # Only one value, must be 1.0 (but this violates min/max if 1.0 not in range)
        scaled_values = np.array([1.0])
    else:
        if np.max(top_scores) == np.min(top_scores):
            # All scores equal - distribute evenly but respect min/max
            # Check if even distribution is possible within bounds
            even_value = 1.0 / n
            if even_value < 0.009 or even_value > 0.017:
                a = 0.009
                b = 0.017
                indices = np.arange(n)
                scaled_values = a + (indices / (n - 1)) * (b - a)
                # Adjust to make sum exactly 1
                current_sum = np.sum(scaled_values)
                scaled_values = scaled_values * (1.0 / current_sum)
            else:
                scaled_values = np.full(n, even_value)
        else:
            # Scale scores to [0.009, 0.017] range
            min_score = np.min(top_scores)
            max_score = np.max(top_scores)
            
            # Initial linear scaling to [0.009, 0.017]
            scaled_values = 0.009 + (top_scores - min_score) / (max_score - min_score) * (0.017 - 0.009)       
            # Keep min and max fixed, adjust intermediate values
            current_sum = np.sum(scaled_values)
            target_sum = 1.0
            
            if current_sum != target_sum:
                # Adjust only the intermediate values (not min or max)
                min_idx = np.argmin(top_scores)
                max_idx = np.argmax(top_scores)
                
                # Sum of min + max
                min_max_sum = scaled_values[min_idx] + scaled_values[max_idx]
                
                # Sum needed from remaining n-2 values
                remaining_target = target_sum - min_max_sum
                remaining_current = current_sum - min_max_sum
                
                if remaining_current != 0:
                    # Scale only the intermediate values
                    intermediate_indices = [i for i in range(n) if i not in [min_idx, max_idx]]
                    scale_factor = remaining_target / remaining_current
                    for idx in intermediate_indices:
                        scaled_values[idx] *= scale_factor
    
    # Assign scaled values back to the strategy array
    strat[top_indices] = scaled_values
    
    return strat
    
def calculate_division(close_time, asset, flag):
    if asset == 1:
        db = load_data(asset)
        db = pd.DataFrame(db)

        ma_current = calculate_ma(db, close_time)
        rsi = calculate_rsi(db, close_time)
        ma_current = ma_current[(ma_current['open'] >= 50) & (ma_current['open'] <= 300)]
        ma_current_netuid = ma_current['netuid']

        ema_fast = calculate_ema_fast(db, close_time)
        ema_slow = calculate_ema_slow(db, close_time)   
        avg_volume, close_volume = calculate_volume(db, close_time)
        resistance_max, resistance_min, close_price = calculate_resistance(db, close_time)
        investing = {}

        for netuid in ma_current_netuid:
            # --- make sure all data exists ---
            if not all(netuid in d for d in [
                ema_fast, ema_slow, rsi,
                avg_volume, close_volume,
                resistance_max, resistance_min
            ]):
                continue

            # --- extract values ---
            ef = float(ema_fast[netuid])
            es = float(ema_slow[netuid])
            r = float(rsi[netuid])
            cv = float(close_volume[netuid])
            av = float(avg_volume[netuid])
            cp = float(close_price[netuid])
            res_max = float(resistance_max[netuid])
            res_min = float(resistance_min[netuid])

            if r == 0:
                continue

            ema_score = (ef - es) / es
            rsi_score = (r - 60) / 60
            volume_score = (cv - av) / cv
            price_score = (cp - res_max) / res_max
            score = 0.2 * ema_score + 0.3 * rsi_score + 0.2 * volume_score + 0.3 * price_score

            ema_score_1 = (es - ef) / ef
            rsi_score_1 = (35 - r) / r
            price_score_1 = (res_min - cp) / cp
            score_1 = 0.2 * ema_score_1 + 0.3 * rsi_score_1 + 0.2 * volume_score + 0.3 * price_score_1
            # if close_time.weekday() == 0:
            #     investing[netuid] = score
            # else:
            #     investing[netuid] = score, score_1
            investing[netuid] = score

        strat = generate_stocks_strat_by_score(investing)
        strat_string = json.dumps(strat)
        strat_string = json.loads(strat_string)
    else:
        db = load_data(asset)
        db = pd.DataFrame(db)
        columns_to_keep = ['date', 'block', 'netuid', 'alpha_in', 'tao_in', 'price']
        db = db[columns_to_keep]
        db_data = pd.read_csv("data.csv")
        db_data['date'] = pd.to_datetime(db_data['date']).dt.strftime('%Y-%m-%d')
        close_block = datetime_to_blocks(close_time)
        max_block_db = db['block'].max()
        if max_block_db < close_block:
            additional_data = db_data[(db_data['block'] > max_block_db) & (db_data['block'] <= close_block)]
            db = pd.concat([db, additional_data], ignore_index=True)

        flow_amount = calculate_flow_amount(db, close_time)
        flow_amount = np.array(flow_amount)
        cutoff = np.percentile(flow_amount, 12)
        flow_amount[flow_amount <= cutoff] = 0
        flow_score = normalize_flow(flow_amount)
        momentum_score = calculate_momentum(db, close_time)
        tao_flow_rate = calculate_tao_flow(db, close_time)
        tao_flow_score = normalize_tao_flow(tao_flow_rate)
        
        score = []
        for i in range(len(flow_score)):
            if flow_score[i] == 0:
                score.append(0)
            else:
                sc = 0.3 * flow_score[i] + 0.3 * momentum_score[i] + 0.4 * tao_flow_score[i]
                score.append(sc)

        strat = generate_staking_strat_by_score(score)

        for i in range(len(strat)):
            strat[i] = math.floor(strat[i] * 10**12) / 10**12

        strat_dict = {n: float(strat[n-1]) for n in range(1, len(strat) + 1)}
        strat_dict = {k: v for k, v in strat_dict.items() if v != 0}

        strat_string = str(strat_dict)
        
    return strat_string