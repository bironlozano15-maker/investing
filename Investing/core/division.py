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

def calculate_remove_subnet(db, close_time):
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
    
    flow_amounts = np.array(flow_amounts)
    cutoff = np.percentile(flow_amounts, 12)
    flow_amounts[flow_amounts <= cutoff] = 0

    tao_in_values = []
    for i in range(len(flow_amounts)):
        if flow_amounts[i] == 0:
            tao_in_value = 0
        else:
            tao_in_value = (tao_in_start[i] + tao_in_close[i]) / 2
        tao_in_values.append(tao_in_value)

    tao_in_values = np.array(tao_in_values)
    non_zero_idx = np.where(tao_in_values != 0)[0]
    smallest_17_idx = non_zero_idx[np.argsort(tao_in_values[non_zero_idx])[:17]]
    tao_in_values[smallest_17_idx] = 0
    
    remove_subnets = []
    for i in range(len(tao_in_values)):
        if tao_in_values[i] == 0:
            remove_subnets.append(i + 1)
    return remove_subnets

def calculate_probability(db, close_time):
    df = db.copy()
    df = df[df['netuid'] != 0]
    df['date'] = pd.to_datetime(df['date'])

    alpha_prices = []
    for i in range(31):
        start_time = close_time - pd.Timedelta(days=i+1)
        end_time = close_time - pd.Timedelta(days=i)
        start_block = datetime_to_blocks(start_time)
        end_block = datetime_to_blocks(end_time)

        df_1d = df[(df['block'] >= start_block) & (df['block'] <= end_block)]
        df_1d = df_1d.sort_values(['netuid', 'block'])
        alpha_price = df_1d.groupby('netuid')['price'].mean().values
        alpha_prices.append(alpha_price)

    daily_returns = [[] for _ in range(128)]

    for subnet in range(128):
        for t in range(30, 0, -1):
            p_current = alpha_prices[t][subnet]
            p_next = alpha_prices[t-1][subnet]
            daily_returns[subnet].append((p_next - p_current) / p_current)
    daily_returns = np.array(daily_returns)

    weights = []
    for i in range(30):
        weight = 1 + (i / 29) * 2
        weights.append(weight)
    weights = np.array(weights)
    total_weight = np.sum(weights)
    pick_probabilities = weights / total_weight

    probabilities = []
    for subnet_idx in range(128):
        daily_return = daily_returns[subnet_idx]

        #run simulations
        simulation_results = []
        for sim in range(1000):
            picked_indices = np.random.choice(30, size=7, replace=True, p=pick_probabilities)
            picked_returns = daily_return[picked_indices]

            multiplier = 1.0
            for ret in picked_returns:
                multiplier = multiplier * (1 + ret)

            simulation_results.append(multiplier - 1)

        expected_return = np.mean(simulation_results)

        #convert to probability
        probability = 0.50 + (expected_return * 2)
        probability = max(0.0, min(1.0, probability))
        
        probabilities.append(probability)
    
    return probabilities

def calculate_division(close_time, asset):
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
        db_data = pd.read_csv(DATA_NAME)
        db_data['date'] = pd.to_datetime(db_data['date']).dt.strftime('%Y-%m-%d')
        close_block = datetime_to_blocks(close_time)
        max_block_db = db['block'].max()
        if max_block_db < close_block:
            additional_data = db_data[(db_data['block'] > max_block_db) & (db_data['block'] <= close_block)]
            db = pd.concat([db, additional_data], ignore_index=True)

        remove_subnet = calculate_remove_subnet(db, close_time)
        probability_score = calculate_probability(db, close_time)
        for subnet_num in remove_subnet:
            index = subnet_num - 1
            probability_score[index] = 0
        probability_score = [p if 0.5 <= p <= 0.9 else 0 for p in probability_score]
        total_score = sum(probability_score)
        if total_score > 0:
            strat = [p / total_score for p in probability_score]
        else:
            strat = [0] * len(probability_score)

        for i in range(len(strat)):
            strat[i] = math.floor(strat[i] * 10**12) / 10**12

        strat_dict = {n: float(strat[n-1]) for n in range(1, len(strat) + 1)}
        strat_dict = {k: v for k, v in strat_dict.items() if v != 0}

        strat_string = str(strat_dict)
        
    return strat_string