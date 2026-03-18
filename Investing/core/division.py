import pandas as pd
import numpy as np
import math
from Investing.core.define import *

def normalize_minmax(arr):
    arr = np.array(arr, dtype=float)
    
    # Mask of non-zero values
    non_zero_mask = arr != 0
    
    # If no non-zero values, return as-is
    if not np.any(non_zero_mask):
        return arr
    
    non_zero_vals = arr[non_zero_mask]
    
    min_val = np.min(non_zero_vals)
    max_val = np.max(non_zero_vals)
    
    # Edge case: all non-zero values are equal
    if max_val == min_val:
        arr[non_zero_mask] = 1.0
        return arr
    
    # Scale non-zero values to [0.5, 1]
    arr[non_zero_mask] = 0.5 + 0.5 * (non_zero_vals - min_val) / (max_val - min_val)
    
    return arr

def custom_score_transform(score):
    score = np.array(score, dtype=float)
    
    # Mask for non-zero values
    non_zero_mask = score != 0
    non_zero_indices = np.where(non_zero_mask)[0]
    
    # Get the number of non-zero values
    non_zero_count = len(non_zero_indices)
    
    if non_zero_count > 0:
        # Sort non-zero indices by their corresponding scores in descending order
        sorted_indices = non_zero_indices[np.argsort(score[non_zero_indices])[::-1]]
        
        # Calculate counts for each tier based on percentages of non-zero values
        tier1_count = int(non_zero_count * 0.435)  # Largest 43.5%
        tier2_count = int(non_zero_count * 0.13)   # Next largest 13%
        tier3_count = int(non_zero_count * 0.22)   # Next largest 22%
        
        # The remaining values get the last tier
        # Ensure we don't exceed the total count due to rounding
        total_assigned = tier1_count + tier2_count + tier3_count
        if total_assigned < non_zero_count:
            tier4_count = non_zero_count - total_assigned
        else:
            # Adjust if rounding caused us to exceed
            tier4_count = 0
            # Recalculate to ensure exact distribution
            tier1_count = int(non_zero_count * 0.435)
            tier2_count = int(non_zero_count * 0.13)
            tier3_count = non_zero_count - tier1_count - tier2_count
        
        # Assign values to each tier
        current_idx = 0
        
        # Tier 1 (largest 43.5%)
        if tier1_count > 0:
            tier1_indices = sorted_indices[current_idx:current_idx + tier1_count]
            score[tier1_indices] = 0.59 / tier1_count
            current_idx += tier1_count
        
        # Tier 2 (next 13%)
        if tier2_count > 0:
            tier2_indices = sorted_indices[current_idx:current_idx + tier2_count]
            score[tier2_indices] = 0.13 / tier2_count
            current_idx += tier2_count
        
        # Tier 3 (next 22%)
        if tier3_count > 0:
            tier3_indices = sorted_indices[current_idx:current_idx + tier3_count]
            score[tier3_indices] = 0.19 / tier3_count
            current_idx += tier3_count
        
        # Tier 4 (remaining)
        if current_idx < non_zero_count:
            tier4_indices = sorted_indices[current_idx:]
            score[tier4_indices] = 0.09 / (non_zero_count - current_idx)

    return score

def calculate_index_time(db, close_time):
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
    start_time = close_time - pd.Timedelta(hours=3)
    
    # Filter for the 30-minute window
    mask = (db['time'] > start_time) & (db['time'] < close_time)
    db_3h = db[mask].copy()
    
    if db_3h.empty:
        return []
    
    # Remove netuid 0
    df = db_3h[db_3h['netuid'] != 0]
    
    if df.empty:
        return []
    
    # Sort and get first alpha_in for each netuid
    df = df.sort_values(['netuid', 'time'])
    alpha_in_start = df.groupby('netuid')['alpha_in'].first()
    
    # Get netuids where alpha_in < 150000
    index = alpha_in_start[alpha_in_start < 150000].index.tolist()
    
    return index

def calculate_flow_time(db, close_time):
    start_time = close_time - pd.Timedelta(minutes=90)

    db_hour = db[db['time'].between(start_time, close_time, inclusive="neither")].copy()
    df = db_hour[db_hour['netuid'] != 0]

    if df.empty:
        return [0.0] * 128

    # Sort so "first" is well-defined (important!)
    df = df.sort_values(['netuid', 'time'])
    alpha_in_start = df.groupby('netuid')['alpha_in'].first().values
    alpha_in_close = df.groupby('netuid')['alpha_in'].last().values

    df = df.sort_values(['netuid', 'time'])
    tao_in_start = df.groupby('netuid')['tao_in'].first().values
    tao_in_close = df.groupby('netuid')['tao_in'].last().values

    flow_hour = []
    for i in range(len(alpha_in_start)):
        tao_return = (tao_in_close[i] - tao_in_start[i]) / tao_in_start[i]
        alpha_return = (alpha_in_close[i] - alpha_in_start[i]) / alpha_in_start[i]
        flow = tao_return - alpha_return
        flow_hour.append(flow)

    return flow_hour

def calculate_flow_times(db, close_time, delta):
    start_time = close_time - pd.Timedelta(hours=delta)

    db_hour = db[db['time'].between(start_time, close_time, inclusive="neither")].copy()
    df = db_hour[db_hour['netuid'] != 0]

    # Sort so "first" is well-defined (important!)
    df = df.sort_values(['netuid', 'time'])
    alpha_in_start = df.groupby('netuid')['alpha_in'].first().values
    alpha_in_close = df.groupby('netuid')['alpha_in'].last().values

    df = df.sort_values(['netuid', 'time'])
    tao_in_start = df.groupby('netuid')['tao_in'].first().values
    tao_in_close = df.groupby('netuid')['tao_in'].last().values

    flow_hour = []
    for i in range(len(alpha_in_start)):
        tao_return = (tao_in_close[i] - tao_in_start[i]) / tao_in_start[i]
        alpha_return = (alpha_in_close[i] - alpha_in_start[i]) / alpha_in_start[i]
        flow = tao_return - alpha_return
        flow_hour.append(flow)

    return flow_hour

def calculate_ema_time(db, close_time):
    start_time = close_time - pd.Timedelta(hours=24)

    db_24h = db[db['time'].between(start_time, close_time, inclusive="neither")].copy()
    df = db_24h[db_24h['netuid'] != 0]

    # Sort so "first" is well-defined (important!)
    df = df.sort_values(['netuid', 'time'])
    price_close = df.groupby('netuid')['price'].last().values

    pre_close_time = close_time - pd.Timedelta(hours=2)
    pre_start_time = start_time - pd.Timedelta(hours=2)

    pre_db_24h = db[db['time'].between(pre_start_time, pre_close_time, inclusive="neither")].copy()
    pre_df = pre_db_24h[pre_db_24h['netuid'] != 0]

    average_24h = pre_df.groupby('netuid')['price'].mean().values

    dist = []
    for i in range(len(price_close)):
        ema = 0.08 * price_close[i] + 0.92 * average_24h[i]
        distance = (price_close[i] - ema) / ema
        dist.append(distance)

    return dist

def calculate_division(close_time):
    db = pd.read_csv(data_name)
    db = pd.DataFrame(db)
    remove_index = calculate_index_time(db, close_time)
    flow_signal_1h = calculate_flow_time(db, close_time)
    flow_signal_3h = calculate_flow_times(db, close_time, delta = 3)
    flow_signal_24h = calculate_flow_times(db, close_time, delta = 24)
    dist = calculate_ema_time(db, close_time)

    for i in remove_index:
        flow_signal_1h[i - 1] = 0
        flow_signal_3h[i - 1] = 0
        flow_signal_24h[i - 1] = 0
        dist[i - 1] = 0

    flow_signal_1h = normalize_minmax(flow_signal_1h)
    flow_signal_3h = normalize_minmax(flow_signal_3h)
    flow_signal_24h = normalize_minmax(flow_signal_24h)
    dist = normalize_minmax(dist)

    score = []
    for i in range(len(flow_signal_1h)):
        sc = 0.4 * flow_signal_3h[i] + 0.3 * flow_signal_1h[i] + 0.2 * flow_signal_24h[i] + 0.1 * dist[i] 
        score.append(sc)

    strat = custom_score_transform(score)

    # for i in range(len(strat)):
    #     strat[i] = math.floor(strat[i] * 10**12) / 10**12

    return strat
