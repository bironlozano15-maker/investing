import pandas as pd
import sqlite3, math
from Investing.core.define import *
from datetime import timedelta
import numpy as np

def datetime_to_blocks(time, db):
    base_time = time.replace(hour=0, minute=0, second=0, microsecond=0)
    db['date'] = pd.to_datetime(db['date'], format='%Y-%m-%d', errors='coerce')
    db = db.dropna(subset=['date'])
    yesterday = pd.Timestamp(time.date() - timedelta(days=1))
    filtered = db[(db['date'].dt.date == yesterday.date()) & (db['netuid'] == 0)]
    base_block = filtered['block'].iat[-1]
    delta_time = (time - base_time).total_seconds()
    block = base_block + delta_time / 12
    block = math.ceil(block)
    return block

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

def calculate_remove_subnet(db, close_time):
    df = db.copy()
    df = df[df['netuid'] != 0]
    df['date'] = pd.to_datetime(df['date'])

    start_time = close_time - pd.Timedelta(hours=1)
    start_block = datetime_to_blocks(start_time, db)
    close_block = datetime_to_blocks(close_time, db)

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
    cutoff = np.percentile(flow_amounts, 10)
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
    smallest_6_idx = non_zero_idx[np.argsort(tao_in_values[non_zero_idx])[:6]]
    tao_in_values[smallest_6_idx] = 0
    
    remove_subnets = []
    for i in range(len(tao_in_values)):
        if tao_in_values[i] == 0:
            remove_subnets.append(i + 1)
    return remove_subnets

def calculate_probability(db, close_time):
    df = db.copy()
    df = df[df['netuid'] != 0]
    df['date'] = pd.to_datetime(df['date'])

    start_time = close_time - pd.Timedelta(hours=1)
    end_time = close_time - pd.Timedelta(hours=0)
    start_block = datetime_to_blocks(start_time, db)
    end_block = datetime_to_blocks(end_time, db)

    alpha_prices = []
    for i in range(121):
        df_1h = df[(df['block'] >= start_block) & (df['block'] <= end_block)]
        df_1h = df_1h.sort_values(['netuid', 'block'])
        alpha_price = df_1h.groupby('netuid')['price'].mean().values
        alpha_prices.append(alpha_price)
        start_block = start_block - 300 # 300 blocks is round 1 hour
        end_block = end_block - 300     # 300 blocks is round 1 hour

    hourly_returns = [[] for _ in range(128)]

    for subnet in range(128):
        for t in range(120, 0, -1):
            p_current = alpha_prices[t][subnet]
            p_next = alpha_prices[t-1][subnet]
            hourly_returns[subnet].append((p_next - p_current) / p_current)
    hourly_returns = np.array(hourly_returns)

    weights = []
    for i in range(120):
        weight = 1 + (i / 119) * 2
        weights.append(weight)
    weights = np.array(weights)
    total_weight = np.sum(weights)
    pick_probabilities = weights / total_weight

    probability_scores = []
    for subnet_idx in range(128):
        hourly_return = hourly_returns[subnet_idx]
        #run simulations
        simulation_results = []
        positive_count = 0
        for sim in range(1000):
            picked_indices = np.random.choice(120, size=120, replace=True, p=pick_probabilities)
            picked_returns = hourly_return[picked_indices]

            multiplier = 1.0
            for ret in picked_returns:
                multiplier = multiplier * (1 + ret)

            if multiplier - 1 > 0:
                positive_count += 1

            simulation_results.append(multiplier - 1)

        expected_return = np.mean(simulation_results)
        probability = positive_count / 1000
        score = expected_return * probability
        probability_scores.append(score)
    
    return probability_scores

def scale_values(result):
    min_val = 0.009
    max_val = 0.021
    step = (max_val - min_val) / 65

    # Find indices of non-zero values
    indices = [i for i, x in enumerate(result) if x != 0]
    
    if not indices:
        return result.copy()
    
    # Sort indices by original value (ascending)
    indices.sort(key=lambda i: result[i])
    
    new_result = result.copy()
    
    # Assign spaced values
    for rank, idx in enumerate(indices):
        new_result[idx] = min_val + step * rank
    
    # Force max exactly
    new_result[indices[-1]] = max_val
    
    return new_result

def calculate_division(close_time, asset, raw_db):
    if asset == 0:
        if raw_db is None:
            db = load_data(asset)
        else:
            db = raw_db.copy()
        db = pd.DataFrame(db)
        columns_to_keep = ['date', 'block', 'netuid', 'alpha_in', 'tao_in', 'price']
        db = db[columns_to_keep]
        db_data = pd.read_csv(DATA_NAME)
        db_data['date'] = pd.to_datetime(db_data['date']).dt.strftime('%Y-%m-%d')
        close_block = datetime_to_blocks(close_time, db)
        max_block_db = db['block'].max()
        if max_block_db < close_block:
            additional_data = db_data[(db_data['block'] > max_block_db) & (db_data['block'] <= close_block)]
            db = pd.concat([db, additional_data], ignore_index=True)
        #calculate remove subnet
        remove_subnet = calculate_remove_subnet(db, close_time)
        #calculate probability
        probability_score = calculate_probability(db, close_time)
        for subnet_num in remove_subnet:
            index = subnet_num - 1
            probability_score[index] = 0
        #maintain 60%
        probability_score = np.array(probability_score)
        # 1. get indices of non-zero values
        idx = np.where(probability_score != 0)[0]
        # 2. sort those indices by value (largest → smallest)
        idx = idx[np.argsort(probability_score[idx])[::-1]]
        # 3. keep top 66
        keep = idx[:66]
        # 4. create zero array
        result = np.zeros_like(probability_score)
        # 5. put back only top 66 values
        result[keep] = probability_score[keep]
        #make the strategy
        strat = scale_values(result)      
        for i in range(len(strat)):
            strat[i] = math.floor(strat[i] * 10**12) / 10**12

        strat_dict = {n: float(strat[n-1]) for n in range(1, len(strat) + 1)}
        strat_dict = {k: v for k, v in strat_dict.items() if v != 0}

        strat_string = str(strat_dict)
        return strat_string