from Investing.core.division import calculate_division
from Investing.core.define import *
from Investing.core.data_load import fetch_alpha_prices, append_rows_to_csv

def generate_strat(start_time):
    strat = calculate_division(start_time)
    
    with open(STRATEGY_PATH, 'w') as file:
        file.write(strat)

    return strat

def fetch_data():
    rows = fetch_alpha_prices()
    append_rows_to_csv(rows, data_name)
