from Investing.core.division import calculate_division
from Investing.core.define import *

def generate_strat(start_time):
    strat = calculate_division(start_time)
    strat_dict = {n: float(strat[n-1]) for n in range(1, len(strat) + 1)}
    strat_dict = {k: v for k, v in strat_dict.items() if v != 0}
    strat_string = str(strat_dict)
    
    with open(STRATEGY_PATH, 'w') as file:
        file.write(strat_string)

    return strat_string
