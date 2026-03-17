from decimal import Decimal

SOURCE_URL = "https://taostats.io/subnets"
RAO_PER_UNIT = Decimal("1000000000")

an = 2
hotkey = "5F1xUaixHnpfyqUj77YHEDPNssYkyZwd2T7wdGCVtJ8U7a9o"
uid = 85

BASE_BLOCK = 7718400
BASE_TIME_STR = '2026-03-11 01:00:00'
BLOCK_SECONDS = 12

STRATEGY_PATH = 'Investing/strat/{}'.format(hotkey)
STRATEGY_PATH_CSV = 'Investing/strat/{}.csv'.format(hotkey)
data_name = "data.csv"