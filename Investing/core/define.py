from decimal import Decimal

SOURCE_URL = "https://taostats.io/subnets"
TAO_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price"
RAO_PER_UNIT = Decimal("1000000000")

AN = 2
STAKING_HOTKEY = "5F1xUaixHnpfyqUj77YHEDPNssYkyZwd2T7wdGCVtJ8U7a9o"
STAKING_UPDATE_TIME = "update_time"
STOCKS_HOTKEY = "5Ccq57P5jtBKzgKDRPkxMViJrJSDyCWDcqkkpbxsKDMJbJcP"
FILE_NAME = "tao_pyth_1min"
STAKING_UID = 85
STOCKS_UID = 246

BASE_BLOCK = 7718400
BASE_TIME_STR = '2026-03-11 01:00:00'
BLOCK_SECONDS = 12

STAKING_STRATEGY_PATH = 'Investing/strat/{}'.format(STAKING_HOTKEY)
STAKING_STRATEGY_PATH_CSV = 'Investing/strat/{}.csv'.format(STAKING_HOTKEY)
STAKING_STRATEGY_UPDATE_TIME = 'Investing/strat/{}'.format(STAKING_UPDATE_TIME)
STOCKS_STRATEGY_PATH = 'Investing/strat/{}'.format(STOCKS_HOTKEY)
STOCKS_STRATEGY_PATH_CSV = 'Investing/strat/{}.csv'.format(STOCKS_HOTKEY)
DATA_NAME = "data.csv"
TAO_DATA_NAME = 'Investing/core/database/{}.csv'.format(FILE_NAME)

STAKING_FUND = 1000
STOCKS_FUND = 10000000

FLUCT_TIME = 15
FLUCT_RATE = 0.15
STABLE_RATE = 0.08
STANDARD_PROB = 0.97

ASSET = 0
