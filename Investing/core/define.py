from decimal import Decimal

ASSET = 0

STAKING_FUND = 1000
STOCKS_FUND = 10000000

STAKING_UID = 85
STOCKS_UID = 246

STAKING_HOTKEY = "5F1xUaixHnpfyqUj77YHEDPNssYkyZwd2T7wdGCVtJ8U7a9o"
STOCKS_HOTKEY = "5Ccq57P5jtBKzgKDRPkxMViJrJSDyCWDcqkkpbxsKDMJbJcP"

STAKING_STRATEGY_PATH = 'Investing/strat/{}'.format(STAKING_HOTKEY)
STOCKS_STRATEGY_PATH = 'Investing/strat/{}'.format(STOCKS_HOTKEY)
SCORE_PATH = 'Investing/core/tmp/{}'.format("strategy_score")
STAKING_STRATEGY_PATH_CSV = 'Investing/core/tmp/{}.csv'.format(STAKING_HOTKEY)
STOCKS_STRATEGY_PATH_CSV = 'Investing/core/tmp/{}.csv'.format(STOCKS_HOTKEY)

TOP_STRATEGY = 'top_miner.csv'
DATA_NAME = 'data.csv'
RAO_PER_UNIT = Decimal("1000000000")
SOURCE_URL = "https://taostats.io/subnets"
SIGNAL = 0

K_MIN = 40
K_MAX = 108