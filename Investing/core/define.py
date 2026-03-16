from decimal import Decimal
import os


SOURCE_URL = "https://taostats.io/subnets"
RAO_PER_UNIT = Decimal("1000000000")

an = 2
hotkey = "5F4tQyWrhfGVcNhoqeiNsR6KjD4wMZ2kfhLj4oHYuyHbZAc3"
uid = 2
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PATH = '../strat/{}.csv'.format(hotkey)

BASE_BLOCK = 7718400
BASE_TIME_STR = '2026-03-11 01:00:00'
BLOCK_SECONDS = 12

strat_direct = 'strat'
data_name = "data.csv"