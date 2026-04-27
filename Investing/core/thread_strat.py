from Investing.core.generate_st import generate_strat
import time
from datetime import datetime, timezone
from Investing.core.define import *

if __name__ == "__main__":
    while True:
        generate_strat(datetime.now(timezone.utc), 0)
        time.sleep(3600)