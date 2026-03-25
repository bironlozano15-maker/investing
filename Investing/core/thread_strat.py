from Investing.core.generate_st import generate_strat
import time
from datetime import datetime, timezone

if __name__ == "__main__":
    while True:
        if datetime.utcnow().hour == 13 and datetime.utcnow().minute == 5:
            generate_strat(datetime.utcnow(), 0)
            print("Generated new staking strategy at", datetime.now(timezone.utc).replace(microsecond=0))
            time.sleep(300)
        elif datetime.utcnow().hour == 7 and datetime.utcnow().minute == 30:
            generate_strat(datetime.utcnow(), 1)
            print("Generated new stocks strategy at", datetime.now(timezone.utc).replace(microsecond=0))
            time.sleep(300)
        else:
            time.sleep(30)