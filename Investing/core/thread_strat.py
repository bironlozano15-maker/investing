from Investing.core.generate_st import generate_strat
import time
from datetime import datetime, timezone

if __name__ == "__main__":
    while True:
        # generate_strat(datetime.utcnow())
        # print("Generated new strategy at", datetime.now(timezone.utc).replace(microsecond=0))
        # time.sleep(30)
        asset = 0
        if datetime.utcnow().hour == 13 and datetime.utcnow().minute == 5:
            asset = 0
            generate_strat(datetime.utcnow(), asset)
            print("Generated new strategy at", datetime.now(timezone.utc).replace(microsecond=0))
            time.sleep(300)
        elif datetime.utcnow().hour == 7 and datetime.utcnow().minute == 30:
            asset = 1
            generate_strat(datetime.utcnow(), asset)
            print("Generated new strategy at", datetime.now(timezone.utc).replace(microsecond=0))
            time.sleep(300)
        else:
            time.sleep(30)