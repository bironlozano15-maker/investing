from Investing.core.generate_st import generate_strat
import time
from datetime import datetime, timezone

if __name__ == "__main__":
    while True:
        # generate_strat(datetime.utcnow())
        # print("Generated new strategy at", datetime.now(timezone.utc).replace(microsecond=0))
        # time.sleep(30)

        if datetime.utcnow().hour == 13 and datetime.utcnow().minute == 5:
            generate_strat(datetime.utcnow())
            print("Generated new strategy at", datetime.now(timezone.utc).replace(microsecond=0))
            time.sleep(300)
        else:
            time.sleep(30)