from Investing.core.generate_st import fetch_data
import time
from datetime import datetime, timezone
from Investing.core.fetch_tao import fetch_tao

if __name__ == "__main__":
    while True:
        fetch_data()
        fetch_tao()
        print("Fetched new data at", datetime.now(timezone.utc).replace(microsecond=0))
        time.sleep(300)