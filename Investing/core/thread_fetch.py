from Investing.core.generate import fetch_data
import time
from datetime import datetime, timezone

if __name__ == "__main__":
    while True:
        fetch_data()
        print("Fetched new data at", datetime.now(timezone.utc).replace(microsecond=0))
        time.sleep(300)