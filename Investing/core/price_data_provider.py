import logging
import requests
from datetime import datetime, timezone, timedelta
from tenacity import (
    before_log,
    retry,
    stop_after_attempt,
    wait_random_exponential,
)


from helpers import from_iso_to_unix_time

# Pyth API benchmarks doc: https://benchmarks.pyth.network/docs
# get the list of stocks supported by pyth: https://benchmarks.pyth.network/v1/shims/tradingview/symbol_info?group=pyth_stock
# get the list of crypto supported by pyth: https://benchmarks.pyth.network/v1/shims/tradingview/symbol_info?group=pyth_crypto
# get the ticket: https://benchmarks.pyth.network/v1/shims/tradingview/symbols?symbol=Metal.XAU/USD


class PriceDataProvider:
    BASE_URL = "https://benchmarks.pyth.network/v1/shims/tradingview/history"

    TOKEN_MAP = {
        "BTC": "Crypto.BTC/USD",
        "ETH": "Crypto.ETH/USD",
        "XAU": "Crypto.XAUT/USD",
        "SOL": "Crypto.SOL/USD",
        "SPYX": "Crypto.SPYX/USD",
        "NVDAX": "Crypto.NVDAX/USD",
        "TSLAX": "Crypto.TSLAX/USD",
        "AAPLX": "Crypto.AAPLX/USD",
        "GOOGLX": "Crypto.GOOGLX/USD",
        "TAO": "Crypto.TAO/USD",
    }

    _logger = logging.getLogger(__name__)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_random_exponential(multiplier=7),
        reraise=True,
        before=before_log(_logger, logging.DEBUG),
    )
    def fetch_data(
        self, token: str, start_time: str, time_length: int, transformed=True
    ):
        """
        Fetch real prices data from an external REST service.
        Returns an array of time points with prices.

        :return: List of dictionaries with 'time' and 'price' keys.
        """

        start_time_int = from_iso_to_unix_time(start_time)
        end_time_int = start_time_int + time_length

        params = {
            "symbol": self._get_token_mapping(token),
            "resolution": 5,
            "from": start_time_int,
            "to": end_time_int,
        }

        # Timeout so we don't hang indefinitely on slow/unresponsive API or network
        # (connect_timeout, read_timeout) in seconds
        response = requests.get(self.BASE_URL, params=params, timeout=(15, 60))
        response.raise_for_status()

        data = response.json()

        if not transformed:
            return data

        transformed_data = self._transform_data(data, start_time_int)
        return transformed_data

    @staticmethod
    def _transform_data(data, start_time) -> list[dict]:
        if data is None or len(data) == 0:
            return []

        timestamps = data["t"]
        close_prices = data["c"]

        transformed_data = []

        for t, c in zip(timestamps, close_prices):
            if (
                t >= start_time and (t - start_time) % 60 == 0
            ):  # 300s = 5 minutes
                transformed_data.append(
                    {
                        "time": datetime.fromtimestamp(
                            t, timezone.utc
                        ).isoformat(),
                        "price": float(c),
                    }
                )

        return transformed_data

    @staticmethod
    def _get_token_mapping(token: str) -> str:
        """
        Retrieve the mapped value for a given token.
        If the token is not in the map, raise an exception or return None.
        """
        if token in PriceDataProvider.TOKEN_MAP:
            return PriceDataProvider.TOKEN_MAP[token]
        else:
            raise ValueError(f"Token '{token}' is not supported.")