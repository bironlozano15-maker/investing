"""Utility functions for datetime and data manipulation."""

from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd


def convert_to_datetime(dt) -> datetime:
    """Convert pd.Timestamp or datetime to datetime."""
    if isinstance(dt, pd.Timestamp):
        return dt.to_pydatetime()
    elif isinstance(dt, str):
        return datetime.fromisoformat(dt)
    return dt


def round_to_30_minutes(dt: datetime) -> datetime:
    """Round datetime down to nearest 30-minute mark."""
    dt = convert_to_datetime(dt)
    rounded_minutes = 30 if dt.minute >= 30 else 0
    return dt.replace(minute=rounded_minutes, second=0, microsecond=0)


def database_csv_path(filename: str) -> str:
    """Return an absolute path for a database CSV file.

    If the database directory does not exist, create it.
    """
    database_dir = Path(__file__).resolve().parent / "database"
    database_dir.mkdir(parents=True, exist_ok=True)
    return str(database_dir / filename)


def returns_to_price_path(price_change_pcts: np.ndarray, current_price: float) -> np.ndarray:
    """Convert an array of step-wise returns to a price path (including initial price at index 0)."""
    cumulative_returns = np.cumprod(1.0 + price_change_pcts)
    cumulative_returns = np.insert(cumulative_returns, 0, 1.0)
    return current_price * cumulative_returns
