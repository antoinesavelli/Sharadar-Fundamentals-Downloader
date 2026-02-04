"""
Utility functions
"""
import pandas as pd
from datetime import datetime
import config


def setup_logging():
    """Configure logging for verbose output"""
    if config.VERBOSE:
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)


def get_trading_days(start_date: str, end_date: str) -> pd.DatetimeIndex:
    """Get list of trading days (excludes weekends and holidays)"""
    # Note: This is a simplified version. For production, use pandas_market_calendars
    all_days = pd.date_range(start=start_date, end=end_date, freq='D')
    trading_days = all_days[all_days.dayofweek < 5]  # Monday=0, Friday=4
    return trading_days
