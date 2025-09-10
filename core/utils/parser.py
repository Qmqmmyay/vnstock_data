import urllib.parse
import pandas as pd
from datetime import datetime, timedelta

def encode_url(string:str, safe:str=''):
    """
    Encode a string to url format.
    """
    return urllib.parse.quote(string, safe=safe)

def days_between (start: str, end: str, format: str = '%m/%d/%Y') -> int:
    """
    Calculate the number of days between two given datestrings.

    Parameters:
        start (str): The start date string.
        end (str): The end date string.
        format (str): The format of the date strings. Default is '%m/%d/%Y'.
        
    Returns:
        int: The number of days between the two dates.
    """
    start = pd.to_datetime(start, format=format)
    end = pd.to_datetime(end, format=format)
    days = (end - start).days
    return days


def lookback_date(period: str) -> str:
    """
    Calculates the start date based on a lookback period.

    Parameters:
        period (str): Lookback period (e.g., '1D', '5M', '10Y').
                      D = days, M = months, Y = years.

    Returns:
        str: Calculated start date in YYYY-MM-DD format.

    Raises:
        ValueError: If the period format is invalid.
    """
    try:
        unit = period[-1].upper()
        value = int(period[:-1])
        if unit == 'D':
            start_date = datetime.now() - timedelta(days=value)
        elif unit == 'M':
            start_date = datetime.now() - timedelta(days=value * 30)
        elif unit == 'Y':
            start_date = datetime.now() - timedelta(days=value * 365)
        else:
            raise ValueError("Invalid period format. Use 'D', 'M', or 'Y' for days, months, or years.")
        return start_date.strftime('%Y-%m-%d')
    except Exception as e:
        raise ValueError(f"Error parsing period: {e}")
