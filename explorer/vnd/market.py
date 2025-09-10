from typing import Optional
from datetime import datetime, timedelta
import requests
import pandas as pd
from vnai import agg_execution
from vnstock.core.utils.logger import get_logger
from vnstock.core.utils.user_agent import get_headers
from vnstock_data.core.utils.parser import lookback_date
from vnstock_data.explorer.vnd.const import _INDEX_MAPPING

logger = get_logger(__name__)

class Market:
    """
    Provides market insights, including P/E and P/B ratios over time.

    Attributes:
        index (str): Market index (e.g., 'VNINDEX', 'HNX').
        base_url (str): Base URL for retrieving data.
        headers (dict): HTTP headers for API requests.
    """

    def __init__(self, index: str = 'VNINDEX', random_agent: bool = False, show_log=False):
        self.index = self._index_validation(index)
        self.base_url = "https://api-finfo.vndirect.com.vn/v4/ratios"
        self.headers = get_headers(data_source='VND', random_agent=random_agent)
        
        if not show_log:
            logger.setLevel('CRITICAL')

    def _index_validation(self, index: str) -> str:
        """
        Validates the index input.

        Parameters:
            index (str): The index to validate. Valid indices are 'VNINDEX' and 'HNX'.

        Returns:
            str: The validated index.
        """
        index = index.upper()
        if index not in ['VNINDEX', 'HNXINDEX']:
            raise ValueError(f"Invalid index: {index}. Valid indices are: 'VNINDEX', 'HNX'")
        
        return _INDEX_MAPPING[index]

    def _fetch_data(self, ratio_code: str, start_date: str) -> pd.DataFrame:
        """
        Fetches market ratio data from the API.

        Parameters:
            ratio_code (str): Ratio code ('PRICE_TO_BOOK' or 'PRICE_TO_EARNINGS').
            start_date (str): Start date for fetching data (format: YYYY-MM-DD).

        Returns:
            pd.DataFrame: A DataFrame containing the report date and ratio values.
        """
        url = (
            f"{self.base_url}?q=ratioCode:{ratio_code}~code:{self.index}~reportDate:gte:{start_date}"
            "&sort=reportDate:desc&size=10000&fields=value,reportDate"
        )

        try:
            logger.info(f"Fetching {ratio_code} data for index {self.index}...")
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json().get("data", [])

            if not data:
                logger.warning("No data returned from API.")
                return pd.DataFrame()

            # Convert data to DataFrame
            df = pd.DataFrame(data)
            df["reportDate"] = pd.to_datetime(df["reportDate"])
            df = df.rename(columns={"value": ratio_code.lower()})
            # rename price_to_earnings to pe and price_to_book to pb
            df = df.rename(columns={'price_to_earnings': 'pe', 'price_to_book': 'pb'})
            return df.set_index("reportDate").sort_index()

        except requests.RequestException as e:
            logger.error(f"Failed to fetch data: {e}")
            return pd.DataFrame()

    @agg_execution("VND.ext")
    def pe(self, duration: str = '5Y') -> pd.DataFrame:
        """
        Retrieves P/E (Price-to-Earnings) ratio data.

        Parameters:
            duration (str): Number of years to retrieve data back (e.g., '1Y', '5Y', '10Y').

        Returns:
            pd.DataFrame: A DataFrame containing the P/E ratio data.
        """
        start_date = lookback_date(duration)
        return self._fetch_data(ratio_code="PRICE_TO_EARNINGS", start_date=start_date)

    @agg_execution("VND.ext")
    def pb(self, duration: str = '5Y') -> pd.DataFrame:
        """
        Retrieves P/B (Price-to-Book) ratio data.

        Parameters:
            duration (str): Number of years to retrieve data back (e.g., '1Y', '5Y', '10Y').

        Returns:
            pd.DataFrame: A DataFrame containing the P/B ratio data.
        """
        start_date = lookback_date(duration)
        return self._fetch_data(ratio_code="PRICE_TO_BOOK", start_date=start_date)

    @agg_execution("VND.ext")
    def evaluation(self, duration: str = '5Y') -> pd.DataFrame:
        """
        Retrieves an overview of the market with both P/E and P/B ratios.

        Parameters:
            duration (str): Number of years to retrieve data back (e.g., '1Y', '5Y', '10Y').

        Returns:
            pd.DataFrame: A DataFrame containing P/E and P/B ratio data.
        """
        start_date = lookback_date(duration)
        pe_data = self.pe(duration=duration)
        pb_data = self.pb(duration=duration)

        if pe_data.empty and pb_data.empty:
            logger.warning("No data available for both P/E and P/B ratios.")
            return pd.DataFrame()

        # Merge P/E and P/B data
        overview = pd.concat([pe_data, pb_data], axis=1)
        return overview
