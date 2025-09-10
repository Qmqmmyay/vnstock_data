import pandas as pd
from typing import Dict, Any
from vnstock_data.core.utils.fetcher import Fetcher
from vnstock_data.core.utils.user_agent import HEADERS_MAPPING_SOURCE
from .const import BASE_URL

class SPLFetcher(Fetcher):
    """
    Specific fetcher for SPL data provider.
    """

    def __init__(self):
        """
        Initialize the SPLFetcher with SPL-specific settings.
        """
        super().__init__(
            base_url=BASE_URL,
            headers=HEADERS_MAPPING_SOURCE.get("SIMPLIZE", {}),
        )

    def validate(self, params: Dict[str, Any]):
        """
        Validate SPL-specific query parameters.

        Parameters:
            params (dict): Query parameters to validate.
        
        Raises:
            ValueError: If required parameters are missing or invalid.
        """
        if not params.get("ticker"):
            raise ValueError("Ticker is required for SPL requests.")
        if params.get("interval") not in {"1d"}:
            raise ValueError("Invalid interval. Only '1d' is supported.")

    def to_dataframe(self, raw_data: list) -> pd.DataFrame:
        """
        Convert SPL-specific raw data to a Pandas DataFrame.

        Parameters:
            raw_data (list): Raw data from the SPL API response.
        
        Returns:
            pd.DataFrame: A Pandas DataFrame with columns ['time', 'open', 'high', 'low', 'close', 'volume'].
        """
        columns = ["time", "open", "high", "low", "close", "volume"]
        df = pd.DataFrame(raw_data, columns=columns)
        df["time"] = pd.to_datetime(df["time"], unit="s")
        return df
