import requests
import pandas as pd
from typing import Dict, Any
from vnstock_data.core.const import ERROR_MESSAGES

class Fetcher:
    """
    Common request fetcher for all data providers, ready to be extended for specific requirements.
    """

    def __init__(self, base_url: str, headers: Dict[str, str], api_key: str = None):
        """
        Initialize the Fetcher with a base URL and headers.

        Parameters:
            base_url (str): Base URL for the API.
            headers (dict): Default headers for the API requests.
            api_key (str, optional): API key for authentication. Default is None.
        """
        self.base_url = base_url
        self.headers = headers
        self.api_key = api_key

    def fetch(self, endpoint: str, params: Dict[str, Any], extra_headers: Dict[str, str] = None) -> Dict[str, Any]:
        """
        Fetch raw data from a specific API endpoint.

        Parameters:
            endpoint (str): The API endpoint to fetch data from.
            params (dict): Query parameters for the API request.
            extra_headers (dict, optional): Additional headers specific to the request. Default is None.
        
        Returns:
            dict: Raw JSON response data.

        Raises:
            RuntimeError: If the API request fails.
            ValueError: If the response status code is not 200 or the data is empty.
        """
        url = f"{self.base_url}{endpoint}"
        merged_headers = self.headers.copy()
        if extra_headers:
            merged_headers.update(extra_headers)

        try:
            response = requests.get(url, headers=merged_headers, params=params)
            response.raise_for_status()
        except requests.RequestException as err:
            raise RuntimeError(f"{ERROR_MESSAGES['api_failure']}: {err}")

        if response.status_code != 200:
            raise ValueError("No available data: Non-200 status code received.")

        response_data = response.json()

        # Check if the data is empty using generic length check
        if not self._has_data(response_data):
            raise ValueError("No available data: Empty response content.")

        return response_data

    def _has_data(self, response_data: Any) -> bool:
        """
        Check if the response data contains any data.

        Parameters:
            response_data (Any): The data returned from the API response.
        
        Returns:
            bool: True if data is present, False otherwise.
        """
        if isinstance(response_data, (dict, list)):
            return bool(len(response_data))
        return False

    def validate(self, params: Dict[str, Any]):
        """
        Placeholder for parameter validation. Extend in local data provider modules.

        Parameters:
            params (dict): Query parameters to validate.

        Raises:
            NotImplementedError: If not overridden in a subclass.
        """
        raise NotImplementedError("Override 'validate' method in the specific provider fetcher.")

    def to_dataframe(self, raw_data: Any) -> pd.DataFrame:
        """
        Placeholder for converting raw data to a Pandas DataFrame. Extend in local data provider modules.

        Parameters:
            raw_data (Any): Raw data from the API response.
        
        Returns:
            pd.DataFrame: A Pandas DataFrame representation of the raw data.

        Raises:
            NotImplementedError: If not overridden in a subclass.
        """
        raise NotImplementedError("Override 'to_dataframe' method in the specific provider fetcher.")
