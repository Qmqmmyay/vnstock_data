from typing import List, Dict, Optional
from datetime import datetime, timedelta
import json
import requests
import pandas as pd
from vnai import agg_execution
from vnstock.core.utils.logger import get_logger
from vnstock.core.utils.parser import get_asset_type, camel_to_snake, flatten_data
from vnstock_data.core.utils.user_agent import get_headers
from vnstock_data.core.utils.validation import validate_date
from vnstock_data.core.utils.browser import get_cookie
from vnstock_data.explorer.vds.const import _BASE_URL, _ORDER_MATCH_MAPPING

logger = get_logger(__name__)

class Quote:
    """
    Truy xuất dữ liệu giao dịch của mã chứng khoán từ nguồn dữ liệu VDSC.
    """
    def __init__(self, symbol:Optional[str], cookie=None, random_agent=False, show_log:Optional[bool]=False):
        self.symbol = symbol.upper()
        self.asset_type = get_asset_type(self.symbol)

        self.headers = get_headers(data_source='VDS', random_agent=random_agent)
        self.headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=UTF-8'
        if cookie is None:
            cookie = get_cookie('https://livedragon.vdsc.com.vn/general/intradayBoard.rv', headers=self.headers)
            self.headers['Cookie'] = cookie
        else:
            self.headers['Cookie'] = cookie

        if not show_log:
            logger.setLevel('CRITICAL')
            
        self.show_log = show_log

    @agg_execution("VDS.ext")
    def intraday (self, date:Optional[str]=None):
        """
        Get the matched transactions data for a specific stock symbol and date from Live Dragon website.

        Parameters:
            date (str): The date for which to retrieve the matched transactions data, in the format 'YYYY-MM-DD'. If None, today's date will be used.
            cookie (str): The cookie value to include in the request headers. Default is None. If the automatic method fails, you may need to copy the actual cookie value from your web browser.
        """

        # if date is None, use today's date
        if date is None:
            date_obj = datetime.now()   
        else:
            # Convert to datetime object
            date_obj = datetime.strptime(date, "%Y-%m-%d")

        # Convert to desired format DD/MM/YYYY
        formatted_date_str = date_obj.strftime("%d/%m/%Y")

        url = f"{_BASE_URL}general/intradaySearch.rv"
        payload = f"stockCode={self.symbol}&boardDate={formatted_date_str}"

        if self.show_log:
            logger.info(f'Request data to {url}, using payload as details: {payload}. Headers values: {self.headers}')

        response = requests.request("POST", url, headers=self.headers, data=payload)
        if response.status_code != 200:
            logger.debug(f"Error: {response.text}")

        data = response.json()

        if self.show_log:
            logger.info(data)

        df = pd.DataFrame(data['list'])

        df.columns = [camel_to_snake(col) for col in df.columns]
        df.rename(columns=_ORDER_MATCH_MAPPING, inplace=True)

        return df
            