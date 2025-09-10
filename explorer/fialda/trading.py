from typing import List, Dict, Optional, Union
from datetime import datetime, timedelta
from .const import _BASE_URL
import pandas as pd
import requests
import json
from vnai import agg_execution
from vnstock.core.utils.parser import get_asset_type, camel_to_snake, flatten_data
from vnstock.core.utils.logger import get_logger
from vnstock.core.utils.user_agent import get_headers
from vnstock_data.core.utils.validation import validate_date

logger = get_logger(__name__)


class Trading:
    """
    Truy xuất dữ liệu giao dịch của mã chứng khoán từ nguồn dữ liệu Fialda.
    """
    def __init__(self, symbol:Optional[str], random_agent=False, show_log:Optional[bool]=False):
        self.symbol = symbol.upper()
        self.asset_type = get_asset_type(self.symbol)
        self.base_url = _BASE_URL
        self.headers = get_headers(data_source='FIALDA', random_agent=random_agent)

        if not show_log:
            logger.setLevel('CRITICAL')

    @agg_execution("FAD.ext")
    def prop_trades (self, start_date:Union[str,None]=None, end_date:Union[str, None]=None, limit:int=360, page:int=1)->pd.DataFrame:
        
        # assign start_date to the last 30 days if start_date is None
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        # assign end_date to the current date if end_date is None
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        # if start_date and end_date not None, then verify start_date and end_date should be in the format 'YYYY-MM-DD'
        for date_str in [start_date, end_date]:
            if date_str:
                if not validate_date(date_str):
                    raise ValueError(f"Invalid date format: {date_str}. Please use the format YYYY-mm-dd. For example: 2022-06-01")
                
        date_diff = (datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days
        if limit < date_diff:
            limit = date_diff
            logger.info('Limit is set to %s', limit)
        
        url = f"https://fwtapi2.fialda.com/api/services/app/StockInfo/GetSelfTrading"
        params = {'symbol': self.symbol,
                  'fromDate': start_date,
                  'toDate': end_date,
                  'pageNumber': page,
                  'pageSize': limit}

        response = requests.request("GET", url, headers=self.headers, params=params)

        if response.status_code != 200:
            raise ValueError(f"Failed to get data from {url}. Error code: {response.status_code}")
        
        data = response.json()
        logger.info('Total records: %s', data['result']['totalCount'])

        df = pd.DataFrame(data['result']['items'])

        # replace tT with deal
        df.columns = [col.replace('tT', 'deal') for col in df.columns]
        df.columns = [col.replace('kL_', '') for col in df.columns]
        df.columns = [camel_to_snake(col) for col in df.columns]

        df.name = self.symbol
        return df

