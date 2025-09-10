# Đồ thị giá, đồ thị dư mua dư bán, đồ thị mức giá vs khối lượng, thống kê hành vi thị tường
from typing import Dict, Optional
from datetime import datetime
# from .const import _GROUP_CODE
import requests
import pandas as pd
from vnai import agg_execution
from vnstock.core.utils.parser import camel_to_snake
from vnstock.core.utils.logger import get_logger
from vnstock.core.utils.user_agent import get_headers

logger = get_logger(__name__)

class Listing:
    """
    Cấu hình truy cập dữ liệu lịch sử giá chứng khoán từ VCI.
    """
    def __init__(self, random_agent:bool=False, show_log:bool=False):
        self.data_source = 'VND'
        self.headers = get_headers(data_source=self.data_source, random_agent=random_agent)
        
        if not show_log:
            logger.setLevel('CRITICAL')
    
    @agg_execution("VND.ext")
    def all_symbols (self, exchange=['HOSE', 'HNX', 'UPCOM'],  show_log:Optional[bool]=False, to_df:Optional[bool]=True) -> Dict:
        """
        Truy xuất toàn bộ danh sách cổ phiếu và trạng thái giao dịch.

        Tham số:
            - show_log (tùy chọn): Hiển thị thông tin log giúp debug dễ dàng. Mặc định là False.
            - to_df (tùy chọn): Chuyển đổi dữ liệu danh sách mã cổ phiếu trả về dưới dạng DataFrame. Mặc định là True. Đặt là False để trả về dữ liệu dạng JSON.
        """
        # if exchange list has more than 1, join them with comma
        if len(exchange) > 1:
            exchange = ','.join(exchange)
        else:
            exchange = exchange[0]

        url = f'https://api-finfo.vndirect.com.vn/v4/stocks?q=type:stock,ifc~floor:{exchange}&size=9999'

        if show_log:
            logger.info(f'Requested URL: {url}')

        response = requests.request("GET", url, headers=self.headers)

        if response.status_code != 200:
            raise ConnectionError(f"Failed to fetch data: {response.status_code} - {response.reason}")

        json_data = response.json()

        df = pd.DataFrame(json_data['data'])

        # rename floor column to be exchange
        if 'floor' in df.columns:
            df = df.rename(columns={'floor':'exchange'})

        # try to rename column code to symbol
        if 'code' in df.columns:
            df = df.rename(columns={'code':'symbol'})

        if to_df:
            if not json_data:
                raise ValueError("JSON data is empty or not provided.")
            # Convert camel to snake case
            df.columns = [camel_to_snake(col) for col in df.columns]
            # Set metadata attributes
            df.source = "VND"
            return df
        else:
            json_data = df.to_json(orient='records')
            return json_data