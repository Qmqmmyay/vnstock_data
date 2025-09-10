"""History module for vnd."""

# Đồ thị giá, đồ thị dư mua dư bán, đồ thị mức giá vs khối lượng, thống kê hành vi thị tường
from typing import List, Dict, Optional
from datetime import datetime
from .const import _CHART_BASE, _INTERVAL_MAP, _OHLC_MAP, _OHLC_DTYPE, _INTRADAY_MAP, _INTRADAY_DTYPE, _INDEX_MAPPING
from vnstock.explorer.vci.models import TickerModel
import requests
import pandas as pd
from vnai import agg_execution
from vnstock.core.utils.parser import get_asset_type
from vnstock.core.utils.logger import get_logger
from vnstock.core.utils.user_agent import get_headers

logger = get_logger(__name__)

class Quote:
    """
    VND data source for fetching stock market data, accommodating requests with large date ranges.
    """
    def __init__(self, symbol:str, random_agent:bool=False, show_log:bool=False):
        self.symbol = symbol.upper()
        self._history = None  # Cache for historical data
        self.asset_type = get_asset_type(self.symbol)
        self.base_url = _CHART_BASE
        # self.headers = _VND_HEADERS
        self.headers = get_headers(data_source='VND', random_agent=random_agent)
        self.interval_map = _INTERVAL_MAP
        self.data_source = 'VND'

        if not show_log:
            logger.setLevel('CRITICAL')

        if 'INDEX' in self.symbol:
            self.symbol = self._index_validation()

    def _index_validation(self) -> str:
        """
        If symbol contains 'INDEX' substring, validate it with _INDEX_MAPPING.
        """
        if self.symbol not in _INDEX_MAPPING.keys():
            raise ValueError(f"Không tìm thấy mã chứng khoán {self.symbol}. Các giá trị hợp lệ: {', '.join(_INDEX_MAPPING.keys())}")
        # return mapped symbol
        return _INDEX_MAPPING[self.symbol]

    def _input_validation(self, start: str, end: str, interval: str):
        """
        Validate input data
        """
        # Validate input data
        ticker = TickerModel(symbol=self.symbol, start=start, end=end, interval=interval)

        # if interval is not in the interval_map, raise an error
        if ticker.interval not in self.interval_map:
            raise ValueError(f"Giá trị interval không hợp lệ: {ticker.interval}. Vui lòng chọn: 1m, 5m, 15m, 30m, 1H, 1D, 1W, 1M")
        
        return ticker

    @agg_execution("VND.ext")
    def history(self, start: str, end: Optional[str], interval: Optional[str] = "1D", to_df: Optional[bool]=True, show_log: Optional[bool]=False, count_back: Optional[int]=None) -> Dict:
        """
        Tải lịch sử giá của mã chứng khoán từ nguồn dữ liệu VN Direct.

        Tham số:
            - start (bắt buộc): thời gian bắt đầu lấy dữ liệu, có thể là ngày dạng string kiểu "YYYY-MM-DD" hoặc "YYYY-MM-DD HH:MM:SS".
            - end (tùy chọn): thời gian kết thúc lấy dữ liệu. Mặc định là None, chương trình tự động lấy thời điểm hiện tại. Có thể nhập ngày dạng string kiểu "YYYY-MM-DD" hoặc "YYYY-MM-DD HH:MM:SS". 
            - interval (tùy chọn): Khung thời gian trích xuất dữ liệu giá lịch sử. Giá trị nhận: 1m, 5m, 15m, 30m, 1H, 1D, 1W, 1M. Mặc định là "1D".
            - to_df (tùy chọn): Chuyển đổi dữ liệu lịch sử trả về dưới dạng DataFrame. Mặc định là True. Đặt là False để trả về dữ liệu dạng JSON.
            - show_log (tùy chọn): Hiển thị thông tin log giúp debug dễ dàng. Mặc định là False.
            - count_back (tùy chọn): Số lượng dữ liệu trả về từ thời điểm cuối. Mặc định là 365.
        """
        # Validate inputs
        ticker = self._input_validation(start, end, interval)

        if end is None:
            end_stamp = int(datetime.now().timestamp())
        else:
            end_stamp = int(datetime.strptime(ticker.end, "%Y-%m-%d").timestamp())

        # convert start and end date to timestamp
        start_stamp = int(datetime.strptime(ticker.start, "%Y-%m-%d").timestamp())        
        
        interval = self.interval_map[ticker.interval]

        # Construct the URL for fetching data
        url = f"{self.base_url}/dchart/history?resolution={interval}&symbol={self.symbol}&from={start_stamp}&to={end_stamp}"

        if show_log:
            logger.info(f"Tải dữ liệu từ {url}")

        # Send a GET request to fetch the data
        response = requests.get(url, headers=self.headers)

        if response.status_code != 200:
            raise ConnectionError(f"Failed to fetch data: {response.status_code} - {response.reason}")

        json_data = response.json()

        if show_log:
            logger.info(f'Truy xuất thành công dữ liệu {ticker.symbol} từ {ticker.start} đến {ticker.end}, khung thời gian {ticker.interval}.')

        df = self._as_df(json_data, self.asset_type)

        # if interval is note 1D, 1W, 1M, then add 7 hours to the time column
        if ticker.interval not in ['1D', '1W', '1M']:
            df['time'] = df['time'] + pd.Timedelta(hours=7)

        if count_back is not None:
            df = df.tail(count_back)

        if to_df:
            return df
        else:
            json_data = df.to_json(orient='records')
            return json_data
    
    def _as_df(self, history_data: Dict, asset_type: str) -> pd.DataFrame:
        """
        Chuyển đổi dữ liệu lịch sử giá chứng khoán từ dạng JSON sang DataFrame.

        Tham số:
            - history_data: Dữ liệu lịch sử giá chứng khoán dạng JSON.
        Trả về:
            - DataFrame: Dữ liệu lịch sử giá chứng khoán dưới dạng DataFrame.
        """
        df = pd.DataFrame(history_data)

        # drop s column
        df.drop(columns=["s"], inplace=True)

        # rename columns using OHLC_MAP
        df.rename(columns=_OHLC_MAP, inplace=True)

        # format the output by HistoryPriceModel
        df["time"] = pd.to_datetime(df["time"], unit="s")

        # set datatype for each column using history_price_dtype
        for col, dtype in _OHLC_DTYPE.items():
            df[col] = df[col].astype(dtype)

        # Set metadata attributes
        df.attrs['name'] = self.symbol
        df.attrs['category'] = asset_type
        df.attrs['source'] = "VND" 

        return df

    @agg_execution("VND.ext")
    def intraday(self, page_size: Optional[int]=100000, to_df: Optional[bool]=True, show_log: bool=False) -> Dict:
        """
        Truy xuất dữ liệu khớp lệnh của mã chứng khoán bất kỳ từ nguồn dữ liệu VCI

        Tham số:
            - page_size (tùy chọn): Số lượng dữ liệu trả về trong một lần request. Mặc định là 100. Không giới hạn số lượng tối đa. Tăng số này lên để lấy toàn bộ dữ liêu, ví dụ 10_000.
            - trunc_time (tùy chọn): Thời gian cắt dữ liệu, dùng để lấy dữ liệu sau thời gian cắt. Mặc định là None.
            - to_df (tùy chọn): Chuyển đổi dữ liệu lịch sử trả về dưới dạng DataFrame. Mặc định là True. Đặt là False để trả về dữ liệu dạng JSON.
            - show_log (tùy chọn): Hiển thị thông tin log giúp debug dễ dàng. Mặc định là False.
        """
        # if self.symbol is not defined, raise ValueError
        if self.symbol is None:
            raise ValueError("Vui lòng nhập mã chứng khoán cần truy xuất khi khởi tạo Trading Class.")

        url = f"https://api-finfo.vndirect.com.vn/v4/stock_intraday_latest?q=code:{self.symbol}&sort=time&size={page_size}"

        if show_log:
            logger.info(f'Requested URL: {url}')

        response = requests.get(url, headers=self.headers)

        if response.status_code != 200:
            raise ConnectionError(f"Tải dữ liệu không thành công: {response.status_code} - {response.reason}")

        data = response.json()['data']

        # if there is no data, return None
        if len(data) == 0:
            logger.warning(f"Dữ liệu {self.symbol} không có sẵn hoặc chưa đến thời gian khớp lệnh.")
            return None

        if show_log:
            logger.info(data)

        df = pd.DataFrame(data)

        drop_columns = ['tradingDate', 'time', 'adLast', 'code', 'floor']
        for col in drop_columns:
            df.drop(columns=col, inplace=True)

        # select columns in _INTRADAY_MAP values
        # make sure columns from df are in _INTRADAY_MAP.keys()
        df = df[df.columns.intersection(_INTRADAY_MAP.keys())]
        # rename columns
        df.rename(columns=_INTRADAY_MAP, inplace=True)
        # replace b with Buy, s with Sell, unknown with ATO/ATC in match_type column
        # if the match_type is exist then run replace, otherwise skip
        if 'match_type' in df.columns:
            df['match_type'] = df['match_type'].replace({'PB': 'Buy', 'PS': 'Sell'})

        # convert time to datetime
        df['time'] = pd.to_datetime(df['time'])

        # sort by time
        df = df.sort_values(by='time').reset_index(drop=True)

        # apply _INTRADAY_DTYPE to columns for those columns available in _INTRADAY_DTYPE keys, ignore if not
        for col, dtype in _INTRADAY_DTYPE.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)

        df.name = self.symbol
        df.category = self.asset_type
        df.source = self.data_source

        if to_df:
            return df
        else:
            json_data = df.to_json(orient='records')
            return json_data
