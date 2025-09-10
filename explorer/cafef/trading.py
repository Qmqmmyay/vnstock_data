import requests
import pandas as pd
from typing import Union, Optional, Dict
from vnstock_data.explorer.cafef.const import _BASE_URL, _PRICE_HISTORY_MAP, _FOREIGN_TRADE_MAP, _PROP_TRADE_MAP, _ORDER_STATS_MAP, _INSIDER_DEAL_MAP
from vnstock_data.core.utils.parser import days_between
from vnstock_data.core.utils.user_agent import get_headers
from vnstock.core.utils.logger import get_logger

logger = get_logger(__name__)

class Trading:
    def __init__(self, symbol:str, random_agent:bool=False):
        self.symbol = symbol.upper()
        self.base_url = _BASE_URL
        self.headers = get_headers(data_source='CAFEF', random_agent=random_agent)

    def _df_standardized (self, history_data:pd.DataFrame, mapping_dict:Dict) -> pd.DataFrame:
        """
        Định dạng lại dữ liệu lịch sử giá của mã chứng khoán

        Tham số:
            - history_data (DataFrame): Dữ liệu lịch sử giá của mã chứng khoán.
        Return:
            - DataFrame: Dữ liệu lịch sử giá của mã chứng khoán.
        """
        history_data = history_data.rename(columns=mapping_dict)
        sorted_columns = mapping_dict.values()
        if history_data.empty:
            logger.info('No data found')
            return None
        else:
            try:
                history_data = history_data[sorted_columns]
            except:
                logger.debug(f'Failed to sort column names. Actual columns and predefine mapping dict mismatched. Actual columns: {history_data.columns}. Expected columns: {sorted_columns}')
                pass

            # Set time index
            if 'time' in history_data.columns:
                history_data['time'] = pd.to_datetime(history_data['time'], format='%d/%m/%Y')
                history_data.set_index('time', inplace=True)
            return history_data

    def price_history (self, start:str, end:str, page:Optional[int]=1, limit:Optional[Union[int, None]]=None) -> pd.DataFrame:
        """
        Truy xuất dữ liệu lịch sử giá của mã chứng khoán bất kỳ từ nguồn dữ liệu CafeF

        Tham số:
            - start (bắt buộc): Ngày bắt đầu lấy dữ liệu, định dạng YYYY-mm-dd. Ví dụ '2024-06-01'.
            - end (bắt buộc): Ngày kết thúc lấy dữ liệu, định dạng YYYY-mm-dd. Ví dụ '2024-07-31'.
            - page (tùy chọn): Trang hiện tại. Mặc định là 1.
            - limit (tùy chọn): Số lượng dữ liệu trả về trong một lần request. Mặc định là None để đặt giới hạn tương ứng số ngày giữa khung thời gian start và end.
        Return:
            - DataFrame: Dữ liệu lịch sử giá của mã chứng khoán.
        """
        if limit is None:
            limit = days_between(start=start, end=end, format='%Y-%m-%d')
        # convert start and end string to %m/%d/%Y format
        start = pd.to_datetime(start).strftime('%m/%d/%Y')
        end = pd.to_datetime(end).strftime('%m/%d/%Y')
        url = f"{self.base_url}/PriceHistory.ashx?Symbol={self.symbol}&StartDate={start}&EndDate={end}&PageIndex={page}&PageSize={limit}"
        response = requests.request("GET", url, headers=self.headers)
        if response.status_code != 200:
            raise ConnectionError(f"Tải dữ liệu không thành công: {response.status_code} - {response.text}")
        data = response.json()['Data']
        records = data['TotalCount']
        logger.info(f'Lịch sử giá:\nMã CK: {self.symbol}. Số bản ghi hợp lệ: {records}')
        df = pd.DataFrame(data['Data'])
        try:
            # if change in columns list
            if 'ThayDoi' in df.columns:
                # Extract change_pct in floating format from change column
                df['change_pct'] = pd.to_numeric(
                                                    df['ThayDoi']
                                                    .str.split('(', expand=True)[1]
                                                    .str.replace(' %)', '', regex=False)
                                                    .str.replace(')', '', regex=False)
                                                    .str.strip(),
                                                    errors='coerce'
                                                ) / 100
        except:
            logger.debug('Failed to extract change_pct from ThayDoi column')
            pass
        try:
            df = self._df_standardized(df, _PRICE_HISTORY_MAP)
        except Exception as e:
            logger.error(f'Failed to standardize data: {e}')

        # drop change column
        try:
            df.drop(columns=['change'], inplace=True)
        except:
            logger.debug('Failed to drop change column')
            pass
        # Set properties
        df.name = self.symbol
        df.category = 'history_price'
        df.source = 'CafeF'
        return df

    def foreign_trade (self, start:str, end:str, page:Optional[int]=1, limit:Optional[Union[int, None]]=None) -> pd.DataFrame:
        """
        Truy xuất dữ liệu lịch sử giao dịch nhà đầu tư nước ngoài của mã chứng khoán bất kỳ từ nguồn dữ liệu CafeF

        Tham số:
            - start (bắt buộc): Ngày bắt đầu lấy dữ liệu, định dạng YYYY-mm-dd. Ví dụ '2024-06-01'.
            - end (bắt buộc): Ngày kết thúc lấy dữ liệu, định dạng YYYY-mm-dd. Ví dụ '2024-07-31'.
            - page (tùy chọn): Trang hiện tại. Mặc định là 1.
            - limit (tùy chọn): Số lượng dữ liệu trả về trong một lần request. Mặc định là None để đặt giới hạn tương ứng số ngày giữa khung thời gian start và end.
        Return:
            - DataFrame: Dữ liệu lịch sử giá của mã chứng khoán.
        """
        if limit is None:
            limit = days_between(start=start, end=end, format='%Y-%m-%d')
        # convert start and end string to %m/%d/%Y format
        start = pd.to_datetime(start).strftime('%m/%d/%Y')
        end = pd.to_datetime(end).strftime('%m/%d/%Y')
        url = f"{self.base_url}/GDKhoiNgoai.ashx?Symbol={self.symbol}&StartDate={start}&EndDate={end}&PageIndex={page}&PageSize={limit}"
        response = requests.request("GET", url, headers=self.headers)
        if response.status_code != 200:
            raise ConnectionError(f"Tải dữ liệu không thành công: {response.status_code} - {response.text}")
        data = response.json()['Data']
        records = data['TotalCount']
        logger.info(f'Lịch sử GD Nước ngoài:\nMã CK: {self.symbol}. Số bản ghi hợp lệ: {records}')
        df = pd.DataFrame(data['Data'])
        df = self._df_standardized(df, _FOREIGN_TRADE_MAP)
        # drop change column
        try:
            df.drop(columns=['change'], inplace=True)
        except:
            logger.debug('Failed to drop change column')
            pass
        # Set properties
        df.name = self.symbol
        df.category = 'foreign_trade'
        df.source = 'CafeF'
        return df

    def prop_trade (self, start:str, end:str, page:Optional[int]=1, limit:Optional[Union[int, None]]=None) -> pd.DataFrame:
        """
        Truy xuất dữ liệu lịch sử giao dịch tự doanh của mã chứng khoán bất kỳ từ nguồn dữ liệu CafeF

        Tham số:
            - start (bắt buộc): Ngày bắt đầu lấy dữ liệu, định dạng YYYY-mm-dd.
            - end (bắt buộc): Ngày kết thúc lấy dữ liệu, định dạng YYYY-mm-dd.
            - page (tùy chọn): Trang hiện tại. Mặc định là
            - limit (tùy chọn): Số lượng dữ liệu trả về trong một lần request. Mặc định là None để đặt giới hạn tương ứng số ngày giữa khung thời gian start và end.
        Return:
            - DataFrame: Dữ liệu lịch sử giá của mã chứng khoán.
        """
        if limit is None:
            limit = days_between(start=start, end=end, format='%Y-%m-%d')
        # convert start and end string to %m/%d/%Y format
        start = pd.to_datetime(start).strftime('%m/%d/%Y')
        end = pd.to_datetime(end).strftime('%m/%d/%Y')
        url = f"{self.base_url}/GDTuDoanh.ashx?Symbol={self.symbol}&StartDate={start}&EndDate={end}&PageIndex={page}&PageSize={limit}"
        response = requests.request("GET", url, headers=self.headers)
        if response.status_code != 200:
            raise ConnectionError(f"Tải dữ liệu không thành công: {response.status_code} - {response.text}")
        data = response.json()['Data']
        records = data['TotalCount']
        logger.info(f'Lịch sử GD Tự Doanh:\nMã CK: {self.symbol}. Số bản ghi hợp lệ: {records}')
        df = pd.DataFrame(data['Data']['ListDataTudoanh'])
        df = self._df_standardized(df, _PROP_TRADE_MAP)
        try:
            df.drop(columns='symbol', inplace=True)
        except:
            logger.debug('Failed to drop symbol column')
            pass
        # Set properties
        df.name = self.symbol
        df.category = 'prop_trade'
        df.source = 'CafeF'
        return df

    def order_stats (self, start:str, end:str, page:Optional[int]=1, limit:Optional[Union[int, None]]=None) -> pd.DataFrame:
        """
        Truy xuất dữ liệu lịch sử thống kê đặt lệnh của mã chứng khoán bất kỳ từ nguồn dữ liệu CafeF

        Tham số:
            - start (bắt buộc): Ngày bắt đầu lấy dữ liệu, định dạng YYYY-mm-dd.
            - end (bắt buộc): Ngày kết thúc lấy dữ liệu, định dạng YYYY-mm-dd.
            - page (tùy chọn): Trang hiện tại. Mặc định là
            - limit (tùy chọn): Số lượng dữ liệu trả về trong một lần request. Mặc định là None để đặt giới hạn tương ứng số ngày giữa khung thời gian start và end.
        Return:
            - DataFrame: Dữ liệu lịch sử giá của mã chứng khoán.
        """
        if limit is None:
            limit = days_between(start=start, end=end, format='%Y-%m-%d')
        # convert start and end string to %m/%d/%Y format
        start = pd.to_datetime(start).strftime('%m/%d/%Y')
        end = pd.to_datetime(end).strftime('%m/%d/%Y')
        url = f"{self.base_url}/ThongKeDL.ashx?Symbol={self.symbol}&StartDate={start}&EndDate={end}&PageIndex={page}&PageSize={limit}"
        response = requests.request("GET", url, headers=self.headers)
        if response.status_code != 200:
            raise ConnectionError(f"Tải dữ liệu không thành công: {response.status_code} - {response.text}")
        data = response.json()['Data']
        records = data['TotalCount']
        logger.info(f'Thống kê đặt lệnh:\nMã CK: {self.symbol}. Số bản ghi hợp lệ: {records}')
        df = pd.DataFrame(data['Data'])
        df = self._df_standardized(df, _ORDER_STATS_MAP)
        # drop change column
        try:
            df.drop(columns=['change'], inplace=True)
        except:
            logger.debug('Failed to drop change column')
            pass

        # Set properties
        df.name = self.symbol
        df.category = 'order_stats'
        df.source = 'CafeF'
        return df

    def insider_deal (self, start:str, end:str, page:Optional[int]=1, limit:Optional[Union[int, None]]=None) -> pd.DataFrame:
        """
        Truy xuất dữ liệu lịch sử thống kê giao dịch của cổ đông & nội bộ cho mã chứng khoán bất kỳ từ nguồn dữ liệu CafeF

        Tham số:
            - start (bắt buộc): Ngày bắt đầu lấy dữ liệu, định dạng YYYY-mm-dd.
            - end (bắt buộc): Ngày kết thúc lấy dữ liệu, định dạng YYYY-mm-dd.
            - page (tùy chọn): Trang hiện tại. Mặc định là
            - limit (tùy chọn): Số lượng dữ liệu trả về trong một lần request. Mặc định là None để đặt giới hạn tương ứng số ngày giữa khung thời gian start và end.
        Return:
            - DataFrame: Dữ liệu lịch sử giá của mã chứng khoán.
        """
        if limit is None:
            limit = days_between(start=start, end=end, format='%Y-%m-%d')
        # convert start and end string to %m/%d/%Y format
        start = pd.to_datetime(start).strftime('%m/%d/%Y')
        end = pd.to_datetime(end).strftime('%m/%d/%Y')
        url = f"{self.base_url}/GDCoDong.ashx?Symbol={self.symbol}&StartDate={start}&EndDate={end}&PageIndex={page}&PageSize={limit}"
        response = requests.request("GET", url, headers=self.headers)
        if response.status_code != 200:
            raise ConnectionError(f"Tải dữ liệu không thành công: {response.status_code} - {response.text}")
        data = response.json()['Data']
        records = data['TotalCount']
        logger.info(f'Thống kê giao dịch Cổ Đông & Nội bộ:\nMã CK: {self.symbol}. Số bản ghi hợp lệ: {records}')
        df = pd.DataFrame(data['Data'])
        df = self._df_standardized(df, _INSIDER_DEAL_MAP)

        for col in ["plan_begin_date", "plan_end_date", "real_end_date", "published_date", "order_date"]:
            df[col] = df[col].str.replace(r'\D', '', regex=True)
            # Convert to numeric type before calling to_datetime
            df[col] = pd.to_datetime(pd.to_numeric(df[col]), unit='ms')
            # localize time to Asia/Ho_Chi_Minh and format
            # df[col] = (df[col] + pd.Timedelta(hours=7)).dt.strftime('%Y-%m-%d')

        # Set properties
        df.name = self.symbol
        df.category = 'insider_deals'
        df.source = 'CafeF'
        return df