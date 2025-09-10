from pandas import json_normalize
import pandas as pd
import json
import time
from typing import Union
from ssi_fc_data import fc_md_client, model
# from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient

class StockData:
    def __init__(self, client, config):
        self.client = client
        self.config = config

    def listing(self, market='HOSE', page=1, size=1000) -> pd.DataFrame:
        """
        Lấy danh sách chứng khoán.

        Args:
            market (str): Tên sàn giao dịch: HOSE, HNX, UPCOM. Mặc định là HOSE.
            page (int): Số trang. Mặc định là 1.
            size (int): Kích thước trang. Mặc định là 1000.

        Returns:
            pd.DataFrame: DataFrame chứa danh sách chứng khoán.
        """
        req = model.securities(market, page, size)
        response = self.client.securities(self.config, req)
        print(f'Tổng số bản ghi: {response["totalRecord"]}')
        return pd.DataFrame(response['data'])

    def details(self, symbol='ACB', market='HOSE', page=1, size=100) -> pd.DataFrame:
        """
        Lấy chi tiết của mã chứng khoán.

        Args:
            symbol (str): Mã chứng khoán. Mặc định là ACB.
            market (str): Tên sàn giao dịch. Mặc định là HOSE.
            page (int): Số trang. Mặc định là 1.
            size (int): Kích thước trang. Mặc định là 100.

        Returns:
            pd.DataFrame: DataFrame chứa chi tiết chứng khoán.
        """
        req = model.securities_details(market, symbol, page, size)
        response = self.client.securities_details(self.config, req)['data']
        df = json_normalize(response[0]['RepeatedInfo'])
        df['ReportDate'] = response[0]['ReportDate']
        df = df.dropna(axis=1, how='all')
        return df

    def daily_ohlc(self, symbol='SSI', start='01/07/2023', end='31/07/2023', ascending=True, page=1, size=1000) -> pd.DataFrame:
        """
        Lấy dữ liệu OHLC hàng ngày.

        Args:
            symbol (str): Mã chứng khoán. Mặc định là SSI.
            start (str): Ngày bắt đầu (dd/MM/yyyy). Mặc định là 01/07/2023.
            end (str): Ngày kết thúc (dd/MM/yyyy). Mặc định là 31/07/2023.
            ascending (bool): Sắp xếp tăng dần hoặc giảm dần. Mặc định là True.
            page (int): Số trang. Mặc định là 1.
            size (int): Kích thước trang. Mặc định là 1000.

        Returns:
            pd.DataFrame: DataFrame chứa dữ liệu OHLC hàng ngày.
        """
        req = model.daily_ohlc(symbol, start, end, page, size, ascending)
        response = self.client.daily_ohlc(self.config, req)['data']
        df = json_normalize(response)
        df = df.drop(columns=['Time'])
        return df

    def intraday_ohlc(self, symbol='SSI', start='25/07/2023', end='31/07/2023', page=1, size=1000, ascending=True, resolution=1) -> pd.DataFrame:
        """
        Lấy dữ liệu OHLC trong ngày.

        Args:
            symbol (str): Mã chứng khoán. Mặc định là SSI.
            start (str): Ngày bắt đầu (dd/MM/yyyy). Mặc định là 25/07/2023.
            end (str): Ngày kết thúc (dd/MM/yyyy). Mặc định là 31/07/2023.
            page (int): Số trang. Mặc định là 1.
            size (int): Kích thước trang. Mặc định là 1000.
            ascending (bool): Sắp xếp tăng dần hoặc giảm dần. Mặc định là True.
            resolution (int): Nhóm theo phút. Mặc định là 1.

        Returns:
            pd.DataFrame: DataFrame chứa dữ liệu OHLC trong ngày.
        """
        req = model.intraday_ohlc(symbol, start, end, page, size, ascending, resolution)
        response = self.client.intraday_ohlc(self.config, req)
        return json_normalize(response['data'])

    def daily_price(self, symbol='SSI', start='25/07/2023', end='31/07/2023', page=1, size=1000, market='') -> pd.DataFrame:
        """
        Lấy giá cổ phiếu hàng ngày.

        Args:
            symbol (str): Mã chứng khoán. Mặc định là SSI.
            start (str): Ngày bắt đầu (dd/MM/yyyy). Mặc định là 25/07/2023.
            end (str): Ngày kết thúc (dd/MM/yyyy). Mặc định là 31/07/2023.
            page (int): Số trang. Mặc định là 1.
            size (int): Kích thước trang. Mặc định là 1000.
            market (str): Thị trường cho mã chứng khoán. Mặc định là chuỗi rỗng.

        Returns:
            pd.DataFrame: DataFrame chứa giá cổ phiếu hàng ngày.
        """
        req = model.daily_stock_price(symbol, start, end, page, size, market)
        response = self.client.daily_stock_price(self.config, req)['data']
        return json_normalize(response)

class IndexData:
    def __init__(self, client, config):
        self.client = client
        self.config = config

    def listing(self, exchange='', page=1, size=100) -> pd.DataFrame:
        """
        Lấy danh sách mã và tên các chỉ số.

        Args:
            exchange (str, optional): Tên sàn giao dịch. Nếu không có giá trị sẽ trả về tất cả các sàn.

        Returns:
            pd.DataFrame: DataFrame chứa danh sách chỉ số.
        """
        req = model.index_list(exchange, page, size)
        response = self.client.index_list(self.config, req)['data']
        return json_normalize(response)

    def component(self, index='VN30', page=1, size=100) -> pd.DataFrame:
        """
        Lấy danh sách mã chứng khoán trong một chỉ số.

        Args:
            index (str): Mã chỉ số. Mặc định là VN30.
            page (int): Số trang. Mặc định là 1.
            size (int): Kích thước trang. Mặc định là 1000.

        Returns:
            pd.DataFrame: DataFrame chứa danh sách mã chứng khoán.
        """
        req = model.index_components(index, page, size)
        response = self.client.index_components(self.config, req)['data'][0]
        code = response['IndexCode']
        exchange = response['Exchange']
        total = response['TotalSymbolNo']
        print(f'Chỉ số: {code} - {exchange}. Tổng số {total} mã chứng khoán')
        df = json_normalize(response['IndexComponent'])
        df = df.drop(columns=['Isin'])
        return df

    def daily_ohlc(self, index='VN30', start='25/07/2023', end='31/07/2023', page=1, size=1000, orderBy='Tradingdate', order='desc', request_id='') -> pd.DataFrame:
        """
        Trả về dữ liệu chỉ số hàng ngày.

        Args:
            index (str): Tên chỉ số. Mặc định là VN30.
            start (str): Ngày bắt đầu (dd/MM/yyyy). Mặc định là 25/07/2023.
            end (str): Ngày kết thúc (dd/MM/yyyy). Mặc định là 31/07/2023.
            page (int): Số trang. Mặc định là 1.
            size (int): Kích thước trang. Mặc định là 1000.
            orderBy (str): Cột sắp xếp. Mặc định là Tradingdate.
            order (str): Kiểu sắp xếp. Mặc định là giảm dần (desc).
            request_id (str): ID yêu cầu. Mặc định là chuỗi rỗng.

        Returns:
            pd.DataFrame: DataFrame chứa dữ liệu chỉ số hàng ngày.
        """
        req = model.daily_index(request_id, index, start, end, page, size, orderBy, order)
        response = self.client.daily_index(self.config, req)['data']
        df = json_normalize(response)
        df = df.dropna(axis=1, how='all')
        return df

class Config:
    def __init__(self, consumer_id: str, consumer_secret: str, access_token:Union[str,None]=None):
        """
        Khởi tạo đối tượng cấu hình chứa thông tin cần thiết để kết nối tới API SSI Fast Connect.

        Tham số:
            - auth_type (str): Loại xác thực (ví dụ: 'Bearer').
            - consumerID (str): ID người dùng.
            - consumerSecret (str): Mật khẩu người dùng.
            - access_token (str): Token tạo ra từ thông tin đăng nhập, sinh ra bằng cách gọi hàm get_token.

        Thuộc tính:
            - auth_type (str): Loại xác thực.
            - consumerID (str): ID người dùng.
            - consumerSecret (str): Mật khẩu người dùng.
            - access_jwt (str): Mã thông báo JWT truy cập.
            - url (str): URL của API dữ liệu thị trường.
            - stream_url (str): URL của API dữ liệu dòng.
        """
        self.consumerID = consumer_id
        self.consumerSecret = consumer_secret
        self.auth_type = 'Bearer'
        self.url = 'https://fc-data.ssi.com.vn/'
        self.stream_url = 'https://fc-data.ssi.com.vn/'
        self.access_jwt = access_token

def get_token (config):
    client = fc_md_client.MarketDataClient(config)
    token = client.access_token(model.accessToken(config.consumerID, config.consumerSecret))
    return token


class FCData:
    def __init__(self, config):
        """
        Khởi tạo đối tượng để truy xuất dữ liệu từ SSI Fast Connect Data API.

        Tham số:
            - config (object): Đối tượng cấu hình chứa các thông tin cần thiết để kết nối tới API.

        Thuộc tính:
            - config (object): Đối tượng cấu hình được truyền vào.
            - client (MarketDataClient): Đối tượng khách hàng để tương tác với API dữ liệu thị trường.
            - stock (StockData): Đối tượng để truy xuất dữ liệu chứng khoán.
            - index (IndexData): Đối tượng để truy xuất dữ liệu chỉ số.
        """
        self.config = config
        self.client = fc_md_client.MarketDataClient(config)
        self.stock = StockData(self.client, config)
        self.index = IndexData(self.client, config)