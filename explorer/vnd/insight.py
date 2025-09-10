import pandas as pd
import requests
from typing import Union
from datetime import datetime
from vnai import agg_execution
from vnstock_data.explorer.vnd.const import _INSIGHT_BASE, _TOP_STOCK_INDEX, _TOP_STOCK_COLS
from vnstock.core.utils.logger import get_logger
from vnstock.core.utils.user_agent import get_headers

logger = get_logger(__name__)

class TopStock:
    """
    Lớp để lấy dữ liệu cổ phiếu hàng đầu từ API VND.

    Lớp này cung cấp các phương thức để truy xuất dữ liệu về top 10 cổ phiếu
    dựa trên các tiêu chí hiệu suất khác nhau như tăng giá, giảm giá,
    giá trị giao dịch, khối lượng đột biến, giao dịch thỏa thuận đột biến,
    và mua/bán của nhà đầu tư nước ngoài.

    Thuộc tính:
    -----------
    base_url : str
        URL cơ sở cho API VND.
    headers : dict
        Headers HTTP được sử dụng trong các yêu cầu API, bao gồm user-agent.
    data_source : str
        Tên nguồn dữ liệu, trong trường hợp này là 'VND'.

    Phương thức:
    -----------
    gainer(index: str='VNINDEX', limit: int=10) -> pd.DataFrame:
        Lấy top 10 cổ phiếu có tỷ lệ tăng giá cao nhất cho chỉ số cụ thể.
    
    loser(index: str='VNINDEX', limit: int=10) -> pd.DataFrame:
        Lấy top 10 cổ phiếu có tỷ lệ giảm giá cao nhất cho chỉ số cụ thể.
    
    trade(index: str='VNINDEX', limit: int=10) -> pd.DataFrame:
        Lấy top 10 cổ phiếu có giá trị giao dịch lớn nhất cho chỉ số cụ thể.
    
    volume(index: str='VNINDEX', limit: int=10) -> pd.DataFrame:
        Lấy top 10 cổ phiếu có khối lượng đột biến lớn nhất cho chỉ số cụ thể.
    
    deal(index: str='VNINDEX', limit: int=10) -> pd.DataFrame:
        Lấy top 10 cổ phiếu có giao dịch thỏa thuận đột biến lớn nhất cho chỉ số cụ thể.

    top_foreign_buy(trading_date: str, limit: int=10) -> pd.DataFrame:
        Lấy top 10 cổ phiếu có giá trị mua ròng lớn nhất từ nhà đầu tư nước ngoài.

    top_foreign_trade(trading_date: str, limit: int=10) -> pd.DataFrame:
        Lấy top 10 cổ phiếu có giá trị giao dịch ròng lớn nhất từ nhà đầu tư nước ngoài.
    """

    def __init__(self, show_log: bool = False, random_agent: bool = False):
        """
        Khởi tạo lớp TopStock với tùy chọn sử dụng user-agent ngẫu nhiên và hiển thị log.

        Tham số:
        -----------
        show_log : bool, tùy chọn
            Nếu True, hiển thị thông báo log.
        random_agent : bool, tùy chọn
            Nếu True, sử dụng user-agent ngẫu nhiên trong headers HTTP (mặc định là False).
        """
        self.show_log = show_log
        self.base_url = _INSIGHT_BASE
        self.headers = get_headers(data_source='VND', random_agent=random_agent)
        self.data_source = 'VND'

        if not show_log:
            logger.setLevel('CRITICAL')

    def _fetch_data(self, url: str) -> pd.DataFrame:
        """
        Phương thức nội bộ để lấy dữ liệu từ URL đã cho và trả về dưới dạng DataFrame của pandas.

        Tham số:
        -----------
        url : str
            URL để gửi yêu cầu GET.

        Trả về:
        --------
        pd.DataFrame
            Một DataFrame chứa dữ liệu lấy từ API, hoặc một DataFrame trống nếu có lỗi xảy ra.
        """
        try:
            if self.show_log:
                logger.info(f"Lấy dữ liệu từ URL: {url}")
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            if 'data' in data:
                df = pd.DataFrame(data['data'])
                # Rename columns using the provided dictionary
                df.rename(columns=_TOP_STOCK_COLS, inplace=True)
                return df
            else:
                logger.error("Không có trường 'data' trong phản hồi JSON")
                return pd.DataFrame()
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi khi lấy dữ liệu: {e}")
            return pd.DataFrame()

    def _fetch_foreign_data(self, url: str) -> pd.DataFrame:
        """
        Phương thức nội bộ để lấy dữ liệu giao dịch nước ngoài từ URL đã cho và trả về dưới dạng DataFrame.

        Tham số:
        -----------
        url : str
            URL để gửi yêu cầu GET.

        Trả về:
        --------
        pd.DataFrame
            Một DataFrame chứa dữ liệu giao dịch nước ngoài, với các cột được đổi tên thành 'symbol', 'date', và 'net_value'.
        """
        try:
            if self.show_log:
                logger.info(f"Lấy dữ liệu từ URL: {url}")
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            if 'data' in data:
                df = pd.DataFrame(data['data'])
                # Rename columns specifically for foreign transaction data
                df.rename(columns={
                    'code': 'symbol',
                    'tradingDate': 'date',
                    'netVal': 'net_value'
                }, inplace=True)
                return df
            else:
                logger.error("Không có trường 'data' trong phản hồi JSON")
                return pd.DataFrame()
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi khi lấy dữ liệu: {e}")
            return pd.DataFrame()

    def _get_index_code(self, index: str) -> str:
        """
        Phương thức nội bộ để lấy mã chỉ số từ tên chỉ số.

        Tham số:
        -----------
        index : str
            Tên chỉ số được người dùng cung cấp (ví dụ: 'VNINDEX').

        Trả về:
        --------
        str
            Mã chỉ số tương ứng (ví dụ: 'VNIndex').
        """
        return _TOP_STOCK_INDEX.get(index.upper(), 'VNIndex')

    @agg_execution("VND.ext")
    def gainer(self, index: str = 'VNINDEX', limit: int = 10) -> pd.DataFrame:
        """
        Lấy top 10 cổ phiếu có tỷ lệ tăng giá cao nhất cho chỉ số cụ thể.

        Tham số:
        -----------
        index : str, tùy chọn
            Tên chỉ số (mặc định là 'VNINDEX').
        limit : int, tùy chọn
            Số lượng cổ phiếu muốn lấy (mặc định là 10).

        Trả về:
        --------
        pd.DataFrame
            Một DataFrame chứa top cổ phiếu tăng giá.
        """
        index_code = self._get_index_code(index)
        url = f"{self.base_url}/top_stocks?q=index:{index_code}~nmVolumeAvgCr20D:gte:10000~priceChgPctCr1D:gt:0&size={limit}&sort=priceChgPctCr1D"
        return self._fetch_data(url)

    @agg_execution("VND.ext")
    def loser(self, index: str = 'VNINDEX', limit: int = 10) -> pd.DataFrame:
        """
        Lấy top 10 cổ phiếu có tỷ lệ giảm giá cao nhất cho chỉ số cụ thể.

        Tham số:
        -----------
        index : str, tùy chọn
            Tên chỉ số (mặc định là 'VNINDEX').
        limit : int, tùy chọn
            Số lượng cổ phiếu muốn lấy (mặc định là 10).

        Trả về:
        --------
        pd.DataFrame
            Một DataFrame chứa top cổ phiếu giảm giá.
        """
        index_code = self._get_index_code(index)
        url = f"{self.base_url}/top_stocks?q=index:{index_code}~nmVolumeAvgCr20D:gte:10000~priceChgPctCr1D:lt:0&size={limit}&sort=priceChgPctCr1D:asc"
        return self._fetch_data(url)

    @agg_execution("VND.ext")
    def value (self, index: str = 'VNINDEX', limit: int = 10) -> pd.DataFrame:
        """
        Lấy top 10 cổ phiếu có giá trị giao dịch lớn nhất cho chỉ số cụ thể.

        Tham số:
        -----------
        index : str, tùy chọn
            Tên chỉ số (mặc định là 'VNINDEX').
        limit : int, tùy chọn
            Số lượng cổ phiếu muốn lấy (mặc định là 10).

        Trả về:
        --------
        pd.DataFrame
            Một DataFrame chứa top cổ phiếu có giá trị giao dịch lớn nhất.
        """
        index_code = self._get_index_code(index)
        url = f"{self.base_url}/top_stocks?q=index:{index_code}~accumulatedVal:gt:0&size={limit}&sort=accumulatedVal"
        return self._fetch_data(url)

    @agg_execution("VND.ext")
    def volume(self, index: str = 'VNINDEX', limit: int = 10) -> pd.DataFrame:
        """
        Lấy top 10 cổ phiếu có khối lượng đột biến lớn nhất cho chỉ số cụ thể.

        Tham số:
        -----------
        index : str, tùy chọn
            Tên chỉ số (mặc định là 'VNINDEX').
        limit : int, tùy chọn
            Số lượng cổ phiếu muốn lấy (mặc định là 10).

        Trả về:
        --------
        pd.DataFrame
            Một DataFrame chứa top cổ phiếu có khối lượng đột biến lớn nhất.
        """
        index_code = self._get_index_code(index)
        url = f"{self.base_url}/top_stocks?q=index:{index_code}~nmVolumeAvgCr20D:gte:10000~nmVolNmVolAvg20DPctCr:gte:100&size={limit}&sort=nmVolNmVolAvg20DPctCr"
        return self._fetch_data(url)

    @agg_execution("VND.ext")
    def deal(self, index: str = 'VNINDEX', limit: int = 10) -> pd.DataFrame:
        """
        Lấy top 10 cổ phiếu có giao dịch thỏa thuận đột biến lớn nhất cho chỉ số cụ thể.

        Tham số:
        -----------
        index : str, tùy chọn
            Tên chỉ số (mặc định là 'VNINDEX').
        limit : int, tùy chọn
            Số lượng cổ phiếu muốn lấy (mặc định là 10).

        Trả về:
        --------
        pd.DataFrame
            Một DataFrame chứa top cổ phiếu có giao dịch thỏa thuận đột biến lớn nhất.
        """
        index_code = self._get_index_code(index)
        url = f"{self.base_url}/top_stocks?size={limit}&q=index:{index_code}~nmVolumeAvgCr20D:gte:10000&sort=ptVolTotalVolAvg20DPctCr"
        return self._fetch_data(url)

    @agg_execution("VND.ext")
    def foreign_buy (self, date: Union[str, None]=None, limit: int = 10) -> pd.DataFrame:
        """
        Lấy top 10 cổ phiếu có giá trị mua ròng lớn nhất từ nhà đầu tư nước ngoài.

        Tham số:
        -----------
        date : str
            Ngày giao dịch dưới dạng 'YYYY-mm-dd'. Ví dụ: '2024-08-16'.
        limit : int, tùy chọn
            Số lượng cổ phiếu muốn lấy (mặc định là 10).

        Trả về:
        --------
        pd.DataFrame
            Một DataFrame chứa top cổ phiếu có giá trị mua ròng lớn nhất từ nhà đầu tư nước ngoài.
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        url = f"https://api-finfo.vndirect.com.vn/v4/foreigns?q=type:STOCK,IFC,ETF~netVal:gt:0~tradingDate:{date}&sort=tradingDate~netVal:desc&size={limit}&fields=code,netVal,tradingDate"
        return self._fetch_foreign_data(url)

    @agg_execution("VND.ext")
    def foreign_sell (self, date: Union[str, None]=None, limit: int = 10) -> pd.DataFrame:
        """
        Lấy top 10 cổ phiếu có giá trị giao dịch ròng lớn nhất từ nhà đầu tư nước ngoài.

        Tham số:
        -----------
        date : str
            Ngày giao dịch dưới dạng 'YYYY-mm-dd'. Ví dụ: '2024-08-16'.
        limit : int, tùy chọn
            Số lượng cổ phiếu muốn lấy (mặc định là 10).

        Trả về:
        --------
        pd.DataFrame
            Một DataFrame chứa top cổ phiếu có giá trị giao dịch ròng lớn nhất từ nhà đầu tư nước ngoài.
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        url = f"https://api-finfo.vndirect.com.vn/v4/foreigns?q=type:STOCK,IFC,ETF~netVal:lt:0~tradingDate:{date}&sort=tradingDate~netVal:asc&size={limit}&fields=code,netVal,tradingDate"
        return self._fetch_foreign_data(url)
