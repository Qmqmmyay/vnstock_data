import pandas as pd
from datetime import datetime
from .spl_fetcher import SPLFetcher
from typing import Dict, Any, Optional, List
from vnai import agg_execution
from vnstock.core.utils.logger import get_logger

logger = get_logger(__name__)

class CommodityPrice:
    """
    Lớp cung cấp các phương thức để lấy dữ liệu giá hàng hóa từ nguồn SPL.
    """

    def __init__(self, start: Optional[str] = None, end: Optional[str] = None, show_log: Optional[bool] = False):
        """
        Khởi tạo đối tượng CommodityPrice với tùy chọn ngày bắt đầu và kết thúc mặc định.

        Các tham số:
            start (str, optional): Ngày bắt đầu mặc định (định dạng 'YYYY-MM-DD'). Mặc định là None.
            end (str, optional): Ngày kết thúc mặc định (định dạng 'YYYY-MM-DD'). Mặc định là None.
        """
        self.fetcher = SPLFetcher()
        self.default_start = start
        self.default_end = end

        if not show_log:
            logger.setLevel('CRITICAL')

    def _fetch_commodity(self, ticker: str, start: Optional[str] = None, end: Optional[str] = None, interval: str = "1d", columns:Optional[List]=None) -> pd.DataFrame:
        """
        Lấy dữ liệu giá hàng hóa từ API SPL.

        Các tham số:
            ticker (str): Mã hàng hóa cần lấy dữ liệu.
            start (str, optional): Ngày bắt đầu (định dạng 'YYYY-MM-DD'). Ưu tiên tham số nếu có, mặc định là giá trị khởi tạo của lớp.
            end (str, optional): Ngày kết thúc (định dạng 'YYYY-MM-DD'). Ưu tiên tham số nếu có, mặc định là giá trị khởi tạo của lớp.
            interval (str, optional): Khoảng thời gian của dữ liệu (mặc định là '1d').
            columns (List, optional): Danh sách tên cột cần lấy dữ liệu. Mặc định là None, lấy tất cả các cột.
        
        Giá trị trả về:
            dict: Dữ liệu thô dạng từ điển JSON được chuyển đổi thành DataFrame.
        """
        params = {
            "ticker": ticker,
            "interval": interval,
            "type": "commodity",
        }

        # Chuyển đổi ngày start và end sang timestamp, ưu tiên giá trị phương thức nếu có
        start = start or self.default_start
        end = end or self.default_end

        if start:
            params["from"] = int(datetime.strptime(start, "%Y-%m-%d").timestamp())
        if end:
            params["to"] = int(datetime.strptime(end, "%Y-%m-%d").timestamp())

        self.fetcher.validate(params)
        raw_data = self.fetcher.fetch(endpoint="/historical/prices/ohlcv", params=params)
        df = self.fetcher.to_dataframe(raw_data["data"])
        # convert time column from YYYY-mm-dd string to datetime
        df["time"] = pd.to_datetime(df["time"])
        # set time as index
        df.set_index("time", inplace=True)

        # if columns is not None and available in df, return only selected columns
        if columns is not None:
            return df[columns]
        return df
        

    def _gold_vn_buy(self, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, Any]:
        """Lấy giá vàng Việt Nam (mua vào)."""
        return self._fetch_commodity("GOLD:VN:BUY", start, end, columns=['close'])

    def _gold_vn_sell(self, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, Any]:
        """Lấy giá vàng Việt Nam (bán ra)."""
        return self._fetch_commodity("GOLD:VN:SELL", start, end, columns=['close'])
    
    @agg_execution("SPL.ext")
    def gold_vn(self, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, Any]:
        """Lấy giá vàng Việt Nam."""
        buy = self._gold_vn_buy(start, end)
        sell = self._gold_vn_sell(start, end)
        # return as Pandas DataFrame
        df = pd.concat([buy, sell], axis=1)
        df.columns = ["buy", "sell"]
        return df

    @agg_execution("SPL.ext")
    def gold_global(self, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, Any]:
        """Lấy giá vàng thế giới."""
        return self._fetch_commodity("GC=F", start, end)

    @agg_execution("SPL.ext")
    def _gas_ron92(self, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, Any]:
        """Lấy giá xăng RON92 tại Việt Nam."""
        return self._fetch_commodity("GAS:RON92:VN", start, end, columns=['close'])

    @agg_execution("SPL.ext")
    def _gas_ron95(self, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, Any]:
        """Lấy giá xăng RON95 tại Việt Nam."""
        return self._fetch_commodity("GAS:RON95:VN", start, end, columns=['close'])

    @agg_execution("SPL.ext")
    def _oil_do(self, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, Any]:
        """Lấy giá dầu DO tại Việt Nam."""
        return self._fetch_commodity("GAS:DO:VN", start, end, columns=['close'])

    @agg_execution("SPL.ext")
    def gas_vn(self, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, Any]:
        """Lấy giá xăng và dầu DO tại Việt Nam."""
        ron92 = self._gas_ron92(start, end)
        ron95 = self._gas_ron95(start, end)
        oil_do = self._oil_do(start, end)
        df = pd.concat([ron95, ron92, oil_do], axis=1)
        df.columns = ["ron95", "ron92", "oil_do"]
        return df

    @agg_execution("SPL.ext")
    def oil_crude(self, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, Any]:
        """Lấy giá dầu thô."""
        return self._fetch_commodity("CL=F", start, end)

    @agg_execution("SPL.ext")
    def gas_natural(self, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, Any]:
        """Lấy giá khí thiên nhiên."""
        return self._fetch_commodity("NG=F", start, end)

    @agg_execution("SPL.ext")
    def coke(self, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, Any]:
        """Lấy giá than cốc."""
        return self._fetch_commodity("ICEEUR:NCF1!", start, end)

    @agg_execution("SPL.ext")
    def steel_d10(self, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, Any]:
        """Lấy giá thép D10 tại Việt Nam."""
        return self._fetch_commodity("STEEL:D10:VN", start, end, columns=['close'])

    @agg_execution("SPL.ext")
    def iron_ore(self, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, Any]:
        """Lấy giá quặng sắt."""
        return self._fetch_commodity("COMEX:TIO1!", start, end)

    @agg_execution("SPL.ext")
    def steel_hrc(self, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, Any]:
        """Lấy giá thép HRC."""
        return self._fetch_commodity("COMEX:HRC1!", start, end)

    @agg_execution("SPL.ext")
    def fertilizer_ure(self, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, Any]:
        """Lấy giá phân ure."""
        return self._fetch_commodity("CBOT:UME1!", start, end)

    @agg_execution("SPL.ext")
    def soybean(self, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, Any]:
        """Lấy giá đậu tương."""
        return self._fetch_commodity("ZM=F", start, end)

    @agg_execution("SPL.ext")
    def corn(self, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, Any]:
        """Lấy giá ngô (bắp)."""
        return self._fetch_commodity("ZC=F", start, end)

    @agg_execution("SPL.ext")
    def sugar(self, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, Any]:
        """Lấy giá đường."""
        return self._fetch_commodity("SB=F", start, end)

    @agg_execution("SPL.ext")
    def pork_north_vn(self, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, Any]:
        """Lấy giá heo hơi miền Bắc Việt Nam."""
        return self._fetch_commodity("PIG:NORTH:VN", start, end, columns=['close'])

    @agg_execution("SPL.ext")
    def pork_china(self, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, Any]:
        """Lấy giá heo hơi Trung Quốc."""
        return self._fetch_commodity("PIG:CHINA", start, end, columns=['close'])
