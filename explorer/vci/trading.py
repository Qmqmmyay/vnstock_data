from typing import List, Dict, Optional
from datetime import datetime
import pandas as pd
import requests
import json
from vnstock.core.utils.logger import get_logger
from vnstock.core.utils.user_agent import get_headers
from vnstock.core.utils.parser import get_asset_type, camel_to_snake, flatten_data
from vnstock.explorer.vci.const import _GRAPHQL_URL, _TRADING_URL
from vnstock_data.core.utils.validation import validate_date

logger = get_logger(__name__)

class Trading:
    """
    Truy xuất dữ liệu giao dịch của mã chứng khoán từ nguồn dữ liệu VCI.
    """
    def __init__(self, symbol:Optional[str], random_agent=False, show_log:Optional[bool]=False):
        self.symbol = symbol.upper()
        self.asset_type = get_asset_type(self.symbol)
        # self.base_url = _BASE_URL
        self.headers = get_headers(data_source='VCI', random_agent=random_agent)
        
        if not show_log:
            logger.setLevel('CRITICAL')
        
        self.show_log = show_log

    def trading_stats (self, start:Optional[str]='2000-01-01', end:Optional[str]='2099-12-31', limit:Optional[int]=5000, offset:Optional[int]=0, dropna:Optional[bool]=True, to_df:Optional[bool]=True):
        """
        Truy xuất thống kê lịch sử giao dịch của mã chứng khoán được chọn.

        Tham số:
            - limit (tùy chọn): Số lượng bản ghi trả về. Mặc định là 100.
            - offset (tùy chọn): Số lượng bản ghi bỏ qua. Mặc định là 0.
            - from_date (tùy chọn): Ngày bắt đầu lấy dữ liệu theo định dạng YYYY-mm-dd. Mặc định là '2000-01-01'.
            - to_date (tùy chọn): Ngày kết thúc lấy dữ liệu theo định dạng YYYY-mm-dd. Mặc định là '2099-12-31'.
        """
        # loop through start and end date to validate date format and show log if invalid
        if not validate_date(start) or not validate_date(end):
            logger.error(f"Invalid date format. Please use the format YYYY-mm-dd.")
            return None

        payload = "{\"query\":\"query Query($ticker: String!, $offset: Int!, $offsetInsider: Int!, $limit: Int!, $fromDate: String!, $toDate: String!) {\\n  TickerPriceHistory(\\n    ticker: $ticker\\n    offset: $offset\\n    limit: $limit\\n    fromDate: $fromDate\\n    toDate: $toDate\\n  ) {\\n    history {\\n      tradingDate\\n      stockType\\n      ceilingPrice\\n      floorPrice\\n      referencePrice\\n      openPrice\\n      closePrice\\n      matchPrice\\n      priceChange\\n      percentPriceChange\\n      highestPrice\\n      lowestPrice\\n      averagePrice\\n      totalMatchVolume\\n      totalMatchValue\\n      totalDealVolume\\n      totalDealValue\\n      totalVolume\\n      totalValue\\n      foreignNetTradingVolume\\n      foreignNetTradingValue\\n      foreignBuyValueMatched\\n      foreignBuyVolumeMatched\\n      foreignSellValueMatched\\n      foreignSellVolumeMatched\\n      foreignBuyValueDeal\\n      foreignBuyVolumeDeal\\n      foreignSellValueDeal\\n      foreignSellVolumeDeal\\n      foreignBuyValueTotal\\n      foreignBuyVolumeTotal\\n      foreignSellValueTotal\\n      foreignSellVolumeTotal\\n      foreignTotalRoom\\n      foreignCurrentRoom\\n      foreignHoldingVolume\\n      suspension\\n      delist\\n      haltResumeFlag\\n      split\\n      benefit\\n      meeting\\n      notice\\n      totalTrade\\n      totalBuyTrade\\n      totalBuyTradeVolume\\n      totalSellTrade\\n      totalSellTradeVolume\\n      referencePriceAdjusted\\n      openPriceAdjusted\\n      closePriceAdjusted\\n      priceChangeAdjusted\\n      percentPriceChangeAdjusted\\n      highestPriceAdjusted\\n      lowestPriceAdjusted\\n      unMatchedBuyTradeVolume\\n      unMatchedSellTradeVolume\\n      difVolumeBuySell\\n      averageVolumeBuyOrder\\n      averageVolumeSellOrder\\n      __typename\\n    }\\n    totalRecords\\n    __typename\\n  }\\n  OrganizationDeals(\\n    ticker: $ticker\\n    offset: $offsetInsider\\n    limit: $limit\\n    fromDate: $fromDate\\n    toDate: $toDate\\n  ) {\\n    history {\\n      id\\n      organCode\\n      tradeTypeCode\\n      dealTypeCode\\n      actionTypeCode\\n      tradeStatusCode\\n      traderOrganCode\\n      shareBeforeTrade\\n      ownershipBeforeTrade\\n      shareRegister\\n      shareAcquire\\n      shareAfterTrade\\n      ownershipAfterTrade\\n      startDate\\n      endDate\\n      sourceUrl\\n      publicDate\\n      ticker\\n      traderPersonId\\n      traderName\\n      en_TraderName\\n      positionShortName\\n      en_PositionShortName\\n      positionName\\n      en_PositionName\\n      __typename\\n    }\\n    totalRecords\\n    __typename\\n  }\\n}\\n\",\"variables\":{\"ticker\":\"VCI\",\"limit\":15,\"offset\":0,\"offsetInsider\":0,\"fromDate\":\"2000-01-01\",\"toDate\":\"2100-01-01\"}}"
        payload_json = json.loads(payload)
        payload_json['variables']['limit'] = limit
        payload_json['variables']['ticker'] = self.symbol
        payload_json['variables']['offset'] = offset
        payload_json['variables']['fromDate'] = start
        payload_json['variables']['toDate'] = end

        # convert payload_json to string
        payload_json = json.dumps(payload_json)

        if self.show_log:
            logger.info(f"Querying data from {url}, payload: {payload_json}")

        response = requests.post(_GRAPHQL_URL, data=payload_json, headers=self.headers)
        if response.status_code != 200:
            logger.error(f"Error {response.status_code}: {response.text}")
            return None
        data = response.json()

        if self.show_log:
            logger.info(f"Response data: {data}")

        data = data['data']['TickerPriceHistory']['history']
        df = pd.DataFrame(data)

        try:
            # drop columns: '__typename'
            df = df.drop(columns=['__typename', 'stockType'])
            # convert column names to snake case
            df.columns = [camel_to_snake(col) for col in df.columns]
            # rename columns: un_matched_buy_trade_volume, un_matched_sell_trade_volume
            df = df.rename(columns={'un_matched_buy_trade_volume':'unmatched_buy_trade_volume', 
                                    'un_matched_sell_trade_volume':'unmatched_sell_trade_volume',
                                    'trading_date': 'time',
                                    # 'open_price': 'open',
                                    # 'close_price': 'close',
                                    # 'highest_price': 'high',
                                    # 'lowest_price': 'low',
                                    })

            if dropna:
                df = df.dropna(how='all')
                # drop rows that all values are zero
                # df = df[(df != 0).any(axis=1)]  # drop rows where all values are 0
                
                # # Handle columns where all values are zero
                zero_columns = [col for col in df.columns if df[col].sum() == 0]
                df = df.drop(columns=zero_columns)

            # convert trading_date to datetime from unix timestamp
            df['time'] = pd.to_datetime(df['time'], unit='ms')
            # sort by time column
            df = df.sort_values(by='time').reset_index(drop=True)

        except Exception as e:
            logger.error(f'Error details: {e}')

        # Add metadata attributes
        df.attrs['name'] = self.symbol
        df.attrs['category'] = self.asset_type
        df.attrs['source'] = 'VCI'

        if to_df:
            return df
        else:
            return df.to_dict(orient='records')
        
    def side_stats (self, dropna:Optional[bool]=True, to_df:Optional[bool]=True):
        """
        Truy xuất dữ liệu cung cầu của mã chứng khoán được chọn.
        """

        url = f'{_TRADING_URL}price/symbols/getList'
        payload = {"symbols": ["VN30F2406"]}
        # payload_json = json.loads(payload)
        payload['symbols'] = [self.symbol]

        if self.show_log:
            logger.info(f"Querying data from {url}, payload: {payload}")

        response = requests.post(url, headers=self.headers, data=json.dumps(payload))
        
        if response.status_code != 200:
            logger.error(f"Error {response.status_code}: {response.text}")
            return None
        
        data = response.json()[0]

        if self.show_log:
            logger.info(f"Response data: {data}")

        listing_info = data['listingInfo']
        bid_ask = data['bidAsk']
        match_info = data['matchPrice']

        return listing_info, bid_ask, match_info
