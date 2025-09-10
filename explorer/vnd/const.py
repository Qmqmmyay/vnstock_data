"""Const module for vnd."""
_CHART_BASE = 'https://dchart-api.vndirect.com.vn'
_INSIGHT_BASE = "https://api-finfo.vndirect.com.vn/v4"

_INTERVAL_MAP = {'1m' : '1',
            '5m' : '5',
            '15m' : '15',
            '30m' : '30',
            '1H' : '60',
            '1D' : 'D',
            '1W' : 'W',
            '1M' : 'M'
            }
        

_OHLC_MAP = {
    't': 'time',
    'o': 'open',
    'h': 'high',
    'l': 'low',
    'c': 'close',
    'v': 'volume',
}        


# Pandas data type mapping for history price data
_OHLC_DTYPE = {
    "time": "datetime64[ns]",  # Convert timestamps to datetime
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "close": "float64",
    "volume": "int64",
}

_INDEX_MAPPING = {'VNINDEX': 'VNINDEX', 'HNXINDEX': 'HNX', 'UPCOMINDEX': 'UPCOM'}

_EXCHANGE_MAP = {
                "VNINDEX": "HOSE",
                "HNX": "HNX"
                }


_INTRADAY_MAP = {
    "tradingDate_Time": "time",
    "last": "price",
    "lastVol": "volume",
    "side": "match_type",
    "accumulatedVol": "accumulated_vol",
    "accumulatedVal": "accumulated_val",
    # "code": "symbol",
    # "floor": "exchange",
    # "open": "open",
    # "high": "high",
    # "low": "low",
    # "adLast": "ad_last",
    # "tradingDate": "date",
    # "time": "timestamp",
}

_INTRADAY_DTYPE = {
                    "time": "datetime64[ns]",
                    "price": "float64",
                    "volume": "int64",
                    "match_type": "str",
                    "accumulated_val": "int64",
                    "accumulated_vol": "int64",
                }

_TOP_STOCK_INDEX = {'VNINDEX': 'VNIndex',
                        'HNX': 'HNX',
                        'VN30': 'VN30'
                        }

_TOP_STOCK_COLS = {
                'code': 'symbol',
                'index': 'index',
                'lastPrice': 'last_price',
                'lastUpdated': 'last_updated',
                'priceChgCr1D': 'price_change_1d',
                'priceChgPctCr1D': 'price_change_pct_1d',
                'accumulatedVal': 'accumulated_value',
                'nmVolumeAvgCr20D': 'avg_volume_20d', #nm: net moving (average)
                'nmVolNmVolAvg20DPctCr': 'volume_spike_20d_pct', 
                'totalVolumeAvgCr20D': 'total_volume_avg_20d',
                'ptVolTotalVolAvg20DPctCr': 'deal_volume_spike_20d_pct', # pt mean put-through or private transaction = giao dịch thỏa thuận
                'ptVolAvg5DTotalVolAvg20DPctCr': 'deal_volume_spike_5d_20d_pct',
                'ptVolSumCr5D': 'deal_volume_sum_5d',
                'ptValAvgCr5D': 'deal_value_avg_5d',
                'ptVolAvgCr5D': 'deal_volume_avg_5d'
            }
