from fake_useragent import UserAgent
from vnstock.core.utils.user_agent import HEADERS_MAPPING_SOURCE, DEFAULT_HEADERS

VDS_HEADERS = {
  'Accept': 'application/json, text/javascript, */*; q=0.01',
  'Accept-Language': 'en-US,en;q=0.9',
  'Connection': 'keep-alive',
  'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
  'Sec-Fetch-Dest': 'empty',
  'Sec-Fetch-Mode': 'cors',
  'Sec-Fetch-Site': 'same-origin',
  'X-Requested-With': 'XMLHttpRequest',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"'
}

# Add headers mapping for VDSC
HEADERS_MAPPING_SOURCE['VND'] = {'Referer': 'https://mkw.vndirect.com.vn', 'Origin': 'https://mkw.vndirect.com.vn'}
HEADERS_MAPPING_SOURCE['VDS'] = {'Referer': 'https://livedragon.vdsc.com.vn/general/intradayBoard.rv', 'Origin': 'https://livedragon.vdsc.com.vn'}
HEADERS_MAPPING_SOURCE['FIALDA'] = {'Referer': 'https://fwt.fialda.com/', 'Origin': 'https://fwt.fialda.com'}
HEADERS_MAPPING_SOURCE['FIINTRADE'] = {'Referer': 'https://fiintrade.vn', 'Origin': 'https://fiintrade.vn/'}
HEADERS_MAPPING_SOURCE['FIDT'] = {'Referer': 'https://portal.fidt.vn', 'Origin': 'https://portal.fidt.vn/'}
HEADERS_MAPPING_SOURCE['CAFEF'] = {'Referer': 'https://s.cafef.vn/lich-su-giao-dich-vnindex-3.chn', 'Origin': 'https://s.cafef.vn/lich-su-giao-dich-vnindex-3.chn'}

def get_headers(data_source='SSI', random_agent=True):
    """
    Tạo headers cho request theo nguồn dữ liệu.
    """
    data_source = data_source.upper()
    ua = UserAgent(fallback='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36')
    if data_source == 'VDS':
        headers = VDS_HEADERS
    else:
        headers = DEFAULT_HEADERS.copy()
        
    if random_agent:
        headers['User-Agent'] = ua.random
    else:
        headers['User-Agent'] = ua.chrome
    headers.update(HEADERS_MAPPING_SOURCE.get(data_source, {}))
    return headers