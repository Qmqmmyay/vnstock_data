import requests

def get_cookie (url, headers=None):
  """
  Trích xuất giá trị cookie từ phản hồi của lần gửi yêu cầu đầu tiên. Giá trị Cookie này được sử dụng cho các yêu cầu tiếp theo.

  Tham số
    url (str): URL cần gửi yêu cầu.
    headers (dict): Thông tin nhận dạng trong phần header căn bản.
  """
  if headers is None:
      headers = {
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9,vi-VN;q=0.8,vi;q=0.7',
                'Connection': 'keep-alive',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0'
                }
  response = requests.get(url, headers=headers, verify=True)
  # Check if the request was successful (status code 200)
  if response.status_code == 200:
      # Extract the 'Set-Cookie' header from the response
      set_cookie_header = response.headers.get('Set-Cookie')
      # If there's a 'Set-Cookie' header, extract the cookie value
      if set_cookie_header:
          # Split the header to get the cookie value
          cookie_value = set_cookie_header#.split(';')[0]
          # print("Extracted Cookie Value Successfully!")
          return cookie_value
      else:
          print("No 'Set-Cookie' header found.")
  else:
      print(f"Error: {response.status_code}")