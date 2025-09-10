"""Models module for vnd."""
# Chứa mô hình validation cho nhập liệu
from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class TickerModel(BaseModel):
    symbol: str
    start: str
    end: Optional[str] = None
    interval: Optional[str] = "1D"
    # asset_type: str

class PaginationModel(BaseModel):
    page: int # Trang bắt đầu lấy dữ liệu
    size: int # Số kết quả trong 1 trang
    period: int # Số kỳ lấy báo cáo

class FinancialReportModel(BaseModel):
    type: str
    frequency: str

