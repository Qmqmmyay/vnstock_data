from datetime import datetime
from vnstock.core.utils.logger import get_logger

logger = get_logger(__name__)

# define a function to validate date string format in the format YYYY-mm-dd
def validate_date(date_str:str):
    """
    Validate date string format in the format YYYY-mm-dd.
    """
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        logger.error(f"Invalid date format: {date_str}. Please use the format YYYY-mm-dd.")
        return False
