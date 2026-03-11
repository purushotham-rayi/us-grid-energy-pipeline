import azure.functions as func
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'data_ingestion'))

from datetime import date, timedelta
from region_data_ingestion import fetch_regional_usage_data
from utils import get_logger

logger = get_logger(__name__)

def main(mytimer: func.TimerRequest) -> None:
    yesterday = date.today() - timedelta(days=1)
    logger.info(f"Timer triggered. Fetching region data for {yesterday}")

    try:
        fetch_regional_usage_data(start_date=yesterday, end_date=yesterday)
        logger.info(f"Completed region data ingestion for {yesterday}")
    except Exception as e:
        logger.error(f"Ingestion failed for {yesterday}: {e}")
        raise
