import azure.functions as func
import sys, os
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'data_ingestion'))

from datetime import date, timedelta
from fuel_type_ingestion import fetch_fuel_type_data



logger = logging.getLogger(__name__)

def main(mytimer: func.TimerRequest) -> None:
    yesterday = date.today() - timedelta(days=1)
    logger.info(f"Timer triggered. Fetching fuel type data for {yesterday}")

    try:
        fetch_fuel_type_data(start_date=yesterday, end_date=yesterday)
        logger.info(f"Completed fuel type data ingestion for {yesterday}")
    except Exception as e:
        logger.error(f"Ingestion failed for {yesterday}: {e}")
        raise
