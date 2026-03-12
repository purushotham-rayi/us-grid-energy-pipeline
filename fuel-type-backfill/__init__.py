import azure.functions as func
import sys, os
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'data_ingestion'))

from datetime import date
from fuel_type_ingestion import fetch_fuel_type_data

logger = logging.getLogger(__name__)

def main(req: func.HttpRequest) -> func.HttpResponse:
    start_str = req.params.get("start_date")
    end_str   = req.params.get("end_date")

    if not start_str or not end_str:
        return func.HttpResponse(
            "Please pass start_date and end_date as query params (YYYY-MM-DD)",
            status_code=400
        )
    
    try:
        start_date = date.fromisoformat(start_str)
        end_date   = date.fromisoformat(end_str)
    except ValueError:
        return func.HttpResponse(
            "Invalid date format. Use YYYY-MM-DD (e.g. 2024-01-15)",
            status_code=400
        )

    if start_date > end_date:
        return func.HttpResponse(
            "start_date cannot be after end_date",
            status_code=400
        )

    logger.info(f"Backfill triggered: {start_date} to {end_date}")

    try:
        fetch_fuel_type_data(start_date=start_date, end_date=end_date)
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        return func.HttpResponse(f"Ingestion failed: {str(e)}", status_code=500)
    
    logger.info(f"Backfill complete: {start_date} to {end_date}")

    return func.HttpResponse(f"Backfill complete for {start_date} to {end_date}", status_code=200)
