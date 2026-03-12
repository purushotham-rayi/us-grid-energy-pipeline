import requests
import logging
from datetime import date, timedelta, datetime


from utils import get_api_key, get_adls_client, save_to_adls

API_KEY = get_api_key()
logger = logging.getLogger(__name__)

regions = ["ERCO", "MISO", "PJM", "CISO", "ISNE", "NYIS", "SWPP"]

def fetch_fuel_type_data(start_date, end_date):
    current_date = start_date
    try:
        while current_date<=end_date:
            start=f"{current_date}T00"
            end=f"{current_date}T23"

            for region in regions:
                
                url=f"https://api.eia.gov/v2/electricity/rto/fuel-type-data/data?api_key={API_KEY}&data[]=value&facets[respondent][]={region}&frequency=hourly&start={start}&end={end}&sort[0][column]=period&sort[0][direction]=asc&length=240"
                response=requests.get(url)
                json_data=response.json()

                logger.info(f"Repspnse Code: {response.status_code}")
                logger.info(f"Fetched data for {region}......................")

                payload={
                'region':region,
                'pulled_at': str(datetime.now()),
                'date':str(datetime.now().date()),
                'raw_data':json_data
                }

                adls_client=get_adls_client()

                save_to_adls(
                    adls_client=adls_client,
                    payload=payload,
                    container="bronze",
                    file_path=f"{region}/fuel_type_data/year={current_date.year}/month={str(current_date.month).zfill(2)}/{current_date}.json"
                )
            current_date+=timedelta(days=1)

    except Exception as e:
        logger.error(f"Ingestion failed: {e}",exc_info=True)
        raise