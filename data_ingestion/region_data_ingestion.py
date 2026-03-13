import requests
import logging
from datetime import date, timedelta, datetime


from utils import get_api_key, get_adls_client, save_to_adls

API_KEY = get_api_key()
logger = logging.getLogger(__name__)

regions = ["ERCO", "MISO", "PJM", "CISO", "ISNE", "NYIS", "SWPP"]
# regions=["ERCO"]
facet_type = {
    'Demand':'D',
    'Demand Forecast':'DF',
    'Net Generation':'NG',
    'Total Interchange':'TI'
    }

# start_date = date(2026,3,1)
# end_date = date(2026,3,1)


def fetch_regional_usage_data(start_date, end_date):
    current_date = start_date
    
    while current_date<=end_date:
        start=f"{current_date}T00"
        end=f"{current_date}T23"

        for region in regions:
            try:
                json_data=[]
                for key,value in facet_type.items():
                    url = f"https://api.eia.gov/v2/electricity/rto/region-data/data?api_key={API_KEY}&data[]=value&facets[respondent][]={region}&facets[type][]={value}&frequency=hourly&start={start}&end={end}&sort[0][column]=period&sort[0][direction]=asc&length=96"
                    response = requests.get(url)
                    # If there is no response for a particular geion job still continues
                    if not response.text.strip():
                        logger.warning(f"Empty response for {region} / {key}, skipping.")
                        continue

                    json_data.append({
                        'region':region,
                        'facet_type': value,
                        'facet_name':key,
                        'facet_data':response.json()
                    })

                    logger.info(f"Fetched the data for {key}")
                
                logger.info(f"Fetched data for {region}......................")

                payload={
                'region':region,
                'pulled_at': str(datetime.now()),
                'date':str(datetime.now().date()),
                'raw_data':json_data
                }
                #Save to ADLS
                adls_client=get_adls_client()
                save_to_adls(
                    adls_client=adls_client,
                    payload=payload,
                    container="bronze",
                    file_path=f"{region}/region-data/year={current_date.year}/month={str(current_date.month).zfill(2)}/{current_date}.json"
                )
            except Exception as e:
                logger.error(f"Ingestion failed for {region} on {current_date}: {e}",exc_info=True)
                continue
            
        current_date+=timedelta(days=1)

