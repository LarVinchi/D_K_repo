import pandas as pd
from sqlalchemy import create_engine
import os

def run_etl():
    # K8s will inject this environment variable securely
    DB_URL = os.getenv("DATABASE_URL")
    if not DB_URL:
        raise ValueError("DATABASE_URL environment variable is missing!")
    
    print("Starting K8s ETL Process...")
    
    # 1. Pull Data (Using Jan 2024 as an example)
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    print(f"Downloading data from {url}")
    df = pd.read_parquet(url)
    
    # 2. Transformation: Join with Location CSV
    zones = pd.read_csv('/app/scripts/taxi_zone_lookup.csv') 
    df = df.merge(zones, left_on='PULocationID', right_on='LocationID', how='left')
    
    # 3. Load to Postgres (Limiting to 500 rows so our 2-minute runs finish quickly)
    engine = create_engine(DB_URL)
    df.head(500).to_sql('trips', engine, if_exists='append', index=False)
    print("Successfully loaded transformed data to Postgres on K8s!")

if __name__ == "__main__":
    run_etl()