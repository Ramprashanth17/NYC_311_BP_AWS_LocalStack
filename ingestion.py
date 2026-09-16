"""
Ingestion script for the project. This script downloads the dataset (NYC311) from the specified URL, extracts it, and saves it to the designated 
directory. It also handles any necessary preprocessing steps before saving the data for further analysis.
"""

import json

import requests
import os
import boto3
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test", 
    region_name="us-east-1"
)

nyc_311_endpoint = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

headers = {
    "X-App-Token": os.getenv("X-APP-TOKEN")
}

all_records = []

params = {
    "$where": "created_date > '2026-09-01T00:00:00' AND created_date <= '2026-10-01T00:00:00'",
    "$limit": 1000,
    "$offset": 0,
    "$order": "created_date ASC"
}
#### Conditional check: Whether data is already present in the S3 bucket. If yes, skip the extract and load process.
#### If not, proceed with the extract and load process.
while True:
    try:
        s3.head_object(Bucket="nyc311-bucket", Key="bronze/nyc_311_data.json")
        print("Data already exists in the S3 bucket. Skipping the extract and load process.")
        break  # Exit the loop if data is found
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            print("Data not found in the S3 bucket. Proceeding with the extract and load process.")
        else:
            print(f"An error occurred while checking for data in the S3 bucket: {e}")
            raise

while True:
    try:
        nyc_311_response = requests.get(nyc_311_endpoint, headers=headers, params=params, timeout=60)
        nyc_311_response.raise_for_status()
    
    except requests.exceptions.RequestException as e:
        print(f"Error occurred while downloading NYC 311 data: {e}")
        raise

    page_data = nyc_311_response.json()
    if not page_data:
        print(f" Finished extracting data. Total records downloaded: {len(all_records)}")
        break

    all_records.extend(page_data)
    print(f"Fetched offset {params['$offset']} — {len(page_data)} records this page, {len(all_records)} total so far")
    params["$offset"] += params["$limit"]
    
### Save the loaded data to the s3 bucket under bronze layer

s3.put_object(Bucket="nyc311-bucket", Key="bronze/nyc_311_data.json", Body=json.dumps(all_records))