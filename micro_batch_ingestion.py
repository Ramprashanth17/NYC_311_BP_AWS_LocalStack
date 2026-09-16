"""Micro-batch Ingestion Script for NYC 311 Data:

The idea of this script is to implement pagination logic with date as partition key. However, instead of downloading the 
entire day's data which is prone to errors, and timeouts, we will implement a micro-batch ingestion approach. Meaning, we
will download a day's data in smaller chunks, say 1000 records at a time, and save it to the S3 bucket. 
This will help in avoiding timeouts and errors during the ingestion, and also make the ingestion process more efficient and
manageable. Aside from that, we can also implement an idempotent ingestion process, meaning that if the ingestion process
is interrupted or fails, we can resume the ingestion from the last successful micro-batch, instead of starting from scratch. 
This will help in avoiding data duplication.

"""

import json
import requests
import os
import boto3
from datetime import timedelta, datetime
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)

### Since this is technically a batch pipeline, we'll be ingesting data that is already present in the API
## before the current date.

current_date = datetime.now().date()
target_date = current_date - timedelta(days=1) #Targeting the previous day's data for ingestion.

#### Grabbing the year, month, and day from the target date to use as partition keys for the S3 bucket.
year = target_date.strftime("%Y")
month = target_date.strftime("%m")
day = target_date.strftime("%d")

target_start_date = target_date.strftime("%Y-%m-%dT00:00:00")
target_end_date = target_date.strftime("%Y-%m-%dT23:59:59")
s3_prefix = f"bronze/nyc311/{year}-{month}-{day}/"  # S3 prefix for the current date's data.


## print(s3_prefix) # Uncomment this line to see the S3 prefix for the current date.


nyc_311_endpoint = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

headers = {
    "X-App-Token": os.getenv("X-APP-TOKEN") ### Token to go beyond the 1000 records limit, and avoid rate limiting.
}



# paged_data = [] 

### Conditional check: Whether yesterday's data is already present in the S3 bucket. If yes, skip the extract and load process.
### If not, proceed with the extract and load process.


# objects_in_s3 = s3.list_objects_v2(Bucket="nyc311-bucket", Prefix=f"bronze/nyc311/{year}-{month}-{day}/page_{{page_number}}.json")

"""Function: Load Recent data, which by the update frequency of API is yesterday, we'll 
be loading just one day's data. Check if the data is present in the bucket,
if so skip the EL process, if not proceed with it"""

def load_recent_data():
    page_number = 0  # Initialize the page number for pagination.
    total_records_ingested = 0  # Initialize the total records ingested counter.
    limit = 2000  # Set the limit for the number of records to fetch per request.
    offset = 0  # Initialize the offset for pagination.

    # s3_response = s3.list_objects_v2(Bucket="nyc311-bucket", Prefix=f"bronze/nyc311/{year}-{month}-{day}/")
    # if any(f"bronze/nyc311/{year}-{month}-{day}/_SUCCESS" in obj['Key'] for obj in s3_response.get('Contents', [])):
    #     print(f"Data for {year}-{month}-{day} already exists in the S3 bucket. Skipping the extract and load process.")
    #     return
    is_completed = False
    success_key = f"{s3_prefix}_SUCCESS"
    try:
        s3.head_object(Bucket="nyc311-bucket", Key=success_key)
        is_completed = True
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] in ('404', 'NoSuchKey'):
            print(f"Data for {year}-{month}-{day} is incomplete. Proceeding with the extract and load process.")
        else:
            print(f"An error occurred while checking for data in the S3 bucket: {e}")
            raise
    if is_completed:
        print(f"Data for {year}-{month}-{day} already exists in the S3 bucket. Skipping the extract and load process.")
        return

    print(f"Data for {year}-{month}-{day} is partially filled. Proceeding with the extract and load process.")
    #print(f"Data for {year}-{month}-{day} not found in the S3 bucket. Proceeding with the extract and load process.")

    #### 
    # Implementing the mid-crash recovery logic. If ingestion is incomplete, this will check for the last 
    # ingested file, and resume from there, instead of starting from scratch.
    s3_response = s3.list_objects_v2(Bucket="nyc311-bucket", Prefix=s3_prefix)
    existing_pages = []
    if 'Contents' in s3_response:
        for obj in s3_response['Contents']:
            key = obj['Key']
            if "page_" in key and key.endswith('.json'):
                page_num = int(key.split("page_")[1].replace('.json', ''))
                existing_pages.append(page_num)

    if existing_pages:
        last_ingested_page = max(existing_pages)
        start_page = last_ingested_page + 1
        offset = start_page * limit
        page_number = start_page
        #total_records_ingested = offset  
        print(f"Resuming ingestion from page {page_number} with offset {offset}. Total records ingested so far: {total_records_ingested}")
    else:
        start_page = 0
        offset = 0
        page_number = start_page
        print("No existing pages found. Starting ingestion from the beginning.")

    ####
    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "created_date ASC, unique_key ASC",
            "$where": f"created_date > '{target_start_date}' AND created_date <= '{target_end_date}'"
        }
        try:
            nyc_311_response = requests.get(nyc_311_endpoint, headers=headers, params=params, timeout=60)
            nyc_311_response.raise_for_status()
            page_data = nyc_311_response.json()
        except Exception as e:
            print(f"Error occurred while downloading NYC 311 data: {e}")
            raise

        if not page_data:
            print(f"Finished extracting data. Total records downloaded: {total_records_ingested}")
            s3.put_object(Bucket="nyc311-bucket", Key=f"bronze/nyc311/{year}-{month}-{day}/_SUCCESS", Body=b"")
            break

        file_name = f"page_{page_number:04d}.json"
        s3_key = f"bronze/nyc311/{year}-{month}-{day}/{file_name}"

        try:
            ## for Json lines, we can use the following code to write each record as a separate line in the S3 object.
            s3.put_object(Bucket="nyc311-bucket", Key=s3_key, Body="".join(json.dumps(record) + "\n" for record in page_data))
            records_in_page = len(page_data)
            total_records_ingested += records_in_page
            print(f"Uploaded {records_in_page} records to S3 bucket at {s3_key}")
        except Exception as e:
            print(f"Error occurred while uploading data to {s3_key}: {e}")
            raise

        offset += limit
        page_number += 1

load_recent_data()






