"""
Micro-batch ingestion of NYC 311 data into S3 (bronze layer).
Handles pagination, crash-resume, and idempotent re-runs.
See README.md for full design rationale.
"""

import json
import requests
import os
import boto3
import time
from datetime import timedelta, datetime
from dotenv import load_dotenv

load_dotenv() # Load environment variables from a .env file, which is useful for storing sensitive information like API tokens.


### Setting up the S3 client to interact with the S3 bucket. We are using LocalStack for local testing, and we are using the test credentials provided by LocalStack. The endpoint_url is set to the LocalStack S3 endpoint, and the region_name is set to us-east-1.
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test", 
    region_name="us-east-1"
)

current_date = datetime.now().date()
target_date = current_date - timedelta(days=1)  # Targeting the previous day's data for ingestion. 


year = target_date.strftime("%Y")
month = target_date.strftime("%m")
day = target_date.strftime("%d")
target_start_date = target_date.strftime("%Y-%m-%dT00:00:00") # Setting the start of the target date for filtering records.
target_end_date = target_date.strftime("%Y-%m-%dT23:59:59") # Setting the end of the target date for filtering records.

"""
API Settings and Details:

The data is from NYC 311 Service Requests API, which provides information about various service requests made by the public in New York City.
It has a limit of 1000 records per request, and we can use the $limit and $offset parameters to paginate through the data. 
It is recommended to have an App Token to avoid rate limiting and to access more records. The API supports filtering based on date, which we will use to get the data for the target date.
We can use SoQL-like queries to filter the data based on the created_date field, which indicates when the service request was created.
If you would directly take the endpoint URL from the site, it will make you download the entire dataset, which is not efficient. Instead, we will use the $where parameter to filter the data based on the created_date field, and use pagination to download the data in smaller chunks.
"""

nyc_311_endpoint = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

headers = {
    "X-App-Token": os.getenv("X-APP-TOKEN")  # Token to go beyond the 1000 records limit, and avoid rate limiting. It is stored in the .env file for security reasons.      
}


## Setting some variables for limit, offset, and page number for pagination. We will use these variables to download the data in smaller chunks, and to keep track of the progress of the ingestion process.

limit = 2000
offset = 0
page_number = 0
bucket = "nyc311-bucket"
s3_prefix = f"bronze/nyc311/{year}-{month}-{day}/"

### Variables for retries
max_retries = 5
wait_interval_seconds = 10

def is_day_complete(s3_prefix: str) -> bool:
    """ Returns True is _SUCCESS file available in the day's partition marking ingestion as success. 
    We can exit the execution as the data is already available.
    """
    try:
        s3.head_object(Bucket=bucket, Key=f"{s3_prefix}_SUCCESS")
        return True
    except s3.exceptions.ClientError  as e:
        if e.response["Error"]["Code"] in ("404", 'NoSuchKey'):
            print(f"Data for {s3_prefix} is not present. Proceeding with the EL logic")
            return False
        else:
            print(f"An error occured while checking the s3 bucket, due to: {e}")
            raise # Fail Loudly in case of other errors

def get_resume_point(s3_prefix: str, limit: int) -> tuple[int, int]:
    """
    Scans existing page files under this prefix and returns (page_number, offset)
    to resume from. Empty prefix -> (0, 0), i.e. start fresh.
    """
    s3_response = s3.list_objects_v2(Bucket = bucket, Prefix = s3_prefix)
    existing_pages = []
    if 'Contents' in s3_response:
        for obj in s3_response.get('Contents',[]):
            key = obj["Key"]
            if key.startswith(f"{s3_prefix}page_") and key.endswith(".json"):
                page_num = int(key.split("page_")[1].replace(".json", ""))
                existing_pages.append(page_num)

    if not existing_pages:
        return 0, 0

    last_page = max(existing_pages)
    resume_page = last_page + 1
    return resume_page, resume_page * limit

def fetch_page(offset: int, limit: int) -> list:
    """One paginated API call. Returns parsed JSON list (empty list = no data).
    Added exponential backoff for addressing network failures, etc."""
    
    params = {
        "$limit":limit,
        "$offset":offset,
        "$order": "created_date ASC, unique_key ASC",
        "$where": f"created_date > '{target_start_date}' and created_date <= '{target_end_date}'"
    }
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(nyc_311_endpoint, headers=headers, params=params, timeout=60)
            response.raise_for_status
            return response.json()
        # In case of timeout exception
        except requests.exceptions.Timeout:
            print(f"[Attempt {attempt}/{max_retries}] Timeout-retrying.")
        # In case of Connection error
        except requests.exceptions.ConnectionError:
            print(f"[Attempt {attempt}/{max_retries}] Connection dropped -retrying.")
        # In case of other exceptions, such as 400, 404, 405
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code
            if status == 429:
                print(f"[Attempt {attempt}/{max_retries}] Rate limited (429), retrying.")
            elif status in (400, 404, 405):
                print(f"Non-retryable error ({status}). Not retrying")
                raise
            else:
                print(f"Unexpected HTTP error ({status}). Not retrying")
                raise

        # Exponential-backoff logic
        if attempt < max_retries:
            wait = wait_interval_seconds * ( 2 ** (attempt - 1))
            print(f"Waiting {wait}s before retry....")
            time.sleep(wait)

    raise RuntimeError(f"fetch_page failed after {max_retries} attempts at offset {offset}")


    # response = requests.get(nyc_311_endpoint, params=params, headers=headers, timeout=60)
    # response.raise_for_status()
    # return response.json()

def write_page(s3_prefix: str, page_number:int, page_data:list) -> None:
    """Writes one page of data as newline-delimited JSON (JSONL) -streamable, not a single giant array"""
    key = f"{s3_prefix}page_{page_number:04d}.json"
    body = "".join(json.dumps(record) + "\n" for record in page_data)
    s3.put_object(Bucket=bucket, Key=key, Body=body)
    print(f"Uploaded {len(page_data)} records to {key}")

def mark_complete(s3_prefix:str) -> None:
    """Written only after every page for the day is ingested, and succeeded, a metafile to prove that ingestion was successful"""
    s3.put_object(Bucket=bucket, Key=f"{s3_prefix}_SUCCESS", Body=b"")

def load_recent_data() -> None:
    if is_day_complete(s3_prefix):
        print(f"{year}-{month}-{day} already completed. Skipping... ")
        return

    page_number, offset = get_resume_point(s3_prefix, limit)
    if page_number > 0:
        print(f"Resuming from page {page_number}, offset {offset}")
    else:
        print(f"No existing data for {year}-{month}-{day}. Starting afresh.")

    while True:
        page_data = fetch_page(offset, limit)
        if not page_data:
            mark_complete(s3_prefix)
            print(f"Finished. {year}-{month}-{day}. Marked complete")
            break
        write_page(s3_prefix, page_number, page_data)
        offset += limit
        page_number += 1

if __name__ == "__main__":
    load_recent_data()


        
