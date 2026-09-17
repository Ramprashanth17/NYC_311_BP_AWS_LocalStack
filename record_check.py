import requests
import os
from dotenv import load_dotenv

load_dotenv()

nyc_311_endpoint = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

headers = {
    "X-App-Token": os.getenv("X-APP-TOKEN")  # Token to go beyond the 1000 records limit, and avoid rate limiting. It is stored in the .env file for security reasons.      
}
count_params = {
    "$select": "count(*)",
    "$where": "created_date > '2026-09-16T00:00:00' AND created_date <= '2026-09-16T23:59:59'"
}
r = requests.get(nyc_311_endpoint, headers=headers, params=count_params)
print(r.json())