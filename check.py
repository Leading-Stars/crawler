import os
import asyncio
import time
import json

from datetime import timedelta

import requests
from crawlee import ConcurrencySettings
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from dotenv import load_dotenv
from utils.enums import Status
from utils.google_maps_utils import google_map_consent_check

load_dotenv('.env')
LOCAL_STORAGE = os.getenv("LOCAL_STORAGE_MODE", "false").lower() == "true"

COUNTRY = os.getenv("COUNTRY", "usa")
MACHINE_ID = os.getenv("MACHINE_ID", None)
TASK_SPREADER_API_URL = os.getenv("TASK_SPREADER_API_URL")
FETCHER_MIN_CONCURRENCY = os.getenv("FETCHER_MIN_CONCURRENCY", 5)

if not TASK_SPREADER_API_URL:
  raise Exception("TASK_SPREADER_API_URL is not set")

queries = {"country": COUNTRY, "machine_id": MACHINE_ID, "queries": []}

# def check_db_schema():
#     required_fields = [
#         'email', 'social_links', 'star_rating', 'plus_code',
#         'booking_link', 'check_in_info', 'coordinates'
#     ]
    
#     try:
#         response = requests.get(f"{TASK_SPREADER_API_URL}/queries/schema")
#         existing_fields = response.json().get('fields', [])
#         print(response.json())
        
        
#         missing_fields = [f for f in required_fields if f not in existing_fields]
#         if missing_fields:
#             raise Exception(f"Database missing required fields: {missing_fields}")
            
#     except Exception as e:
#         print(f"Schema validation error: {str(e)}")
#         raise

def check_db_schema():
    required_fields = [
        'email', 'social_links', 'star_rating', 'plus_code',
        'booking_link', 'check_in_info', 'coordinates'
    ]
    
    try:
        # Fetch the schema details from the server
        response = requests.get(f"{TASK_SPREADER_API_URL}/queries/schema")
        schema_data = response.json()
        print(schema_data)

        # Extract the required fields for the 'laptopfifo' section
        existing_fields = schema_data.get('laptopfifo', {}).get('required_fields', {})

        # Check which required fields are missing from the schema
        missing_fields = [field for field in required_fields if field not in existing_fields]

        # If there are missing fields, raise an exception
        if missing_fields:
            raise Exception(f"Database missing required fields: {missing_fields}")
        
    except Exception as e:
        print(f"Schema validation error: {str(e)}")
        raise

check_db_schema()
