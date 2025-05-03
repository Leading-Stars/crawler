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

COUNTRY = os.getenv("COUNTRY", "usa_blockdata")
MACHINE_ID = os.getenv("MACHINE_ID", None)
TASK_SPREADER_API_URL = os.getenv("TASK_SPREADER_API_URL")
FETCHER_MIN_CONCURRENCY = os.getenv("FETCHER_MIN_CONCURRENCY", 5)

if not TASK_SPREADER_API_URL:
  raise Exception("TASK_SPREADER_API_URL is not set")

queries = {"country": COUNTRY, "machine_id": MACHINE_ID, "queries": []}

crawler = PlaywrightCrawler(
  request_handler_timeout=timedelta(minutes=10),
  max_request_retries=1,
  concurrency_settings=ConcurrencySettings(
    min_concurrency=int(FETCHER_MIN_CONCURRENCY),
  ),
)

async def main():
    global queries
    print("Fetcher started")
    
    #check_db_schema()
    
    try:
        while True:
            queries_urls = get_queries_to_process()
            if queries_urls:
                # Preserve metadata when processing
                original_queries = {q['url']: q for q in queries['queries']}
                await crawler.run(queries_urls)
                
                # Merge results back with original metadata
                for query in queries['queries']:
                    if query['url'] in original_queries:
                        query.update({
                            'id': original_queries[query['url']].get('id'),
                            'metadata': original_queries[query['url']].get('metadata', {})
                        })
                
                push_results_to_db()
    except Exception as e:
        error_msg = str(e)

        if error_msg == "READ_TIMEOUT":
            print("Script stopped because it failed to fetch queries after multiple retries (READ_TIMEOUT).")

        elif error_msg == "CONNECT_TIMEOUT":
            print("Script stopped because it failed to establish a connection after multiple retries (CONNECT_TIMEOUT).")

        elif error_msg.startswith("REQUEST_FAILED"):
            print(f"Script stopped due to repeated request errors: {error_msg}")

        elif error_msg == "CONNECTION_ERROR":
            print("Script stopped due to repeated connection errors while pushing results to DB.")

        else:
            print(f"Script stopped due to an unexpected error: {error_msg}")
            raise

@crawler.router.default_handler
async def request_handler(context: PlaywrightCrawlingContext) -> None:
    status = Status.FAILED.value
    context.log.info(f'Processing Url {context.page.url} ...')

    try:
        await google_map_consent_check(context)
        links = await scroll_to_bottom_results_section(context)
        links = links if links else []
        status = Status.PROCESSED.value
    except Exception as e:
        context.log.info(f"Error processing page: {e}")
        status = Status.FAILED.value
    finally:
        update_query_status(context.page.url, status)
    
    if status == Status.PROCESSED.value:
        save_query_results(context.page.url, links)

async def scroll_to_bottom_results_section(context: PlaywrightCrawlingContext):
    selector = '[role="feed"]'

    try:
        scrollable_section = await context.page.query_selector(selector)
        
        while True:
            await scrollable_section.evaluate('''(element) => {
                const scrollHeight = element.scrollHeight;
                const scrollStep = scrollHeight * 1;
                element.scrollBy(0, scrollStep);
                return element.scrollTop;
            }''')

            end_signal = await scrollable_section.query_selector("span.HlvSq")
            if end_signal:
                button = await scrollable_section.evaluate('() => document.querySelector("span.HlvSq").innerText')
                if "reached the end" in button:
                    links = await scrollable_section.evaluate('''() => {
                        return Array.from(document.querySelectorAll("a")).map(a => a.href);
                    }''')
                    links = [element for element in links if element.startswith("https://www.google.com/maps/place")]
                    return links
    
    except Exception as e:
        context.log.info(f"Error finding scrollable section: {e}")
        raise e

def get_queries_to_process():
    global queries

    queries_urls_to_process = get_queries_to_process_from_cache()
    if not queries_urls_to_process:
        queries_urls_to_process = get_queries_to_process_from_db()

    return queries_urls_to_process

def get_queries_to_process_from_db():
    global queries

    retries = [10, 20, 30]  # wait times in seconds
    url = f"{TASK_SPREADER_API_URL}/queries?country={COUNTRY}&machine_id={MACHINE_ID}"

    for i, delay in enumerate(retries):
        try:
            response = requests.get(url, timeout=60)
            data = response.json()
            raw_queries = data['queries']
            queries['country'] = data.get('country')
            queries['queries'] = [{
                "url": q["query_url"],
                "id": q["id"],
                "metadata": {
                    "geoid": q.get("geoid"),
                    "industry": q.get("industry"),
                    "latitude": q.get("latitude"),
                    "longitude": q.get("longitude"),
                    "zoom_level": q.get("zoom_level"),
                    "created_at": q.get("created_at")
                },
                "status": Status.PENDING.value
            } for q in raw_queries]
            
            cache_queries()
            print(f"Received {len(data['queries'])} queries from database")
            return [q["url"] for q in queries["queries"]]
        
        except requests.ReadTimeout:
            if i == len(retries) - 1:
                raise Exception("READ_TIMEOUT")
            print(f"[ReadTimeout] Retrying in {delay} seconds...")
            time.sleep(delay)

        except requests.ConnectTimeout:
            if i == len(retries) - 1:
                raise Exception("CONNECT_TIMEOUT")
            print(f"[ConnectTimeout] Retrying in {delay} seconds...")
            time.sleep(delay)

        except requests.exceptions.RequestException as e:
            if i == len(retries) - 1:
                raise Exception(f"REQUEST_FAILED: {str(e)}")
            print(f"[RequestException] {e}. Retrying in {delay} seconds...")
            time.sleep(delay)

def get_queries_to_process_from_cache():
    global queries
    
    try:
        with open('queries_cache.json', 'r') as f:
            content = f.read().strip()
            if not content:
                return None  
            cached_queries = json.loads(content)
    except FileNotFoundError:
        return None

    if not cached_queries or len(cached_queries['queries']) == 0:
        return None
    
    if len(cached_queries['queries']) > 0:
        pending_queries = [
            query for query in cached_queries['queries'] 
            if query['status'] == Status.PENDING.value
        ]

        if len(pending_queries) == 0:
            print(f"All queries {len(cached_queries['queries'])} are processed, pushing results to database")
            queries['queries'] = cached_queries['queries']
            push_results_to_db()
            return None
        else:
            print(f"Found {len(pending_queries)} pending queries in cache")
            queries['queries'] = cached_queries['queries']
            return [query['url'] for query in pending_queries]

def get_query_from_queries(query_url):
    global queries
    for query in queries['queries']:
        if query_url == query['url']:
            return query
    raise Exception(f"Query {query_url} not found in queries")

def update_query_status(query_url, status):
    global queries
    query = get_query_from_queries(query_url)
    query['status'] = status
    cache_queries()

def save_query_results(query_url, links):
    query = get_query_from_queries(query_url)
    query['results'] = links
    cache_queries()

def count_queries_results():
    global queries
    total_results = 0
    for query in queries['queries']:
        if query.get('status', None) == Status.PROCESSED.value:
            total_results += len(query['results'])

    return total_results

def push_results_to_db():
    global queries

    num_queries_results = count_queries_results()
    print(f"Pushing {num_queries_results} results to database...")

    retries = [10, 20, 30]
    url = f"{TASK_SPREADER_API_URL}/queries/results"

    for i, delay in enumerate(retries):
        try:
            transformed_data = {
                'results': [transform_query_for_db(query) for query in queries['queries'] if query['status'] == Status.PROCESSED.value],
                'country_code': queries.get('country'),
                'machine_id': queries.get('machine_id')
            }

            response = requests.post(
                url,
                json=transformed_data,
                timeout=120
            )
            if response.status_code != 200:
                raise Exception("Failed to push results to database.")
            else:
                print("Results pushed to database successfully")
                clear_queries()
                return

        except requests.ConnectionError:
            if i == len(retries) - 1:
                raise Exception("CONNECTION_ERROR")
            print(f"Connection error while pushing results. Retrying in {delay} seconds...")
            time.sleep(delay)

def cache_queries():
    global queries
    with open('queries_cache.json', 'w') as f:
        json.dump(queries, f, indent=4)

def clear_queries():
    global queries
    queries['queries'] = []
    cache_queries()

def check_db_schema():
    required_fields = [
        'email', 'social_links', 'star_rating', 'plus_code',
        'booking_link', 'check_in_info', 'coordinates'
    ]
    
    try:
        response = requests.get(f"{TASK_SPREADER_API_URL}/schema-check")
        existing_fields = response.json().get('fields', [])

        
        missing_fields = [f for f in required_fields if f not in existing_fields]
        if missing_fields:
            raise Exception(f"Database missing required fields: {missing_fields}")
            
    except Exception as e:
        print(f"Schema validation error: {str(e)}")
        raise


def transform_query_for_db(query):
    metadata = query.get('metadata', {})
    results = query.get('results', [])

    def safe_get(key, default=None):
        return metadata.get(key, default)

    return {
        'url': query.get('url'),
        'source_url': query.get('url'),
        'title': safe_get('title', ''),
        'headline': safe_get('headline', ''),
        'category': safe_get('industry', ''),
        'description': safe_get('description', ''),
        'address': safe_get('address', ''),
        'plus_code': safe_get('plus_code', ''),
        'phone': safe_get('phone', ''),
        'website': safe_get('website', ''),
        'booking_link': safe_get('booking_link', ''),
        'check_in_info': safe_get('check_in_info', ''),
        'email': safe_get('email', ''),
        'social_links': safe_get('social_links', []),
        'star_rating': safe_get('star_rating', 0.0),
        'review_count': safe_get('review_count', 0),
        'price_level': safe_get('price_level', ''),
        'open_hours': safe_get('open_hours', {}),
        'current_status': safe_get('current_status', ''),
        'coordinates': f"({safe_get('latitude', 0.0)},{safe_get('longitude', 0.0)})",
        'latitude': safe_get('latitude', 0.0),
        'longitude': safe_get('longitude', 0.0),
        'photos': safe_get('photos', []),
        'cover_photo': safe_get('cover_photo', ''),
        'attributes': safe_get('attributes', {}),
        'services': safe_get('services', []),
        'review_summary': safe_get('review_summary', {}),
        'last_review_date': safe_get('last_review_date'),
        'scraped_at': safe_get('scraped_at', time.strftime('%Y-%m-%dT%H:%M:%SZ')),
        'processed': False,
        'processing_attempts': 0,
        'country_code': safe_get('country_code', (queries.get('country') or 'XX')[:2].upper()),
        'machine_id': queries.get('machine_id'),
        'raw_data': safe_get('raw_data', {})
    }

if __name__ == "__main__":
    asyncio.run(main())
