import os
import asyncio
import time
import json
from datetime import timedelta, datetime, timezone
import requests
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from dotenv import load_dotenv
from utils.enums import Status
from utils.google_maps_utils import google_map_consent_check
import re
from decimal import Decimal
load_dotenv('.env')

LOCAL_STORAGE = os.getenv("LOCAL_STORAGE_MODE", "false").lower() == "true"
COUNTRY = os.getenv("COUNTRY", "usa_blockdata")
MACHINE_ID = os.getenv("MACHINE_ID", None)
TASK_SPREADER_API_URL = os.getenv("TASK_SPREADER_API_URL")

if not TASK_SPREADER_API_URL:
    raise Exception("TASK_SPREADER_API_URL is not set")

queries = {"country": COUNTRY, "machine_id": MACHINE_ID, "queries": []}

crawler = PlaywrightCrawler(
    request_handler_timeout=timedelta(minutes=10),
    max_request_retries=1,
)

async def main():
    global queries
    print("Fetcher started")
    try:
        while True:
            queries_urls = get_queries_to_process()
            if queries_urls:
                original_queries = {q['url']: q for q in queries['queries']}
                await crawler.run(queries_urls)
                # Merge metadata back
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
            print("Script stopped due to READ_TIMEOUT.")
        elif error_msg == "CONNECT_TIMEOUT":
            print("Script stopped due to CONNECT_TIMEOUT.")
        elif error_msg.startswith("REQUEST_FAILED"):
            print(f"Script stopped: {error_msg}")
        else:
            print(f"Unexpected error: {error_msg}")
            raise

@crawler.router.default_handler
async def request_handler(context: PlaywrightCrawlingContext) -> None:
    status = Status.FAILED.value
    context.log.info(f'Processing Url {context.page.url} ...')
    try:
        await google_map_consent_check(context)
        data = await process_business(context)
        if data:
            status = Status.PROCESSED.value
            save_query_results(context.page.url, [data])
    except Exception as e:
        context.log.info(f"Error processing page: {e}")
        status = Status.FAILED.value
    finally:
        update_query_status(context.page.url, status)

async def process_business(context: PlaywrightCrawlingContext):
    page = context.page
    url = context.request.url

    try:
        await page.wait_for_selector("h1", timeout=30_000)
    except:
        return None

    # Extract and format phone number
    phone_el = await page.query_selector("button[aria-label*='Phone']")
    phone = await phone_el.inner_text() if phone_el else None
    if phone:
        phone = re.sub(r'[^\d+]', '', phone)  # Keep only digits and +
        if not phone.startswith('+'):
            phone = f"+1{phone}"  # Default to US country code if not specified

    # Extract and format website URL
    website_el = await page.query_selector("a[data-item-id='authority']")
    website = await website_el.get_attribute("href") if website_el else None
    if website and not website.startswith(('http://', 'https://')):
        website = f"https://{website}"

    # Extract minimal required fields
    title_el = await page.query_selector("h1")
    title = await title_el.inner_text() if title_el else None

    # Address
    address_el = await page.query_selector("button[data-item-id='address']")
    address = await address_el.get_attribute("aria-label") if address_el else None
    address = address.replace("Address: ", "") if address else None

    # Category
    category_el = await page.query_selector("button.DkEaL")
    category = await category_el.inner_text() if category_el else None

    # Email via regex
    try:
        page_text = await page.inner_text("body")
        email_match = re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", page_text)
        email = email_match.group(0) if email_match else None
    except:
        email = None

    # Social links
    try:
        social_links = await page.evaluate("""
            () => Array.from(document.querySelectorAll("a")).map(a => a.href)
                .filter(href => 
                    /twitter\\.com|x\\.com|facebook\\.com|linkedin\\.com|instagram\\.com|youtube\\.com|tiktok\\.com/.test(href))
        """)
        social_links = list(set(social_links))  # deduplicate
    except:
        social_links = []

    # Star rating & Review count
    review_el = await page.query_selector("div.F7nice")
    star_rating = None
    review_count = None
    if review_el:
        review_text = await review_el.inner_text()
        parts = review_text.split('(')
        if len(parts) > 0:
            try:
                star_rating = float(parts[0].strip())
            except:
                pass
        if len(parts) > 1:
            try:
                review_count = int(parts[1].replace(')', '').replace(',', '').strip().split(' ')[0])
            except:
                pass

    # Current status (Open/Closed)
    status_el = await page.query_selector("span.shop营业时间")
    current_status = await status_el.inner_text() if status_el else None

    # Price level
    price_level_el = await page.query_selector("span[jsan='7.priceSection']")
    price_level = await price_level_el.inner_text() if price_level_el else None

    # Coordinates (not used here but extracted for consistency)
    coordinates = parse_coordinate_from_map_url(url)

    return {
        'title': title.strip() if title else None,
        'category': category.strip() if category else None,
        'address': address.strip() if address else None,
        'phone': phone,
        'website': website,
        'email': email.lower().strip() if email else None,
        'social_links': [link.strip() for link in social_links] if social_links else [],
        'star_rating': float(star_rating) if star_rating else None,
        'review_count': int(review_count) if review_count else None,
        'price_level': price_level.strip() if price_level else None,
        'current_status': current_status.strip().upper() if current_status else None,
        'source_url': url,
        'scraped_at': datetime.now(timezone.utc).isoformat(),
        'coordinates': coordinates
    }

def parse_coordinate_from_map_url(url):
    try:
        coordinate_match = re.search(r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)', url)
        if coordinate_match:
            return {
                'latitude': float(coordinate_match.group(1)),
                'longitude': float(coordinate_match.group(2))
            }
        return None
    except Exception as e:
        print(f"Error parsing URL: {e}")
        return None

def get_queries_to_process():
    global queries
    urls = get_queries_to_process_from_cache()
    if not urls:
        urls = get_queries_to_process_from_db()
    return urls

def get_queries_to_process_from_db():
    global queries
    retries = [10, 20, 30]
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
                    "industry": q.get("industry"),
                    "latitude": q.get("latitude"),
                    "longitude": q.get("longitude"),
                    "zoom_level": q.get("zoom_level")
                },
                "status": Status.PENDING.value
            } for q in raw_queries]
            cache_queries()
            print(f"Received {len(data['queries'])} queries from database")
            return [q["url"] for q in queries["queries"]]
        except requests.ReadTimeout:
            if i == len(retries) - 1:
                raise Exception("READ_TIMEOUT")
            print(f"[ReadTimeout] Retrying in {delay}s...")
            time.sleep(delay)
        except requests.ConnectTimeout:
            if i == len(retries) - 1:
                raise Exception("CONNECT_TIMEOUT")
            print(f"[ConnectTimeout] Retrying in {delay}s...")
            time.sleep(delay)
        except requests.RequestException as e:
            if i == len(retries) - 1:
                raise Exception(f"REQUEST_FAILED: {str(e)}")
            print(f"[RequestException] {e}. Retrying in {delay}s...")
            time.sleep(delay)

def get_queries_to_process_from_cache():
    global queries
    try:
        with open('queries_cache.json', 'r') as f:
            content = f.read().strip()
            if not content:
                return None
            try:
                cached_queries = json.loads(content)
            except json.JSONDecodeError:
                print("Invalid JSON in cache file, ignoring cache")
                return None
                
            pending_queries = [q for q in cached_queries['queries'] if q['status'] == Status.PENDING.value]
            if len(pending_queries) == 0:
                print("All queries processed, pushing results...")
                queries['queries'] = cached_queries['queries']
                push_results_to_db()
                return None
            queries['queries'] = cached_queries['queries']
            return [q['url'] for q in pending_queries]
    except FileNotFoundError:
        return None

def get_query_from_queries(query_url):
    global queries
    for query in queries['queries']:
        if query_url == query['url']:
            return query
    raise Exception(f"Query {query_url} not found in queries")

def update_query_status(query_url, status):
    global queries
    try:
        query = get_query_from_queries(query_url)
        query['status'] = status
        cache_queries()
    except Exception as e:
        print(f"[WARNING] Could not update status for {query_url}: {str(e)}")

def save_query_results(query_url, links):
    query = get_query_from_queries(query_url)
    query['results'] = links
    cache_queries()

def count_queries_results():
    return sum(len(q['results']) for q in queries['queries'] if q.get('status') == Status.PROCESSED.value)



def push_results_to_db():
    global queries
    num_queries_results = count_queries_results()
    print(f"Pushing {num_queries_results} results to database...")
    url = f"{TASK_SPREADER_API_URL}/queries/results"
    
    inserts = []
    for query in queries['queries']:
        if query['status'] == Status.PROCESSED.value:
            for result in query.get('results', []):
                # Validate and transform each result
                if not (result.get('title') or result.get('address') or result.get('website')):
                    continue
                    
                result_dict = {
                    'id': query.get('id'),
                    'title': result.get('title'),
                    'category': result.get('category'),
                    'address': result.get('address'),
                    'phone': result.get('phone'),
                    'website': result.get('website'),
                    'email': result.get('email'),
                    'social_links': result.get('social_links', []),
                    'star_rating': float(result.get('star_rating')) if result.get('star_rating') else None,
                    'review_count': int(result.get('review_count')) if result.get('review_count') else None,
                    'price_level': result.get('price_level'),
                    'current_status': result.get('current_status'),
                    'source_url': result.get('source_url'),
                    'scraped_at': result.get('scraped_at')
                }
                inserts.append(result_dict)

    if not inserts:
        print("No valid data to insert.")
        return

    payload = {
        "country": COUNTRY,
        "machine_id": MACHINE_ID,
        "queries": inserts
    }

    for i in range(3):  # Retry up to 3 times
        try:
            response = requests.post(
                url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=120
            )

            if response.status_code != 200:
                print(f"Failed to push results: {response.status_code}, {response.text}")
                continue

            clear_queries()
            print("Results pushed successfully.")
            return

        except requests.ConnectionError as e:
            print(f"Connection failed: {e}. Retrying...")
            time.sleep(10 * (i + 1))
    
    raise Exception("Failed to push results to database after multiple attempts.")

def transform_query_for_db(query):
    metadata = query.get('metadata', {})
    def safe_get(key, default=None):
        val = metadata.get(key)
        if isinstance(val, Decimal):
            return float(val)
        if isinstance(val, datetime):
            return val.isoformat()
        return val

    return {
        'title': query.get('title'),
        'category': query.get('category'),
        'address': query.get('address'),
        'phone': query.get('phone'),
        'website': query.get('website'),
        'email': query.get('email'),
        'social_links': query.get('social_links', []),
        'star_rating': query.get('star'),  # Make sure this is float
        'review_count': query.get('review_count'),  # Make sure this is int
        'price_level': query.get('price_level'),
        'current_status': query.get('current_status'),
        'source_url': query.get('url'),
        'machine_id': MACHINE_ID,
        'country_code': COUNTRY[:2].upper() if COUNTRY else None,
        'scraped_at': query.get('scraped_at') or datetime.now(timezone.utc).isoformat(),
    }
def cache_queries():
    global queries
    with open('queries_cache.json', 'w') as f:
        json.dump(queries, f, indent=4)

def clear_queries():
    global queries
    queries['queries'] = []
    cache_queries()

def validate_result(result):
    if not result['title'] and not result['address'] and not result['website']:
        return False
    if result['star_rating'] is not None and not isinstance(result['star_rating'], (float, type(None))):
        result['star_rating'] = float(result['star_rating']) if isinstance(result['star_rating'], (int, float, str)) else None
    if result['review_count'] is not None and not isinstance(result['review_count'], (int, type(None))):
        result['review_count'] = int(result['review_count']) if isinstance(result['review_count'], (str, float)) else None
    if result['scraped_at'] and isinstance(result['scraped_at'], str):
        try:
            result['scraped_at'] = datetime.fromisoformat(result['scraped_at'])
        except ValueError:
            result['scraped_at'] = None
    return True

if __name__ == "__main__":
    asyncio.run(main())