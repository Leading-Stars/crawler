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

# Set max concurrency manually via semaphore
MAX_CONCURRENCY = 2
semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

# Initialize crawler instance
crawler = PlaywrightCrawler(
    request_handler_timeout=timedelta(minutes=10),
    max_request_retries=2,
)
async def main():
    global queries
    print("Fetcher started")
    try:
        while True:
            urls = get_queries_to_process()
            if urls:
                original_queries = {q['url']: q for q in queries['queries']}

                # ❌ Remove parallel crawling
                # Instead, run crawler once and pass all URLs
                await crawler.run(urls)

                # Merge metadata back
                for query in queries['queries']:
                    if query['url'] in original_queries:
                        query.update({
                            'id': original_queries[query['url']].get('id'),
                            'metadata': original_queries[query['url']].get('metadata', {})
                        })
                push_results_to_db()
            else:
                print("No more URLs to process.")
                await asyncio.sleep(60)
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
    url = context.request.url
    status = Status.FAILED.value
    context.log.info(f'Processing URL: {url}')
    try:
        # Navigate with longer timeout
        await context.page.goto(url, timeout=90_000)

        # Handle Google consent banner
        await google_map_consent_check(context)

        # Skip redirect pages
        if context.page.url.startswith('https://consent.google.com/m?continue='):
            return

        data = await process_business(context)
        if data and validate_result(data):
            status = Status.PROCESSED.value
            save_query_results(url, [data])
        else:
            context.log.warning(f"No valid data extracted from {url}")
    except Exception as e:
        context.log.error(f"Error processing page {url}: {e}")
        status = Status.FAILED.value
    finally:
        update_query_status(url, status)


async def process_business(context: PlaywrightCrawlingContext):
    page = context.page
    url = context.request.url

    result = {
        'title': None,
        'category': None,
        'address': None,
        'phone': None,
        'website': None,
        'email': None,
        'social_links': [],
        'star_rating': None,
        'review_count': None,
        'price_level': None,
        'current_status': None,
        'source_url': url,
        'scraped_at': datetime.now(timezone.utc).isoformat(),
        'coordinates': parse_coordinate_from_map_url(url),
    }

    try:
        # Wait for title or fallback to body
        try:
            await page.wait_for_selector("h1", timeout=60_000)
        except:
            context.log.warning("Timed out waiting for h1 - trying body")
            content = await page.content()
            context.log.warning(f"First 500 chars of page: {content[:500]}")
            return result

        # Title
        title_el = await page.query_selector("h1")
        result['title'] = (await title_el.inner_text()).strip() if title_el else None

        # Category
        category_el = await page.query_selector("button.DkEaL")
        result['category'] = (await category_el.inner_text()).strip() if category_el else None

        # Address
        address_el = await page.query_selector("button[data-item-id='address']")
        if address_el:
            result['address'] = (
                await address_el.get_attribute("aria-label")
            ).replace("Address: ", "").strip()

        # Phone
        phone_el = await page.query_selector("button[aria-label*='Phone']")
        if phone_el:
            phone = await phone_el.inner_text()
            result['phone'] = re.sub(r"[^\d+]", "", phone)
            if not result['phone'].startswith('+'):
                result['phone'] = f"+1{result['phone']}"

        # Website
        website_el = await page.query_selector("a[data-item-id='authority']")
        if website_el:
            result['website'] = await website_el.get_attribute("href")

        # Email via <a href="mailto:">
        email_link = await page.query_selector("a[href^='mailto:']")
        if email_link:
            result['email'] = (await email_link.inner_text()).strip()
        else:
            # Fallback: scan body text
            try:
                page_text = await page.inner_text("body")
                email_match = re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", page_text)
                if email_match:
                    result['email'] = email_match.group(0)
            except Exception as e:
                context.log.warning(f"Regex email extraction failed: {e}")

        # Social Links
        try:
            links = await page.evaluate("""
                () => Array.from(document.querySelectorAll("a"))
                    .map(a => a.href)
                    .filter(href => /twitter\\.com|x\\.com|facebook\\.com|linkedin\\.com|instagram\\.com|youtube\\.com|tiktok\\.com/.test(href))
            """)
            result['social_links'] = list(set(links))  # deduplicate
        except Exception as e:
            context.log.warning(f"Social link extraction failed: {e}")

        context.log.info(f"Scraped data: {result}")
        return result

    except Exception as e:
        context.log.error(f"Exception during scraping: {e}")
        return result


def validate_result(result):
    if not result['title'] and not result['address'] and not result['website']:
        return False
    return True


def parse_coordinate_from_map_url(url):
    try:
        match = re.search(r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)', url)
        if match:
            return {
                'latitude': float(match.group(1)),
                'longitude': float(match.group(2))
            }
        return None
    except Exception as e:
        print(f"Error parsing coordinates: {e}")
        return None


# === Utility Functions Below ===

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


def cache_queries():
    global queries
    with open('queries_cache.json', 'w') as f:
        json.dump(queries, f, indent=4)


def clear_queries():
    global queries
    queries['queries'] = []
    cache_queries()


if __name__ == "__main__":
    asyncio.run(main())