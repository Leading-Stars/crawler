import asyncio
import json
import os
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from dotenv import load_dotenv
from utils.enums import Status  # Optional – remove if not used
from datetime import datetime, timezone, timedelta

load_dotenv('.env')

# Configuration
MAX_CONCURRENCY = 1
PLACE_LINKS_FILE = "place_links.json"
SEARCH_PAGES = [
    "https://www.google.com/maps/search/acaraje%20restaurant/@32.2742073,-84.9989355,15z?hl=en",
    "https://www.google.com/maps/search/accounting%20firm/@32.2742073,-84.9989355,15z?hl=en",
    "https://www.google.com/maps/search/abortion%20clinic/@32.3882171,-85.0673029,13z?hl=en"
]

# Initialize crawler instance
crawler = PlaywrightCrawler(
    request_handler_timeout=timedelta(minutes=5),
    max_request_retries=2,
)

# Store collected place links
collected_links = set()

def save_collected_links():
    with open(PLACE_LINKS_FILE, 'w') as f:
        json.dump(list(collected_links), f, indent=2)
    print(f"Saved {len(collected_links)} place links to {PLACE_LINKS_FILE}")

async def extract_place_links_from_page(page):
    try:
        links = await page.evaluate("""
            () => Array.from(document.querySelectorAll('a'))
                .map(a => a.href)
                .filter(href => href.startsWith('https://www.google.com/maps/place/'))
        """)
        for link in links:
            collected_links.add(link)
        print(f"Found {len(links)} place links on current page.")
    except Exception as e:
        print(f"[ERROR] Failed to extract links: {e}")

@crawler.router.default_handler
async def handle_search_page(context: PlaywrightCrawlingContext):
    url = context.request.url
    context.log.info(f"Processing search page: {url}")

    try:
        await context.page.goto(url, timeout=90_000)

        # Handle consent banner
        try:
            await context.page.click("button#introAgreeButton", timeout=5000)
        except:
            pass

        # Scroll down to load more results
        for _ in range(3):  # Adjust based on how many scrolls needed
            await context.page.mouse.wheel(delta_y=2000)
            await asyncio.sleep(2)

        # Extract place links
        await extract_place_links_from_page(context.page)

    except Exception as e:
        context.log.error(f"Error processing {url}: {e}")
    finally:
        save_collected_links()

async def main():
    print("Starting place link extractor...")
    global collected_links

    # Load existing links if any
    if os.path.exists(PLACE_LINKS_FILE):
        with open(PLACE_LINKS_FILE, 'r') as f:
            try:
                data = json.load(f)
                collected_links = set(data)
                print(f"Loaded {len(collected_links)} previously saved place links.")
            except json.JSONDecodeError:
                print("Place links file is empty or corrupted. Starting fresh.")

    # Run crawler
    await crawler.run(SEARCH_PAGES)

    # Final save
    save_collected_links()
    print("Extraction complete.")

if __name__ == "__main__":
    asyncio.run(main())