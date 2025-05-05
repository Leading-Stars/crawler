import asyncio
import json
from datetime import datetime, timezone, timedelta
import re
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext

# === CONFIGURATION ===
# Hardcoded list of Google Maps business URLs to test
TEST_URLS = [
    "https://www.google.com/maps/place/Acaraje+Restaurant/@32.2742073,-84.9989355,15z/data=!4m7!3m6!1s0x88f5b9ef57d2450d:0x9215cde4455474b7!8m2!3d32.2742073!4d-84.9989355!10e2!16s%3Bm%3B89187155!5m1!1e2",
    "https://www.google.com/maps/place/Accounting+Firm+Example/@32.2742073,-84.9989355,15z/data=!4m7!3m6!1s0x88f5b9ef57d2450d:0x9215cde4455474b7!8m2!3d32.2742073!4d-84.9989355!10e2!16s%3Bm%3B89187155!5m1!1e2",
    "https://www.google.com/maps/place/Abortion+Clinic+Sample/@32.3882171,-85.0673029,13z/data=!4m7!3m6!1s0x88f5b9ef57d2450d:0x9215cde4455474b7!8m2!3d32.2742073!4d-84.9989355!10e2!16s%3Bm%3B89187155!5m1!1e2"
]

# Output file
OUTPUT_FILE = 'scraped_test_results.json'

# Email & Social Patterns
EMAIL_PATTERN = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
OBFUSCATED_EMAIL_PATTERN = r"([a-zA-Z0-9._%+-]+)\s*$$at$$\s*([a-zA-Z0-9.-]+)\s*$$dot$$\s*([a-zA-Z]{2,})"
SOCIAL_DOMAINS = {
    'twitter.com', 'x.com', 'facebook.com', 'linkedin.com',
    'instagram.com', 'youtube.com', 'tiktok.com'
}

# Results storage
scraped_results = []

# Concurrency control
MAX_CONCURRENCY = 1  # Only 1 page at a time
semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

# Helper: Extract Emails
def extract_emails(text):
    matches = re.findall(EMAIL_PATTERN, text)
    obfuscated_matches = [f"{m[0]}@{m[1]}.{m[2]}" for m in re.findall(OBFUSCATED_EMAIL_PATTERN, text)]
    return list(set(matches + obfuscated_matches))

# Helper: Parse Coordinates from URL
def parse_coordinate_from_map_url(url):
    try:
        match = re.search(r"@(-?\d+\.\d+),(-?\d+\.\d+)", url)
        if match:
            return {
                'latitude': float(match.group(1)),
                'longitude': float(match.group(2))
            }
        match = re.search(r"!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)", url)
        if match:
            return {
                'latitude': float(match.group(1)),
                'longitude': float(match.group(2))
            }
        return None
    except Exception as e:
        print(f"Error parsing coordinates: {e}")
        return None

# Initialize crawler
crawler = PlaywrightCrawler(
    request_handler_timeout=timedelta(minutes=5),
    max_request_retries=2,
)

# Main scraping logic
@crawler.router.default_handler
async def handle_place_page(context: PlaywrightCrawlingContext):
    async with semaphore:
        page = context.page
        url = context.request.url
        context.log.info(f"Processing: {url}")

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
            'source_url': url,
            'coordinates': parse_coordinate_from_map_url(url),
            'scraped_at': datetime.now(timezone.utc).isoformat()
        }

        try:
            # Block images/fonts/media
            await page.route("**/*.{png,jpg,jpeg,gif,webp,css,woff2,ttf,otf}", lambda route: route.abort())

            # Set small viewport to reduce memory footprint
            await page.set_viewport_size({"width": 800, "height": 600})

            # Navigate
            await page.goto(url, timeout=60_000)

            # Wait for title
            try:
                await page.wait_for_selector("h1.DUwDvf", timeout=45_000)
            except Exception:
                context.log.warning("Timed out waiting for h1")
                content = await page.content()
                context.log.warning(f"First 500 chars: {content[:500]}")
                return

            # TITLE
            result['title'] = await page.inner_text("h1.DUwDvf")

            # CATEGORY
            category_el = await page.query_selector("button.DkEaL")
            if category_el:
                result['category'] = await category_el.inner_text()

            # STAR RATING
            rating_el = await page.query_selector("div.F7nice > span[itemprop='ratingValue']")
            if rating_el:
                rating_text = await rating_el.inner_text()
                try:
                    result['star_rating'] = float(rating_text.strip())
                except ValueError:
                    context.log.warning(f"Invalid rating format: {rating_text}")

            # REVIEW COUNT
            review_count_el = await page.query_selector("div.F7nice > span[aria-label*='reviews']")
            if review_count_el:
                review_text = await review_count_el.inner_text()
                match = re.search(r"(\d[\d,.]*)", review_text)
                if match:
                    result['review_count'] = int(match.group(1).replace(',', '').replace('.', ''))

            # ADDRESS
            address_el = await page.query_selector("button[data-item-id='address']")
            if address_el:
                aria_label = await address_el.get_attribute("aria-label")
                result['address'] = aria_label.replace("Address: ", "").strip() if aria_label else None

            # PHONE
            phone_el = await page.query_selector("button[aria-label*='Phone']")
            if phone_el:
                phone = await phone_el.inner_text()
                result['phone'] = re.sub(r"[^\d]", "", phone)
                if not result['phone'].startswith('+'):
                    result['phone'] = f"+1{result['phone']}"

            # WEBSITE
            website_el = await page.query_selector("a[data-item-id='authority']")
            if website_el:
                result['website'] = await website_el.get_attribute("href")

            # EMAIL
            email_link = await page.query_selector("a[href^='mailto:']")
            if email_link:
                result['email'] = await email_link.inner_text()
            else:
                body_text = await page.inner_text("body")
                emails = extract_emails(body_text)
                if emails:
                    result['email'] = emails[0]

            # SOCIAL LINKS
            all_links = await page.evaluate("Array.from(document.querySelectorAll('a')).map(a => a.href)")
            result['social_links'] = [
                link for link in all_links
                if any(domain in link for domain in SOCIAL_DOMAINS)
            ]

            scraped_results.append(result)
            context.log.info(f"✅ Scraped: {result['title']}")

        except Exception as e:
            context.log.error(f"Failed to scrape {url}: {e}")
        finally:
            await page.close()  # Always close page after done
            await asyncio.sleep(2)  # Rate limiting

# Run the scraper
async def main():
    print(f"Starting scraper with {len(TEST_URLS)} hardcoded URLs...")
    await crawler.run(TEST_URLS)

    # Save results
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(scraped_results, f, indent=2, ensure_ascii=False)

    print(f"✅ Saved {len(scraped_results)} results to {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(main())