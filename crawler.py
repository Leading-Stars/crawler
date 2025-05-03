import re

from datetime import datetime, timedelta
from urllib.parse import urlparse

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from dotenv import load_dotenv

from utils.enums import Status
from utils.google_maps_utils import google_map_consent_check

load_dotenv('.env')


# Add this near the top of the file after imports
from crawlee.crawlers import PlaywrightCrawler
from datetime import timedelta

# Initialize crawler instance
crawler = PlaywrightCrawler(
    request_handler_timeout=timedelta(minutes=10),
    max_request_retries=1
)


@crawler.router.default_handler
async def request_handler(context: PlaywrightCrawlingContext) -> None:
  url = context.request.url
  await google_map_consent_check(context)

  if context.page.url.startswith('https://consent.google.com/m?continue='):
    return
  try:
    data = await process_business(context)
    update_local_query_status(url, Status.PROCESSED.value)
    save_results_local(url, data)
  except Exception as e:
    print(f"Error processing {url}: {e}")
    update_local_query_status(url, Status.FAILED.value)


async def process_business(context: PlaywrightCrawlingContext) -> dict:
    page = context.page
    url = context.request.url

    # Scroll to load all content
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    await page.wait_for_timeout(2000)

    # Title
    title_el = await page.query_selector("h1")
    title = await title_el.inner_text() if title_el else None

    # Star rating & review count
    review_el = await page.query_selector("div.F7nice")
    star, review_count = None, None
    if review_el:
        review_text = await review_el.inner_text()
        parts = review_text.split('(')
        if len(parts) >= 1:
            try:
                star = float(parts[0].strip())
            except:
                pass
        if len(parts) > 1:
            try:
                review_count = int(parts[1].replace(')', '').replace(',', '').strip().split(' ')[0])
            except:
                pass

    # Headline
    headline_el = await page.query_selector("div[aria-label*='About'] div[jslog*='metadata']")
    headline = await headline_el.inner_text() if headline_el else ""

    # Category
    category_el = await page.query_selector("button.DkEaL")
    category = await category_el.inner_text() if category_el else None

    # Address
    address_el = await page.query_selector("button[data-item-id='address']")
    address = await address_el.get_attribute("aria-label") if address_el else ""
    address = address.replace("Address: ", "") if address else ""

    # Phone
    phone_el = await page.query_selector("button[aria-label*='Phone']")
    phone = await phone_el.inner_text() if phone_el else None
    phone = re.sub(r'^[^+]*', '', phone).strip() if phone else None

    # Website
    website_el = await page.query_selector("a[data-item-id='authority']")
    website = await website_el.get_attribute("href") if website_el else None

    # Plus Code
    pluscode_el = await page.query_selector("button[aria-label*='Plus code']")
    pluscode = await pluscode_el.inner_text() if pluscode_el else None

    # Booking Link
    book_el = await page.query_selector("a.M77dve")
    book = await book_el.get_attribute("href") if book_el else None

    # Check-in Info
    check_in_el = await page.query_selector("div[data-item-id='place-info-links:'] .Io6YTe")
    check_in = await check_in_el.inner_text() if check_in_el else None

    # Open Hours
    open_hours = {}
    open_hours_el = await page.query_selector("div[aria-label*='Open']")
    if open_hours_el:
        open_hours_label = await open_hours_el.get_attribute("aria-label")
        if open_hours_label:
            lines = open_hours_label.strip().split('\n')
            for line in lines:
                if ':' in line:
                    day, time = line.split(':', 1)
                    open_hours[day.strip()] = time.strip()

    # Coordinates
    coordinates = parse_coordinate_from_map_url(url)

    # Photos
    photos = []
    photo_section = await page.query_selector_all("div[role='listitem'] img[srcset]")
    for img in photo_section:
        src = await img.get_attribute("src")
        if src and "lh3.googleusercontent.com" in src:
            photos.append(src)

    # Cover Photo
    cover_photo = photos[0] if photos else ""

    # About Section / Attributes
    about_data = await process_about(page)

    # Reviews
    review_summary, last_review_date = {}, None
    reviews = await process_reviews(context)
    if reviews and len(reviews) > 0:
        review_summary = {
            'total': len(reviews),
            'avg_rating': sum(r['rating'] for r in reviews if r.get('rating')) / len([r for r in reviews if r.get('rating')]),
            'sample': reviews[:3]
        }
        last_review_date = reviews[0]['date'] if reviews[0].get('date') else None

    return {
        'url': url,
        'title': title,
        'star': star,
        'review_count': review_count,
        'headline': headline,
        'category': category,
        'address': address,
        'open_hours': open_hours,
        'check_in': check_in,
        'book': book,
        'website': website,
        'phone': phone,
        'pluscode': pluscode,
        'coordinates': coordinates,
        'photos': photos,
        'cover_photo': cover_photo,
        'attributes': about_data.get('attributes', {}),
        'services': about_data.get('services', []),
        'email': about_data.get('email'),
        'social_links': about_data.get('social_links', []),
        'review_summary': review_summary,
        'last_review_date': last_review_date,
        'scraped_at': datetime.utcnow().isoformat(),
    }

async def process_about(page):
    data = {
        'about': [],
        'attributes': {},
        'services': [],
        'email': None,
        'social_links': []
    }

    if not (await page.query_selector("button[aria-label*='About']")):
        return data

    await page.click("button[aria-label*='About']")
    await page.wait_for_selector("h2", timeout=1000 * 10)

    sections = await page.query_selector_all("div.fontBodyMedium")

    for section in sections:
        h2 = await section.query_selector("h2")
        if not h2:
            continue
        heading = await h2.inner_text()
        items = []

        list_items = await section.query_selector_all("li")
        for li in list_items:
            span = await li.query_selector("span[aria-label]")
            if span:
                label = await span.get_attribute("aria-label")
                items.append(label)

        if heading == "About":
            data['about'].extend(items)
        elif heading == "Amenities" or heading == "Services":
            data['services'].extend(items)
        elif heading == "Accessibility" or heading == "Highlights":
            data['attributes'][heading.lower()] = items

    # 🔍 Extract email using regex from all visible text
    try:
        page_text = await page.inner_text("body")
        email_match = re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", page_text)
        if email_match:
            data['email'] = email_match.group(0)
    except Exception as e:
        print(f"Error extracting email: {e}")

    # 🔗 Extract social media links
    try:
        social_links = await page.evaluate("""
            () => Array.from(document.querySelectorAll("a"))
                .map(a => a.href)
                .filter(href => 
                    /twitter\\.com|x\\.com|facebook\\.com|linkedin\\.com|instagram\\.com|youtube\\.com|tiktok\\.com/.test(href)
                )
        """)
        data['social_links'] = list(set(social_links))  # deduplicate
    except Exception as e:
        print(f"Error extracting social links: {e}")

    return data

async def process_reviews(context):
  page = context.page
  if not (await page.query_selector("button[aria-label*='Reviews']")):
    return None
  await page.click("button[aria-label*='Reviews']")
  
  # Wait for either button to appear (with a 10-second timeout)
  await page.wait_for_selector("button[aria-label*='relevant'], button[aria-label*='Sort']", timeout=1000 * 10)
  
  # Check if the 'relevant' button exists and click it, otherwise click 'Sort' button
  relevant_button = await page.query_selector("button[aria-label*='relevant']")
  sort_button = await page.query_selector("button[aria-label*='Sort']")
  if relevant_button:
    await relevant_button.click()
  elif sort_button:
    await sort_button.click()
  
  # Wait for the selector to appear with a 10-second timeout
  await page.wait_for_selector("div[id='action-menu'] div[data-index='1']", timeout=1000 * 10)

  # Check if the element exists, if not return None
  menu_item = await page.query_selector("div[id='action-menu'] div[data-index='1']")
  if not menu_item:
    return None
  
  # Click the element if found
  await menu_item.click()

  # Wait for the next selector to appear
  await page.wait_for_selector("div.d4r55", timeout=1000 * 10)
  await scroll_page(context, '.DxyBCb')

  reviews = await page.evaluate("""
    () => Array.from(document.querySelectorAll(".jftiEf")).map(el => {
      const reviews = el.querySelector("div.RfnDt")?.textContent;

      const [isLocalGuide, reviewCount] = (() => {
        if (!reviews) {
          return [false, null];
        }

        const parts = reviews.split("·").map(part => part.trim());
        const localGuide = parts.some(part => part === "Local Guide");
        const reviewMatch = parts.find(part => part.endsWith("review") || part.endsWith("reviews"))?.match(/\\d+/);

        return [
          localGuide, 
          reviewMatch ? parseInt(reviewMatch[0]) : null,
        ];
      })();
                                            
      return {
        user: {
          name: el.querySelector(".d4r55")?.textContent.trim(),
          link: el.querySelector(".al6Kxe")?.getAttribute("data-href"),
          thumbnail: el.querySelector(".NBa7we")?.getAttribute("src"),
          localGuide: isLocalGuide ? true : undefined,
          reviews: reviewCount,
        },
        
        rating: parseFloat(el.querySelector(".kvMYJc")?.getAttribute("aria-label") || parseInt(el.querySelector(".fzvQIb")?.textContent.split("/")[0]) / 5),
        snippet: el.querySelector(".MyEned")?.textContent.trim(),
        date: el.querySelector(".rsqaWe")?.textContent.trim() || el.querySelector(".xRkPPb")?.textContent.trim().split(" on")[0],
      };
    });
  """)
  
  for review in reviews:
    if review.get("date"):
      seconds_ago = parse_text_duration(review["date"])
      review["date"] = (datetime.now() - timedelta(seconds=seconds_ago)).isoformat()

  return reviews

async def scroll_page(context, scroll_container, limit=30):
  count = 0
  page = context.page
  
  while True:
    if count >= limit:
      context.log.info(f"reached scoll page limit ${limit}, url ${page.url}")
      break
  
    # Scroll to the bottom of the scroll container
    await page.evaluate(f'document.querySelector("{scroll_container}").scrollTo(0, document.querySelector("{scroll_container}").scrollHeight)')

    count += 1

    if await page.query_selector("span.xRkPPb"):
      # Evaluate to extract dates from elements
      dates = await page.evaluate("""
        () => Array.from(document.querySelectorAll("span.xRkPPb")).map(el => {
          console.log(`date ${el.innerText.trim().split("on")[0]}`);
          return el.innerText.trim().split("on")[0];
        })
      """)

      # Get the last date from the extracted list
      date = dates[-1] if dates else None

      # Check the date condition
      if date and ("year ago" in date or "years ago" in date):
        break

    if await page.query_selector("span.rsqaWe"):
      # Extract dates using evaluate
      dates = await page.evaluate("""
        () => Array.from(document.querySelectorAll("span.rsqaWe")).map(el => el.innerText.trim())
      """)

      # Get the last date from the extracted list
      date = dates[-1] if dates else None

      # Check the date condition
      if date and ("year ago" in date or "years ago" in date):
        break

def parse_coordinate_from_map_url(url):
  try:
    url_obj = urlparse(url)
    coordinate_match = re.search(r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)', url_obj.path)
    
    if coordinate_match:
      latitude = float(coordinate_match.group(1))
      longitude = float(coordinate_match.group(2))
      return {'latitude': latitude, 'longitude': longitude}
    
    return None
  
  except Exception as e:
    print(f"Error parsing URL: {e}")
    return None

def parse_text_duration(duration_text):
  """
  Parses a duration string like "2 days ago", "3 weeks ago", etc., into a duration in seconds.
  """
  match = re.match(r"(\d+)\s*(second|minute|hour|day|week|month|year)s?\s*ago", duration_text.lower())
  if not match:
    return 0
  value, unit = int(match.group(1)), match.group(2)
  multiplier = {
    "second": 1,
    "minute": 60,
    "hour": 3600,
    "day": 86400,
    "week": 604800,
    "month": 2592000,  # Approximate, assumes 30 days per month
    "year": 31536000,  # Approximate, assumes 365 days per year
  }
  return value * multiplier.get(unit, 0)