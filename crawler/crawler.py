# ============================================
# SECTION 1: IMPORTS
# What: Load all the tools our crawler needs
# ============================================

import requests          # Tool to fetch/download web pages
import redis             # Tool to talk to Redis (tracks visited URLs)
import psycopg2          # Tool to talk to PostgreSQL (saves pages)
import time              # Tool to add delays between requests
import logging           # Tool to print status messages nicely
import re                # Tool to clean up text using patterns

from bs4 import BeautifulSoup        # Tool to read and parse HTML
from urllib.parse import urljoin, urlparse  # Tools to handle URLs
from dotenv import load_dotenv       # Tool to read our .env file
import os                            # Tool to access environment variables

# Load passwords from .env file
load_dotenv()

# ============================================
# SECTION 2: LOGGING SETUP
# What: Makes our terminal output look clean
#       and shows us what the crawler is doing
# ============================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

# Now we can use:
# log.info("message")    → normal updates
# log.error("message")   → when something goes wrong
# log.warning("message") → warnings

# ============================================
# SECTION 3: DATABASE CONNECTIONS
# What: Connect to Redis and PostgreSQL
# ============================================

# Connect to Redis
# Redis is like a fast notepad — we use it to
# remember which URLs we already visited
r = redis.Redis(
    host=os.getenv('REDIS_HOST', 'localhost'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=0
)

# Connect to PostgreSQL
# PostgreSQL is our main database — stores all
# crawled pages permanently
conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST', 'localhost'),
    database=os.getenv('POSTGRES_DB', 'searchdb'),
    user=os.getenv('POSTGRES_USER', 'admin'),
    password=os.getenv('POSTGRES_PASSWORD', 'password')
)
cursor = conn.cursor()

log.info("✅ Connected to Redis and PostgreSQL!")

# ============================================
# SECTION 4: CREATE TABLES
# What: Create tables in PostgreSQL to store
#       our crawled data if they don't exist
# ============================================

def create_tables():
    # Pages table — stores every webpage we crawl
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pages (
            id          SERIAL PRIMARY KEY,
            url         TEXT UNIQUE,
            title       TEXT,
            content     TEXT,
            page_rank   FLOAT DEFAULT 1.0,
            crawled_at  TIMESTAMP DEFAULT NOW()
        )
    """)
    # SERIAL = auto-incrementing ID (1, 2, 3...)
    # TEXT UNIQUE = no duplicate URLs allowed
    # DEFAULT NOW() = automatically saves current time

    # Links table — stores connections between pages
    # We need this to calculate PageRank later
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS links (
            source_url  TEXT,
            target_url  TEXT
        )
    """)

    conn.commit()
    log.info("✅ Database tables created!")

# Run it immediately
create_tables()

# ============================================
# SECTION 5: HELPER FUNCTIONS
# What: Small reusable functions used by crawler
# ============================================

def is_valid_url(url):
    """
    Check if a URL is valid and safe to crawl.
    Example:
      is_valid_url("https://google.com") → True
      is_valid_url("mailto:test@test.com") → False
      is_valid_url("javascript:void(0)") → False
    """
    try:
        parsed = urlparse(url)
        # Must have both a domain and http/https scheme
        return (
            bool(parsed.netloc) and
            bool(parsed.scheme) and
            parsed.scheme in ['http', 'https']
        )
    except:
        return False


def save_page(url, title, content):
    """
    Save a crawled page to PostgreSQL.
    If URL already exists, skip it (ON CONFLICT DO NOTHING)
    """
    try:
        cursor.execute(
            """
            INSERT INTO pages (url, title, content)
            VALUES (%s, %s, %s)
            ON CONFLICT (url) DO NOTHING
            """,
            (url, title, content[:10000])  # limit to 10000 chars
        )
        conn.commit()
        log.info(f"💾 Saved: {title[:50]}")
    except Exception as e:
        log.error(f"❌ DB Save Error: {e}")
        conn.rollback()  # undo failed transaction


def save_links(source_url, target_urls):
    """
    Save all links found on a page.
    We need these links to calculate PageRank later.
    """
    try:
        for target in target_urls:
            cursor.execute(
                "INSERT INTO links (source_url, target_url) VALUES (%s, %s)",
                (source_url, target)
            )
        conn.commit()
    except Exception as e:
        log.error(f"❌ Links Save Error: {e}")
        conn.rollback()

# ============================================
# SECTION 6: MAIN CRAWL FUNCTION
# What: Visits a URL, reads its content,
#       saves it, and returns new links found
# ============================================

def crawl_page(url):
    """
    Crawl a single page:
    1. Check if already visited
    2. Download the page
    3. Extract title + content
    4. Extract all links
    5. Save everything
    6. Return new links found
    """

    # Step 1: Check if already visited using Redis
    # Redis SET is super fast for this check
    if r.sismember("visited_urls", url):
        log.info(f"⏭️  Already visited: {url[:50]}")
        return []

    try:
        # Step 2: Download the page
        # We set a User-Agent so websites don't block us
        headers = {
            'User-Agent': 'SearchEngineBot/1.0 (Educational Project)'
        }
        response = requests.get(url, timeout=5, headers=headers)

        # Skip if page didn't load successfully
        if response.status_code != 200:
            log.warning(f"⚠️  Status {response.status_code}: {url[:50]}")
            return []

        # Skip non-HTML pages (PDFs, images, etc.)
        content_type = response.headers.get('content-type', '')
        if 'text/html' not in content_type:
            return []

        # Mark as visited in Redis IMMEDIATELY
        # So other workers don't crawl same page
        r.sadd("visited_urls", url)

        # Step 3: Parse the HTML
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract title
        title_tag = soup.find('title')
        title = title_tag.text.strip() if title_tag else "No Title"

        # Extract all visible text
        # Remove script and style tags first
        for script in soup(["script", "style"]):
            script.decompose()
        content = soup.get_text(separator=' ', strip=True)

        # Step 4: Extract all links on this page
        links = []
        for a_tag in soup.find_all('a', href=True):
            # Convert relative URLs to absolute
            # Example: "/wiki/Python" → "https://en.wikipedia.org/wiki/Python"
            full_url = urljoin(url, a_tag['href'])
            if is_valid_url(full_url):
                links.append(full_url)

        # Step 5: Save page and links to database
        save_page(url, title, content)
        save_links(url, links[:20])  # save max 20 links per page

        log.info(f"✅ Crawled: {title[:50]} | Found {len(links)} links")

        # Step 6: Be polite — wait 1 second between requests
        # Without this, websites will ban our bot!
        time.sleep(1)

        return links

    except requests.exceptions.Timeout:
        log.error(f"⏱️  Timeout: {url[:50]}")
        return []
    except requests.exceptions.ConnectionError:
        log.error(f"🔌 Connection Error: {url[:50]}")
        return []
    except Exception as e:
        log.error(f"❌ Unknown Error crawling {url[:50]}: {e}")
        return []
    
# ============================================
# SECTION 7: START THE CRAWLER
# What: Kicks off crawling from seed URLs
# ============================================

def start_crawl(seed_urls, max_pages=200):
    """
    Start crawling from seed URLs.
    Uses a queue — visits page, adds new links
    to queue, repeats until max_pages reached.
    """
    queue = list(seed_urls)  # start with seed URLs
    crawled_count = 0

    log.info(f"🚀 Starting crawl with {len(seed_urls)} seed URLs")
    log.info(f"🎯 Target: {max_pages} pages")

    while queue and crawled_count < max_pages:
        # Take first URL from queue
        url = queue.pop(0)

        # Crawl it and get new links
        new_links = crawl_page(url)

        # Add new links to queue (max 5 per page to avoid explosion)
        queue.extend(new_links[:5])

        crawled_count += 1
        log.info(f"📊 Progress: {crawled_count}/{max_pages} | Queue: {len(queue)}")

    log.info(f"🎉 Crawling complete! Total pages: {crawled_count}")


# ============================================
# MAIN ENTRY POINT
# What: Runs when you execute: python crawler.py
# ============================================

if __name__ == "__main__":
    # These are our starting pages
    # The crawler will start here and follow links
    seed_urls = [
        "https://en.wikipedia.org/wiki/Python_(programming_language)",
        "https://en.wikipedia.org/wiki/Computer_science",
        "https://en.wikipedia.org/wiki/Artificial_intelligence",
        "https://en.wikipedia.org/wiki/Machine_learning",
        "https://en.wikipedia.org/wiki/Web_scraping",
    ]

    start_crawl(seed_urls, max_pages=200)