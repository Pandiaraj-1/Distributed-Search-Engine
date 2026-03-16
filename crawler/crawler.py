import requests          
import redis             
import psycopg2         
import time             
import logging          
import re               
from bs4 import BeautifulSoup        
from urllib.parse import urljoin, urlparse  
from dotenv import load_dotenv       
import os                            

from kafka import KafkaProducer, KafkaConsumer
import json
import threading

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)


r = redis.Redis(
    host=os.getenv('REDIS_HOST', 'localhost'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=0
)

conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST', 'localhost'),
    database=os.getenv('POSTGRES_DB', 'searchdb'),
    user=os.getenv('POSTGRES_USER', 'admin'),
    password=os.getenv('POSTGRES_PASSWORD', 'password')
)
cursor = conn.cursor()

log.info("Connected to Redis and PostgreSQL!")


# KAFKA SETUP
try:
    producer = KafkaProducer(
        bootstrap_servers=os.getenv('KAFKA_BROKER', 'localhost:9092'),
        # Convert Python dict to JSON text before sending
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Wait max 1 second for Kafka to confirm receipt
        request_timeout_ms=1000,
        api_version=(2, 5, 0)
    )
    log.info("Connected to Kafka!")
except Exception as e:
    log.error(f"Kafka connection failed: {e}")
    producer = None

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
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS links (
            source_url  TEXT,
            target_url  TEXT
        )
    """)

    conn.commit()
    log.info("Database tables created!")

create_tables()

def is_valid_url(url):

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
        log.info(f"Saved: {title[:50]}")
    except Exception as e:
        log.error(f"DB Save Error: {e}")
        conn.rollback()  # undo failed transaction


def save_links(source_url, target_urls):
 
    try:
        for target in target_urls:
            cursor.execute(
                "INSERT INTO links (source_url, target_url) VALUES (%s, %s)",
                (source_url, target)
            )
        conn.commit()
    except Exception as e:
        log.error(f"Links Save Error: {e}")
        conn.rollback()



def crawl_page(url):

    if r.sismember("visited_urls", url):
        log.info(f"Already visited: {url[:50]}")
        return []

    try:
        headers = {
            'User-Agent': 'SearchEngineBot/1.0 (Educational Project)'
        }
        response = requests.get(url, timeout=5, headers=headers)

        if response.status_code != 200:
            log.warning(f"Status {response.status_code}: {url[:50]}")
            return []

        content_type = response.headers.get('content-type', '')
        if 'text/html' not in content_type:
            return []

        r.sadd("visited_urls", url)

        soup = BeautifulSoup(response.text, 'html.parser')

        title_tag = soup.find('title')
        title = title_tag.text.strip() if title_tag else "No Title"

        for script in soup(["script", "style"]):
            script.decompose()
        content = soup.get_text(separator=' ', strip=True)

        links = []
        for a_tag in soup.find_all('a', href=True):
         
            full_url = urljoin(url, a_tag['href'])
            if is_valid_url(full_url):
                links.append(full_url)

        save_page(url, title, content)
        save_links(url, links[:20])  # save max 20 links per page

        log.info(f"Crawled: {title[:50]} | Found {len(links)} links")


        time.sleep(1)

        return links

    except requests.exceptions.Timeout:
        log.error(f"Timeout: {url[:50]}")
        return []
    except requests.exceptions.ConnectionError:
        log.error(f"Connection Error: {url[:50]}")
        return []
    except Exception as e:
        log.error(f"Unknown Error crawling {url[:50]}: {e}")
        return []

# KAFKA PRODUCER FUNCTION

def push_urls_to_kafka(urls, depth=0):

    if not producer:
        log.error("Kafka producer not available!")
        return

    for url in urls:
        message = {
            'url': url,
            'depth': depth
        }
        producer.send('urls-to-crawl', message)

    producer.flush()  # make sure all messages are sent
    log.info(f"Pushed {len(urls)} URLs to Kafka queue")


# KAFKA WORKER FUNCTION

def kafka_worker(worker_id):
    
    log.info(f"Worker {worker_id} started!")

    # Each worker creates its own Kafka consumer
    consumer = KafkaConsumer(
        'urls-to-crawl',
        bootstrap_servers=os.getenv('KAFKA_BROKER', 'localhost:9092'),

        group_id='crawler-workers',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        consumer_timeout_ms=30000,  # stop if no messages for 30 seconds
        api_version=(2, 5, 0)
    )

    crawled = 0
    max_per_worker = 100 # each worker crawls max 100 pages to avoid infinite crawling
    for message in consumer:
        if crawled >= max_per_worker:
            break

        data = message.value
        url = data.get('url')
        depth = data.get('depth', 0)

        if depth > 3:
            continue

        new_links = crawl_page(url)

        if new_links and producer:
            fresh = [l for l in new_links[:5]
                    if not r.sismember("visited_urls", l)]
            if fresh:
                push_urls_to_kafka(fresh, depth=depth + 1)

        crawled += 1
        log.info(f"Worker-{worker_id} | Crawled: {crawled}/{max_per_worker}")

    log.info(f"Worker {worker_id} finished! Crawled {crawled} pages")
    consumer.close()


def start_crawl(seed_urls, max_pages=200, fresh_start=True):


    if fresh_start:
        r.delete("visited_urls")
        log.info("Cleared previous visited URLs from Redis")

    queue = list(seed_urls)
    crawled_count = 0

    log.info(f"Starting crawl with {len(seed_urls)} seed URLs")
    log.info(f"Target: {max_pages} pages")

    while queue and crawled_count < max_pages:
        url = queue.pop(0)
        new_links = crawl_page(url)

        fresh_links = [
            link for link in new_links
            if not r.sismember("visited_urls", link)
        ]

        # Add up to 10 fresh links to queue
        queue.extend(fresh_links[:10])

        # Remove duplicates from queue
        queue = list(dict.fromkeys(queue))

        crawled_count += 1
        log.info(f"Progress: {crawled_count}/{max_pages} | Queue size: {len(queue)}")

    log.info(f"Crawling complete! Total pages crawled: {crawled_count}")
    
    # Show final count in database
    cursor.execute("SELECT COUNT(*) FROM pages")
    total = cursor.fetchone()[0]
    log.info(f"Total pages in database: {total}")

if __name__ == "__main__":
    import sys

 

    mode = sys.argv[1] if len(sys.argv) > 1 else "normal"

    seed_urls = [
        "https://en.wikipedia.org/wiki/Python_(programming_language)",
        "https://en.wikipedia.org/wiki/Computer_science",
        "https://en.wikipedia.org/wiki/Artificial_intelligence",
        "https://en.wikipedia.org/wiki/Machine_learning",
        "https://en.wikipedia.org/wiki/Web_scraping",
        "https://en.wikipedia.org/wiki/Data_structure",
        "https://en.wikipedia.org/wiki/Algorithm",
        "https://en.wikipedia.org/wiki/Database",
        "https://en.wikipedia.org/wiki/Computer_network",
        "https://en.wikipedia.org/wiki/Operating_system",
    ]

    if mode == "kafka":
        # DISTRIBUTED MODE 
        # 3 workers crawl in parallel via Kafka
        log.info("Starting DISTRIBUTED crawler with 3 Kafka workers!")

        r.delete("visited_urls")
        log.info("Cleared Redis visited URLs")

        push_urls_to_kafka(seed_urls)
        threads = []
        for i in range(3):
            t = threading.Thread(
                target=kafka_worker,
                args=(i,),
                daemon=True  # thread stops when main program stops
            )
            t.start()
            threads.append(t)
            log.info(f"Started Worker-{i}")

        # Wait for all 3 workers to finish
        for t in threads:
            t.join()

        # Show final results
        cursor.execute("SELECT COUNT(*) FROM pages")
        total = cursor.fetchone()[0]
        log.info(f"Distributed crawl complete!")
        log.info(f"Total pages in database: {total}")

    else:
        # NORMAL SINGLE MODE 
        start_crawl(seed_urls, max_pages=200, fresh_start=True)