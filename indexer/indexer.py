import psycopg2            
from elasticsearch import Elasticsearch 
import nltk                
import re                   
import os                   
import logging               
from datetime import datetime
from dotenv import load_dotenv
from nltk.corpus import stopwords   
from nltk.stem import PorterStemmer # Reduce words to root form

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)


log.info("Downloading NLTK data...")
nltk.download('stopwords', quiet=True)
nltk.download('punkt', quiet=True)
log.info("NLTK data ready!") 


es = Elasticsearch(
    os.getenv('ELASTIC_HOST', 'http://localhost:9200')
)

# Test Elasticsearch connection
if es.ping():
    log.info("Connected to Elasticsearch!")
else:
    log.error("SCannot connect to Elasticsearch!")
    exit(1)

# Connect to PostgreSQL (where crawler saved pages)
conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST', 'localhost'),
    database=os.getenv('POSTGRES_DB', 'searchdb'),
    user=os.getenv('POSTGRES_USER', 'admin'),
    password=os.getenv('POSTGRES_PASSWORD', 'password')
)
cursor = conn.cursor()
log.info("Connected to PostgreSQL!")


# CREATE ELASTICSEARCH INDEX

def create_search_index():
    index_name = "web-pages"

    # Delete old index if exists (fresh start)
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        log.info("Deleted old search index")

    # Create new index with settings
    es.indices.create(
        index=index_name,
        body={
            "settings": {
               
                "number_of_shards": 3,
                "number_of_replicas": 0,  # 0 replicas for local dev
                "analysis": {
                    "analyzer": {
                        "custom_analyzer": {
                            "type": "standard",
                            "stopwords": "_english_"
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "url": {
                        "type": "keyword"  # exact match, not analyzed
                    },
                    "title": {
                        "type": "text",
                        "analyzer": "custom_analyzer",
                    },
                    "content": {
                        "type": "text",
                        "analyzer": "custom_analyzer"
                    },
                    "page_rank": {
                        "type": "float"  # number for ranking
                    },
                    "crawled_at": {
                        "type": "date"  # date field for sorting
                    }
                }
            }
        }
    )
    log.info(f" Created search index: {index_name}")

# TEXT PROCESSING
stemmer = PorterStemmer()
stop_words = set(stopwords.words('english'))

def clean_text(text):
    
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'[^a-zA-Z0-9\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    words = text.split()
    cleaned_words = []
    for word in words:
       
        if len(word) > 2 and word not in stop_words:
        
            stemmed = stemmer.stem(word)
            cleaned_words.append(stemmed)

    return ' '.join(cleaned_words)

#  PAGERANK ALGORITHM

def compute_pagerank(iterations=30, damping=0.85):
 
    log.info(" Starting PageRank calculation...")

    # Get all page URLs
    cursor.execute("SELECT DISTINCT url FROM pages")
    all_pages = [row[0] for row in cursor.fetchall()]

    # Get all links between pages
    cursor.execute("SELECT source_url, target_url FROM links")
    all_links = cursor.fetchall()

    if not all_pages:
        log.warning("⚠️ No pages found for PageRank!")
        return {}

    log.info(f" Computing PageRank for {len(all_pages)} pages with {len(all_links)} links...")

    # Initialize all pages with rank 1.0
    rank = {page: 1.0 for page in all_pages}

    # Iterate to converge on final ranks
    for iteration in range(iterations):
        new_rank = {}

        for page in all_pages:
            # Find all pages that link TO this page
            incoming_pages = [
                link[0] for link in all_links
                if link[1] == page
            ]
            rank_sum = 0
            for src in incoming_pages:
                # How many links does the source page have?
                outgoing_count = len([
                    l for l in all_links if l[0] == src
                ])
                if outgoing_count > 0:
                    rank_sum += rank.get(src, 1.0) / outgoing_count

            # PageRank formula
            new_rank[page] = (1 - damping) + damping * rank_sum

        rank = new_rank

        if (iteration + 1) % 10 == 0:
            log.info(f"  ✓ Iteration {iteration + 1}/{iterations} done")

    log.info(" PageRank calculation complete!")
    return rank

#  INDEX PAGES INTO ELASTICSEARCH

def index_all_pages(pagerank_scores):

    # Get all pages from database
    cursor.execute("SELECT id, url, title, content, crawled_at FROM pages")
    pages = cursor.fetchall()

    log.info(f" Indexing {len(pages)} pages into Elasticsearch...")

    success_count = 0
    error_count = 0

    for page_id, url, title, content, crawled_at in pages:
        try:
            # Clean the text
            clean_content = clean_text(content or "")
            clean_title = clean_text(title or "No Title")

            # Get PageRank score for this page
            pr_score = pagerank_scores.get(url, 1.0)

            # Update PageRank in PostgreSQL too
            cursor.execute(
                "UPDATE pages SET page_rank = %s WHERE id = %s",
                (pr_score, page_id)
            )

            # Index into Elasticsearch
            es.index(
                index="web-pages",
                id=page_id,  # use same ID as PostgreSQL
                body={
                    "url": url,
                    "title": title or "No Title",
                    "content": clean_content[:50000],  # limit content size
                    "page_rank": pr_score,
                    "crawled_at": crawled_at.isoformat() if crawled_at else datetime.now().isoformat()
                }
            )

            success_count += 1

            if success_count % 20 == 0:
                log.info(f" Progress: {success_count}/{len(pages)} indexed")

        except Exception as e:
            error_count += 1
            log.error(f" Error indexing {url[:50]}: {e}")

    conn.commit()
    log.info(f" Indexing complete! Success: {success_count} | Errors: {error_count}")
    return success_count

def test_search(query="python"):
    """
    Test that our index is working by
    running a quick search
    """
    log.info(f" Testing search for: '{query}'")

    es.indices.refresh(index="web-pages")

    response = es.search(
        index="web-pages",
        body={
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["title^3", "content"],
                    "fuzziness": "AUTO"  # handles typos
                }
            },
            "size": 5  # return top 5 results
        }
    )

    hits = response["hits"]["hits"]
    total = response["hits"]["total"]["value"]

    log.info(f" Found {total} results for '{query}'")
    log.info(" Top 5 results:")

    for i, hit in enumerate(hits, 1):
        title = hit["_source"]["title"]
        score = hit["_score"]
        pr = hit["_source"]["page_rank"]
        log.info(f"  {i}. {title[:60]} (score: {score:.2f}, PR: {pr:.3f})")

    return hits

if __name__ == "__main__":
    log.info(" Starting Indexer Pipeline...")
    log.info("=" * 50)

    log.info("Step 1: Creating Elasticsearch index...")
    create_search_index()

 
    log.info(" Step 2: Computing PageRank...")
    pagerank_scores = compute_pagerank(iterations=30)

    # Show top 5 highest ranked pages
    if pagerank_scores:
        top_pages = sorted(
            pagerank_scores.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]
        log.info("🏆 Top 5 pages by PageRank:")
        for url, score in top_pages:
            log.info(f"  PR:{score:.3f} → {url[:60]}")

    
    log.info("Step 3: Indexing pages into Elasticsearch...")
    total_indexed = index_all_pages(pagerank_scores)

    # Step 4: Test search works
    log.info(" Step 4: Testing search...")
    test_search("python programming")
    test_search("machine learning")
    test_search("artificial intelligence")

    log.info("=" * 50)
    log.info(f" Indexer complete! {total_indexed} pages now searchable!")
    log.info(" Ready to build the Search API next!")

