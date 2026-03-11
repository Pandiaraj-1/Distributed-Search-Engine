# ============================================
# SECTION 1: IMPORTS
# ============================================

import psycopg2              # Connect to PostgreSQL (read crawled pages)
from elasticsearch import Elasticsearch  # Connect to Elasticsearch
import nltk                  # Natural Language Processing toolkit
import re                    # Regular expressions for text cleaning
import os                    # Read environment variables
import logging               # Clean log messages
from datetime import datetime
from dotenv import load_dotenv
from nltk.corpus import stopwords   # Common words to remove (the, is, a...)
from nltk.stem import PorterStemmer # Reduce words to root form

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

# Download NLTK data (only downloads once, skips if already downloaded)
# stopwords = common English words like "the", "is", "at"
# punkt = sentence tokenizer
log.info("📥 Downloading NLTK data...")
nltk.download('stopwords', quiet=True)
nltk.download('punkt', quiet=True)
log.info("✅ NLTK data ready!") 

# ============================================
# SECTION 2: CONNECT TO SERVICES
# ============================================

# Connect to Elasticsearch
# Elasticsearch is our search engine — stores
# pages in a way that makes searching super fast
es = Elasticsearch(
    os.getenv('ELASTIC_HOST', 'http://localhost:9200')
)

# Test Elasticsearch connection
if es.ping():
    log.info("✅ Connected to Elasticsearch!")
else:
    log.error("❌ Cannot connect to Elasticsearch!")
    exit(1)

# Connect to PostgreSQL (where crawler saved pages)
conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST', 'localhost'),
    database=os.getenv('POSTGRES_DB', 'searchdb'),
    user=os.getenv('POSTGRES_USER', 'admin'),
    password=os.getenv('POSTGRES_PASSWORD', 'password')
)
cursor = conn.cursor()
log.info("✅ Connected to PostgreSQL!")

# ============================================
# SECTION 3: CREATE ELASTICSEARCH INDEX
# What: Set up the "table" in Elasticsearch
#       where we store searchable pages
#
# Think of it like creating a table in SQL
# but for a search engine
# ============================================

def create_search_index():
    index_name = "web-pages"

    # Delete old index if exists (fresh start)
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        log.info("🗑️  Deleted old search index")

    # Create new index with settings
    es.indices.create(
        index=index_name,
        body={
            "settings": {
                # number_of_shards = how many pieces to split data into
                # 3 shards = data split into 3 parts for faster search
                "number_of_shards": 3,
                "number_of_replicas": 0,  # 0 replicas for local dev
                "analysis": {
                    "analyzer": {
                        # Custom analyzer that:
                        # 1. Lowercases all text
                        # 2. Removes common words (stopwords)
                        "custom_analyzer": {
                            "type": "standard",
                            "stopwords": "_english_"
                        }
                    }
                }
            },
            "mappings": {
                # Mappings = tell Elasticsearch what type each field is
                # Like column types in a SQL table
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
    log.info(f"✅ Created search index: {index_name}")

# ============================================
# SECTION 4: TEXT PROCESSING
# What: Clean and normalize text before indexing
#
# Raw text from websites is messy — has HTML
# artifacts, extra spaces, weird characters.
# We clean it so search works better.
# ============================================

stemmer = PorterStemmer()
stop_words = set(stopwords.words('english'))

def clean_text(text):
    """
    Clean raw webpage text:
    1. Lowercase everything
    2. Remove special characters
    3. Remove extra spaces
    4. Remove stopwords (the, is, a, an...)
    5. Stem words (running → run, cats → cat)

    Example:
      Input:  "The Quick Brown Fox Jumps!!!"
      Output: "quick brown fox jump"
    """
    if not text:
        return ""

    # Step 1: Lowercase
    text = text.lower()

    # Step 2: Remove special characters, keep only letters and numbers
    text = re.sub(r'[^a-zA-Z0-9\s]', ' ', text)

    # Step 3: Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()

    # Step 4 & 5: Remove stopwords and stem
    words = text.split()
    cleaned_words = []
    for word in words:
        # Skip very short words and stopwords
        if len(word) > 2 and word not in stop_words:
            # Stem: "running" → "run", "cats" → "cat"
            stemmed = stemmer.stem(word)
            cleaned_words.append(stemmed)

    return ' '.join(cleaned_words)

# ============================================
# SECTION 5: PAGERANK ALGORITHM
# What: Calculate importance of each page
#       based on how many other pages link to it
#
# Simple explanation:
# - A page linked by many pages = important
# - A page linked by important pages = very important
# - Like counting votes, but votes from important
#   pages count more
# ============================================

def compute_pagerank(iterations=30, damping=0.85):
    """
    PageRank formula:
    PR(page) = (1 - damping) + damping * sum(PR(linking_page) / links(linking_page))

    damping = 0.85 means 85% of rank comes from
    incoming links, 15% is base rank
    """
    log.info("🔢 Starting PageRank calculation...")

    # Get all page URLs
    cursor.execute("SELECT DISTINCT url FROM pages")
    all_pages = [row[0] for row in cursor.fetchall()]

    # Get all links between pages
    cursor.execute("SELECT source_url, target_url FROM links")
    all_links = cursor.fetchall()

    if not all_pages:
        log.warning("⚠️ No pages found for PageRank!")
        return {}

    log.info(f"📊 Computing PageRank for {len(all_pages)} pages with {len(all_links)} links...")

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

            # Sum up rank contributions from incoming pages
            rank_sum = 0
            for src in incoming_pages:
                # How many links does the source page have?
                outgoing_count = len([
                    l for l in all_links if l[0] == src
                ])
                if outgoing_count > 0:
                    rank_sum += rank.get(src, 1.0) / outgoing_count

            # Apply PageRank formula
            new_rank[page] = (1 - damping) + damping * rank_sum

        rank = new_rank

        if (iteration + 1) % 10 == 0:
            log.info(f"  ✓ Iteration {iteration + 1}/{iterations} done")

    log.info("✅ PageRank calculation complete!")
    return rank

# ============================================
# SECTION 6: INDEX PAGES INTO ELASTICSEARCH
# What: Read every page from PostgreSQL,
#       clean the text, and store in Elasticsearch
#       so it becomes searchable
# ============================================

def index_all_pages(pagerank_scores):
    """
    Read all pages from PostgreSQL and
    index them into Elasticsearch
    """
    # Get all pages from database
    cursor.execute("SELECT id, url, title, content, crawled_at FROM pages")
    pages = cursor.fetchall()

    log.info(f"📚 Indexing {len(pages)} pages into Elasticsearch...")

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
                log.info(f"📈 Progress: {success_count}/{len(pages)} indexed")

        except Exception as e:
            error_count += 1
            log.error(f"❌ Error indexing {url[:50]}: {e}")

    conn.commit()
    log.info(f"✅ Indexing complete! Success: {success_count} | Errors: {error_count}")
    return success_count

# ============================================
# SECTION 7: TEST SEARCH
# What: Quick test to verify Elasticsearch
#       is returning results correctly
# ============================================

def test_search(query="python"):
    """
    Test that our index is working by
    running a quick search
    """
    log.info(f"🔍 Testing search for: '{query}'")

    # Refresh index so new documents are searchable
    es.indices.refresh(index="web-pages")

    response = es.search(
        index="web-pages",
        body={
            "query": {
                "multi_match": {
                    "query": query,
                    # Search in both title and content
                    # title^3 means title matches worth 3x more
                    "fields": ["title^3", "content"],
                    "fuzziness": "AUTO"  # handles typos
                }
            },
            "size": 5  # return top 5 results
        }
    )

    hits = response["hits"]["hits"]
    total = response["hits"]["total"]["value"]

    log.info(f"📊 Found {total} results for '{query}'")
    log.info("🏆 Top 5 results:")

    for i, hit in enumerate(hits, 1):
        title = hit["_source"]["title"]
        score = hit["_score"]
        pr = hit["_source"]["page_rank"]
        log.info(f"  {i}. {title[:60]} (score: {score:.2f}, PR: {pr:.3f})")

    return hits

# ============================================
# SECTION 8: MAIN — Run Everything
# ============================================

if __name__ == "__main__":
    log.info("🚀 Starting Indexer Pipeline...")
    log.info("=" * 50)

    # Step 1: Create fresh Elasticsearch index
    log.info("📋 Step 1: Creating Elasticsearch index...")
    create_search_index()

    # Step 2: Compute PageRank scores
    log.info("📋 Step 2: Computing PageRank...")
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

    # Step 3: Index all pages into Elasticsearch
    log.info("📋 Step 3: Indexing pages into Elasticsearch...")
    total_indexed = index_all_pages(pagerank_scores)

    # Step 4: Test search works
    log.info("📋 Step 4: Testing search...")
    test_search("python programming")
    test_search("machine learning")
    test_search("artificial intelligence")

    log.info("=" * 50)
    log.info(f"🎉 Indexer complete! {total_indexed} pages now searchable!")
    log.info("✅ Ready to build the Search API next!")

