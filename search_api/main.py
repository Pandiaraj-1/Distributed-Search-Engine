# ============================================
# SECTION 1: IMPORTS
# ============================================

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from elasticsearch import Elasticsearch
import redis
import psycopg2
import json
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

# ============================================
# SECTION 2: CREATE FASTAPI APP
# What: FastAPI is our web server
#       It listens for search requests and
#       returns results back to the browser
#
# Think of it as the WAITER in a restaurant:
#   User types query → Waiter (FastAPI) takes order
#   → Kitchen (Elasticsearch) finds results
#   → Waiter brings results back to user
# ============================================

app = FastAPI(
    title="Distributed Search Engine API",
    description="Search 172+ Wikipedia pages with TF-IDF + PageRank ranking",
    version="1.0.0"
)

# CORS = Cross Origin Resource Sharing
# Without this, our React frontend CANNOT
# talk to this API (browser security rule)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # allow all origins for dev
    allow_methods=["*"],      # allow GET, POST, etc
    allow_headers=["*"],      # allow all headers
)

# ============================================
# SECTION 3: CONNECT TO SERVICES
# ============================================

# Elasticsearch — our search engine
es = Elasticsearch(
    os.getenv('ELASTIC_HOST', 'http://localhost:9200')
)

# Redis — our cache
# When same query is searched twice,
# Redis returns it instantly without
# hitting Elasticsearch again
r = redis.Redis(
    host=os.getenv('REDIS_HOST', 'localhost'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=1  # db=1 to separate from crawler's db=0
)

# PostgreSQL — our main database
conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST', 'localhost'),
    database=os.getenv('POSTGRES_DB', 'searchdb'),
    user=os.getenv('POSTGRES_USER', 'admin'),
    password=os.getenv('POSTGRES_PASSWORD', 'password')
)
cursor = conn.cursor()

log.info("✅ All services connected!")

# ============================================
# SECTION 4: HEALTH CHECK ENDPOINT
# URL: GET http://localhost:8000/
# What: Quick check that API is running
#       Recruiters love seeing this —
#       shows you understand production systems
# ============================================

@app.get("/")
def root():
    """Health check endpoint"""
    return {
        "status": "running",
        "message": "Search Engine API is live!",
        "version": "1.0.0",
        "endpoints": {
            "search": "/search?q=your+query",
            "autocomplete": "/autocomplete?q=py",
            "stats": "/stats",
            "docs": "/docs"   # FastAPI auto-generates API docs!
        }
    }

# ============================================
# SECTION 5: MAIN SEARCH ENDPOINT
# URL: GET http://localhost:8000/search?q=python
# What: The core search functionality
#       Combines TF-IDF + PageRank for ranking
# ============================================

@app.get("/search")
def search(
    q: str = Query(..., min_length=1, description="Search query"),
    page: int = Query(1, ge=1, description="Page number"),
    size: int = Query(10, ge=1, le=50, description="Results per page")
):
    """
    Search endpoint:
    1. Check Redis cache first (super fast)
    2. If not cached, search Elasticsearch
    3. Combine TF-IDF score with PageRank
    4. Remove duplicate URLs
    5. Cache results for 5 minutes
    6. Return ranked results
    """

    # ---- STEP 1: Check Redis Cache ----
    # Cache key is unique per query + page
    cache_key = f"search:{q.lower()}:{page}:{size}"
    cached_result = r.get(cache_key)

    if cached_result:
        log.info(f"⚡ Cache HIT for: '{q}'")
        result = json.loads(cached_result)
        result["source"] = "cache"
        return result

    log.info(f"🔍 Searching Elasticsearch for: '{q}'")

    # ---- STEP 2: Search Elasticsearch ----
    try:
        response = es.search(
            index="web-pages",
            body={
                "query": {
                    "multi_match": {
                        "query": q,
                        # title^3 = title matches worth 3x more than content
                        "fields": ["title^3", "content"],
                        # AUTO fuzziness handles small typos
                        # "pythn" will still find "python"
                        "fuzziness": "AUTO"
                    }
                },
                # from = skip first N results (for pagination)
                "from": (page - 1) * size,
                "size": size * 2,  # fetch 2x to account for deduplication
                # Only return these fields (saves bandwidth)
                "_source": ["url", "title", "page_rank", "crawled_at"]
            }
        )
    except Exception as e:
        log.error(f"❌ Elasticsearch error: {e}")
        raise HTTPException(status_code=500, detail="Search engine error")

    hits = response["hits"]["hits"]
    total = response["hits"]["total"]["value"]

    # ---- STEP 3: Combine TF-IDF + PageRank ----
    results = []
    seen_urls = set()  # track seen URLs to remove duplicates

    for hit in hits:
        url = hit["_source"]["url"]

        # ---- STEP 4: Remove Duplicates ----
        if url in seen_urls:
            continue
        seen_urls.add(url)

        # TF-IDF score from Elasticsearch
        tfidf_score = hit["_score"] or 0

        # PageRank score from our algorithm
        page_rank = hit["_source"].get("page_rank", 1.0)

        # Combined score formula:
        # 70% TF-IDF (text relevance) + 30% PageRank (authority)
        combined_score = (0.7 * tfidf_score) + (0.3 * page_rank)

        results.append({
            "url": url,
            "title": hit["_source"].get("title", "No Title"),
            "score": round(combined_score, 4),
            "tfidf_score": round(tfidf_score, 4),
            "page_rank": round(page_rank, 4),
            "crawled_at": hit["_source"].get("crawled_at", "")
        })

    # Sort by combined score (highest first)
    results.sort(key=lambda x: x["score"], reverse=True)

    # Trim to requested size after deduplication
    results = results[:size]

    # Build final response
    final_response = {
        "source": "elasticsearch",
        "query": q,
        "total": total,
        "page": page,
        "size": size,
        "results": results
    }

    # ---- STEP 5: Cache Results for 5 minutes ----
    # 300 seconds = 5 minutes
    r.setex(cache_key, 300, json.dumps(final_response))

    # Track popular searches in Redis sorted set
    # zincrby = increment score of 'q' by 1
    r.zincrby("popular_searches", 1, q.lower())

    log.info(f"✅ Found {total} results for '{q}' | Returned {len(results)}")
    return final_response

# ============================================
# SECTION 6: AUTOCOMPLETE ENDPOINT
# URL: GET http://localhost:8000/autocomplete?q=py
# What: Returns suggestions as user types
#       Like Google's dropdown suggestions
# ============================================

@app.get("/autocomplete")
def autocomplete(
    q: str = Query(..., min_length=1, description="Partial search query")
):
    """
    Returns up to 8 search suggestions:
    1. From popular past searches (Redis)
    2. From page titles (Elasticsearch)
    Combined and deduplicated
    """
    suggestions = []

    # Source 1: Popular past searches from Redis
    # Get searches that start with the query
    try:
        popular = r.zrevrangebyscore(
            "popular_searches", "+inf", "-inf",
            start=0, num=20
        )
        for term in popular:
            decoded = term.decode('utf-8')
            if decoded.startswith(q.lower()):
                suggestions.append(decoded)
    except Exception as e:
        log.warning(f"Redis autocomplete error: {e}")

    # Source 2: Page titles from Elasticsearch
    try:
        es_response = es.search(
            index="web-pages",
            body={
                "query": {
                    "match_phrase_prefix": {
                        "title": {
                            "query": q,
                            "max_expansions": 10
                        }
                    }
                },
                "size": 5,
                "_source": ["title"]
            }
        )
        for hit in es_response["hits"]["hits"]:
            title = hit["_source"].get("title", "")
            if title and title not in suggestions:
                suggestions.append(title)
    except Exception as e:
        log.warning(f"ES autocomplete error: {e}")

    # Return max 8 unique suggestions
    unique_suggestions = list(dict.fromkeys(suggestions))[:8]

    return {
        "query": q,
        "suggestions": unique_suggestions
    }

# ============================================
# SECTION 7: STATS ENDPOINT
# URL: GET http://localhost:8000/stats
# What: Shows system statistics
#       Great for your demo video and resume!
# ============================================

@app.get("/stats")
def stats():
    """
    Returns system statistics:
    - Total pages crawled
    - Total links collected
    - Elasticsearch document count
    - Cache statistics
    - Top 5 popular searches
    """
    try:
        # PostgreSQL stats
        cursor.execute("SELECT COUNT(*) FROM pages")
        total_pages = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM links")
        total_links = cursor.fetchone()[0]

        cursor.execute("SELECT MIN(crawled_at), MAX(crawled_at) FROM pages")
        date_range = cursor.fetchone()

        # Elasticsearch stats
        es_stats = es.indices.stats(index="web-pages")
        es_doc_count = es_stats["_all"]["total"]["docs"]["count"]

        # Redis stats
        cache_keys = r.dbsize()
        popular = r.zrevrangebyscore(
            "popular_searches", "+inf", "-inf",
            start=0, num=5
        )
        top_searches = [
            {
                "query": p.decode('utf-8'),
                "count": int(r.zscore("popular_searches", p))
            }
            for p in popular
        ]

        return {
            "database": {
                "total_pages": total_pages,
                "total_links": total_links,
                "first_crawled": str(date_range[0]),
                "last_crawled": str(date_range[1])
            },
            "search_engine": {
                "indexed_documents": es_doc_count,
                "index_name": "web-pages"
            },
            "cache": {
                "total_cache_keys": cache_keys
            },
            "popular_searches": top_searches
        }

    except Exception as e:
        log.error(f"Stats error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
# ============================================
# SECTION 8: RUN SERVER
# ============================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True   # auto-restart when code changes
    )

