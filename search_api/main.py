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

app = FastAPI(
    title="Distributed Search Engine API",
    description="Search 172+ Wikipedia pages with TF-IDF + PageRank ranking",
    version="1.0.0"
)

# CORS = Cross Origin Resource Sharing
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # allow all origins for dev
    allow_methods=["*"],      # allow GET, POST, etc
    allow_headers=["*"],      # allow all headers
)

es = Elasticsearch(
    os.getenv('ELASTIC_HOST', 'http://localhost:9200')
)
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

log.info("All services connected!")

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
            "docs": "/docs"  
        }
    }

@app.get("/search")
def search(
    q: str = Query(..., min_length=1, description="Search query"),
    page: int = Query(1, ge=1, description="Page number"),
    size: int = Query(10, ge=1, le=50, description="Results per page")
):

    cache_key = f"search:{q.lower()}:{page}:{size}"
    cached_result = r.get(cache_key)

    if cached_result:
        log.info(f"⚡ Cache HIT for: '{q}'")
        result = json.loads(cached_result)
        result["source"] = "cache"
        return result

    log.info(f"🔍 Searching Elasticsearch for: '{q}'")
    try:
        response = es.search(
            index="web-pages",
            body={
                "query": {
                    "multi_match": {
                        "query": q,
                        "fields": ["title^3", "content"],
                        "fuzziness": "AUTO"
                    }
                },
                # from = skip first N results (for pagination)
                "from": (page - 1) * size,
                "size": size * 2,  
                "_source": ["url", "title", "page_rank", "crawled_at"]
            }
        )
    except Exception as e:
        log.error(f"❌ Elasticsearch error: {e}")
        raise HTTPException(status_code=500, detail="Search engine error")

    hits = response["hits"]["hits"]
    total = response["hits"]["total"]["value"]
    results = []
    seen_urls = set()  

    for hit in hits:
        url = hit["_source"]["url"]
        url = url.split('#')[0]
        if url in seen_urls:
            continue
        seen_urls.add(url)

        tfidf_score = hit["_score"] or 0
        page_rank = hit["_source"].get("page_rank", 1.0)
        combined_score = (0.7 * tfidf_score) + (0.3 * page_rank)

        results.append({
            "url": url,
            "title": hit["_source"].get("title", "No Title"),
            "score": round(combined_score, 4),
            "tfidf_score": round(tfidf_score, 4),
            "page_rank": round(page_rank, 4),
            "crawled_at": hit["_source"].get("crawled_at", "")
        })
    results.sort(key=lambda x: x["score"], reverse=True)
    results = results[:size]
    final_response = {
        "source": "elasticsearch",
        "query": q,
        "total": total,
        "page": page,
        "size": size,
        "results": results
    }

    r.setex(cache_key, 300, json.dumps(final_response))

    r.zincrby("popular_searches", 1, q.lower())

    log.info(f"Found {total} results for '{q}' | Returned {len(results)}")
    return final_response

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True 
    )

