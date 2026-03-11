import { useState, useEffect } from "react";
import axios from "axios";

const API = "http://localhost:8000";

interface SearchResult {
  url: string;
  title: string;
  score: number;
  tfidf_score: number;
  page_rank: number;
  crawled_at: string;
}

interface Stats {
  database: { total_pages: number; total_links: number };
  search_engine: { indexed_documents: number };
}

function ResultCard({ result }: { result: SearchResult }) {
  const fmtUrl  = (url: string) => url.replace(/https?:\/\//, "").slice(0, 60);
  const fmtDate = (d: string)   => d ? new Date(d).toLocaleDateString() : "";

  return (
    <div style={{ background:"#fff", borderRadius:12, padding:"18px 22px", marginBottom:12, boxShadow:"0 1px 4px rgba(0,0,0,.08)", borderLeft:"4px solid #1a73e8" }}>
      <a href={result.url} target="_blank" rel="noreferrer"
        style={{ fontSize:18, color:"#1a0dab", textDecoration:"none", fontWeight:500, display:"block", marginBottom:4 }}>
        {result.title}
      </a>
      <div style={{ color:"#137333", fontSize:13, marginBottom:8 }}>
        {fmtUrl(result.url)}
      </div>
      <div style={{ display:"flex", gap:8, flexWrap:"wrap" }}>
        <span style={{ background:"#e8f0fe", color:"#1a73e8", padding:"3px 10px", borderRadius:12, fontSize:12 }}>Score: {result.score}</span>
        <span style={{ background:"#fce8e6", color:"#c5221f", padding:"3px 10px", borderRadius:12, fontSize:12 }}>TF-IDF: {result.tfidf_score}</span>
        <span style={{ background:"#e6f4ea", color:"#137333", padding:"3px 10px", borderRadius:12, fontSize:12 }}>PageRank: {result.page_rank}</span>
        <span style={{ background:"#fef7e0", color:"#b06000", padding:"3px 10px", borderRadius:12, fontSize:12 }}>📅 {fmtDate(result.crawled_at)}</span>
      </div>
    </div>
  );
}

export default function App() {
  const [query, setQuery]                     = useState("");
  const [results, setResults]                 = useState<SearchResult[]>([]);
  const [suggestions, setSuggestions]         = useState<string[]>([]);
  const [loading, setLoading]                 = useState(false);
  const [searched, setSearched]               = useState(false);
  const [total, setTotal]                     = useState(0);
  const [stats, setStats]                     = useState<Stats | null>(null);
  const [currentPage, setCurrentPage]         = useState(1);
  const [searchTime, setSearchTime]           = useState(0);
  const [source, setSource]                   = useState("");
  const [showSuggestions, setShowSuggestions] = useState(false);

  useEffect(() => {
    axios.get(`${API}/stats`).then(res => setStats(res.data)).catch(() => {});
  }, []);

  const handleSearch = async (searchQuery: string, page: number = 1) => {
    if (!searchQuery.trim()) return;
    setLoading(true);
    setSearched(true);
    setShowSuggestions(false);
    const t0 = Date.now();
    try {
      const res = await axios.get(
        `${API}/search?q=${encodeURIComponent(searchQuery)}&page=${page}&size=10`
      );
      setResults(res.data.results);
      setTotal(res.data.total);
      setCurrentPage(page);
      setSource(res.data.source);
      setSearchTime(Date.now() - t0);
    } catch (err) {
      console.error(err);
    }
    setLoading(false);
  };

  const handleTyping = async (val: string) => {
    setQuery(val);
    if (val.length >= 2) {
      try {
        const res = await axios.get(`${API}/autocomplete?q=${encodeURIComponent(val)}`);
        setSuggestions(res.data.suggestions || []);
        setShowSuggestions(true);
      } catch { setSuggestions([]); }
    } else {
      setSuggestions([]);
      setShowSuggestions(false);
    }
  };

  const pickSuggestion = (s: string) => {
    setQuery(s);
    setSuggestions([]);
    setShowSuggestions(false);
    handleSearch(s);
  };

  const resetHome = () => { setSearched(false); setQuery(""); setResults([]); };

  const QUICK = ["Python", "Machine learning", "Artificial intelligence", "Database", "Algorithm"];
  const PAGES = Math.min(5, Math.ceil(total / 10));

  return (
    <div style={{ fontFamily:"Arial,sans-serif", minHeight:"100vh", background:"#f8f9fa" }}>

      {/* HEADER */}
      <div style={{ background:"linear-gradient(135deg,#1a73e8,#0d47a1)", color:"#fff", padding: searched ? "20px" : "80px 20px", textAlign:"center", transition:"padding .3s" }}>
        <h1 onClick={resetHome} style={{ fontSize: searched ? 28 : 56, margin:"0 0 8px", cursor:"pointer", transition:"font-size .3s" }}>
          🔍 SearchX
        </h1>
        {stats && (
          <p style={{ margin:"0 0 20px", opacity:0.85, fontSize: searched ? 13 : 16 }}>
            {stats.database.total_pages.toLocaleString()} pages indexed &nbsp;•&nbsp; {stats.database.total_links.toLocaleString()} links mapped
          </p>
        )}

        {/* SEARCH BOX */}
        <div style={{ position:"relative", maxWidth:650, margin:"0 auto" }}>
          <div style={{ display:"flex", gap:10 }}>
            <input
              value={query}
              onChange={e => handleTyping(e.target.value)}
              onKeyDown={e => { if (e.key==="Enter") handleSearch(query); if (e.key==="Escape") setShowSuggestions(false); }}
              placeholder="Search Wikipedia pages..."
              style={{ flex:1, padding:"14px 20px", fontSize:17, border:"none", borderRadius:30, outline:"none", boxShadow:"0 2px 8px rgba(0,0,0,.2)" }}
            />
            <button onClick={() => handleSearch(query)}
              style={{ padding:"14px 28px", background:"#fbbc04", color:"#333", border:"none", borderRadius:30, fontSize:16, fontWeight:"bold", cursor:"pointer" }}>
              Search
            </button>
          </div>

          {/* AUTOCOMPLETE */}
          {showSuggestions && suggestions.length > 0 && (
            <div style={{ position:"absolute", top:54, left:0, right:90, background:"#fff", borderRadius:12, boxShadow:"0 4px 20px rgba(0,0,0,.15)", zIndex:100, overflow:"hidden", textAlign:"left" }}>
              {suggestions.map((s, i) => (
                <div key={i} onClick={() => pickSuggestion(s)}
                  style={{ padding:"12px 20px", cursor:"pointer", borderBottom:"1px solid #f0f0f0", color:"#333", fontSize:15 }}>
                  🔍 {s}
                </div>
              ))}
            </div>
          )}
        </div>

        {/* QUICK BUTTONS */}
        {!searched && (
          <div style={{ marginTop:16, display:"flex", gap:8, justifyContent:"center", flexWrap:"wrap" }}>
            {QUICK.map(term => (
              <button key={term} onClick={() => { setQuery(term); handleSearch(term); }}
                style={{ padding:"6px 16px", background:"rgba(255,255,255,.2)", color:"#fff", border:"1px solid rgba(255,255,255,.4)", borderRadius:20, cursor:"pointer", fontSize:13 }}>
                {term}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* MAIN CONTENT */}
      <div style={{ maxWidth:750, margin:"0 auto", padding:20 }}>

        {loading && (
          <div style={{ textAlign:"center", padding:40, color:"#666" }}>
            <div style={{ fontSize:32, marginBottom:10 }}>⏳</div>
            <p>Searching documents...</p>
          </div>
        )}

        {searched && !loading && results.length > 0 && (
          <div style={{ display:"flex", justifyContent:"space-between", marginBottom:16, color:"#666", fontSize:14 }}>
            <span>
              About <strong>{total}</strong> results ({searchTime}ms)
              {source === "cache" && (
                <span style={{ marginLeft:8, background:"#e8f5e9", color:"#2e7d32", padding:"2px 8px", borderRadius:10, fontSize:12 }}>
                  ⚡ Cached
                </span>
              )}
            </span>
          </div>
        )}

        {!loading && results.map((result, i) => <ResultCard key={i} result={result} />)}

        {searched && !loading && results.length === 0 && (
          <div style={{ textAlign:"center", padding:"60px 20px", color:"#666" }}>
            <div style={{ fontSize:48, marginBottom:16 }}>🔍</div>
            <h3>No results found for "{query}"</h3>
            <p>Try different keywords or check spelling</p>
          </div>
        )}

        {searched && !loading && total > 10 && (
          <div style={{ display:"flex", justifyContent:"center", gap:8, marginTop:24, flexWrap:"wrap" }}>
            {Array.from({ length: PAGES }, (_, i) => i + 1).map(pageNum => (
              <button key={pageNum} onClick={() => handleSearch(query, pageNum)}
                style={{ padding:"8px 16px", background: currentPage===pageNum ? "#1a73e8" : "#fff", color: currentPage===pageNum ? "#fff" : "#1a73e8", border:"1px solid #1a73e8", borderRadius:8, cursor:"pointer", fontWeight: currentPage===pageNum ? "bold" : "normal" }}>
                {pageNum}
              </button>
            ))}
          </div>
        )}

        {!searched && !loading && (
          <div style={{ textAlign:"center", padding:"40px 20px", color:"#666" }}>
            <div style={{ fontSize:48, marginBottom:16 }}>🌐</div>
            <h3 style={{ color:"#333" }}>What would you like to find?</h3>
            <p>Search across {stats?.database.total_pages || 0} crawled Wikipedia pages</p>
            <p style={{ fontSize:13, marginTop:8 }}>Powered by Elasticsearch + TF-IDF + PageRank</p>
          </div>
        )}

      </div>
    </div>
  );
}
