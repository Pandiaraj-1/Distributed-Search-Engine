import { useState, useEffect, useRef } from "react";
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
}

/* ── tiny Google-colour logo ── */
function Logo({ size = 92 }: { size?: number }) {
  const s = { fontSize: size, fontWeight: 700, letterSpacing: -2, lineHeight: 1, fontFamily: "Product Sans,Arial,sans-serif", userSelect: "none" as const };
  return (
    <span style={s}>
      <span style={{ color: "#4285F4" }}>S</span>
      <span style={{ color: "#EA4335" }}>e</span>
      <span style={{ color: "#FBBC05" }}>a</span>
      <span style={{ color: "#4285F4" }}>r</span>
      <span style={{ color: "#34A853" }}>c</span>
      <span style={{ color: "#EA4335" }}>h</span>
      <span style={{ color: "#4285F4" }}>X</span>
    </span>
  );
}

function ResultCard({ result, query }: { result: SearchResult; query: string }) {
  const domain = result.url.replace(/https?:\/\//, "").split("/")[0];
  const path   = result.url.replace(/https?:\/\/[^/]+/, "").slice(0, 55) || "/";
  const fmtDate = (d: string) => d ? new Date(d).toLocaleDateString("en-US", { year:"numeric", month:"short", day:"numeric" }) : "";

  const highlight = (text: string) => {
    if (!query) return text;
    const re = new RegExp(`(${query.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")})`, "gi");
    const parts = text.split(re);
    return parts.map((p, i) => re.test(p) ? <strong key={i} style={{ color:"#000", fontWeight:700 }}>{p}</strong> : p);
  };

  return (
    <div style={{ marginBottom: 28, maxWidth: 650 }}>
      {/* Site info row */}
      <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:4 }}>
        <div style={{ width:26, height:26, borderRadius:"50%", background:"#f1f3f4", display:"flex", alignItems:"center", justifyContent:"center", fontSize:13 }}>
          🌐
        </div>
        <div>
          <div style={{ fontSize:14, color:"#202124", lineHeight:1.3 }}>{domain}</div>
          <div style={{ fontSize:12, color:"#4d5156" }}>{domain}{path}</div>
        </div>
      </div>

      {/* Title */}
      <a href={result.url} target="_blank" rel="noreferrer"
        style={{ fontSize:20, color:"#1a0dab", textDecoration:"none", lineHeight:1.3, display:"block", marginBottom:3 }}
        onMouseEnter={e => (e.currentTarget.style.textDecoration = "underline")}
        onMouseLeave={e => (e.currentTarget.style.textDecoration = "none")}
      >
        {highlight(result.title)}
      </a>

      {/* Snippet */}
      <div style={{ fontSize:14, color:"#4d5156", lineHeight:1.58 }}>
        <span style={{ color:"#70757a", fontSize:13 }}>{fmtDate(result.crawled_at)} — </span>
        Wikipedia article about {result.title.replace(" - Wikipedia", "").toLowerCase()}.
        Relevance score <strong>{result.score}</strong>, PageRank <strong>{result.page_rank}</strong>.
      </div>

      {/* Score chips */}
      <div style={{ display:"flex", gap:6, marginTop:6, flexWrap:"wrap" }}>
        <span style={{ background:"#e8f0fe", color:"#1967d2", padding:"2px 10px", borderRadius:12, fontSize:11, fontWeight:500 }}>Score {result.score}</span>
        <span style={{ background:"#fce8e6", color:"#c5221f", padding:"2px 10px", borderRadius:12, fontSize:11 }}>TF-IDF {result.tfidf_score}</span>
        <span style={{ background:"#e6f4ea", color:"#137333", padding:"2px 10px", borderRadius:12, fontSize:11 }}>PageRank {result.page_rank}</span>
      </div>
    </div>
  );
}

/* ── search icon SVG ── */
function SearchIcon() {
  return (
    <svg style={{ width:20, height:20, fill:"#9aa0a6", flexShrink:0 }} viewBox="0 0 24 24">
      <path d="M15.5 14h-.79l-.28-.27A6.471 6.471 0 0 0 16 9.5 6.5 6.5 0 1 0 9.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 4.99L20.49 19l-4.99-5zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14z"/>
    </svg>
  );
}

function MicIcon() {
  return (
    <svg style={{ width:20, height:20, fill:"#4285f4", flexShrink:0, cursor:"pointer" }} viewBox="0 0 24 24">
      <path d="M12 14c1.66 0 3-1.34 3-3V5c0-1.66-1.34-3-3-3S9 3.34 9 5v6c0 1.66 1.34 3 3 3zm-1-9c0-.55.45-1 1-1s1 .45 1 1v6c0 .55-.45 1-1 1s-1-.45-1-1V5zm6 6c0 2.76-2.24 5-5 5s-5-2.24-5-5H5c0 3.53 2.61 6.43 6 6.92V21h2v-3.08c3.39-.49 6-3.39 6-6.92h-2z"/>
    </svg>
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
  const [inputFocused, setInputFocused]       = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    axios.get(`${API}/stats`).then(r => setStats(r.data)).catch(() => {});
  }, []);

  const handleSearch = async (q: string, page = 1) => {
    if (!q.trim()) return;
    setLoading(true); setSearched(true); setShowSuggestions(false);
    const t0 = Date.now();
    try {
      const res = await axios.get(`${API}/search?q=${encodeURIComponent(q)}&page=${page}&size=10`);
      setResults(res.data.results);
      setTotal(res.data.total);
      setCurrentPage(page);
      setSource(res.data.source);
      setSearchTime(Date.now() - t0);
    } catch {}
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
    } else { setSuggestions([]); setShowSuggestions(false); }
  };

  const pickSuggestion = (s: string) => { setQuery(s); setSuggestions([]); setShowSuggestions(false); handleSearch(s); };
  const goHome = () => { setSearched(false); setQuery(""); setResults([]); setSuggestions([]); };

  const QUICK = ["Python", "Machine learning", "Artificial intelligence", "Database", "Algorithm"];
  const PAGES = Math.min(7, Math.ceil(total / 10));

  /* ─────────────── HOME PAGE ─────────────── */
  if (!searched) return (
    <div style={{ minHeight:"100vh", background:"#fff", fontFamily:"arial,sans-serif", display:"flex", flexDirection:"column" }}>

      {/* top nav */}
      <div style={{ display:"flex", justifyContent:"flex-end", padding:"14px 20px", gap:8 }}>
        <a href="#" style={{ fontSize:13, color:"#202124", textDecoration:"none", padding:"8px 12px" }}>About</a>
        <a href="#" style={{ fontSize:13, color:"#202124", textDecoration:"none", padding:"8px 12px" }}>Store</a>
        <div style={{ width:1, background:"#e0e0e0", margin:"4px 4px" }} />
        <a href="#" style={{ fontSize:13, color:"#202124", textDecoration:"none", padding:"8px 12px" }}>Gmail</a>
        <a href="#" style={{ fontSize:13, color:"#202124", textDecoration:"none", padding:"8px 12px" }}>Images</a>
        <div style={{ width:32, height:32, borderRadius:"50%", background:"#4285f4", display:"flex", alignItems:"center", justifyContent:"center", cursor:"pointer", marginLeft:4 }}>
          <span style={{ color:"#fff", fontSize:14, fontWeight:600 }}>P</span>
        </div>
      </div>

      {/* centre content */}
      <div style={{ flex:1, display:"flex", flexDirection:"column", alignItems:"center", justifyContent:"center", paddingBottom:80 }}>
        <div style={{ marginBottom:28 }}>
          <Logo size={92} />
        </div>

        {/* search input */}
        <div style={{ position:"relative", width: 584, maxWidth:"90vw" }}>
          <div style={{ display:"flex", alignItems:"center", border: inputFocused ? "1px solid transparent" : "1px solid #dfe1e5", borderRadius:24, padding:"10px 16px", gap:12, background:"#fff", boxShadow: inputFocused ? "0 4px 12px rgba(0,0,0,0.15)" : "none", transition:"box-shadow .2s" }}
            onMouseEnter={e => { if (!inputFocused) (e.currentTarget as HTMLDivElement).style.boxShadow="0 1px 6px rgba(32,33,36,.28)"; }}
            onMouseLeave={e => { if (!inputFocused) (e.currentTarget as HTMLDivElement).style.boxShadow="none"; }}
          >
            <SearchIcon />
            <input
              ref={inputRef}
              value={query}
              onChange={e => handleTyping(e.target.value)}
              onFocus={() => setInputFocused(true)}
              onBlur={() => setTimeout(() => setInputFocused(false), 150)}
              onKeyDown={e => { if (e.key==="Enter") handleSearch(query); if (e.key==="Escape") setShowSuggestions(false); }}
              style={{ flex:1, border:"none", outline:"none", fontSize:16, color:"#202124", background:"transparent" }}
              autoFocus
            />
            <MicIcon />
          </div>

          {/* suggestions dropdown */}
          {showSuggestions && suggestions.length > 0 && (
            <div style={{ position:"absolute", top:50, left:0, right:0, background:"#fff", borderRadius:"0 0 24px 24px", boxShadow:"0 4px 6px rgba(32,33,36,.28)", zIndex:100, paddingBottom:8, border:"1px solid #dfe1e5", borderTop:"none" }}>
              <div style={{ height:1, background:"#e8eaed", margin:"0 14px 8px" }} />
              {suggestions.map((s, i) => (
                <div key={i} onClick={() => pickSuggestion(s)}
                  style={{ display:"flex", alignItems:"center", gap:12, padding:"8px 16px", cursor:"pointer", fontSize:14, color:"#202124" }}
                  onMouseEnter={e => (e.currentTarget.style.background="#f8f9fa")}
                  onMouseLeave={e => (e.currentTarget.style.background="transparent")}
                >
                  <SearchIcon />
                  <span>{s}</span>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* buttons */}
        <div style={{ display:"flex", gap:12, marginTop:28 }}>
          <button onClick={() => handleSearch(query)}
            style={{ padding:"10px 22px", background:"#f8f9fa", color:"#3c4043", border:"1px solid #f8f9fa", borderRadius:4, fontSize:14, cursor:"pointer", fontFamily:"arial,sans-serif" }}
            onMouseEnter={e => { (e.currentTarget.style.border="1px solid #dadce0"); (e.currentTarget.style.boxShadow="0 1px 2px rgba(0,0,0,.1)"); }}
            onMouseLeave={e => { (e.currentTarget.style.border="1px solid #f8f9fa"); (e.currentTarget.style.boxShadow="none"); }}
          >
            SearchX Search
          </button>
          <button onClick={() => { const t = QUICK[Math.floor(Math.random()*QUICK.length)]; setQuery(t); handleSearch(t); }}
            style={{ padding:"10px 22px", background:"#f8f9fa", color:"#3c4043", border:"1px solid #f8f9fa", borderRadius:4, fontSize:14, cursor:"pointer", fontFamily:"arial,sans-serif" }}
            onMouseEnter={e => { (e.currentTarget.style.border="1px solid #dadce0"); (e.currentTarget.style.boxShadow="0 1px 2px rgba(0,0,0,.1)"); }}
            onMouseLeave={e => { (e.currentTarget.style.border="1px solid #f8f9fa"); (e.currentTarget.style.boxShadow="none"); }}
          >
            I'm Feeling Lucky
          </button>
        </div>

        {/* quick topics */}
        <div style={{ marginTop:20, display:"flex", gap:8, flexWrap:"wrap", justifyContent:"center" }}>
          {QUICK.map(t => (
            <span key={t} onClick={() => { setQuery(t); handleSearch(t); }}
              style={{ padding:"6px 14px", background:"#f1f3f4", borderRadius:16, fontSize:13, color:"#3c4043", cursor:"pointer" }}
              onMouseEnter={e => (e.currentTarget.style.background="#e8eaed")}
              onMouseLeave={e => (e.currentTarget.style.background="#f1f3f4")}
            >
              {t}
            </span>
          ))}
        </div>

        {stats && (
          <p style={{ marginTop:24, fontSize:12, color:"#70757a" }}>
            Searching across <strong>{stats.database.total_pages.toLocaleString()}</strong> pages &nbsp;•&nbsp; Powered by Elasticsearch + TF-IDF + PageRank
          </p>
        )}
      </div>

      {/* footer */}
      <div style={{ background:"#f2f2f2", borderTop:"1px solid #e4e4e4" }}>
        <div style={{ padding:"14px 24px", fontSize:13, color:"#70757a", display:"flex", justifyContent:"space-between", flexWrap:"wrap", gap:8 }}>
          <span>India</span>
          <div style={{ display:"flex", gap:20 }}>
            <a href="#" style={{ color:"#70757a", textDecoration:"none" }}>Privacy</a>
            <a href="#" style={{ color:"#70757a", textDecoration:"none" }}>Terms</a>
            <a href="#" style={{ color:"#70757a", textDecoration:"none" }}>Settings</a>
          </div>
        </div>
      </div>
    </div>
  );

  /* ─────────────── RESULTS PAGE ─────────────── */
  return (
    <div style={{ minHeight:"100vh", background:"#fff", fontFamily:"arial,sans-serif" }}>

      {/* top bar */}
      <div style={{ display:"flex", alignItems:"center", padding:"16px 20px 0", gap:16, borderBottom:"1px solid #ebebeb", paddingBottom:12 }}>
        {/* logo */}
        <div onClick={goHome} style={{ cursor:"pointer", flexShrink:0 }}>
          <Logo size={32} />
        </div>

        {/* search bar */}
        <div style={{ position:"relative", flex:1, maxWidth:690 }}>
          <div style={{ display:"flex", alignItems:"center", border:"1px solid #dfe1e5", borderRadius:24, padding:"8px 16px", gap:12, background:"#fff", boxShadow: inputFocused ? "0 4px 12px rgba(0,0,0,0.15)" : "none" }}
            onMouseEnter={e => (e.currentTarget.style.boxShadow="0 1px 6px rgba(32,33,36,.28)")}
            onMouseLeave={e => { if (!inputFocused) e.currentTarget.style.boxShadow="none"; }}
          >
            <input
              value={query}
              onChange={e => handleTyping(e.target.value)}
              onFocus={() => setInputFocused(true)}
              onBlur={() => setTimeout(() => setInputFocused(false), 150)}
              onKeyDown={e => { if (e.key==="Enter") handleSearch(query); if (e.key==="Escape") setShowSuggestions(false); }}
              style={{ flex:1, border:"none", outline:"none", fontSize:16, color:"#202124", background:"transparent" }}
            />
            {query && (
              <span onClick={() => { setQuery(""); setSuggestions([]); inputRef.current?.focus(); }}
                style={{ cursor:"pointer", color:"#70757a", fontSize:20, lineHeight:1, padding:"0 4px" }}>×</span>
            )}
            <div style={{ width:1, height:24, background:"#dfe1e5", margin:"0 4px" }} />
            <MicIcon />
            <div onClick={() => handleSearch(query)} style={{ cursor:"pointer" }}><SearchIcon /></div>
          </div>

          {/* autocomplete on results page */}
          {showSuggestions && suggestions.length > 0 && (
            <div style={{ position:"absolute", top:48, left:0, right:0, background:"#fff", borderRadius:"0 0 24px 24px", boxShadow:"0 4px 6px rgba(32,33,36,.28)", zIndex:100, paddingBottom:8, border:"1px solid #dfe1e5", borderTop:"none" }}>
              <div style={{ height:1, background:"#e8eaed", margin:"0 14px 8px" }} />
              {suggestions.map((s, i) => (
                <div key={i} onClick={() => pickSuggestion(s)}
                  style={{ display:"flex", alignItems:"center", gap:12, padding:"8px 16px", cursor:"pointer", fontSize:14, color:"#202124" }}
                  onMouseEnter={e => (e.currentTarget.style.background="#f8f9fa")}
                  onMouseLeave={e => (e.currentTarget.style.background="transparent")}
                >
                  <SearchIcon />
                  <span>{s}</span>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* right icons */}
        <div style={{ display:"flex", gap:8, marginLeft:"auto", flexShrink:0 }}>
          <a href="#" style={{ fontSize:13, color:"#202124", textDecoration:"none", padding:"8px" }}>Images</a>
          <div style={{ width:32, height:32, borderRadius:"50%", background:"#4285f4", display:"flex", alignItems:"center", justifyContent:"center", cursor:"pointer" }}>
            <span style={{ color:"#fff", fontSize:14, fontWeight:600 }}>P</span>
          </div>
        </div>
      </div>

      {/* tabs (decorative) */}
      <div style={{ display:"flex", gap:0, padding:"8px 0 0 170px", borderBottom:"1px solid #ebebeb" }}>
        {["All","Images","News","Videos","Maps"].map((tab, i) => (
          <div key={tab} style={{ padding:"8px 16px", fontSize:13, color: i===0 ? "#1a73e8" : "#70757a", borderBottom: i===0 ? "3px solid #1a73e8" : "3px solid transparent", cursor:"pointer", fontWeight: i===0 ? 500 : 400 }}>
            {tab}
          </div>
        ))}
      </div>

      {/* results area */}
      <div style={{ display:"flex", padding:"0 0 0 170px" }}>
        <div style={{ maxWidth:652, flex:1, padding:"20px 0" }}>

          {/* stats bar */}
          {!loading && results.length > 0 && (
            <div style={{ fontSize:13, color:"#70757a", marginBottom:20, display:"flex", alignItems:"center", gap:8 }}>
              About <strong style={{ color:"#202124" }}>{total.toLocaleString()}</strong> results ({(searchTime/1000).toFixed(2)} seconds)
              {source === "cache" && (
                <span style={{ background:"#e6f4ea", color:"#137333", padding:"1px 8px", borderRadius:10, fontSize:11, fontWeight:500 }}>⚡ Cached</span>
              )}
            </div>
          )}

          {/* loading */}
          {loading && (
            <div style={{ padding:"60px 0", textAlign:"center" }}>
              <div style={{ display:"inline-flex", gap:6 }}>
                {["#4285F4","#EA4335","#FBBC05","#34A853"].map((c,i) => (
                  <div key={i} style={{ width:12, height:12, borderRadius:"50%", background:c, animation:`bounce 1s ${i*0.15}s infinite alternate` }} />
                ))}
              </div>
              <style>{`@keyframes bounce { from{transform:translateY(0)} to{transform:translateY(-10px)} }`}</style>
            </div>
          )}

          {/* results */}
          {!loading && results.map((r, i) => <ResultCard key={i} result={r} query={query} />)}

          {/* no results */}
          {!loading && searched && results.length === 0 && (
            <div style={{ padding:"40px 0" }}>
              <p style={{ fontSize:16, color:"#202124" }}>Your search — <strong>{query}</strong> — did not match any documents.</p>
              <p style={{ fontSize:14, color:"#70757a", marginTop:8 }}>Suggestions:</p>
              <ul style={{ color:"#70757a", fontSize:14, lineHeight:2 }}>
                <li>Make sure all words are spelled correctly.</li>
                <li>Try different keywords.</li>
                <li>Try more general keywords.</li>
              </ul>
            </div>
          )}

          {/* pagination */}
          {!loading && total > 10 && (
            <div style={{ display:"flex", alignItems:"center", justifyContent:"center", gap:4, margin:"40px 0 60px", userSelect:"none" }}>
              <Logo size={32} />
              <div style={{ display:"flex", gap:0, marginLeft:16 }}>
                {currentPage > 1 && (
                  <button onClick={() => handleSearch(query, currentPage-1)}
                    style={{ padding:"8px 14px", border:"none", background:"none", color:"#1a73e8", fontSize:13, cursor:"pointer", borderRadius:4 }}>
                    ‹ Previous
                  </button>
                )}
                {Array.from({ length: PAGES }, (_,i) => i+1).map(p => (
                  <button key={p} onClick={() => handleSearch(query, p)}
                    style={{ width:36, height:36, border:"none", color: currentPage===p ? "#fff" : "#1a73e8", background: currentPage===p ? "#1a73e8" : "transparent" as any, borderRadius:"50%", fontSize:13, cursor:"pointer", fontWeight: currentPage===p ? 700 : 400 }}>
                    {p}
                  </button>
                ))}
                {currentPage < PAGES && (
                  <button onClick={() => handleSearch(query, currentPage+1)}
                    style={{ padding:"8px 14px", border:"none", background:"none", color:"#1a73e8", fontSize:13, cursor:"pointer", borderRadius:4 }}>
                    Next ›
                  </button>
                )}
              </div>
            </div>
          )}
        </div>
      </div>

      {/* footer */}
      <div style={{ background:"#f2f2f2", borderTop:"1px solid #e4e4e4" }}>
        <div style={{ padding:"14px 24px", fontSize:13, color:"#70757a", display:"flex", justifyContent:"space-between", flexWrap:"wrap", gap:8 }}>
          <span>India</span>
          <div style={{ display:"flex", gap:20 }}>
            <a href="#" style={{ color:"#70757a", textDecoration:"none" }}>Privacy</a>
            <a href="#" style={{ color:"#70757a", textDecoration:"none" }}>Terms</a>
            <a href="#" style={{ color:"#70757a", textDecoration:"none" }}>Settings</a>
          </div>
        </div>
      </div>
    </div>
  );
}
