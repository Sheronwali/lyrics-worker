# lyrics_api_improved_get_cors.py
import os
import re
import time
import hashlib
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client
from upstash_redis import Redis
import syncedlyrics  # external fetcher
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter, Histogram, Gauge

# --- Load environment variables ---
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
UPSTASH_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase configuration")
if not UPSTASH_URL or not UPSTASH_TOKEN:
    raise RuntimeError("Missing Upstash Redis configuration")

# --- Clients ---
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
r = Redis(url=UPSTASH_URL, token=UPSTASH_TOKEN)

# --- Logging ---
logger = logging.getLogger("lyrics_api")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)

# --- FastAPI App ---
app = FastAPI(title="Lyrics Saver API (GET version, CORS)", version="1.3")
Instrumentator().instrument(app).expose(app)

# --- CORS setup ---
origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:5173",  # Vite dev server
    "http://127.0.0.1:5173",
    "https://9afaf3854bf6.ngrok-free.app",
    "https://be0a9166c4a2.ngrok-free.app",
    "https://eead2fdd112c.ngrok-free.app",
    "https://eead2fdd112c.ngrok-free.app",
    "https://5064c44b8d66.ngrok-free.app",
    "https://0d6a81ce5acb.ngrok-free.app",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # for dev you can use ["*"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Prometheus Metrics ---
cache_hits = Counter('lyrics_cache_hits_total', 'Number of cache hits by source', ['source'])
cache_misses = Counter('lyrics_cache_misses_total', 'Number of times lyrics were not found anywhere')
fetch_duration = Histogram('lyrics_fetch_duration_seconds', 'Time spent fetching lyrics', ['source'], buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0])
external_fetch_failures = Counter('lyrics_external_fetch_failures_total', 'Number of failed external lyrics fetches')
redis_errors = Counter('lyrics_redis_errors_total', 'Number of Redis operation failures', ['operation'])
supabase_errors = Counter('lyrics_supabase_errors_total', 'Number of Supabase operation failures', ['operation'])
migrations_performed = Counter('lyrics_migrations_total', 'Number of legacy rows migrated to new schema')
redis_cache_size = Gauge('lyrics_redis_cache_size', 'Approximate number of songs in Redis cache')
lrc_parse_errors = Counter('lyrics_lrc_parse_errors_total', 'Number of LRC parsing failures')

# --- Models ---
class Line(BaseModel):
    time: int
    text: str

class ParsedLyrics(BaseModel):
    lines: List[Line]
    source: Optional[str] = None
    language: Optional[str] = None
    lrc_raw: Optional[str] = None

# --- Utilities ---
def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())

def normalize_key(artist: str, title: str) -> str:
    norm = f"{normalize_text(artist)}|{normalize_text(title)}"
    return hashlib.sha1(norm.encode("utf-8")).hexdigest()

lrc_timestamp_re = re.compile(r"\[(\d{1,2}):(\d{2})(?:\.(\d{1,3}))?\]")

def parse_lrc(lrc: str) -> ParsedLyrics:
    lines_out: List[Dict[str, Any]] = []
    if not lrc:
        return ParsedLyrics(lines=[], lrc_raw=None)

    try:
        for raw_line in lrc.splitlines():
            raw_line = raw_line.rstrip("\n\r")
            matches = list(lrc_timestamp_re.finditer(raw_line))
            if matches:
                text = raw_line[matches[-1].end():].strip()
                for m in matches:
                    mm = int(m.group(1))
                    ss = int(m.group(2))
                    ms = int(m.group(3) or 0)
                    if len(m.group(3) or "") == 1:
                        ms *= 100
                    elif len(m.group(3) or "") == 2:
                        ms *= 10
                    total_ms = (mm * 60 + ss) * 1000 + ms
                    lines_out.append({"time": total_ms, "text": text})
            elif raw_line.strip():
                lines_out.append({"time": -1, "text": raw_line.strip()})

        lines_out.sort(key=lambda x: (x["time"] if x["time"] >= 0 else 10**12, x["text"]))
        return ParsedLyrics(lines=[Line(**l) for l in lines_out], lrc_raw=lrc)
    except Exception as exc:
        lrc_parse_errors.inc()
        logger.exception("LRC parsing failed: %s", exc)
        return ParsedLyrics(lines=[], lrc_raw=lrc)

# --- Database helpers ---
def db_upsert_lyrics(song_identifier: str, artist: str, title: str, parsed: ParsedLyrics, source: str = "syncedlyrics"):
    payload = {
        "song_identifier": song_identifier,
        "artist": artist,
        "title": title,
        "lines": parsed.dict(),
        "lrc_raw": parsed.lrc_raw,
        "source": source,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "last_accessed_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        resp = supabase.table("lyrics").upsert(payload, on_conflict="song_identifier").execute()
        if resp.error:
            supabase_errors.labels(operation='upsert').inc()
            logger.warning("Supabase upsert error: %s", resp.error)
        else:
            logger.info("Upserted lyrics for %s", song_identifier)
    except Exception as exc:
        supabase_errors.labels(operation='upsert').inc()
        logger.exception("Supabase upsert exception: %s", exc)

def db_get_lyrics(song_identifier: str) -> Optional[Dict[str, Any]]:
    try:
        resp = supabase.table("lyrics").select("*").eq("song_identifier", song_identifier).limit(1).execute()
        if resp.error:
            supabase_errors.labels(operation='read').inc()
            logger.warning("Supabase read error: %s", resp.error)
            return None
        if resp.data:
            return resp.data[0]
        return None
    except Exception as exc:
        supabase_errors.labels(operation='read').inc()
        logger.exception("Supabase read exception: %s", exc)
        return None

def migrate_row_if_plaintext(row: Dict[str, Any]) -> Dict[str, Any]:
    if row.get("lines"):
        return row
    migrations_performed.inc()
    parsed = parse_lrc(row.get("lrc_raw") or "")
    try:
        db_upsert_lyrics(row["song_identifier"], row.get("artist", ""), row.get("title", ""), parsed, source=row.get("source", "legacy"))
    except Exception:
        logger.exception("Migration failed for %s", row.get("song_identifier"))
    row["lines"] = parsed.dict()
    return row

# --- Redis helpers ---
REDIS_TTL = 86400  # 24 hours

def redis_get(song_identifier: str) -> Optional[Dict[str, Any]]:
    try:
        val = r.get(song_identifier)
        if not val:
            return None
        import json
        return json.loads(val if isinstance(val, str) else val.decode("utf-8"))
    except Exception as exc:
        redis_errors.labels(operation='get').inc()
        logger.warning("Redis GET failed: %s", exc)
        return None

def redis_set(song_identifier: str, payload: Dict[str, Any], ttl: int = REDIS_TTL):
    try:
        import json
        r.setex(song_identifier, ttl, json.dumps(payload))
        logger.info("Cached in Redis: %s", song_identifier)
    except Exception as exc:
        redis_errors.labels(operation='set').inc()
        logger.warning("Redis SET failed: %s", exc)

# --- External fetcher ---
def fetch_online(artist: str, title: str) -> Optional[ParsedLyrics]:
    try:
        raw = syncedlyrics.search(f"{artist} - {title}")
        if not raw:
            return None
        parsed = parse_lrc(raw)
        parsed.source = "syncedlyrics"
        return parsed
    except Exception as exc:
        external_fetch_failures.inc()
        logger.exception("External fetch failed: %s", exc)
        return None

# --- API Endpoint ---
@app.get("/lyrics")
def get_or_store_lyrics(
    background_tasks: BackgroundTasks,
    artist: str = Query(..., description="Artist name"),
    title: str = Query(..., description="Song title"),
    raw: bool = Query(False, description="Return raw LRC text instead of parsed lines")
):
    song_id = normalize_key(artist, title)
    timings = {}

    # Redis
    t0 = time.perf_counter()
    cached = redis_get(song_id)
    timings["redis_lookup_ms"] = round((time.perf_counter() - t0) * 1000, 2)
    fetch_duration.labels(source='redis').observe(timings["redis_lookup_ms"] / 1000)

    if cached:
        cache_hits.labels(source='redis').inc()
        background_tasks.add_task(
            lambda: supabase.table("lyrics").update({"last_accessed_at": "now()"}).eq("song_identifier", song_id).execute()
        )
        return {
            "message": "✅ Lyrics from Redis cache",
            "artist": artist,
            "title": title,
            "source": cached.get("source", "redis"),
            "timings": timings,
            "lyrics": cached.get("lrc_raw") if raw else cached.get("lines")
        }

    # Supabase
    t0 = time.perf_counter()
    row = db_get_lyrics(song_id)
    timings["supabase_lookup_ms"] = round((time.perf_counter() - t0) * 1000, 2)
    fetch_duration.labels(source='supabase').observe(timings["supabase_lookup_ms"] / 1000)

    if row:
        cache_hits.labels(source='supabase').inc()
        row = migrate_row_if_plaintext(row)
        redis_set(song_id, {"lines": row["lines"], "lrc_raw": row.get("lrc_raw"), "source": row.get("source")})
        return {
            "message": "✅ Lyrics from Supabase (cached)",
            "artist": artist,
            "title": title,
            "source": row.get("source", "supabase"),
            "timings": timings,
            "lyrics": row.get("lrc_raw") if raw else row.get("lines")
        }

    # Online fetch
    t0 = time.perf_counter()
    parsed = fetch_online(artist, title)
    timings["online_fetch_ms"] = round((time.perf_counter() - t0) * 1000, 2)
    fetch_duration.labels(source='online').observe(timings["online_fetch_ms"] / 1000)

    if not parsed:
        cache_misses.inc()
        raise HTTPException(status_code=404, detail="Lyrics not found anywhere")

    cache_hits.labels(source='online').inc()
    redis_set(song_id, {"lines": parsed.dict(), "lrc_raw": parsed.lrc_raw, "source": parsed.source})
    background_tasks.add_task(db_upsert_lyrics, song_id, artist, title, parsed, parsed.source or "online")

    return {
        "message": "🌐 Lyrics fetched online and cached",
        "artist": artist,
        "title": title,
        "source": parsed.source or "online",
        "timings": timings,
        "lyrics": parsed.lrc_raw if raw else parsed.dict()
    }

@app.get("/healthz")
def healthz():
    ok = True
    reasons = []
    try:
        r.ping()
    except Exception as exc:
        ok = False
        reasons.append(f"redis: {exc}")
    try:
        resp = supabase.table("lyrics").select("song_identifier").limit(1).execute()
        if resp.error:
            ok = False
            reasons.append("supabase: bad response")
    except Exception as exc:
        ok = False
        reasons.append(f"supabase: {exc}")
    return {"status": "ok" if ok else "unhealthy", "reasons": reasons}

@app.on_event("shutdown")
def shutdown():
    logger.info("Shutdown event triggered.")
