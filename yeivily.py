#!/usr/bin/env python3
"""
Yeivily 1.0 - local, zero-key, Tavily-style search server.

Search backend : DuckDuckGo through the `ddgs` package (the only external dependency).
Endpoints      : POST /search            Tavily-style search
                 POST /search/context    Yeivily extension: one text blob for RAG
                 POST /extract           Tavily-style page extraction
                 GET|POST /html          DuckDuckGo-HTML-like page (for scrapers)
                 GET  /health            liveness probe
Highlights     : LRU + TTL cache, single-flight de-duplication, upstream concurrency
                 limit with rate-limit cool-down (errors are reported, never disguised
                 as "0 results"), SSRF-safe page extraction, charset-aware decoding,
                 optional bearer auth, browser/DNS-rebinding guard.

Run:
    python yeivily.py [--host 127.0.0.1] [--port 8000] [-q] [--fake-answer]

Environment variables (all optional):
    YEIVILY_HOST, YEIVILY_PORT        bind address (default 127.0.0.1:8000)
    YEIVILY_API_KEY                   if set, require "Authorization: Bearer <key>"
    YEIVILY_REGION                    ddgs region, default "wt-wt" (no regional bias)
    YEIVILY_SAFESEARCH                on | moderate | off   (default "moderate")
    YEIVILY_CACHE_TTL                 seconds (default 600)
    YEIVILY_CACHE_MAX                 entries (default 256)
    YEIVILY_DEEP_TOP_N                results enriched with page text (default 3)
    YEIVILY_ALLOW_PRIVATE_FETCH=1     let page extraction reach private/LAN addresses
    YEIVILY_ALLOWED_HOSTS             extra Host header values accepted (comma list)
    YEIVILY_FAKE_ANSWER=1             fill "answer" with concatenated snippets (opt-in)
"""

from __future__ import annotations

import argparse
import codecs
import copy
import hmac
import html
import ipaddress
import json
import logging
import os
import random
import re
import socket
import ssl
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, wait
from html.parser import HTMLParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

__version__ = "1.0.0"
log = logging.getLogger("yeivily")

# ------------------------------------------------------------------------------
# Search backend (modern `ddgs`, legacy `duckduckgo_search` as fallback)
# ------------------------------------------------------------------------------
try:
    from ddgs import DDGS
except ImportError:
    try:
        from duckduckgo_search import DDGS  # legacy package name
    except ImportError:
        DDGS = None


# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
def _env_num(name: str, default, cast=float):
    try:
        return cast(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


CACHE_TTL_SECONDS = _env_num("YEIVILY_CACHE_TTL", 600.0)
CACHE_MAX_ENTRIES = _env_num("YEIVILY_CACHE_MAX", 256, int)
REGION = os.environ.get("YEIVILY_REGION", "wt-wt")
SAFESEARCH = os.environ.get("YEIVILY_SAFESEARCH", "moderate")
API_KEY = os.environ.get("YEIVILY_API_KEY") or None
DEEP_TOP_N = _env_num("YEIVILY_DEEP_TOP_N", 3, int)
DEEP_MAX_CHARS = 4000          # per result, search enrichment
EXTRACT_MAX_CHARS = 12000      # per URL, /extract
DEEP_DEADLINE = 8.0            # seconds to wait for page fetches in /search
EXTRACT_DEADLINE = 20.0        # seconds to wait for page fetches in /extract
MAX_FETCH_BYTES = 400_000      # hard cap per page download
MAX_BODY_BYTES = 65_536        # hard cap per request body
UPSTREAM_CONCURRENCY = 2       # simultaneous queries sent to DuckDuckGo
RATELIMIT_COOLDOWN = 15.0      # seconds of fail-fast after an upstream rate-limit
ALLOW_PRIVATE_FETCH = _env_flag("YEIVILY_ALLOW_PRIVATE_FETCH")
FAKE_ANSWER = _env_flag("YEIVILY_FAKE_ANSWER")

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0"} | {
    h.strip().lower()
    for h in os.environ.get("YEIVILY_ALLOWED_HOSTS", "").split(",")
    if h.strip()
}

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


class ApiError(Exception):
    """An error that maps cleanly onto an HTTP status code."""

    def __init__(self, status: int, message: str, headers: dict | None = None):
        super().__init__(message)
        self.status = status
        self.message = message
        self.headers = headers or {}


# ------------------------------------------------------------------------------
# Cache (LRU + TTL, thread-safe) and single-flight de-duplication
# ------------------------------------------------------------------------------
class TTLCache:
    def __init__(self, ttl: float, max_entries: int):
        self.ttl = ttl
        self.max_entries = max(1, max_entries)
        self._data: OrderedDict = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key):
        now = time.monotonic()
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            ts, value = entry
            if now - ts >= self.ttl:
                del self._data[key]
                return None
            self._data.move_to_end(key)
            return copy.deepcopy(value)  # callers can never corrupt the cache

    def set(self, key, value) -> None:
        with self._lock:
            self._data[key] = (time.monotonic(), copy.deepcopy(value))
            self._data.move_to_end(key)
            while len(self._data) > self.max_entries:
                self._data.popitem(last=False)

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)


class _Call:
    __slots__ = ("event", "value", "error")

    def __init__(self):
        self.event = threading.Event()
        self.value = None
        self.error: BaseException | None = None


class SingleFlight:
    """Concurrent identical requests share one upstream call (agents love to repeat themselves)."""

    def __init__(self):
        self._lock = threading.Lock()
        self._calls: dict = {}

    def do(self, key, fn):
        with self._lock:
            call = self._calls.get(key)
            leader = call is None
            if leader:
                call = self._calls[key] = _Call()
        if not leader:
            call.event.wait()
            if call.error is not None:
                raise call.error
            return copy.deepcopy(call.value)
        try:
            call.value = fn()
            return call.value
        except BaseException as exc:
            call.error = exc
            raise
        finally:
            with self._lock:
                self._calls.pop(key, None)
            call.event.set()


_cache = TTLCache(CACHE_TTL_SECONDS, CACHE_MAX_ENTRIES)
_flight = SingleFlight()
_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="yeivily-fetch")
_upstream_sem = threading.BoundedSemaphore(UPSTREAM_CONCURRENCY)
_cooldown_until = 0.0
_cooldown_lock = threading.Lock()


# ------------------------------------------------------------------------------
# Text extraction (stdlib only)
# ------------------------------------------------------------------------------
_BLOCK_TAGS = frozenset({
    "p", "div", "li", "ul", "ol", "h1", "h2", "h3", "h4", "h5", "h6", "tr", "br",
    "hr", "article", "section", "pre", "blockquote", "table", "dd", "dt", "figcaption",
})
_SKIP_ALWAYS = frozenset({"script", "style", "noscript", "svg", "template", "iframe", "canvas", "title"})
_SKIP_BOILERPLATE = frozenset({"nav", "footer", "aside", "form", "button", "select"})


def _normalize(text: str) -> str:
    out: list[str] = []
    blank = False
    for line in text.splitlines():
        line = re.sub(r"[ \t\u00a0]+", " ", line).strip()
        if line:
            out.append(line)
            blank = False
        elif out and not blank:
            out.append("")
            blank = True
    return "\n".join(out).strip()


def _truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    cut = text[:max_chars]
    space = cut.rfind(" ")
    if space > max_chars * 0.8:
        cut = cut[:space]
    return cut.rstrip() + " ... [truncated]"


class _TextExtractor(HTMLParser):
    """
    Stack-based skipper (a stray unclosed <nav> cannot silently swallow the rest of
    the page the way a plain counter can). <header> is kept on purpose: it usually
    holds the <h1>. Content inside <main>/<article> is preferred when substantial.
    """

    def __init__(self, drop_boilerplate: bool = True):
        super().__init__(convert_charrefs=True)
        self._skip = _SKIP_ALWAYS | (_SKIP_BOILERPLATE if drop_boilerplate else frozenset())
        self._stack: list[str] = []
        self._main_depth = 0
        self._all: list[str] = []
        self._main: list[str] = []

    def _emit(self, s: str) -> None:
        self._all.append(s)
        if self._main_depth:
            self._main.append(s)

    def handle_starttag(self, tag, attrs):
        if tag in self._skip:
            self._stack.append(tag)
            return
        if self._stack:
            return
        if tag in ("main", "article"):
            self._main_depth += 1
        if tag in _BLOCK_TAGS:  # includes <br>, which never gets an end tag
            self._emit("\n")

    def handle_endtag(self, tag):
        if tag in self._skip:
            if tag in self._stack:
                while self._stack and self._stack.pop() != tag:
                    pass
            return
        if self._stack:
            return
        if tag in ("main", "article") and self._main_depth:
            self._main_depth -= 1
        if tag in _BLOCK_TAGS:
            self._emit("\n")

    def handle_data(self, data):
        if not self._stack:
            self._emit(re.sub(r"\s+", " ", data))

    def get_text(self, max_chars: int) -> str:
        main = _normalize("".join(self._main))
        text = main if len(main) >= 400 else _normalize("".join(self._all))
        return _truncate(text, max_chars)


def extract_text(html_str: str, max_chars: int = DEEP_MAX_CHARS) -> str:
    best = ""
    for drop in (True, False):  # second pass: boilerplate filter may have been too greedy
        parser = _TextExtractor(drop_boilerplate=drop)
        try:
            parser.feed(html_str)
            parser.close()
        except Exception:  # malformed markup: keep whatever was parsed
            pass
        text = parser.get_text(max_chars)
        if len(text) > len(best):
            best = text
        if len(best) >= 200:
            break
    return best


def _decode(raw: bytes, content_type: str) -> str:
    """Honour HTTP/meta charset; tolerate a multibyte char cut by the download cap."""
    candidates: list[str] = []
    m = re.search(r"charset=([\w\-]+)", content_type, re.I)
    if m:
        candidates.append(m.group(1))
    m = re.search(rb"<meta[^>]+charset=[\"']?([\w\-]+)", raw[:4096], re.I)
    if m:
        candidates.append(m.group(1).decode("ascii", "ignore"))
    candidates += ["utf-8", "cp1252"]
    for enc in candidates:
        try:
            return codecs.getincrementaldecoder(enc)("strict").decode(raw, final=False)
        except (LookupError, UnicodeDecodeError):
            continue
    return raw.decode("utf-8", errors="replace")


# ------------------------------------------------------------------------------
# Safe page fetching (SSRF guard, redirect re-validation, total deadline)
# ------------------------------------------------------------------------------
class _Blocked(Exception):
    pass


def _check_public_url(url: str) -> None:
    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise _Blocked("unsupported URL")
    if ALLOW_PRIVATE_FETCH:
        return
    port = parts.port or (443 if parts.scheme == "https" else 80)
    try:
        infos = socket.getaddrinfo(parts.hostname, port, type=socket.SOCK_STREAM)
    except socket.gaierror:
        raise _Blocked("DNS resolution failed")
    for info in infos:
        ip = ipaddress.ip_address(info[4][0].split("%")[0])
        if ip.version == 6 and ip.ipv4_mapped:
            ip = ip.ipv4_mapped
        if not ip.is_global:
            raise _Blocked("private/local address")
    # Note: a DNS-rebinding race between this check and the connect is still
    # theoretically possible; pinning the resolved IP would need a custom connection class.


class _SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    max_redirections = 4

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        _check_public_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _make_ssl_context() -> ssl.SSLContext | None:
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        try:
            return ssl.create_default_context()
        except Exception:
            return None


def _build_opener() -> urllib.request.OpenerDirector:
    handlers: list[urllib.request.BaseHandler] = [_SafeRedirectHandler()]
    ctx = _make_ssl_context()
    if ctx is not None:
        handlers.append(urllib.request.HTTPSHandler(context=ctx))
    return urllib.request.build_opener(*handlers)


_opener = _build_opener()


def _read_limited(resp, limit: int, deadline: float) -> bytes:
    chunks, total = [], 0
    while total < limit and time.monotonic() < deadline:
        chunk = resp.read(min(16384, limit - total))
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
    return b"".join(chunks)


def _fetch(url: str, max_chars: int = DEEP_MAX_CHARS, timeout: float = 3.5,
           deadline_s: float = 6.0) -> tuple[str | None, str | None]:
    """Return (text, None) or (None, reason). Never raises."""
    deadline = time.monotonic() + deadline_s
    try:
        _check_public_url(url)
        req = urllib.request.Request(url, headers={
            "User-Agent": _BROWSER_UA,
            "Accept": "text/html,application/xhtml+xml,text/plain;q=0.8,*/*;q=0.5",
            "Accept-Encoding": "identity",
        })
        with _opener.open(req, timeout=timeout) as resp:
            ctype = (resp.headers.get("Content-Type") or "").lower()
            is_html = "html" in ctype or not ctype
            is_text = ctype.startswith("text/") or "json" in ctype
            if not (is_html or is_text):
                return None, f"unsupported content-type: {ctype.split(';')[0]}"
            raw = _read_limited(resp, MAX_FETCH_BYTES, deadline)
        decoded = _decode(raw, ctype)
        if is_html:
            text = extract_text(decoded, max_chars)
        else:
            text = _truncate(_normalize(decoded), max_chars)
        if len(text) < 80:
            return None, "no extractable text (JavaScript-rendered page?)"
        return text, None
    except _Blocked as exc:
        return None, f"blocked: {exc}"
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def fetch_page_content(url: str, timeout: float = 3.5, max_chars: int = DEEP_MAX_CHARS) -> str | None:
    """Backwards-compatible helper (v1 API)."""
    return _fetch(url, max_chars=max_chars, timeout=timeout)[0]


# ------------------------------------------------------------------------------
# Search
# ------------------------------------------------------------------------------
def _truthy(value) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "markdown", "text", "basic", "advanced"}
    return bool(value)


def _as_int(value, default: int, lo: int, hi: int, name: str) -> int:
    if value is None or value == "":
        return default
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ApiError(400, f"'{name}' must be an integer")
    return max(lo, min(number, hi))


_DOMAIN_RE = re.compile(r"^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$")


def _clean_domains(value) -> list[str]:
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, (list, tuple)):
        return []
    out = []
    for item in list(value)[:10]:
        if not isinstance(item, str):
            continue
        domain = re.sub(r"^[a-z]+://", "", item.strip().lower()).split("/")[0]
        if _DOMAIN_RE.match(domain):
            out.append(domain)
    return out


_TIME_MAP = {"d": "d", "day": "d", "w": "w", "week": "w", "m": "m", "month": "m", "y": "y", "year": "y"}


def _timelimit(time_range, days) -> str | None:
    if isinstance(time_range, str) and time_range.strip().lower() in _TIME_MAP:
        return _TIME_MAP[time_range.strip().lower()]
    if days is not None:
        try:
            n = int(days)
        except (TypeError, ValueError):
            return None
        return "d" if n <= 1 else "w" if n <= 7 else "m" if n <= 31 else "y"
    return None


def _is_ratelimit(exc: BaseException) -> bool:
    text = f"{type(exc).__name__} {exc}".lower()
    return "ratelimit" in text or "rate limit" in text or "429" in text


def _ddg_fetch(query: str, max_results: int, news: bool, timelimit: str | None) -> list[dict]:
    global _cooldown_until
    if DDGS is None:
        raise ApiError(503, "Search backend not installed. Run: pip install ddgs")
    with _cooldown_lock:
        remaining = _cooldown_until - time.monotonic()
    if remaining > 0:
        raise ApiError(429, "Upstream search engine is rate-limiting; retry shortly.",
                       {"Retry-After": str(int(remaining) + 1)})

    last: BaseException | None = None
    for attempt in range(2):
        try:
            with _upstream_sem:
                try:
                    client = DDGS(timeout=10)
                except TypeError:
                    client = DDGS()
                call = client.news if news else client.text
                return list(call(query, region=REGION, safesearch=SAFESEARCH,
                                 timelimit=timelimit, max_results=max_results))
        except Exception as exc:
            if "no results" in str(exc).lower():  # ddgs raises instead of returning []
                return []
            last = exc
        if attempt == 0:
            time.sleep(0.8 + random.random() * 0.7)

    assert last is not None
    if _is_ratelimit(last):
        with _cooldown_lock:
            _cooldown_until = time.monotonic() + RATELIMIT_COOLDOWN
        raise ApiError(429, "Upstream search engine is rate-limiting; retry shortly.",
                       {"Retry-After": str(int(RATELIMIT_COOLDOWN))})
    raise ApiError(502, f"Upstream search failed: {type(last).__name__}: {last}")


def _enrich(results: list[dict]) -> None:
    n = min(len(results), DEEP_TOP_N)
    futures = {_pool.submit(_fetch, results[i]["url"], DEEP_MAX_CHARS): i for i in range(n)}
    done, _ = wait(futures, timeout=DEEP_DEADLINE)
    for fut in done:
        text, _err = fut.result()
        if text:
            results[futures[fut]]["raw_content"] = text


def perform_search(query, max_results=5, include_raw_content=False, search_depth="basic",
                   topic="general", time_range=None, days=None,
                   include_domains=None, exclude_domains=None) -> list[dict]:
    query = re.sub(r"\s+", " ", query).strip() if isinstance(query, str) else ""
    if not query:
        raise ApiError(400, "Query is missing.")
    if len(query) > 400:
        raise ApiError(400, "Query is too long (max 400 characters).")
    max_results = _as_int(max_results, 5, 1, 20, "max_results")
    news = isinstance(topic, str) and topic.strip().lower() == "news"
    deep = _truthy(include_raw_content) or str(search_depth).strip().lower() == "advanced"
    timelimit = _timelimit(time_range, days if news else None)

    inc, exc = _clean_domains(include_domains), _clean_domains(exclude_domains)
    q = query
    if inc:
        q += " (" + " OR ".join(f"site:{d}" for d in inc) + ")"
    q += "".join(f" -site:{d}" for d in exc)

    key = ("search", q.lower(), max_results, deep, news, timelimit, REGION, SAFESEARCH)
    cached = _cache.get(key)
    if cached is not None:
        return cached

    def work() -> list[dict]:
        raw = _ddg_fetch(q, max_results, news, timelimit)
        results = []
        for r in raw:
            url = r.get("href") or r.get("url") or ""
            if not url.startswith(("http://", "https://")):
                continue
            item = {
                "title": r.get("title") or "",
                "url": url,
                "content": r.get("body") or r.get("content") or "",
                "score": 0.0,
                "raw_content": None,
            }
            if news:
                item["published_date"] = r.get("date")
            results.append(item)
        for i, item in enumerate(results):  # rank-derived pseudo-score (see README)
            item["score"] = round(max(0.98 - i * 0.06, 0.50), 2)
        if deep and results:
            _enrich(results)
        if results:
            _cache.set(key, results)
        return results

    return _flight.do(key, work)


# ------------------------------------------------------------------------------
# Endpoint logic
# ------------------------------------------------------------------------------
def _request_id() -> str:
    return str(uuid.uuid4())


def handle_search(data: dict) -> dict:
    t0 = time.perf_counter()
    query = data.get("query") if data.get("query") is not None else data.get("q")
    log.info("POST /search %r depth=%s max=%s topic=%s", query, data.get("search_depth", "basic"),
             data.get("max_results", 5), data.get("topic", "general"))
    results = perform_search(
        query,
        max_results=data.get("max_results", 5),
        include_raw_content=data.get("include_raw_content", False),
        search_depth=data.get("search_depth", "basic"),
        topic=data.get("topic", "general"),
        time_range=data.get("time_range"),
        days=data.get("days"),
        include_domains=data.get("include_domains"),
        exclude_domains=data.get("exclude_domains"),
    )
    answer = None
    if FAKE_ANSWER and _truthy(data.get("include_answer")) and results:
        answer = " ".join(r["content"] for r in results[:2] if r.get("content")) or None
    return {
        "query": str(query),
        "follow_up_questions": None,
        "answer": answer,
        "images": [],
        "results": results,
        "response_time": round(time.perf_counter() - t0, 3),
        "request_id": _request_id(),
    }


def handle_context(data: dict) -> dict:
    query = data.get("query") if data.get("query") is not None else data.get("q")
    log.info("POST /search/context %r", query)
    results = perform_search(query, max_results=data.get("max_results", 3), include_raw_content=True)
    blocks = [
        f"Title: {r['title']}\nURL: {r['url']}\nContent: {r.get('raw_content') or r.get('content') or ''}"
        for r in results
    ]
    return {"query": str(query), "context": "\n\n---\n\n".join(blocks)}


def handle_extract(data: dict) -> dict:
    t0 = time.perf_counter()
    urls = data.get("urls") if data.get("urls") is not None else data.get("url")
    if isinstance(urls, str):
        urls = [urls]
    if not isinstance(urls, list) or not urls:
        raise ApiError(400, "'urls' must be a non-empty list of URLs.")
    urls = [u for u in urls if isinstance(u, str)][:20]
    max_chars = _as_int(data.get("max_chars"), EXTRACT_MAX_CHARS, 200, 100_000, "max_chars")
    log.info("POST /extract %d url(s)", len(urls))

    futures = {_pool.submit(_fetch, u, max_chars): u for u in urls}
    done, _ = wait(futures, timeout=EXTRACT_DEADLINE)
    results, failed = [], []
    for fut, url in futures.items():
        if fut not in done:
            failed.append({"url": url, "error": "timeout"})
            continue
        text, err = fut.result()
        if text:
            results.append({"url": url, "raw_content": text, "images": [], "favicon": None})
        else:
            failed.append({"url": url, "error": err or "unknown error"})
    return {
        "results": results,
        "failed_results": failed,
        "response_time": round(time.perf_counter() - t0, 3),
        "request_id": _request_id(),
    }


def render_html(query: str) -> bytes:
    results = perform_search(query, max_results=8)
    hits = []
    for r in results:
        title = html.escape(r.get("title", ""), quote=True)
        url = html.escape(r.get("url", ""), quote=True)
        snippet = html.escape(r.get("content", ""), quote=True)
        hits.append(
            '<div class="result results_links web-result">'
            f'<h2 class="result__title"><a rel="nofollow" class="result__a" href="{url}">{title}</a></h2>'
            f'<a class="result__url" href="{url}">{url}</a>'
            f'<a class="result__snippet" href="{url}">{snippet}</a>'
            "</div>"
        )
    doc = ("<!DOCTYPE html>\n<html><head><meta charset='utf-8'></head><body>\n"
           "<div id='links' class='results'>\n" + "\n".join(hits) + "\n</div>\n</body></html>")
    return doc.encode("utf-8")


# ------------------------------------------------------------------------------
# HTTP layer
# ------------------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    server_version = f"Yeivily/{__version__}"
    timeout = 15  # drop stalled/slow-loris connections instead of pinning a thread forever

    def log_message(self, fmt, *args):
        log.debug("%s - %s", self.address_string(), fmt % args)

    # -- responses -------------------------------------------------------------
    def _send(self, status: int, body: bytes, ctype: str, headers: dict | None = None):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, status: int, payload: dict, headers: dict | None = None):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self._send(status, body, "application/json; charset=utf-8", headers)

    def _send_error_json(self, status: int, message: str, headers: dict | None = None):
        # Tavily SDKs read response.json()["detail"]["error"]; keep both shapes.
        self._send_json(status, {"detail": {"error": message}, "error": message}, headers)

    # -- request plumbing --------------------------------------------------------
    def _guard(self):
        if self.headers.get("Origin"):
            raise ApiError(403, "Cross-origin browser requests are not allowed.")
        if getattr(self.server, "enforce_host", True):
            host = urllib.parse.urlsplit("//" + (self.headers.get("Host") or "")).hostname or ""
            if host.lower() not in _LOCAL_HOSTS:
                raise ApiError(403, f"Host '{host}' is not allowed.")
        if API_KEY:
            supplied = ""
            auth = self.headers.get("Authorization", "")
            if auth.lower().startswith("bearer "):
                supplied = auth[7:].strip()
            supplied = supplied or self.headers.get("X-API-Key", "")
            if not hmac.compare_digest(supplied.encode(), API_KEY.encode()):
                raise ApiError(401, "Invalid API key.")

    def _read_payload(self) -> dict:
        if self.headers.get("Transfer-Encoding"):
            raise ApiError(411, "Chunked bodies are not supported; send Content-Length.")
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            raise ApiError(400, "Invalid Content-Length.")
        if length < 0:
            raise ApiError(400, "Invalid Content-Length.")
        if length > MAX_BODY_BYTES:
            raise ApiError(413, f"Payload too large (max {MAX_BODY_BYTES} bytes).")
        raw = self.rfile.read(length) if length else b""
        if not raw.strip():
            return {}
        ctype = (self.headers.get("Content-Type") or "").lower()
        if "x-www-form-urlencoded" in ctype:
            parsed = urllib.parse.parse_qs(raw.decode("utf-8", "replace"))
            return {k: v[0] for k, v in parsed.items()}
        try:
            data = json.loads(raw.decode("utf-8-sig"))
        except (UnicodeDecodeError, ValueError):
            raise ApiError(400, "Invalid JSON payload.")
        if not isinstance(data, dict):
            raise ApiError(400, "JSON body must be an object.")
        return data

    def _dispatch(self, fn):
        try:
            self._guard()
            fn()
        except ApiError as exc:
            self._send_error_json(exc.status, exc.message, exc.headers)
        except (ConnectionError, socket.timeout):
            log.debug("client went away")
        except Exception:
            log.exception("unhandled error")
            try:
                self._send_error_json(500, "Internal server error.")
            except OSError:
                pass

    def _path(self) -> str:
        return urllib.parse.urlsplit(self.path).path.rstrip("/") or "/"

    # -- verbs -------------------------------------------------------------------
    def do_POST(self):
        self._dispatch(self._post)

    def do_GET(self):
        self._dispatch(self._get)

    def _post(self):
        path = self._path()
        data = self._read_payload()
        if path == "/search":
            self._send_json(200, handle_search(data))
        elif path == "/search/context":
            self._send_json(200, handle_context(data))
        elif path == "/extract":
            self._send_json(200, handle_extract(data))
        elif path == "/html":
            self._send(200, render_html(str(data.get("q") or data.get("query") or "")),
                       "text/html; charset=utf-8")
        else:
            raise ApiError(404, f"Unknown endpoint: {path}")

    def _get(self):
        parts = urllib.parse.urlsplit(self.path)
        path = parts.path.rstrip("/") or "/"
        if path == "/html":
            q = urllib.parse.parse_qs(parts.query).get("q", [""])[0]
            self._send(200, render_html(q), "text/html; charset=utf-8")
        elif path in ("/", "/health"):
            self._send_json(200, {
                "message": "Yeivily is running",
                "status": "ok",
                "version": __version__,
                "search_backend": "ddgs" if DDGS is not None else None,
                "cache_entries": len(_cache),
            })
        else:
            raise ApiError(404, f"Unknown endpoint: {path}")


class YeivilyServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 64
    enforce_host = True


def _is_loopback(host: str) -> bool:
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return host.lower() == "localhost"


def _setup_stdio() -> None:
    # A Windows console/pipe in cp1252 would raise UnicodeEncodeError on any exotic
    # character in a query and kill the request. Never let logging do that.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(errors="replace")
        except Exception:
            pass


def main(argv: list[str] | None = None) -> int:
    global FAKE_ANSWER
    parser = argparse.ArgumentParser(description="Yeivily: local Tavily-style search server")
    parser.add_argument("-V", "--version", action="version", version=f"yeivily {__version__}")
    parser.add_argument("--host", default=os.environ.get("YEIVILY_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=_env_num("YEIVILY_PORT", 8000, int))
    parser.add_argument("-q", "--quiet", action="store_true", help="do not log queries")
    parser.add_argument("--fake-answer", action="store_true",
                        help="fill 'answer' with concatenated snippets (NOT an LLM answer)")
    args = parser.parse_args(argv)
    if args.fake_answer:
        FAKE_ANSWER = True

    _setup_stdio()
    logging.basicConfig(level=logging.WARNING if args.quiet else logging.INFO,
                        format="%(asctime)s [yeivily] %(message)s", datefmt="%H:%M:%S")

    if DDGS is None:
        log.error("No search backend found. Install it with: pip install ddgs")
    if not _is_loopback(args.host) and not API_KEY:
        log.warning("Listening on %s without YEIVILY_API_KEY: anyone on your network can use this server.",
                    args.host)

    try:
        httpd = YeivilyServer((args.host, args.port), Handler)
    except OSError as exc:
        log.error("Cannot listen on %s:%s (%s). Is the port already in use? Try --port.",
                  args.host, args.port, exc)
        return 1
    httpd.enforce_host = _is_loopback(args.host)

    log.warning("Yeivily %s listening on http://%s:%s", __version__, args.host, args.port)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        log.warning("Shutting down...")
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
