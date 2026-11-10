#!/usr/bin/env python3
"""
Yeivily MCP server (stdio, newline-delimited JSON-RPC 2.0).

Tools : web_search  - search the web (optionally with page excerpts)
        fetch_page  - download one URL and return clean text

Backends, in order:
  1. The Yeivily HTTP daemon (YEIVILY_URL, default http://127.0.0.1:8000) - shared cache.
     Used whenever the daemon answers, even with an error or zero results.
  2. In-process engine: `import yeivily` from the same folder (only when the daemon
     refuses the connection) - same features, no daemon needed.
  3. Minimal direct `ddgs` text search (only if yeivily.py is not next to this file).

Environment: YEIVILY_URL, YEIVILY_API_KEY (sent as Bearer token if set).
"""

from __future__ import annotations

import json
import os
import re
import socket
import sys
import threading
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, wait

__version__ = "1.0.0"
SUPPORTED_VERSIONS = ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"]

DAEMON_URL = os.environ.get("YEIVILY_URL", "http://127.0.0.1:8000").rstrip("/")
DAEMON_KEY = os.environ.get("YEIVILY_API_KEY") or None
DAEMON_TIMEOUT = 40.0  # daemon worst case: 2 upstream attempts + page fetches

# Never route loopback traffic through a corporate/system proxy.
_local_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

# Keep the protocol channel clean: capture the real stdout, then make any stray
# print() (ours or a library's) go to stderr instead of corrupting JSON-RPC.
_OUT = sys.stdout.buffer
sys.stdout = sys.stderr
_write_lock = threading.Lock()


def log(msg: str) -> None:
    try:
        sys.stderr.write(f"[yeivily_mcp] {msg}\n")
        sys.stderr.flush()
    except Exception:
        pass


def send(message: dict) -> None:
    # ensure_ascii=True also escapes U+2028/U+2029, which some line splitters treat as newlines.
    data = (json.dumps(message, ensure_ascii=True, separators=(",", ":")) + "\n").encode("ascii")
    with _write_lock:
        _OUT.write(data)
        _OUT.flush()


class ToolError(Exception):
    """Expected failure: reported to the model as a tool result with isError=true."""


class _DaemonDown(Exception):
    pass


# ------------------------------------------------------------------------------
# Backends
# ------------------------------------------------------------------------------
def _daemon_post(path: str, payload: dict) -> dict:
    headers = {"Content-Type": "application/json"}
    if DAEMON_KEY:
        headers["Authorization"] = f"Bearer {DAEMON_KEY}"
    req = urllib.request.Request(DAEMON_URL + path, data=json.dumps(payload).encode("utf-8"),
                                 headers=headers)
    try:
        with _local_opener.open(req, timeout=DAEMON_TIMEOUT) as resp:
            body = resp.read()
    except urllib.error.HTTPError as exc:  # must precede URLError (it is a subclass)
        detail = ""
        try:
            parsed = json.loads(exc.read().decode("utf-8", "replace"))
            detail = (parsed.get("detail") or {}).get("error") or parsed.get("error") or ""
        except Exception:
            pass
        raise ToolError(f"Yeivily daemon returned HTTP {exc.code}: {detail or exc.reason}")
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, (ConnectionRefusedError, socket.gaierror)):
            raise _DaemonDown() from exc
        raise ToolError(f"Yeivily daemon unreachable: {exc.reason}")
    except (TimeoutError, socket.timeout):
        raise ToolError("Yeivily daemon timed out.")
    try:
        data = json.loads(body.decode("utf-8"))
    except ValueError:
        data = None
    if not isinstance(data, dict):
        raise ToolError(f"Unexpected response from {DAEMON_URL} - is another service using that port?")
    return data


_yv = None
_yv_tried = False


def _load_engine():
    """Import yeivily.py from this folder (in-process fallback)."""
    global _yv, _yv_tried
    if not _yv_tried:
        _yv_tried = True
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        try:
            import yeivily  # noqa: WPS433
            _yv = yeivily
        except ImportError:
            _yv = None
    return _yv


def _minimal_ddgs_search(query: str, max_results: int) -> list[dict]:
    try:
        try:
            from ddgs import DDGS
        except ImportError:
            from duckduckgo_search import DDGS
    except ImportError:
        raise ToolError("No search backend available. Run: pip install ddgs")
    try:
        raw = list(DDGS().text(query, max_results=max_results))
    except Exception as exc:
        if "no results" in str(exc).lower():
            return []
        raise ToolError(f"Search failed: {type(exc).__name__}: {exc}")
    return [{"title": r.get("title"), "url": r.get("href") or r.get("url"), "content": r.get("body")}
            for r in raw]


# ------------------------------------------------------------------------------
# Tools
# ------------------------------------------------------------------------------
def _int(value, default: int, lo: int, hi: int) -> int:
    try:
        return max(lo, min(int(value), hi))
    except (TypeError, ValueError):
        return default


def _bool(value) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return bool(value)


def _md_escape(text: str) -> str:
    return re.sub(r"([\[\]])", r"\\\1", text)


def format_results(results: list[dict], excerpt_chars: int = 1500) -> str:
    if not results:
        return "No results found."
    blocks = []
    for i, r in enumerate(results, 1):
        title = _md_escape(r.get("title") or r.get("url") or "(untitled)")
        block = f"### {i}. [{title}]({r.get('url')})\n{r.get('content') or ''}\n"
        if r.get("published_date"):
            block += f"Published: {r['published_date']}\n"
        if r.get("raw_content"):
            block += f"\nPage excerpt:\n{r['raw_content'][:excerpt_chars]}\n"
        blocks.append(block)
    return "\n".join(blocks)


def tool_web_search(args: dict) -> str:
    query = args.get("query")
    if not isinstance(query, str) or not query.strip():
        raise ToolError("'query' is required and must be a non-empty string.")
    max_results = _int(args.get("max_results"), 5, 1, 20)
    deep = _bool(args.get("deep"))
    topic = args.get("topic") if args.get("topic") in ("general", "news") else "general"
    time_range = args.get("time_range") if args.get("time_range") in ("day", "week", "month", "year") else None

    payload = {"query": query, "max_results": max_results, "topic": topic,
               "search_depth": "advanced" if deep else "basic", "include_raw_content": deep}
    if time_range:
        payload["time_range"] = time_range

    try:
        data = _daemon_post("/search", payload)
        results = data.get("results")
        if not isinstance(results, list):
            raise ToolError(f"Unexpected response from {DAEMON_URL} - is another service using that port?")
    except _DaemonDown:
        engine = _load_engine()
        if engine is not None:
            log("daemon not running; using in-process engine")
            try:
                results = engine.perform_search(
                    query, max_results=max_results, include_raw_content=deep,
                    search_depth=payload["search_depth"], topic=topic, time_range=time_range)
            except engine.ApiError as exc:
                raise ToolError(exc.message)
        else:
            log("daemon not running and yeivily.py not found; using minimal ddgs fallback")
            results = _minimal_ddgs_search(query, max_results)
    return format_results(results)


def tool_fetch_page(args: dict) -> str:
    url = args.get("url")
    if not isinstance(url, str) or not url.startswith(("http://", "https://")):
        raise ToolError("'url' must be an http(s) URL.")
    max_chars = _int(args.get("max_chars"), 12000, 500, 100000)
    try:
        data = _daemon_post("/extract", {"urls": [url], "max_chars": max_chars})
        for item in data.get("results") or []:
            return f"# {item.get('url', url)}\n\n{item.get('raw_content', '')}"
        failed = (data.get("failed_results") or [{}])[0]
        raise ToolError(f"Could not fetch page: {failed.get('error', 'unknown error')}")
    except _DaemonDown:
        engine = _load_engine()
        if engine is None:
            raise ToolError("fetch_page needs the Yeivily daemon or yeivily.py next to yeivily_mcp.py.")
        text, err = engine._fetch(url, max_chars)
        if not text:
            raise ToolError(f"Could not fetch page: {err}")
        return f"# {url}\n\n{text}"


TOOLS = [
    {
        "name": "web_search",
        "description": ("Search the web (DuckDuckGo) for current information, documentation, news or "
                        "anything outside your training data. Returns titles, URLs and snippets. "
                        "Set deep=true to also get text excerpts from the top pages."),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query."},
                "max_results": {"type": "integer", "description": "Results to return (1-20).", "default": 5},
                "deep": {"type": "boolean", "default": False,
                         "description": "Also extract text from the top results (slower)."},
                "topic": {"type": "string", "enum": ["general", "news"], "default": "general"},
                "time_range": {"type": "string", "enum": ["day", "week", "month", "year"],
                               "description": "Only results from this period."},
            },
            "required": ["query"],
        },
        "annotations": {"title": "Web search", "readOnlyHint": True, "idempotentHint": True,
                        "openWorldHint": True},
    },
    {
        "name": "fetch_page",
        "description": ("Download a single web page and return its readable text. Use it to read a URL "
                        "found via web_search. Does not execute JavaScript and does not read PDFs."),
        "inputSchema": {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "http(s) URL to read."},
                "max_chars": {"type": "integer", "default": 12000,
                              "description": "Maximum characters of text to return."},
            },
            "required": ["url"],
        },
        "annotations": {"title": "Fetch page", "readOnlyHint": True, "idempotentHint": True,
                        "openWorldHint": True},
    },
]


# ------------------------------------------------------------------------------
# JSON-RPC plumbing
# ------------------------------------------------------------------------------
def _reply(msg_id, result: dict) -> None:
    send({"jsonrpc": "2.0", "id": msg_id, "result": result})


def _error(msg_id, code: int, message: str) -> None:
    send({"jsonrpc": "2.0", "id": msg_id, "error": {"code": code, "message": message}})


def _tool_result(msg_id, text: str, is_error: bool = False) -> None:
    _reply(msg_id, {"content": [{"type": "text", "text": text}], "isError": is_error})


def run_tool(msg_id, params: dict) -> None:
    name = params.get("name")
    args = params.get("arguments")
    args = args if isinstance(args, dict) else {}
    handlers = {"web_search": tool_web_search, "fetch_page": tool_fetch_page}
    if name not in handlers:
        _error(msg_id, -32602, f"Unknown tool: {name}")
        return
    try:
        _tool_result(msg_id, handlers[name](args))
    except ToolError as exc:
        _tool_result(msg_id, str(exc), is_error=True)
    except Exception as exc:  # never let a worker die silently: the client would hang
        log(f"unexpected error in {name}: {type(exc).__name__}: {exc}")
        _tool_result(msg_id, f"Internal error: {type(exc).__name__}: {exc}", is_error=True)


def handle_message(msg, pool: ThreadPoolExecutor):
    """Handle one message. Returns a Future for tools/call (so shutdown can drain it), else None."""
    if not isinstance(msg, dict):
        _error(None, -32600, "Invalid Request (batches are not supported)")
        return None
    method = msg.get("method")
    msg_id = msg.get("id")
    params = msg.get("params")
    params = params if isinstance(params, dict) else {}

    if not isinstance(method, str):  # a response to something we never asked, or garbage
        return None
    if msg_id is None:               # notification: never answer
        return None

    if method == "initialize":
        requested = params.get("protocolVersion")
        _reply(msg_id, {
            "protocolVersion": requested if requested in SUPPORTED_VERSIONS else SUPPORTED_VERSIONS[0],
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "yeivily-search", "title": "Yeivily Search", "version": __version__},
        })
    elif method == "ping":
        _reply(msg_id, {})
    elif method == "tools/list":
        _reply(msg_id, {"tools": TOOLS})
    elif method == "tools/call":
        return pool.submit(run_tool, msg_id, params)  # searches take seconds; keep ping/list responsive
    else:
        _error(msg_id, -32601, f"Method not found: {method}")
    return None


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if "-h" in args or "--help" in args:
        print("usage: yeivily-mcp [-h] [-V]\n\nYeivily MCP server over stdio (newline-delimited JSON-RPC 2.0).")
        return 0
    if "-V" in args or "--version" in args:
        print(f"yeivily-mcp {__version__}")
        return 0

    log(f"Yeivily MCP {__version__} starting (daemon: {DAEMON_URL})")
    pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="yeivily-mcp")
    pending: set = set()
    stdin = sys.stdin.buffer
    for raw in iter(stdin.readline, b""):
        line = raw.decode("utf-8-sig", errors="replace").strip()  # tolerates a BOM from PowerShell pipes
        if not line:
            continue
        try:
            msg = json.loads(line)
        except ValueError as exc:
            log(f"bad JSON: {exc}")
            _error(None, -32700, "Parse error")
            continue
        try:
            fut = handle_message(msg, pool)
            if fut is not None:
                pending.add(fut)
                fut.add_done_callback(pending.discard)
        except Exception as exc:
            log(f"dispatch error: {type(exc).__name__}: {exc}")
            if isinstance(msg, dict) and msg.get("id") is not None:
                _error(msg["id"], -32603, "Internal error")
    # stdin closed: let in-flight tool calls answer (bounded), then exit without
    # waiting for stragglers (pool threads are non-daemon).
    try:
        wait(list(pending), timeout=30)
        sys.stderr.flush()
    finally:
        os._exit(0)


if __name__ == "__main__":
    main()
