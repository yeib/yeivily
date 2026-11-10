<p align="center">
  <img src="assets/logos/yeivily-logo.svg" width="130" height="130" alt="Yeivily logo">
</p>

<h1 align="center">Yeivily</h1>

<p align="center">
  <a href="https://github.com/yeib/yeivily/actions/workflows/ci.yml"><img src="https://github.com/yeib/yeivily/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License: MIT"></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/python-3.10%2B-blue.svg" alt="Python 3.10+"></a>
  <a href="https://glama.ai/mcp/servers/yeib/yeivily"><img src="https://img.shields.io/badge/Glama-Listed-purple.svg" alt="Glama Listed"></a>
</p>

A tiny, keyless, Tavily-style search server and MCP provider for local LLM setups (Ollama, Goose, Aider, LangChain, ...).
Search comes from DuckDuckGo through [`ddgs`](https://pypi.org/project/ddgs/); everything else is Python standard library.

- **`yeivily`** (`yeivily.py`): local HTTP server with `POST /search`, `POST /extract`, `POST /search/context`, `GET|POST /html`, `GET /health`.
- **`yeivily-mcp`** (`yeivily_mcp.py`): MCP server over stdio with two tools, `web_search` and `fetch_page`. It talks to the HTTP daemon if it is running (sharing its TTL cache) and otherwise runs the same engine in-process.

No Docker, no SearXNG, no API key, no database.

### Why Yeivily? (vs. `ddgs api` / `ddgs mcp` / SearXNG proxies)

`ddgs` itself ships `ddgs api` (FastAPI) and `ddgs mcp`, and several SearXNG-based projects expose Tavily-compatible APIs (Sifthound, Trawl, OrioSearch, ...). **Yeivily's specific niche** is combining all of the following in one tiny package:

1. **Tavily-shaped JSON contract** (`/search`, `/extract`, `/search/context`) for tools/SDKs that speak Tavily rather than raw `ddgs`.
2. **Zero-Docker, zero-FastAPI footprint**: pure Python stdlib + `ddgs` (~44 MB RAM for the HTTP daemon under load; ~24 MB RAM for the stdio MCP server).
3. **Resilient local caching & rate-limit protection**: built-in TTL cache, single-flight deduplication for concurrent identical agent queries, concurrency limiting, and explicit `429 Retry-After` handling instead of silently returning empty lists on upstream blocks.
4. **Dual HTTP + MCP with automatic fallback**: MCP clients work whether the background HTTP daemon is running or not.

## Quick start

### Install with `pipx` or `pip` (no clone needed)

```bash
pipx install git+https://github.com/yeib/yeivily.git
# or: pip install git+https://github.com/yeib/yeivily.git

yeivily                               # starts HTTP server on http://127.0.0.1:8000
```

### Or run directly from source

```bash
pip install -r requirements.txt
python yeivily.py                     # http://127.0.0.1:8000
curl -s localhost:8000/search -H 'Content-Type: application/json' \
     -d '{"query": "ollama latest release", "max_results": 3}'
```

### Use it where Tavily is expected

Works in clients and SDKs that let you override the Tavily base URL (the official SDKs do):

```python
from tavily import TavilyClient
client = TavilyClient(api_key="anything", api_base_url="http://127.0.0.1:8000")
print(client.search("rust async runtimes", max_results=5))
print(client.extract(urls=["https://example.org"]))
```

If a tool hardcodes `https://api.tavily.com`, Yeivily cannot help (there is no TLS and no DNS trickery here).

### MCP Setup

#### Claude Desktop / Cursor / Generic `mcpServers` JSON

If installed via `pipx` / `pip`:

```json
{
  "mcpServers": {
    "yeivily": {
      "command": "yeivily-mcp"
    }
  }
}
```

Or pointing directly to the script (keep `yeivily.py` next to `yeivily_mcp.py` so the in-process fallback works):

```json
{
  "mcpServers": {
    "yeivily": {
      "command": "python",
      "args": ["/path/to/yeivily_mcp.py"]
    }
  }
}
```

#### Block Goose (`~/.config/goose/config.yaml` or `%APPDATA%\Block\goose\config\config.yaml`)

```yaml
extensions:
  yeivily_search:
    name: yeivily_search
    type: stdio
    cmd: yeivily-mcp
    args: []
    enabled: true
    timeout: 30
```

## Verified Compatibility & Footprint

- **Official `tavily-python` SDK**: Verified end-to-end with `tavily-python` (v0.8.5) executing live `TavilyClient(api_key="...", api_base_url="http://127.0.0.1:8000")` calls:
  - `client.search("Chile capital", max_results=2)` -> returns valid structured results.
  - `client.extract(urls=["https://example.com"])` -> returns clean extracted page text without SSL or contract errors.
- **Goose CLI**: Verified on Windows 11 & WSL2 (Ubuntu) with **Goose CLI v1.58.0** and local **Ollama** (`gemma4:12b`) using `yeivily-mcp` over stdio (`web_search` and `fetch_page` tools).
- **Memory Footprint**: Measured via process RSS WorkingSet on Python 3.12 (Windows 11 x64) with `ddgs` loaded under active search and HTTPS page extraction load:
  - **~44 MB RSS** for `yeivily` HTTP daemon.
  - **~24 MB RSS** for `yeivily-mcp` stdio worker.
  - *(Compare to ~180-250 MB for FastAPI/Uvicorn-based search wrappers or 500 MB - 1.5 GB for Dockerized SearXNG).*

## Configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `YEIVILY_HOST` / `YEIVILY_PORT` | `127.0.0.1` / `8000` | Bind address (port 8000 is popular: use `--port` if it clashes) |
| `YEIVILY_API_KEY` | unset | Require `Authorization: Bearer <key>` |
| `YEIVILY_REGION` | `wt-wt` | ddgs region (`es-es`, `cl-es`, `us-en`, ...) |
| `YEIVILY_SAFESEARCH` | `moderate` | `on`, `moderate`, `off` |
| `YEIVILY_CACHE_TTL` / `YEIVILY_CACHE_MAX` | `600` / `256` | Cache seconds / entries |
| `YEIVILY_DEEP_TOP_N` | `3` | Results enriched with page text when `include_raw_content` or `search_depth=advanced` |
| `YEIVILY_ALLOW_PRIVATE_FETCH` | off | Let `/extract` reach LAN/localhost addresses |
| `YEIVILY_FAKE_ANSWER` | off | Fill `answer` with concatenated snippets (not an LLM answer) |
| `YEIVILY_URL` (MCP) | `http://127.0.0.1:8000` | Where the MCP server finds the daemon |

## Honest differences from Tavily

- `score` is derived from rank, not relevance. Do not threshold on it.
- `answer` is `null` unless you opt into the snippet-concatenation hack (`--fake-answer`).
- No `/crawl`, `/map`, `/research`; no images; no JavaScript rendering; no PDF extraction.
- Search quality is whatever DuckDuckGo gives you. Upstream rate limits are real: Yeivily caches, de-duplicates concurrent identical queries, limits concurrency and answers `429` + `Retry-After` instead of pretending there were no results.

## Security notes

- Binds to loopback (`127.0.0.1`) by default; requests with an `Origin` header (browsers) or a foreign `Host` header (DNS rebinding) are rejected with `403`.
- Page extraction refuses private/loopback/link-local addresses (also after HTTP redirects) to prevent SSRF unless you explicitly opt out via `YEIVILY_ALLOW_PRIVATE_FETCH=1`.
- If you bind to `0.0.0.0`, set `YEIVILY_API_KEY`.
- Scraping search engines can conflict with their terms of service. Intended for personal/local development use.

## Tests

```bash
python -m unittest discover -v     # 22 tests; no network and no ddgs required
```

## License & Disclaimer

MIT License. See [LICENSE](LICENSE) for details.

*Disclaimer: Yeivily is an independent open-source project and is not affiliated with, sponsored by, or endorsed by Tavily.*
