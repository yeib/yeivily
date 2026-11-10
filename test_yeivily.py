"""Tests for Yeivily. Run with:  python -m unittest -v   (no network, no ddgs needed)."""

import http.client
import http.server
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import yeivily  # noqa: E402


class FakeDDGS:
    calls = 0
    mode = "ok"  # ok | empty | ratelimit | boom
    delay = 0.0

    def __init__(self, *a, **kw):
        pass

    def _results(self, query):
        return [{"title": f"Title {i} for {query}", "href": f"https://example.org/{i}",
                 "body": f"Snippet {i}"} for i in range(8)]

    def text(self, query, **kw):
        FakeDDGS.calls += 1
        time.sleep(FakeDDGS.delay)
        if FakeDDGS.mode == "empty":
            raise Exception("No results found.")
        if FakeDDGS.mode == "ratelimit":
            raise Exception("https://html.duckduckgo.com/html 202 Ratelimit")
        if FakeDDGS.mode == "boom":
            raise RuntimeError("kaboom")
        return self._results(query)[: kw.get("max_results", 5)]

    news = text


def reset_state():
    FakeDDGS.calls, FakeDDGS.mode, FakeDDGS.delay = 0, "ok", 0.0
    yeivily._cache._data.clear()
    yeivily._cooldown_until = 0.0
    yeivily.DDGS = FakeDDGS


class TextExtractionTests(unittest.TestCase):
    def test_header_kept_nav_dropped_br_respected(self):
        page = ("<html><head><title>T</title><script>var x=1;</script></head><body>"
                "<header><h1>Real Heading</h1></header><nav>MENU MENU</nav>"
                "<main><p>" + "Useful text. " * 40 + "</p><p>line1<br>line2</p></main>"
                "<footer>FOOTER</footer></body></html>")
        text = yeivily.extract_text(page, 4000)
        self.assertNotIn("MENU", text)
        self.assertNotIn("FOOTER", text)
        self.assertNotIn("var x", text)
        self.assertIn("Useful text.", text)
        self.assertIn("line1\nline2", text)

    def test_unclosed_nav_does_not_swallow_page(self):
        page = "<body><nav>menu<p>" + "Body content here. " * 30 + "</p></body>"
        self.assertIn("Body content here.", yeivily.extract_text(page, 4000))

    def test_latin1_and_truncated_utf8(self):
        latin = "<p>ni\xf1o a\xf1o</p>".encode("latin-1")
        self.assertIn("ni\u00f1o", yeivily._decode(latin, "text/html; charset=iso-8859-1"))
        cut = "caf\u00e9 ma\u00f1ana".encode("utf-8")[:-1]  # ends in half a character
        self.assertTrue(yeivily._decode(cut, "text/html").startswith("caf\u00e9 ma"))


class CacheTests(unittest.TestCase):
    def test_lru_ttl_and_isolation(self):
        c = yeivily.TTLCache(ttl=0.2, max_entries=2)
        c.set("a", [{"x": 1}]); c.set("b", 2); c.set("c", 3)
        self.assertIsNone(c.get("a"))  # evicted (LRU)
        got = c.get("b"); self.assertEqual(got, 2)
        c.set("l", [{"x": 1}])
        c.get("l")[0]["x"] = 99          # mutate the copy
        self.assertEqual(c.get("l")[0]["x"], 1)
        time.sleep(0.25)
        self.assertIsNone(c.get("l"))    # expired


class SearchTests(unittest.TestCase):
    def setUp(self):
        reset_state()

    def test_single_flight_one_upstream_call(self):
        FakeDDGS.delay = 0.3
        out = []
        threads = [threading.Thread(target=lambda: out.append(yeivily.perform_search("same query")))
                   for _ in range(6)]
        [t.start() for t in threads]; [t.join() for t in threads]
        self.assertEqual(FakeDDGS.calls, 1)
        self.assertEqual(len(out), 6)

    def test_domains_and_cache(self):
        yeivily.perform_search("rust", include_domains=["https://docs.rs/x"], exclude_domains=["spam.com"])
        yeivily.perform_search("rust", include_domains=["docs.rs"], exclude_domains=["spam.com"])
        self.assertEqual(FakeDDGS.calls, 1)

    def test_empty_is_not_an_error(self):
        FakeDDGS.mode = "empty"
        self.assertEqual(yeivily.perform_search("nothing"), [])

    def test_ratelimit_is_reported_and_cools_down(self):
        FakeDDGS.mode = "ratelimit"
        with self.assertRaises(yeivily.ApiError) as ctx:
            yeivily.perform_search("q1")
        self.assertEqual(ctx.exception.status, 429)
        calls = FakeDDGS.calls
        with self.assertRaises(yeivily.ApiError):
            yeivily.perform_search("q2")
        self.assertEqual(FakeDDGS.calls, calls)  # fail-fast, DDG not hit again


class _PageHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = ("<html><body><article><p>" + "Hola mundo, a\u00f1o ni\u00f1o. " * 20 +
                "</p></article></body></html>").encode("latin-1")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=iso-8859-1")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


class ServerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.httpd = yeivily.YeivilyServer(("127.0.0.1", 0), yeivily.Handler)
        cls.port = cls.httpd.server_address[1]
        threading.Thread(target=cls.httpd.serve_forever, daemon=True).start()
        cls.page = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _PageHandler)
        cls.page_port = cls.page.server_address[1]
        threading.Thread(target=cls.page.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown(); cls.page.shutdown()

    def setUp(self):
        reset_state()
        yeivily.ALLOW_PRIVATE_FETCH = False

    def req(self, method, path, body=None, headers=None, raw=False):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=15)
        if body is not None and not raw:
            body = json.dumps(body)
        hdrs = {"Content-Type": "application/json"}
        hdrs.update(headers or {})
        conn.request(method, path, body=body, headers=hdrs)
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        try:
            return resp.status, json.loads(data), resp
        except ValueError:
            return resp.status, data, resp

    def test_search_contract(self):
        status, data, _ = self.req("POST", "/search", {"query": "hola", "max_results": 3, "include_answer": True})
        self.assertEqual(status, 200)
        for key in ("query", "answer", "images", "results", "response_time", "request_id"):
            self.assertIn(key, data)
        self.assertEqual(len(data["results"]), 3)
        self.assertIsNone(data["answer"])  # no fake answers unless opted in
        self.assertEqual(set(data["results"][0]), {"title", "url", "content", "score", "raw_content"})

    def test_bad_requests_return_json_errors(self):
        self.assertEqual(self.req("POST", "/search", "{not json", raw=True)[0], 400)
        self.assertEqual(self.req("POST", "/search", "[1,2]", raw=True)[0], 400)
        self.assertEqual(self.req("POST", "/search", {"query": "x", "max_results": "abc"})[0], 400)
        status, data, _ = self.req("POST", "/search", {"query": "  "})
        self.assertEqual(status, 400)
        self.assertIn("error", data["detail"])
        self.assertEqual(self.req("POST", "/nope", {})[0], 404)
        self.assertEqual(self.req("GET", "/nope")[0], 404)

    def test_string_booleans_are_not_truthy(self):
        _, data, _ = self.req("POST", "/search", {"query": "x", "include_raw_content": "false"})
        self.assertTrue(all(r["raw_content"] is None for r in data["results"]))

    def test_upstream_errors_are_not_empty_200s(self):
        FakeDDGS.mode = "ratelimit"
        status, _, resp = self.req("POST", "/search", {"query": "x"})
        self.assertEqual(status, 429)
        self.assertIsNotNone(resp.getheader("Retry-After"))
        reset_state(); FakeDDGS.mode = "boom"
        self.assertEqual(self.req("POST", "/search", {"query": "x"})[0], 502)

    def test_browser_and_rebinding_guards(self):
        self.assertEqual(self.req("POST", "/search", {"query": "x"}, {"Origin": "https://evil.example"})[0], 403)
        self.assertEqual(self.req("POST", "/search", {"query": "x"}, {"Host": "evil.example"})[0], 403)

    def test_api_key(self):
        yeivily.API_KEY = "secret"
        try:
            self.assertEqual(self.req("POST", "/search", {"query": "x"})[0], 401)
            ok = self.req("POST", "/search", {"query": "x"}, {"Authorization": "Bearer secret"})
            self.assertEqual(ok[0], 200)
        finally:
            yeivily.API_KEY = None

    def test_form_encoded_html_endpoint(self):
        status, data, _ = self.req("POST", "/html", "q=hola+mundo", {"Content-Type": "application/x-www-form-urlencoded"}, raw=True)
        self.assertEqual(status, 200)
        self.assertIn(b"result__a", data)

    def test_extract_ssrf_blocked_by_default_and_charset_when_allowed(self):
        url = f"http://127.0.0.1:{self.page_port}/"
        _, data, _ = self.req("POST", "/extract", {"urls": [url]})
        self.assertEqual(data["results"], [])
        self.assertIn("blocked", data["failed_results"][0]["error"])
        yeivily.ALLOW_PRIVATE_FETCH = True
        _, data, _ = self.req("POST", "/extract", {"urls": [url]})
        self.assertIn("a\u00f1o ni\u00f1o", data["results"][0]["raw_content"])

    def test_deep_search_enriches_top_n(self):
        yeivily.ALLOW_PRIVATE_FETCH = True
        port = self.page_port
        original = FakeDDGS._results
        FakeDDGS._results = lambda self, q: [
            {"title": f"T{i}", "href": f"http://127.0.0.1:{port}/{i}", "body": "b"} for i in range(5)
        ]
        try:
            _, data, _ = self.req("POST", "/search", {"query": "deep", "search_depth": "advanced", "max_results": 5})
            filled = [r["raw_content"] is not None for r in data["results"]]
            self.assertEqual(filled, [True, True, True, False, False])
        finally:
            FakeDDGS._results = original


class McpTests(unittest.TestCase):
    def test_protocol_roundtrip_with_in_process_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "ddgs.py"), "w") as fh:
                fh.write(textwrap.dedent("""
                    class DDGS:
                        def __init__(self, *a, **k): pass
                        def text(self, q, **k):
                            return [{"title": "A [x]", "href": "https://example.org/a", "body": "snippet"}]
                """))
            env = dict(os.environ, PYTHONPATH=tmp, YEIVILY_URL="http://127.0.0.1:9", PYTHONIOENCODING="utf-8")
            proc = subprocess.Popen([sys.executable, os.path.join(HERE, "yeivily_mcp.py")],
                                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env)
            msgs = [
                {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18"}},
                {"jsonrpc": "2.0", "method": "notifications/initialized"},
                {"jsonrpc": "2.0", "id": 2, "method": "ping"},
                {"jsonrpc": "2.0", "id": 3, "method": "tools/list"},
                {"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {"name": "web_search", "arguments": {"query": "ni\u00f1o"}}},
                {"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {"name": "nope"}},
                {"jsonrpc": "2.0", "id": 6, "method": "tools/call", "params": {"name": "web_search", "arguments": {}}},
                {"jsonrpc": "2.0", "id": 7, "method": "resources/list"},
            ]
            payload = b"\xef\xbb\xbf" + "\n".join(json.dumps(m) for m in msgs).encode() + b"\n[1]\nnot json\n"
            out, _ = proc.communicate(payload, timeout=30)
        replies = {}
        for line in out.decode("utf-8").splitlines():
            m = json.loads(line)
            replies.setdefault(m.get("id"), m)
        self.assertEqual(replies[1]["result"]["protocolVersion"], "2025-06-18")
        self.assertEqual(replies[2]["result"], {})
        self.assertEqual({t["name"] for t in replies[3]["result"]["tools"]}, {"web_search", "fetch_page"})
        res = replies[4]["result"]
        self.assertFalse(res["isError"])
        self.assertIn("https://example.org/a", res["content"][0]["text"])
        self.assertEqual(replies[5]["error"]["code"], -32602)
        self.assertTrue(replies[6]["result"]["isError"])
        self.assertEqual(replies[7]["error"]["code"], -32601)
        self.assertEqual(replies[None]["error"]["code"], -32600)  # the "[1]" line (first null-id reply)


if __name__ == "__main__":
    unittest.main(verbosity=2)
