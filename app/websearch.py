"""Web search run by QueueRouter itself (for council members; see app/council.py).

Members get one extra tool, ``queuerouter_web_search``. When a member calls it, QueueRouter runs the
query with its own search key (Settings > Web search), gives the member the results and asks it again,
until it answers without searching (or reaches the alias's search limit). The judge then sees the
sources each member used. The app (opencode) never sees this tool.

Providers: Tavily, Brave Search, SearXNG (self-hosted: queries stay on your own server).
Queries are written by models, so they could contain code or private names: members are told not to,
and a query that looks like it holds a secret (API keys, tokens, private keys) is refused here.
"""

from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import json
import re
import socket
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit, urlunsplit

import httpx

PROVIDERS = ("off", "tavily", "brave", "searxng")
TOOL_NAME = "queuerouter_web_search"
TOOL = {
    "type": "function",
    "function": {
        "name": TOOL_NAME,
        "description": (
            "Search the web for current facts (documentation, versions, release notes, benchmarks, error "
            "messages). Returns titles, links and snippets. Write short, general queries. Never include code, "
            "secrets, keys, private file or project names in a query."
        ),
        "parameters": {
            "type": "object",
            "properties": {"query": {"type": "string", "description": "What to search for"}},
            "required": ["query"],
        },
    },
}
MAX_QUERY = 300
_SECRET = re.compile(
    r"(sk|pk|rk|ghp|gho|ghs|xox[abp]|AKIA|AIza)[-_A-Za-z0-9]{12,}"  # common key prefixes
    r"|-----BEGIN [A-Z ]*PRIVATE KEY"
    r"|\b[A-Fa-f0-9]{40,}\b"  # long hex (tokens, hashes of secrets)
    r"|\b[A-Za-z0-9+/_-]{48,}={0,2}"  # long base64-ish blobs
    r"|(password|passwd|secret|token|api[_-]?key)\s*[:=]",
    re.I,
)
# In addresses, long hex and base64 are ordinary (commit hashes, doc paths): only clear key shapes, and
# secret-looking query parameters, are refused.
_URL_SECRET = re.compile(
    r"\b(?:(?:sk|pk|rk)[-_][A-Za-z0-9_-]{20,}|(?:ghp|gho|ghs|ghu|github_pat)_[A-Za-z0-9_]{20,}"
    r"|xox[abp]-[A-Za-z0-9-]{10,}|AKIA[0-9A-Z]{16}|AIza[0-9A-Za-z_-]{30,})"
    r"|[?&;](?:password|passwd|secret|token|access_token|api[_-]?key|apikey|sig|signature)=",
    re.I,
)


class SearchError(Exception):
    pass


def check_query(query: str) -> str:
    """The query to send, or SearchError when it must not leave QueueRouter."""
    query = " ".join(str(query or "").split())[:MAX_QUERY]
    if not query:
        raise SearchError("empty query")
    if _SECRET.search(query):
        raise SearchError("refused: the query looks like it contains a secret; search for general terms instead")
    return query


async def search(
    client: httpx.AsyncClient, provider: str, key: str, base_url: str, query: str, count: int
) -> list[dict]:
    """[{title, url, snippet}] for ``query``; SearchError on any failure (with a short reason)."""
    query = check_query(query)
    count = max(1, min(int(count or 5), 10))
    try:
        if provider == "tavily":
            response = await client.post(
                "https://api.tavily.com/search",
                headers={"Authorization": f"Bearer {key}"},
                json={"query": query, "max_results": count, "search_depth": "basic"},
                timeout=20,
            )
            response.raise_for_status()
            items = response.json().get("results") or []
            return [
                {"title": i.get("title", ""), "url": i.get("url", ""), "snippet": i.get("content", "")} for i in items
            ]
        if provider == "brave":
            response = await client.get(
                "https://api.search.brave.com/res/v1/web/search",
                params={"q": query, "count": count},
                headers={"X-Subscription-Token": key, "Accept": "application/json"},
                timeout=20,
            )
            response.raise_for_status()
            items = (response.json().get("web") or {}).get("results") or []
            return [
                {"title": i.get("title", ""), "url": i.get("url", ""), "snippet": i.get("description", "")}
                for i in items[:count]
            ]
        if provider == "searxng":
            if not base_url:
                raise SearchError("SearXNG needs its address (Settings > Web search)")
            response = await client.get(
                f"{base_url.rstrip('/')}/search", params={"q": query, "format": "json"}, timeout=20
            )
            response.raise_for_status()
            items = response.json().get("results") or []
            return [
                {"title": i.get("title", ""), "url": i.get("url", ""), "snippet": i.get("content", "")}
                for i in items[:count]
            ]
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        hint = " (check the key and the plan's credits)" if status in (401, 402, 403, 429, 432) else ""
        raise SearchError(f"{provider} returned HTTP {status}{hint}") from None
    except (httpx.HTTPError, ValueError) as exc:
        raise SearchError(f"{provider}: {type(exc).__name__}") from None
    raise SearchError("web search is off (Settings > Web search)")


def results_text(query: str, results: list[dict]) -> str:
    """Search results as a tool result for the model."""
    if not results:
        return f'No results for "{query}".'
    lines = [f'Results for "{query}":']
    for n, r in enumerate(results, 1):
        snippet = " ".join(str(r.get("snippet") or "").split())[:500]
        lines.append(f"{n}. {r.get('title') or '(no title)'} - {r.get('url')}\n   {snippet}")
    return "\n".join(lines)


def call_query(call: dict) -> str:
    """The query of a queuerouter_web_search tool call ("" if unreadable)."""
    return call_argument(call, "query")


def call_arguments(call: dict) -> dict:
    """All arguments of a tool call ({} if unreadable)."""
    try:
        arguments = json.loads((call.get("function") or {}).get("arguments") or "{}")
    except ValueError:
        return {}
    return arguments if isinstance(arguments, dict) else {}


def call_argument(call: dict, name: str) -> str:
    """One argument of a tool call ("" if unreadable)."""
    try:
        arguments = json.loads((call.get("function") or {}).get("arguments") or "{}")
    except ValueError:
        return ""
    return str(arguments.get(name) or "") if isinstance(arguments, dict) else ""


# -- web_fetch: read a page (built in, no key) -----------------------------------------------------

FETCH_NAME = "queuerouter_web_fetch"
FETCH_TOOL = {
    "type": "function",
    "function": {
        "name": FETCH_NAME,
        "description": (
            "Read a web page (documentation, an article, a changelog) and get its text. Use links from search "
            "results or that the user gave. Never put secrets or private data in the address."
        ),
        "parameters": {
            "type": "object",
            "properties": {"url": {"type": "string", "description": "http(s) address of the page"}},
            "required": ["url"],
        },
    },
}
FETCH_MAX_BYTES = 2 * 1024 * 1024
FETCH_MAX_CHARS = 20_000
_TEXT_TYPES = (
    "text/",
    "application/json",
    "application/xml",
    "application/xhtml",
    "application/rss",
    "application/atom",
)
_SKIP_TAGS = {"script", "style", "noscript", "svg", "nav", "footer", "header", "form"}
_BLOCK_TAGS = {"p", "div", "br", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6", "pre", "section", "article", "table"}


class _Text(HTMLParser):
    """Readable text of an HTML page: no scripts, styles or navigation chrome; line breaks at blocks."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.skip = 0
        self.title = ""
        self.in_title = False

    def handle_starttag(self, tag, attrs):
        if tag in _SKIP_TAGS:
            self.skip += 1
        elif tag == "title":
            self.in_title = True
        elif tag in _BLOCK_TAGS:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in _SKIP_TAGS and self.skip:
            self.skip -= 1
        elif tag == "title":
            self.in_title = False
        elif tag in _BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data):
        if self.in_title:
            self.title += data
        elif not self.skip:
            self.parts.append(data)

    def text(self) -> str:
        lines = (" ".join(line.split()) for line in "".join(self.parts).splitlines())
        return "\n".join(line for line in lines if line)


def readable(text: str) -> str:
    """An HTML page as readable text (title first); other text as it is."""
    parser = _Text()
    parser.feed(text)
    title = parser.title.strip()
    return (f"{title}\n\n" if title else "") + parser.text()


async def public_host(host: str) -> str | None:
    """The address to connect to; SearchError unless every address ``host`` resolves to is public: no
    localhost, LAN, Docker networks, link-local (cloud metadata), multicast or reserved ranges."""
    try:
        infos = await asyncio.get_running_loop().getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    except OSError:
        raise SearchError(f"can't resolve {host}") from None
    for info in infos:
        address = ipaddress.ip_address(info[4][0])
        if not address.is_global or address.is_multicast:
            raise SearchError("refused: that address is not on the public internet")
    return infos[0][4][0] if infos else None


async def fetch(client: httpx.AsyncClient, url: str) -> tuple[str, str]:
    """(final address, readable text) of a page; SearchError on refusal or failure. Redirects are followed
    here, so every hop's address is checked too."""
    url = str(url or "").strip()
    if _URL_SECRET.search(url):
        raise SearchError("refused: the address looks like it contains a secret")
    for _ in range(5):
        parts = urlsplit(url)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise SearchError("only http(s) addresses can be read")
        address = await public_host(parts.hostname)  # looked up here, so tests can stand in for it
        headers = {
            "User-Agent": "QueueRouter-fetch/1.0",
            "Accept": "text/html,text/plain,application/json;q=0.9,*/*;q=0.5",
        }
        target, extensions = url, {}
        if address:  # connect to the address just checked, so a second DNS answer can't point elsewhere
            host = f"[{address}]" if ":" in address else address
            target = urlunsplit(parts._replace(netloc=host + (f":{parts.port}" if parts.port else "")))
            headers["Host"] = parts.netloc.rpartition("@")[2]
            extensions = {"sni_hostname": parts.hostname}  # the certificate is still checked for the name
        try:
            request = client.build_request("GET", target, headers=headers, timeout=20, extensions=extensions)
            response = await client.send(request, stream=True, follow_redirects=False)
            async with contextlib.aclosing(response):
                if response.status_code in (301, 302, 303, 307, 308) and response.headers.get("location"):
                    url = urljoin(url, response.headers["location"])
                    continue
                if response.status_code >= 400:
                    raise SearchError(f"the page answered HTTP {response.status_code}")
                kind = response.headers.get("content-type", "").lower()
                if kind and not kind.startswith(_TEXT_TYPES):
                    raise SearchError(f"not a text page ({kind.split(';')[0]})")
                data = bytearray()
                async for chunk in response.aiter_bytes():
                    data += chunk
                    if len(data) > FETCH_MAX_BYTES:
                        break
                encoding = response.encoding or "utf-8"
        except httpx.HTTPError as exc:
            raise SearchError(f"couldn't read the page ({type(exc).__name__})") from None
        text = bytes(data).decode(encoding, "replace")
        if "html" in kind or text.lstrip()[:15].lower().startswith(("<!doctype", "<html")):
            text = readable(text)
        if len(text) > FETCH_MAX_CHARS:
            text = text[:FETCH_MAX_CHARS] + "\n[... the rest of the page was cut ...]"
        return url, text
    raise SearchError("too many redirects")
