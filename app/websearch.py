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

import json
import re

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
    try:
        arguments = json.loads((call.get("function") or {}).get("arguments") or "{}")
    except ValueError:
        return ""
    return str(arguments.get("query") or "") if isinstance(arguments, dict) else ""
