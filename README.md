# QueueRouter

An OpenAI-compatible proxy that sits in front of [ArliAI](https://www.arliai.com) (or any
OpenAI-compatible API) and **never sends more parallel requests than your plan allows**.
Extra requests are **queued and delayed, not rejected** — your apps see a slower response
instead of a `429 Too Many Requests`.

Built for agentic tools like **opencode**, which fire several requests at once (main model,
title generation, subagents) — more than ArliAI Pro's 2 parallel requests.

> Status: **v0.2** — single upstream, configured with environment variables.
> The full roadmap (web admin UI, multiple upstreams, aliases, fallbacks, async jobs…) is in
> [DESIGN.md](DESIGN.md).
>
> **`main` is mid-way through Phase 1a**: configuration now lives in a database under `/data`
> (mount a volume there). On the first start the environment variables are imported once; after
> that the database is the source of truth and will be edited in the upcoming admin UI.
> Stay on `:latest` / `:0.2` until v0.3 is tagged.

## How it works

```
opencode ───┐                                     ┌─► ArliAI (at most 2 in flight)
scripts ────┼─► Traefik (HTTPS) ─► QueueRouter ───┤
other apps ─┘                      (smart queue)  └─ everything else waits its turn
```

- **True queuing**: requests wait with the connection held open instead of failing.
- **Smart ordering** when a slot frees up:
  1. **Priority** — each access key is `high`, `normal` or `low`; a request can lower (never
     raise) its own with `X-QueueRouter-Priority`. Waiting `AGING_S` bumps a request one level
     up, so nothing starves.
  2. **Finish what you started** — requests from a session that is mid-task go before brand-new
     sessions, so an agent's task completes instead of every task crawling in parallel.
     Sessions come from `X-Session-Id`, or are derived from the conversation's system prompt
     and first message (subagents and title generation get their own).
  3. **Fair share** — among equals, the client served least recently goes first.
  4. **Arrival order**.
- **Slots held until the last byte**: a streaming response keeps its slot until it finishes.
- **Streaming keep-alives**: while queued, streaming clients receive SSE comments
  (`: queued position=3 waited=42s`) so nothing times out.
- **Safety-net retries**: if ArliAI still answers 429/5xx (e.g. another app uses the same
  account), QueueRouter keeps the slot, backs off (honouring `Retry-After`) and retries —
  the error never reaches your app unless retries run out.
- **Cancellation**: if a client disconnects while queued it leaves the queue; mid-stream, the
  upstream request is aborted and the slot freed immediately.
- **Your ArliAI key stays on the server**; apps use their own access keys.

## Deploy on a server

Requirements: a Linux server with Docker and a domain name pointing at it.

### Behind Traefik (default — Docker labels)

Assumes Traefik already runs on the server with a Docker provider, an HTTPS entrypoint and a
certificate resolver, attached to an external Docker network.

```bash
mkdir queuerouter && cd queuerouter
curl -fsSLO https://raw.githubusercontent.com/MRACHINI/QueueRouter/main/docker-compose.yml
curl -fsSL -o .env https://raw.githubusercontent.com/MRACHINI/QueueRouter/main/.env.example
openssl rand -hex 32        # use the output as an access key in ACCESS_KEYS
nano .env                   # DOMAIN, ARLIAI_API_KEY, ACCESS_KEYS,
                            # TRAEFIK_NETWORK / TRAEFIK_ENTRYPOINT / TRAEFIK_CERTRESOLVER
docker compose up -d
```

The labels route `Host(DOMAIN)` on your HTTPS entrypoint to the container's port 8000.
Traefik streams SSE without buffering by default — just don't attach `compress` or
`buffering` middlewares to this router. Its default timeouts don't cut long responses.

### Standalone with Caddy (no existing reverse proxy)

Caddy is a small web server that gets and renews Let's Encrypt certificates automatically;
the included [Caddyfile](deploy/Caddyfile) just forwards `DOMAIN` to QueueRouter. Needs ports
80/443 free.

```bash
curl -fsSL -o docker-compose.yml https://raw.githubusercontent.com/MRACHINI/QueueRouter/main/docker-compose.caddy.yml
mkdir deploy && curl -fsSL -o deploy/Caddyfile https://raw.githubusercontent.com/MRACHINI/QueueRouter/main/deploy/Caddyfile
# .env as above (the TRAEFIK_* lines are ignored)
docker compose up -d
```

### Check and update

```bash
curl https://YOUR_DOMAIN/health
```

```bash
docker compose pull && docker compose up -d
```

To pin a version, set `QUEUEROUTER_TAG=0.2.0` in `.env`.

Other reverse proxies work too: route to `queuerouter:8000`, **disable response buffering**
and allow long responses (Nginx: `proxy_buffering off; proxy_read_timeout 1h;`).

## Use with opencode

`opencode.json` (project or `~/.config/opencode/opencode.json`):

```json
{
  "$schema": "https://opencode.ai/config.json",
  "provider": {
    "queuerouter": {
      "npm": "@ai-sdk/openai-compatible",
      "name": "ArliAI via QueueRouter",
      "options": {
        "baseURL": "https://YOUR_DOMAIN/v1",
        "apiKey": "{env:QUEUEROUTER_KEY}",
        "timeout": false
      },
      "models": {
        "ARLIAI_MODEL_ID": {
          "name": "ArliAI model",
          "limit": { "context": 262144, "output": 16384 }
        }
      }
    }
  },
  "model": "queuerouter/ARLIAI_MODEL_ID",
  "small_model": "queuerouter/ARLIAI_MODEL_ID"
}
```

Then set `QUEUEROUTER_KEY` to one of your `ACCESS_KEYS`.

- `timeout: false` — a queued request may wait longer than opencode's 5-minute default.
- `limit.context` — the smaller of the model's own context and your plan's cap
  (ArliAI Pro: 262,144). opencode doesn't read it from the API, so set it here.
- List available model ids with:
  `curl -H "Authorization: Bearer $QUEUEROUTER_KEY" https://YOUR_DOMAIN/v1/models`

Any other OpenAI-compatible client works the same way: base URL `https://YOUR_DOMAIN/v1`,
API key = one of your access keys.

**Priorities**: give each app its own named key, e.g.
`ACCESS_KEYS=opencode:<key1>:high,scripts:<key2>:low` — opencode then always goes ahead of
your batch scripts, and `/stats` and the logs show usage per app.

## Configuration

All settings are environment variables in `.env` (see [.env.example](.env.example)):

| Variable | Default | Meaning |
|---|---|---|
| `ARLIAI_API_KEY` | — (required) | your real ArliAI key |
| `ACCESS_KEYS` | — (required) | comma-separated `key`, `name:key` or `name:key:priority` |
| `DOMAIN` | — (required for Caddy) | public hostname |
| `UPSTREAM_URL` | `https://api.arliai.com/v1` | any OpenAI-compatible API |
| `MAX_CONCURRENCY` | `2` | parallel requests your plan allows |
| `MIN_SPACING_MS` | `0` | minimum gap between request starts |
| `MAX_QUEUE_WAIT_S` | `0` (forever) | give up after waiting this long (504) |
| `KEEPALIVE_INTERVAL_S` | `15` | keep-alive interval for queued streams |
| `AGING_S` | `120` | waiting this long bumps a request one priority level up |
| `SESSION_AWARE` | `true` | sessions mid-task go before new sessions |
| `SESSION_WINDOW_S` | `60` | how long after a request a session counts as mid-task |
| `FAIR_SHARE` | `true` | among equals, least recently served client first |
| `MAX_RETRIES` | `8` | upstream retries (`-1` forever, `0` none) |
| `RETRY_BASE_DELAY_S` / `RETRY_MAX_DELAY_S` | `2` / `60` | backoff |
| `UPSTREAM_CONNECT_TIMEOUT_S` | `15` | |
| `UPSTREAM_READ_TIMEOUT_S` | `600` | max silence between upstream bytes |
| `MAX_BODY_MB` | `20` | request size limit |
| `AUTH_FAIL_LIMIT` / `AUTH_BLOCK_S` | `10` / `300` | block an IP after repeated bad keys |
| `TRUSTED_PROXIES` | Docker/private ranges | where `X-Forwarded-For` is trusted |
| `LOG_LEVEL` | `INFO` | |

## Endpoints

| Path | Notes |
|---|---|
| `POST /v1/chat/completions`, `POST /v1/*` | queued |
| `GET /v1/models` | passthrough, cached 5 min, doesn't use a slot |
| `GET /v1/*` | passthrough, doesn't use a slot |
| `GET /v1/queue` | queue depth, your client's queued/active, estimated wait |
| `GET /stats` | queue by lane and client, latency and token stats (access key required) |
| `GET /health` | liveness |

**Request headers** (optional, never forwarded upstream):
`X-QueueRouter-Priority: low|normal|high` (can only lower your key's priority),
`X-Session-Id` (group requests into a session).

**Response headers**: `X-Request-Id`; queue hints at admission —
`X-QueueRouter-Queue-Depth`, `X-QueueRouter-Position` (0 = started immediately),
`X-QueueRouter-Est-Wait-Ms`, `X-QueueRouter-Slots` (`active/total`); non-streaming responses
also `X-Queue-Wait-Ms`.

Each request is logged as one JSON line (`docker compose logs -f queuerouter`) with client,
priority, session, queue position and timings — metadata only, never prompt or response content.

## Development

Tests run in a throwaway container (the suite starts a mock ArliAI that enforces 2 slots):

```bash
docker run --rm -v "$PWD":/src -w /src python:3.12-slim sh -c \
  "pip install -q -r requirements-dev.txt && ruff check . && ruff format --check . && pytest -q -p no:cacheprovider"
```

Run locally from source: copy `.env.example` to `.env`, then
`docker compose -f docker-compose.dev.yml up --build` → `http://127.0.0.1:8000/v1`.

Pushing to `main` publishes `ghcr.io/mrachini/queuerouter:main`; tagging `vX.Y.Z` publishes
`X.Y.Z`, `X.Y`, `X` and `latest`.

## License

MIT
