# QueueRouter

An OpenAI-compatible gateway that sits in front of [ArliAI](https://www.arliai.com) (or any
OpenAI-compatible API) and **never sends more parallel requests than your plan allows**.
Extra requests are **queued and delayed, not rejected** — your apps see a slower response
instead of a `429 Too Many Requests`.

Built for agentic tools like **opencode**, which fire several requests at once (main model,
title generation, subagents) — more than ArliAI Pro's 2 parallel requests. Everything is managed
from a **web admin UI**.

> Roadmap and design: [DESIGN.md](DESIGN.md).

## Features

- **True queuing** with the connection held open; slots are held until the last streamed byte;
  SSE keep-alives while queued so nothing times out.
- **Smart ordering**: priority per client (`high` / `normal` / `low`) with aging so nothing
  starves; *finish what you started* (sessions mid-task go before new ones); fair share between
  clients.
- **Several upstreams**: keys and providers grouped into **limit pools** (one per account — keys
  on the same account share its limit), load balancing (least-busy / weighted / ordered),
  model-aware routing, **failover** on errors, **circuit breakers** with health probes.
- **Provider presets** (ArliAI, Google Gemini, NVIDIA, OpenRouter, OpenAI, local servers…) incl. an
  **ArliAI plan selector** (Free → Max) that sets parallel requests
  and context cap.
- **Model aliases** (`smart`, `code`, …) that you can repoint without touching any app; listed in
  `/v1/models` with the target's context size.
- **Fallback chains**: an alias can list backup models, used when the first is unavailable,
  keeps failing, can't fit the prompt, or (opt-in) refuses — detected from content-policy errors,
  `finish_reason: content_filter` or a reply opening with a refusal. The request is resent
  unchanged.
- **Sticky routing**: with several upstreams, a conversation returns to the upstream that served
  it, so its prompt cache stays warm.
- **Provider quotas handled sensibly**: a provider's 429 message is shown (stream notes, errors, the
  Requests page); a used-up *daily* quota (Google `PerDay`, `limit: 0`, OpenAI `insufficient_quota`)
  fails fast or falls back instead of waiting, and that provider is skipped for that model for a
  while; ordinary rate limits wait as long as the provider asks, up to a per-upstream retry limit.
- **Unknown models** get a clear 404 with the closest names instead of being sent on (Settings can
  turn this off for providers with incomplete model lists).
- **Parameter rules** per alias and client: defaults, overrides, clamps (e.g. cap `max_tokens`),
  and stripping parameters an upstream rejects — also **per upstream** (e.g. Google rejects `store`).
- **Context-length pre-check**: prompts that clearly can't fit are refused immediately (or sent
  to a bigger-context fallback) instead of waiting in the queue.
- **Agent guard rails**: per-run budgets (tokens / requests / minutes per `X-Run-Id` or
  conversation), loop detection, and request tags for filtering.
- **Playground** in the admin UI: chat as any client through the real pipeline.
- **Allowed apps per client**: a key can be limited to apps whose User-Agent starts with given
  prefixes (e.g. `myapp/`); other apps get a 403. Every request's User-Agent is shown on the Requests
  page with what was sent to the provider when a rule changed it (click either to copy; also in the
  dashboard's client tooltips).
- **Outgoing User-Agent**: choose what providers see, per client or as an upstream default (the
  client's setting wins): keep the app's own, a neutral `queuerouter/<version>`, or a template with
  `{original} {client} {priority} {model} {upstream} {version}`, e.g. `{original} via {client}`.
- **Context sizes**: each model's context is read from the provider's model list, **learned** from the
  provider's own "too long" error when it doesn't say (NVIDIA, Gemini...), or set by hand on the Models
  page. Requests are checked before queuing, opencode is told the size, and every provider's overflow
  error is rewritten into the standard `context_length_exceeded` form that opencode compacts on.
- **Compaction** (per alias): when a conversation is too big for the model it goes to, QueueRouter shrinks
  it before sending — from ~80% of the model's context down to ~50%: old tool outputs keep their start and
  end, then old middle messages are dropped. The system prompt, the task, the current message and the
  last turns are always kept; the same cut is reused while it fits, so provider prompt caching keeps
  working. Shown on the Requests page ("compacted: trim 180k -> 95k").
  **Summarise** mode replaces the old part with a summary written by a model you choose (any size: long
  histories are read in parts, each building on the summary so far); the summary is kept per conversation
  (in memory, or saved encrypted) and extended rather than rewritten, and updated in the background when a
  conversation near its limit goes quiet. An alias can also tell apps a bigger context than the model has
  (e.g. 1M), so opencode keeps the whole conversation while each request is fitted to the real model.
- **Embedding slots**: a limit pool can give `/v1/embeddings` their own parallel limit (ArliAI counts them
  separately; its plans fill it in), so an indexing burst never makes chat requests wait.
- **Model council** (per alias): on planning steps (the first step after your message, or opencode's plan
  agent) several models answer at once and a judge either **picks** the best answer or **combines** them into
  one; tool steps use the alias's model alone. Mix providers so a council doesn't take all of one account's
  slots. Streaming apps get their response at once (keep-alives while members work); the Requests page shows
  "council: pick 2/3: GLM-4.7", and the members' and judge's requests are tagged `#council`.
- **Web search for council members**: with a search provider set up (Settings > Web search: Tavily, Brave or
  your own SearXNG), council members can search before the judge decides. QueueRouter runs the searches,
  gives each member its results and asks it again (up to a per-alias limit), and the judge sees every member's
  sources. opencode never sees this tool. Queries that look like they contain secrets are refused.
- **Orchestrator** (council mode "orchestrate"): the judge model leads the team. It plans, gives members
  focused tasks (briefs: they see only the task it writes), searches and reads the web, uses a calculator,
  may ask again, then answers (text or the app's own tools). Progress streams live into the app's thinking
  (opencode's "Thought"); budgets per step (rounds, member tasks, seconds) keep it from looping.
- **Choose which models each upstream offers** (Upstreams > "N models"): untick models to hide them from
  opencode, `/v1/models`, the Models page and the Playground; aliases can still use them. A switch decides
  whether models the provider adds later appear automatically.
- **Per-client policies**: allowed models, requests/tokens per minute and per day (delay or
  reject), max queued / active, queue and total timeouts, retry budget.
- **Operating modes**: running, paused, draining, maintenance.
- **Recurring jobs**: cron schedules (UTC, e.g. `0 2 * * *`) that create jobs — from the admin Jobs
  page or `/v1/schedules`; pause, resume or run now.
- **Monthly quotas** per client: requests, tokens and cost (at official prices) per calendar month;
  beyond them requests get 429 until the next month; alerts at 80 % and when used up.
- **Monitoring**: Prometheus `/metrics` (token-protected: queue, pools, circuits, requests, tokens,
  wait histograms, jobs) and OpenTelemetry traces over OTLP/HTTP (honours the caller's
  `traceparent`; GenAI attributes; queue and upstream spans). Both off until set in Settings.
- **Async jobs and batches**: `POST /v1/jobs` (or any request with `X-QueueRouter-Async: true`)
  returns at once and runs later — scheduled (`run_at`), with a `deadline`, only when idle, with a
  signed HTTPS callback; plus the OpenAI **Batch API** (`/v1/files`, `/v1/batches`) so SDK batch code
  works unchanged. Jobs run at low priority, one at a time by default, and survive restarts.
- **Other API shapes**: Anthropic Messages (`/v1/messages`) and OpenAI Responses (`/v1/responses`)
  are translated to chat completions, so tools speaking either API get the same queue, policies,
  aliases and fallbacks. Hosted/server tools (web search, code interpreter…) aren't supported.
- **Backup and restore** (owner): download a zip (database + optional encryption key), daily
  automatic copies in `/data/backups`, and restore from an upload or a listed copy — checked first,
  then QueueRouter restarts itself to load it (the previous data is kept).
- **Real-time admin**: the dashboard is pushed live (server-sent events) with a **Now** panel of
  requests in flight (queued/running, elapsed, time to first byte) and a 1-second chart; the Usage
  page refreshes itself as requests finish. Time to first byte is recorded per request.
- **Charts**: live running/queued chart on the dashboard (last 30 minutes); tokens, requests and
  cost per day on the Usage page (server-rendered SVG, no scripts from elsewhere).
- **Response times** per model and upstream: the provider's own timings for streamed requests (its queue,
  first token, thinking time, output speed, longest pause, retry waits), median and worst 5% over the last
  hour, day or week, with a trend chart; an optional alert when a model gets slow to start.
- **Structured-output repair**: JSON-mode replies and tool-call arguments with common breakage
  (code fences, extra text, trailing commas, cut-off brackets) are fixed before they reach the app
  (`X-QueueRouter-Structured: repaired | invalid`).
- **Opt-in content logging** per client (optionally for N hours): prompts and replies stored
  encrypted, readable only by admins with the content permission (every view audited); the client
  sees `X-QueueRouter-Logging: on`.
- **Usage tracking**: request log (metadata only, never content) and daily usage per client and
  model, CSV export.
- **Cost at official prices**: what your tokens would have cost on the model makers' own APIs
  (prices from [models.dev](https://models.dev), refreshed daily; cached prompt tokens at the
  cached rate), and how much your flat-rate plan saves. Per-model overrides on the Models page;
  set your plan price on the pool (ArliAI plans fill it in).
- **Alerts**: upstream down/recovered, queue backed up, long waits, high error rate, clients hitting
  their limits, keys about to expire, daily tokens — shown in the admin (unread badge) and sent to
  Discord, Slack, ntfy or a signed JSON webhook, with per-rule thresholds and cooldowns.
- **Admin UI**: dashboard, clients (keys kept encrypted, shown again on request; rename a client and its history follows), upstreams, models, usage,
  requests, settings, users with roles (owner / admin / operator / viewer), audit log. Dark mode
  and phone-friendly. Changes apply instantly.
- **Secure by default**: two-factor sign-in (authenticator apps, recovery codes), upstream keys
  encrypted at rest, hashed client keys, argon2 passwords, request-size limits enforced while reading, job
  callbacks pinned to a checked public address (HTTPS, port 443), deleting a client deletes its data,
  CSRF protection, login throttling, strict CSP, optional admin IP allow-list.

## How it works

```
opencode ───┐                                     ┌─► ArliAI account A (2 in flight)
scripts ────┼─► Traefik (HTTPS) ─► QueueRouter ───┼─► ArliAI account B (2 in flight)
n8n ────────┘                      (smart queue)  └─► other provider / local model
```

## Deploy (Docker Compose behind Traefik)

Requirements: a Linux server with Docker, Traefik with a Docker provider, an HTTPS entrypoint
and a certificate resolver, and a domain name pointing at the server.

```yaml
services:
  queuerouter:
    image: ghcr.io/mrachini/queuerouter:0.3
    container_name: queuerouter
    restart: unless-stopped
    environment:
      PUBLIC_URL: "https://queuerouter.example.com"
      # Optional: create the first admin automatically instead of using the setup token.
      # ADMIN_USERNAME: "admin"
      # ADMIN_PASSWORD: "a-long-password"
      # Optional, imported ONCE on the very first start (then managed in the UI):
      # ARLIAI_API_KEY: "your-arliai-key"
      # ACCESS_KEYS: "opencode:<key>:high"
    volumes:
      - qr_data:/data            # database, encryption key, backups — back this up!
    expose: ["8000"]
    read_only: true
    tmpfs: [/tmp]
    security_opt: ["no-new-privileges:true"]
    cap_drop: [ALL]
    cap_add: [CHOWN, SETUID, SETGID]   # only at start: fix /data ownership, then dropped
    stop_grace_period: 60s
    deploy:
      replicas: 1                # never scale: the queue lives in memory
    networks: [traefik]
    labels:
      - traefik.enable=true
      - traefik.docker.network=traefik
      - traefik.http.routers.queuerouter.rule=Host(`queuerouter.example.com`)
      - traefik.http.routers.queuerouter.entrypoints=websecure
      - traefik.http.routers.queuerouter.tls=true
      - traefik.http.routers.queuerouter.tls.certresolver=letsencrypt
      - traefik.http.services.queuerouter.loadbalancer.server.port=8000

volumes:
  qr_data:

networks:
  traefik:
    external: true
```

```bash
docker compose up -d
docker compose logs queuerouter | grep setup_token
```

Open `https://queuerouter.example.com/admin`, enter the setup token from the log, and create your
owner account. Then:

1. **Upstreams** → add ArliAI (pick your plan) with your API key.
2. **Clients** → create a client per app (e.g. `opencode`, priority `high`); copy its key (shown
   with a ready-made opencode config; **Show** on the Clients page displays it again later).
3. Optionally **Models** → add aliases.

No Traefik? Use [docker-compose.caddy.yml](docker-compose.caddy.yml) (Caddy gets certificates
automatically). Other reverse proxies work too: route to `queuerouter:8000`, **disable response
buffering** and allow long responses (Nginx: `proxy_buffering off; proxy_read_timeout 1h;`).

Update: `docker compose pull && docker compose up -d` (database migrations run automatically,
with a backup in `/data/backups` first).

### Data volume permissions

Like linuxserver.io images, the container starts as root only to make `/data` belong to
`PUID:PGID` (default `10001:10001`), then permanently drops to that user with **no
capabilities** before QueueRouter starts — so bind mounts and existing volumes just work. This
needs `cap_add: [CHOWN, SETUID, SETGID]` next to `cap_drop: [ALL]`, as in the compose file above.

- Want the files owned by your own user? Set `PUID` / `PGID` (e.g. `1000`).
- Prefer the container never to start as root? Use `user: "1000:1000"` instead (no `cap_add`
  needed) and make sure that user owns the folder; otherwise QueueRouter refuses to start and
  logs the exact fix.

### Upgrading from 0.1 / 0.2

0.3 keeps its configuration in a database under `/data`. **Add the `/data` volume** (see above)
before upgrading — without it, every container recreate starts from scratch. On the first start
your existing `ARLIAI_API_KEY`, `ACCESS_KEYS` and tuning variables are imported automatically, so
existing keys keep working; after that, edit everything in the admin UI (the variables are then
ignored, and the log says so).

## Use with opencode

opencode doesn't discover models from custom providers, so QueueRouter generates the provider
config for it: every model and alias the key may use, with context limits (capped at your plan,
e.g. ArliAI Pro 262,144) and `timeout: false` (queued requests may wait longer than opencode's
default). The client page in the admin shows these commands with your key filled in.

**Automatic (opencode 1.19+, recommended)** — log in once; opencode stores the key and reloads
the model list from QueueRouter on every start, so new models and aliases just appear:

```bash
QUEUEROUTER_KEY=<client key> opencode auth login https://queuerouter.example.com
```

```powershell
$env:QUEUEROUTER_KEY="<client key>"; opencode auth login https://queuerouter.example.com/windows
```

Then pick a `queuerouter/…` model with `/models`. (This uses opencode's `/.well-known/opencode`
mechanism; the Windows URL just swaps the login command for a PowerShell one.)

**Plugin (optional, recommended)** — QueueRouter also serves an opencode plugin. One file works in the opencode
**terminal** and the **desktop app**, on opencode 1.19 and 2.x (tested with 1.19.3 and 2.1.2). It tags each
request with opencode's session and task, so QueueRouter finishes a conversation (subagents
included) before starting new ones and applies run budgets and loop detection per task; it sends
title generation at low priority so it never holds up the main model; and it loads your models on
every start from `QUEUEROUTER_KEY` (then the login above is optional). It also keeps the list current **while
opencode runs**: it checks QueueRouter every minute and, when models or aliases changed and no session is
busy, reloads opencode's providers so the model picker updates by itself (`QUEUEROUTER_REFRESH_S`
changes the interval, `0` turns it off). Even without the plugin, QueueRouter groups an opencode conversation's
requests (subagents included) from the session headers opencode sends, and tags them `opencode`. Install once into opencode's
global plugin folder and restart opencode (the Clients page shows these with your domain):

```bash
mkdir -p ~/.config/opencode/plugin && curl -fsSL https://queuerouter.example.com/opencode/queuerouter.js -o ~/.config/opencode/plugin/queuerouter.js
```

```powershell
$d="$HOME/.config/opencode/plugin"; New-Item -ItemType Directory -Force $d | Out-Null; Invoke-WebRequest https://queuerouter.example.com/opencode/queuerouter.js -OutFile "$d/queuerouter.js"
```

From 0.41 the plugin **updates itself**: when QueueRouter serves a changed plugin, the installed file is replaced
with it (checked, and swapped in one step). The desktop app reloads it by itself; otherwise opencode uses it from
its next start (the terminal UI says so in a toast). Plugins installed before 0.41 need one last manual install. Turn it off for everyone with **Settings →
opencode → Update the plugin automatically**, or on one machine with `QUEUEROUTER_AUTO_UPDATE=0`. This means your
QueueRouter server hands out code that runs on every machine with the plugin, so keep the server as safe as the
machines.

**Config file** — for older opencode, or if you prefer files: download it from the admin
(**Clients → opencode.json** on the client's row), or with curl as below; save it as
`opencode.json` in your project or point `OPENCODE_CONFIG` at it, then
set `QUEUEROUTER_KEY` to your key. Re-download to refresh.

```bash
curl -H "Authorization: Bearer <client key>" https://queuerouter.example.com/v1/opencode-config -o ~/.config/opencode/queuerouter.json
```

Any OpenAI-compatible client works the same way: base URL `https://<domain>/v1`, API key = a
client key.

### Thinking levels

Models that can think get a level selector in opencode 2.x (next to the model; ctrl+t in the terminal): `none`,
`low`, `high`… as the model offers them, or `off` / `thinking` for models that can only switch thinking on and off.
QueueRouter finds the levels in each upstream's model list (OpenRouter lists them) or, by name, in OpenRouter's public
list, and asks each upstream its own way (OpenRouter `reasoning.effort`, a self-hosted vLLM server's chat-template
switch, OpenAI-style `reasoning_effort`). ArliAI's models get none: they think whatever is sent. Correct a model on **Models → Thinking levels**. Needs the
QueueRouter plugin (re-install it once after upgrading to 0.38).

### Cost in opencode

opencode shows a session cost next to the context size, worked out from each model's price. Through the plugin
QueueRouter gives it that price: yours (**Models → Reference prices**), else nothing extra on a pool with a monthly
price (e.g. ArliAI), the upstream's own price (OpenRouter), or the models.dev match, the highest when several
upstreams serve a model. Councils and summaries make calls opencode can't see; the **Usage** page counts everything.

### Models in strength order

Turn on **Settings → opencode → Order models by strength** and the opencode model list is numbered: your aliases
first (`00 · smart`), then the strongest models with their score (`01 · 58.3 · …`, `02 · 52.0 · …`; `~` marks a
score taken from the base model), then models without a score. Scores come from
the [Artificial Analysis](https://artificialanalysis.ai/) index (intelligence, coding or agentic): as published in
OpenRouter's public model list with no setup, or the full index with a free API key in Settings (read once a day), or
from your own score per model on the **Models** page, which wins. Fine-tunes
and renamed variants get their base model's score: `GLM-4.6-Derestricted-v5` ranks as GLM-4.6.

## Environment variables (bootstrap only)

Everything else is configured in the admin UI.

| Variable | Default | Meaning |
|---|---|---|
| `DATA_DIR` | `/data` | database, `secret.key`, backups |
| `SECRET_KEY` | generated into `/data/secret.key` | encrypts upstream keys at rest — back it up with the data |
| `PUBLIC_URL` | — | used in setup hints and generated snippets |
| `ADMIN_USERNAME` / `ADMIN_PASSWORD` | — | create the first owner without the setup token |
| `ADMIN_ALLOWED_IPS` | any | CIDR allow-list for `/admin` (others get 404) |
| `TRUSTED_PROXIES` | `127.0.0.1/32,::1/128` | your reverse proxy's address or network: only there are `X-Forwarded-For` / `-Proto` believed (see [Behind a reverse proxy](#behind-a-reverse-proxy)) |
| `LOG_LEVEL` | `INFO` | |
| `ARLIAI_API_KEY`, `UPSTREAM_URL`, `MAX_CONCURRENCY`, `ACCESS_KEYS`, … | — | **imported once** on the first start (see [.env.example](.env.example)) |

### Behind a reverse proxy

QueueRouter believes `X-Forwarded-For` (the client's real address, used for the admin allow-list, sign-in
throttling and the Requests page) and `X-Forwarded-Proto` (secure cookies) only from addresses in
`TRUSTED_PROXIES`. The default is the machine itself (`127.0.0.1/32,::1/128`), so without a proxy nobody can
fake their address. Behind Traefik or Caddy in Docker, set it to the proxy's network, ideally that network's own
subnet:

```sh
docker network inspect traefik --format '{{range .IPAM.Config}}{{.Subnet}}{{end}}'   # e.g. 172.18.0.0/16
```

```env
TRUSTED_PROXIES=172.18.0.0/16
```

If it's missing, every request appears to come from the proxy (one address for everyone), and the log says so
once: `untrusted_proxy`.

## API

| Path | Notes |
|---|---|
| `POST /v1/chat/completions`, `POST /v1/*` | queued |
| `POST /v1/jobs`, `GET /v1/jobs[/{id}]`, `DELETE /v1/jobs/{id}` | async jobs: `{"request": {...}, "endpoint", "priority", "run_at", "deadline", "idle_only", "callback_url", "callback_secret", "tags"}` → 202; result on `GET` when done |
| `/v1/files`, `/v1/batches` | OpenAI-compatible Batch API (JSONL in, output/error files out, `completion_window` 24h) |
| `POST /v1/messages` | **Anthropic Messages API** (Claude SDKs and tools), translated to chat completions; streaming, tools, images; `/v1/messages/count_tokens` estimates |
| `POST /v1/responses` | **OpenAI Responses API** (Agents SDK, newer clients); streaming, function tools, structured output, `previous_response_id` (kept encrypted 7 days; `store: false` keeps nothing) |
| `GET /v1/models` | all upstreams' models + aliases, filtered to the client's allowed models |
| `GET /v1/models/{id}` | one model (aliases and `model@upstream` too); other `/v1/` GETs are refused unless listed in Settings → "GET paths passed to the provider" |
| `GET /v1/queue` | queue depth, your client's queued/active, estimated wait |
| `GET /v1/opencode-config` | generated opencode provider config for your key |
| `GET /opencode/queuerouter.js` | opencode plugin (session/task tags, priorities, model list) |
| `GET /.well-known/opencode` | opencode login manifest (`/windows/.well-known/opencode` for Windows) |
| `GET /stats` | full stats as JSON (client key required) |
| `GET /metrics` | Prometheus metrics; `Authorization: Bearer <metrics token>` (Settings → Monitoring) |
| `/v1/schedules` | recurring jobs: `POST` `{"name", "cron", "request", ...job fields}`, `GET`, `PATCH {"enabled"}`, `DELETE`, `POST /{id}/run` |
| `GET /health` | liveness |

**Request headers** (optional, never forwarded upstream): `X-QueueRouter-Priority:
low|normal|high` (can only lower your key's priority), `X-Session-Id` (group requests into a
conversation), `X-Run-Id` (one agent task, for run budgets and loop detection),
`X-QueueRouter-Run-Budget-Tokens` (lower your run's token budget), `X-QueueRouter-Tags`
(comma-separated labels).

**Response headers**: `X-Request-Id`, `X-QueueRouter-Queue-Depth`, `X-QueueRouter-Position`
(0 = started immediately), `X-QueueRouter-Est-Wait-Ms`, `X-QueueRouter-Slots`; non-streaming
responses also `X-Queue-Wait-Ms`.

QueueRouter only refuses requests for reasons you configured: bad key (401), model not allowed
(403), a client's queue/rate limit in `reject` mode, run budget or loop rejection (429),
draining/maintenance (503), body too large (413), and prompts that clearly exceed the model's
context (400 `context_length_exceeded`).

Responses also carry `X-QueueRouter-Model` (the model that actually answered, e.g. after a
fallback); streams announce a fallback with an SSE comment.

## Development

Tests run in a throwaway container (they start mock upstreams that enforce their concurrency
limit and return 429 beyond it):

```bash
docker run --rm -v "$PWD":/src -w /src python:3.14-slim sh -c \
  "pip install -q -r requirements-dev.txt && ruff check . && ruff format --check . && pytest -q -p no:cacheprovider"
```

Run from source: `docker compose -f docker-compose.dev.yml up --build` → `http://127.0.0.1:8000`.

Pushing to `main` publishes `ghcr.io/mrachini/queuerouter:main`; tagging `vX.Y.Z` publishes
`X.Y.Z`, `X.Y`, `X` and `latest`.

## License

MIT
