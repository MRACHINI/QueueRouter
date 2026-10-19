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
- **Provider presets** incl. an **ArliAI plan selector** (Free → Max) that sets parallel requests
  and context cap.
- **Model aliases** (`smart`, `code`, …) that you can repoint without touching any app; listed in
  `/v1/models` with the target's context size.
- **Fallback chains**: an alias can list backup models, used when the first is unavailable,
  keeps failing, can't fit the prompt, or (opt-in) refuses — detected from content-policy errors,
  `finish_reason: content_filter` or a reply opening with a refusal. The request is resent
  unchanged.
- **Sticky routing**: with several upstreams, a conversation returns to the upstream that served
  it, so its prompt cache stays warm.
- **Parameter rules** per alias and client: defaults, overrides, clamps (e.g. cap `max_tokens`),
  and stripping parameters an upstream rejects.
- **Context-length pre-check**: prompts that clearly can't fit are refused immediately (or sent
  to a bigger-context fallback) instead of waiting in the queue.
- **Agent guard rails**: per-run budgets (tokens / requests / minutes per `X-Run-Id` or
  conversation), loop detection, and request tags for filtering.
- **Playground** in the admin UI: chat as any client through the real pipeline.
- **Per-client policies**: allowed models, requests/tokens per minute and per day (delay or
  reject), max queued / active, queue and total timeouts, retry budget.
- **Operating modes**: running, paused, draining, maintenance.
- **Backup and restore** (owner): download a zip (database + optional encryption key), daily
  automatic copies in `/data/backups`, and restore from an upload or a listed copy — checked first,
  then QueueRouter restarts itself to load it (the previous data is kept).
- **Charts**: live running/queued chart on the dashboard (last 30 minutes); tokens, requests and
  cost per day on the Usage page (server-rendered SVG, no scripts from elsewhere).
- **Usage tracking**: request log (metadata only, never content) and daily usage per client and
  model, CSV export.
- **Cost at official prices**: what your tokens would have cost on the model makers' own APIs
  (prices from [models.dev](https://models.dev), refreshed daily; cached prompt tokens at the
  cached rate), and how much your flat-rate plan saves. Per-model overrides on the Models page;
  set your plan price on the pool (ArliAI plans fill it in).
- **Alerts**: upstream down/recovered, queue backed up, long waits, high error rate, clients hitting
  their limits, keys about to expire, daily tokens — shown in the admin (unread badge) and sent to
  Discord, Slack, ntfy or a signed JSON webhook, with per-rule thresholds and cooldowns.
- **Admin UI**: dashboard, clients (keys shown once, stored hashed), upstreams, models, usage,
  requests, settings, users with roles (owner / admin / operator / viewer), audit log. Dark mode
  and phone-friendly. Changes apply instantly.
- **Secure by default**: two-factor sign-in (authenticator apps, recovery codes), upstream keys
  encrypted at rest, hashed client keys, argon2 passwords,
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
2. **Clients** → create a client per app (e.g. `opencode`, priority `high`); copy its key — it's
   shown once, with a ready-made opencode config.
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

**Plugin (optional, recommended)** — QueueRouter also serves an opencode plugin. It tags each
request with opencode's session and task, so QueueRouter finishes a conversation (subagents
included) before starting new ones and applies run budgets and loop detection per task; it sends
title generation at low priority so it never holds up the main model; and it loads your models on
every start from `QUEUEROUTER_KEY` (then the login above is optional). Install once into opencode's
global plugin folder and restart opencode (the Clients page shows these with your domain):

```bash
mkdir -p ~/.config/opencode/plugin && curl -fsSL https://queuerouter.example.com/opencode/queuerouter.js -o ~/.config/opencode/plugin/queuerouter.js
```

```powershell
$d="$HOME/.config/opencode/plugin"; New-Item -ItemType Directory -Force $d | Out-Null; Invoke-WebRequest https://queuerouter.example.com/opencode/queuerouter.js -OutFile "$d/queuerouter.js"
```

**Config file** — for older opencode, or if you prefer files: download it from the admin
(**Clients → opencode.json** on the client's row), or with curl as below; save it as
`opencode.json` in your project or point `OPENCODE_CONFIG` at it, then
set `QUEUEROUTER_KEY` to your key. Re-download to refresh.

```bash
curl -H "Authorization: Bearer <client key>" https://queuerouter.example.com/v1/opencode-config -o ~/.config/opencode/queuerouter.json
```

Any OpenAI-compatible client works the same way: base URL `https://<domain>/v1`, API key = a
client key.

## Environment variables (bootstrap only)

Everything else is configured in the admin UI.

| Variable | Default | Meaning |
|---|---|---|
| `DATA_DIR` | `/data` | database, `secret.key`, backups |
| `SECRET_KEY` | generated into `/data/secret.key` | encrypts upstream keys at rest — back it up with the data |
| `PUBLIC_URL` | — | used in setup hints and generated snippets |
| `ADMIN_USERNAME` / `ADMIN_PASSWORD` | — | create the first owner without the setup token |
| `ADMIN_ALLOWED_IPS` | any | CIDR allow-list for `/admin` (others get 404) |
| `TRUSTED_PROXIES` | Docker/private ranges | where `X-Forwarded-For` is trusted |
| `LOG_LEVEL` | `INFO` | |
| `ARLIAI_API_KEY`, `UPSTREAM_URL`, `MAX_CONCURRENCY`, `ACCESS_KEYS`, … | — | **imported once** on the first start (see [.env.example](.env.example)) |

## API

| Path | Notes |
|---|---|
| `POST /v1/chat/completions`, `POST /v1/*` | queued |
| `GET /v1/models` | all upstreams' models + aliases, filtered to the client's allowed models |
| `GET /v1/queue` | queue depth, your client's queued/active, estimated wait |
| `GET /v1/opencode-config` | generated opencode provider config for your key |
| `GET /opencode/queuerouter.js` | opencode plugin (session/task tags, priorities, model list) |
| `GET /.well-known/opencode` | opencode login manifest (`/windows/.well-known/opencode` for Windows) |
| `GET /stats` | full stats as JSON (client key required) |
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
docker run --rm -v "$PWD":/src -w /src python:3.12-slim sh -c \
  "pip install -q -r requirements-dev.txt && ruff check . && ruff format --check . && pytest -q -p no:cacheprovider"
```

Run from source: `docker compose -f docker-compose.dev.yml up --build` → `http://127.0.0.1:8000`.

Pushing to `main` publishes `ghcr.io/mrachini/queuerouter:main`; tagging `vX.Y.Z` publishes
`X.Y.Z`, `X.Y`, `X` and `latest`.

## License

MIT
