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
  SSE keep-alives while queued, and while a provider sends nothing, so nothing times out.
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
- **Operating modes**: running, paused, draining (ends by itself on the next start), maintenance.
- **Updates clients don't notice**: running requests finish and new ones wait at a small gate in front of
  QueueRouter while it restarts (see [Update](#update)).
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
  aliases and fallbacks: Claude Code too (see [Use with Claude Code](#use-with-claude-code)). Apps that mix chat
  completions' own forms in (tool messages, `tool_calls`, `input_text` parts) are understood. Hosted/server tools (web
  search, code interpreter…) and PDFs aren't supported; a request that can't be translated shows on the Requests page.
- **Claude with prompt caching**: the Anthropic preset talks to Claude's own Messages API, so agents and
  conversations are cached by themselves: what Claude read a moment ago costs a tenth of the input price or less. Tools,
  streaming and images work as before; apps keep speaking chat completions. Writing to the cache costs 1.25 × the input
  price, so a single message without tools only caches its system prompt. Claude's thinking isn't shown, or passed back
  between tool calls; its levels are offered in opencode. A system message sent during a conversation stays where it is,
  so the cache still works, and the conversation is put in the shape Claude insists on (it starts with the user, every
  tool call gets a result). The newest models refuse a forced tool call, so they're asked for one instead. An upstream
  added as Custom with the same address stays on Claude's OpenAI-compatible endpoint.
- **Backup and restore** (owner): download a zip (database + optional encryption key), daily
  automatic copies in `/data/backups`, and restore from an upload or a listed copy — checked first,
  then QueueRouter restarts itself to load it (the previous data is kept).
- **Real-time admin**: the dashboard is pushed live (server-sent events) with a **Now** panel of
  requests in flight (queued/running, elapsed, time to first byte) and a 1-second chart. The Requests and
  Usage pages update themselves as requests finish, and reconnect and catch up by themselves after a
  restart or when the computer wakes. Time to first byte is recorded per request.
- **Costs**: at official prices, split into normal input, cached input and output, for each request (Requests
  page), per client and day, and per model over 1 day, 1 week or 30 days (Usage page). A council's cost adds up
  its members and judge. Click a row on the Requests page for its identifiers, User-Agents and timings.
- **Every model shown with its provider**: `Synthetic|GLM-4.7` on every page, and one entry per provider in opencode
  (`04|32.5|$0.200|GLM-4.7|Synthetic`: number, score, price, name, provider).
  Each provider's model has its own price, score, context size and thinking levels. Give it an optional custom name
  and it shows everywhere, in opencode too, and its automatic score and price are found by that name; the real ID
  is still what's sent to the provider. See [Model names](#model-names).
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
  cached rate, tokens written to Claude's cache at 1.25 × the input price), and how much your flat-rate plan saves. Per-model overrides on the Models page;
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
scripts ────┼─► Traefik ─► gate ─► QueueRouter ───┼─► ArliAI account B (2 in flight)
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
    stop_grace_period: 11m       # an update lets running requests finish (up to 10 minutes)
    deploy:
      replicas: 1                # never scale: the queue lives in memory
    networks: [traefik]
    labels:
      - traefik.enable=false     # Traefik talks to the gate

  gate:                          # holds new requests while QueueRouter restarts; see "Update"
    image: caddy:2.11.7-alpine
    command: [caddy, run, --config, /etc/caddy/gate.caddyfile, --adapter, caddyfile]
    configs:
      - source: gate
        target: /etc/caddy/gate.caddyfile
    environment:
      XDG_CONFIG_HOME: /tmp
      XDG_DATA_HOME: /tmp
    expose: ["8000"]
    restart: unless-stopped
    user: "10001:10001"
    cap_drop: [ALL]
    cap_add: [NET_BIND_SERVICE]  # the Caddy binary carries it and won't start without
    networks: [traefik]
    labels:
      - traefik.enable=true
      - traefik.docker.network=traefik
      - traefik.http.routers.queuerouter.rule=Host(`queuerouter.example.com`)
      - traefik.http.routers.queuerouter.entrypoints=websecure
      - traefik.http.routers.queuerouter.tls=true
      - traefik.http.routers.queuerouter.tls.certresolver=letsencrypt
      - traefik.http.services.queuerouter.loadbalancer.server.port=8000

configs:
  gate:
    content: |
      {
        admin off
        auto_https off
        persist_config off
        servers {
          trusted_proxies static private_ranges
        }
      }
      :8000 {
        reverse_proxy queuerouter:8000 {
          flush_interval -1
          lb_try_duration 5m
          lb_try_interval 500ms
          request_buffers 25MiB
          transport http {
            keepalive off
          }
        }
      }

volumes:
  qr_data:

networks:
  traefik:
    external: true
```

The gate needs Docker Compose 2.23 or newer for `configs` with `content`. It's also why `TRUSTED_PROXIES` must
be a network that includes it (see [Behind a reverse proxy](#behind-a-reverse-proxy)).

```bash
docker compose up -d
docker compose logs queuerouter | grep setup_token
```

Open `https://queuerouter.example.com/admin`, enter the setup token from the log, and create your
owner account. Then:

1. **Upstreams** → add ArliAI (pick your plan) with your API key.
2. **Clients** → create a client per app (e.g. `opencode`, priority `high`); copy its key (shown
   with a ready-made opencode command; **Show** on the Clients page displays it again later, for admins with
   *can read logged content* on the Users page, since a key also reads that client's job results).
3. Optionally **Models** → add aliases.

No Traefik? Use [docker-compose.caddy.yml](docker-compose.caddy.yml) (Caddy gets certificates
automatically). Other reverse proxies work too: route to `queuerouter:8000`, **disable response
buffering** and allow long responses (Nginx: `proxy_buffering off; proxy_read_timeout 1h;`).

### Update

```bash
docker compose pull queuerouter && docker compose up -d queuerouter
```

Database migrations run automatically, with a backup in `/data/backups` first. Clients don't see an error:

1. The old version stops taking new requests and finishes what it has, queued ones included, for up to 10 minutes.
2. New requests wait at the gate, for up to 5 minutes (`lb_try_duration`). Streaming clients see nothing meanwhile.
3. The new version starts and answers them.

New requests wait for the slowest running one, so that's usually seconds, sometimes minutes. A request still
running after 10 minutes is cut, and one held for more than 5 gets an error. The gate itself has a fixed version
and isn't touched by this command; updating it is the one thing that does cut requests.

Prefer to choose the moment? Set **Draining** on the Dashboard: new requests get your message and a 503, and
it says when nothing is left. It turns itself off on the next start.

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

opencode doesn't discover models from custom providers, so QueueRouter gives it a small plugin with every model and
alias the key may use, their context limits (capped at your plan, e.g. ArliAI Pro 262,144) and `timeout: false` (queued
requests may wait longer than opencode's default).

One command installs the plugin and saves your client key in a file beside it, so nothing goes in the environment.
**Clients → Connect opencode** shows it with your domain, and **Show** on a client fills in its key (a new client's page
has it filled in already):

```bash
mkdir -p ~/.config/opencode/plugin && curl -fsSL https://queuerouter.example.com/opencode/queuerouter.js -o ~/.config/opencode/plugin/queuerouter.js && (umask 077; printf %s '<client key>' > ~/.config/opencode/plugin/queuerouter.key)
```

```powershell
$d="$HOME/.config/opencode/plugin"; New-Item -ItemType Directory -Force $d | Out-Null; Invoke-WebRequest https://queuerouter.example.com/opencode/queuerouter.js -OutFile "$d/queuerouter.js"; Set-Content "$d/queuerouter.key" '<client key>' -Encoding ascii
```

Restart opencode (terminal version: `opencode service restart`) and pick a `queuerouter/…` model.

The key sits in plain text in `queuerouter.key`. To change it, run the command again or edit the file, then restart
opencode. `QUEUEROUTER_KEY` in the environment still works, and wins over the file.

Don't use `opencode auth login` on opencode 2.x: it fails while opencode's background service is running, and the key
it stores stays in opencode's database, where the plugin can't read it. Models would show up, but without the live
updates, thinking levels, session cost and self-update below.

**What the plugin does.** One file works in the opencode **terminal** and the **desktop app** (tested with opencode
2.2.5; its code for 1.19 is still there, but no longer tested). It tags each request with opencode's session and task,
so QueueRouter finishes a conversation (subagents included) before starting new ones and applies run budgets and loop
detection per task; it sends title generation at low priority so it never holds up the main model; and it loads your
models on every start. It also keeps the list current **while opencode runs**: it checks QueueRouter every minute and,
when models or aliases changed and no session is busy, reloads opencode's providers so the model picker updates by
itself (`QUEUEROUTER_REFRESH_S` changes the interval, `0` turns it off). Even without the plugin, QueueRouter groups an
opencode conversation's requests (subagents included) from the session headers opencode sends, and tags them `opencode`.

From 0.41 the plugin **updates itself**: when QueueRouter serves a changed plugin, the installed file is replaced
with it (checked, and swapped in one step). The desktop app reloads it by itself; otherwise opencode uses it from
its next start (the terminal UI says so in a toast). Plugins installed before 0.41 need one last manual install. Turn it off for everyone with **Settings →
opencode → Update the plugin automatically**, or on one machine with `QUEUEROUTER_AUTO_UPDATE=0`. This means your
QueueRouter server hands out code that runs on every machine with the plugin, so keep the server as safe as the
machines. From 0.44 a plugin only updates itself over https (or from QueueRouter on the same machine): over plain
http anyone on the network path could hand it other code. With an http address, re-install by hand.

**Config file** — without the plugin: download it from the admin
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
switch, OpenAI-style `reasoning_effort`). ArliAI's models get none: they think whatever is sent. Claude's models (the
Anthropic preset) offer Claude's own levels, `low` to `max`, as its model list states. Correct a model on
**Models → Thinking levels**. Needs the QueueRouter plugin (re-install it once after upgrading to 0.38).

### Cost in opencode

opencode shows a session cost next to the context size, worked out from each model's price. Through the plugin
QueueRouter gives it that price: yours (**Models → Reference prices**), else nothing extra on a pool with a monthly
price (e.g. ArliAI), the upstream's own price (OpenRouter), or the models.dev match, the highest when several
upstreams serve a model. Councils and summaries make calls opencode can't see, and it isn't told about Claude's cache
writes, which it prices as plain input; the **Usage** page counts everything.

### Model names

**Models → Model names** gives a model on a provider your own name, for example `DeepSeek V4 Flash` for
`hf:deepseek-ai/DeepSeek-V4.1-Flash-1017`. It then shows as `Synthetic|DeepSeek V4 Flash` on every page, past
requests included, and as `DeepSeek V4 Flash|Synthetic` in opencode, and the model's automatic score, price and
thinking levels are looked up by it.
Several models can share a name: each still has its own row, and its own price and score if you set them.

Names are for people. The real ID is what's stored and sent to the provider, so aliases, a client's allowed models
and saved model choices keep working, and it's one click away on the Requests page. Apps can't ask for a model by
its custom name, because several can share one: they send the real ID, or an alias.

Each opencode entry is one provider: `GLM-4.7@Synthetic` is only sent to Synthetic, so if it's down, that request
fails. For automatic switching, make an alias with fallbacks. `GLM-4.7` without a provider still works for apps
that send it, on any provider that offers it. Choose your model again in opencode once after updating to 0.51.

### Models in strength order

Turn on **Settings → opencode → Order models by strength** and the opencode model list is numbered: your aliases
first (`00|$0.200|smart → GLM-4.7|Synthetic`), then the strongest models with their score
(`01|58.3|$0.200|GLM-4.7|Synthetic`), then models without a score (`12|$0.200|model|Synthetic`). Scores come from
the [Artificial Analysis](https://artificialanalysis.ai/) index (intelligence, coding or agentic): as published in
OpenRouter's public model list with no setup, or the full index with a free API key in Settings (read once a day), or
from your own score per provider's model on the **Models** page, which wins. The index is searched by the model's
custom name when it has one. Fine-tunes and renamed variants get their base model's score:
`GLM-4.6-Derestricted-v5` ranks as GLM-4.6.

### Prices in model names

opencode names show one price, in USD per million tokens, so models can be compared at a glance. It's each row's input,
cached and output prices weighted by your own token mix of the last 30 days (a typical mix until there's enough usage),
read once a day: the same work priced on every model. For a coding agent most tokens are cached input, so input and
output prices alone would mislead: at 17% input, 83% cached and 0.4% output, a model at $0.60 in, $0.11 cached and $2.20
out shows `$0.203`, while one at $0.30 in, $0.30 cached and $1.20 out shows `$0.304`. The Models page shows your mix.
On Claude the input share counts at 1.25 × the input price, because nearly all of it is written to the prompt cache.

A monthly plan shows `plan`, because a request costs nothing extra, unless its row has a price of its own. A model
without a known price shows `$?`, and a council shows none because one request makes several calls. Very cheap models
can show `$0.000`. Turn it off with **Settings → opencode → Show prices in model names**. opencode's own session cost
still uses the exact rates.

## Use with Claude Code

Claude Code speaks Claude's Messages API, which QueueRouter serves at `/v1/messages`, so it can use any model
QueueRouter has, Claude or not. Anthropic doesn't support Claude Code on other models: it works, but a Claude Code
update can change what it sends.

**In a terminal** (and the VS Code extension, which reads the same file), put this in `~/.claude/settings.json`
(`%USERPROFILE%\.claude\settings.json` on Windows), with no `/v1` after the address:

```json
{
  "env": {
    "ANTHROPIC_BASE_URL": "https://queuerouter.example.com",
    "ANTHROPIC_AUTH_TOKEN": "<client key>",
    "ANTHROPIC_MODEL": "<a model or alias>",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL": "<a cheaper one, for titles and other background work>",
    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1"
  }
}
```

`/status` then shows the address. Requests carry the client key instead of your claude.ai login, so they don't count
against a Claude subscription.

**In the Claude desktop app**: Help → Troubleshooting → Enable Developer Mode, then Developer → Configure Third-Party
Inference. Choose **Gateway**, with your QueueRouter address, a client key, **Bearer**, and the models by their
QueueRouter names (the app's automatic list only shows names that look like Claude's). This switches the whole app,
Chat and Cowork included, to QueueRouter; cloud sessions and Remote Control are off while it's on.

What QueueRouter does for Claude Code:

- Its system messages during a conversation become reminders in the user's turn, where they were (the way Claude Code
  sends them itself where they aren't taken). Many open models' chat templates refuse a system message after the start.
- Its effort (`output_config.effort`) is sent as the model's nearest thinking level, only where QueueRouter knows the
  model's levels.
- It asks for 32,000 tokens of answer (128,000 for a name like Opus's): lowered to what the model can give.
- "Too long" is said the way Claude says it, so Claude Code compacts the conversation and tries again by itself.
- The model's thinking is shown as Claude's thinking, and keep-alives go out while a provider sends nothing. Claude
  Code gives up on a stream that stays silent for about five minutes, which a model thinking over a large review can.

PDFs and Claude Code's web search don't work. For a name it doesn't know, Claude Code assumes a context of 200,000
tokens: on smaller models it compacts once it's told the prompt is too long. Naming an alias like a Claude model
(`claude-opus-5-5`) makes it assume that model's context, 1,000,000 tokens for Opus.

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
once: `untrusted_proxy`. With the gate, the gate is what connects to QueueRouter: a subnet covers it, a single
Traefik address doesn't.

## API

| Path | Notes |
|---|---|
| `POST /v1/chat/completions`, `POST /v1/*` | queued |
| `POST /v1/jobs`, `GET /v1/jobs[/{id}]`, `DELETE /v1/jobs/{id}` | async jobs: `{"request": {...}, "endpoint", "priority", "run_at", "deadline", "idle_only", "callback_url", "callback_secret", "tags"}` → 202; result on `GET` when done |
| `/v1/files`, `/v1/batches` | OpenAI-compatible Batch API (JSONL in, output/error files out, `completion_window` 24h) |
| `POST /v1/messages` | **Anthropic Messages API** (Claude SDKs, Claude Code), translated to chat completions; streaming, tools, images, system messages inside `messages`, structured output; `/v1/messages/count_tokens` estimates |
| `POST /v1/responses` | **OpenAI Responses API** (Agents SDK, newer clients); streaming, function tools, structured output, `previous_response_id` (kept encrypted 7 days; `store: false` keeps nothing) |
| `GET /v1/models` | all upstreams' models + aliases, filtered to the client's allowed models |
| `GET /v1/models/{id}` | one model (aliases and `model@upstream` too); other `/v1/` GETs are refused unless listed in Settings → "GET paths passed to the provider" |
| `GET /v1/queue` | queue depth, your client's queued/active, estimated wait |
| `GET /v1/opencode-config` | generated opencode provider config for your key |
| `GET /opencode/queuerouter.js` | opencode plugin (session/task tags, priorities, model list) |
| `GET /.well-known/opencode` | opencode 1.x login manifest (`/windows/.well-known/opencode` for Windows) |
| `GET /stats` | full stats as JSON (client key required) |
| `GET /metrics` | Prometheus metrics; `Authorization: Bearer <metrics token>` (Settings → Monitoring) |
| `/v1/schedules` | recurring jobs: `POST` `{"name", "cron", "request", ...job fields}`, `GET`, `PATCH {"enabled"}`, `DELETE`, `POST /{id}/run` |
| `GET /health` | liveness |

**Request headers** (optional, never forwarded upstream): `X-QueueRouter-Priority:
low|normal|high` (can only lower your key's priority), `X-Session-Id` (group requests into a
conversation), `X-Run-Id` (one agent task, for run budgets and loop detection),
`X-QueueRouter-Run-Budget-Tokens` (lower your run's token budget), `X-QueueRouter-Tags`
(comma-separated labels), `X-QueueRouter-Effort` (`low` to `max`: sent as the model's nearest thinking level where
QueueRouter knows its levels; `/v1/messages` sets it from `output_config.effort`).

**Response headers**: `X-Request-Id`, `X-QueueRouter-Queue-Depth`, `X-QueueRouter-Position`
(0 = started immediately), `X-QueueRouter-Est-Wait-Ms`, `X-QueueRouter-Slots`; non-streaming
responses also `X-Queue-Wait-Ms`.

QueueRouter only refuses requests for reasons you configured: bad key (401), model not allowed
(403), a client's queue/rate limit in `reject` mode, run budget or loop rejection (429),
draining/maintenance (503), body too large (413), and prompts that clearly exceed the model's
context (400 `context_length_exceeded`). An answer size the model can't give (`max_tokens`) is lowered, not refused.
The API adapters also refuse what they can't translate (400), and those show on the Requests page.

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
