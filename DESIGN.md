# QueueRouter — Design

An OpenAI-compatible LLM gateway that sits in front of ArliAI (and later other providers) and
guarantees no upstream key ever exceeds its concurrency limit. Extra requests are **queued and
delayed, not rejected**. Everything is administered from a **web UI** — no config-file editing.

- Stack: **Python 3.12 + FastAPI + httpx + SQLite**, web UI with **Jinja2 + HTMX** (no JS build step)
- Run: **Docker Compose**, single container behind **Traefik** (Docker labels; Caddy compose provided as an alternative)
- Code: https://github.com/MRACHINI/QueueRouter (public, MIT)
- Image: `ghcr.io/mrachini/queuerouter` (public)
- Exposure: internet-facing behind a TLS reverse proxy
- Clients: chat UIs (SillyTavern), own scripts, coding tools, n8n / automation

---

## 1. Goals and non-goals

**Goals**
- Never exceed a provider's concurrency limit (ArliAI: 2 per account), even when several
  upstream entries share that limit.
- True queuing: callers wait with the connection held open.
- Priority lanes with aging; session-aware, fair ordering within a lane.
- Load balancing across multiple upstream keys/providers, with circuit breakers.
- Per-client policy: priority, rate limits, model allow-list, timeouts, retries.
- Model aliases (`fast`, `smart`, `code` → real models).
- Token usage tracking and optional cost estimates.
- Webhook alerts.
- Async jobs + OpenAI-compatible Batch API, scheduled/deferred and idle-time execution.
- Alias fallback chains, sticky (session-affinity) routing, context-length pre-check.
- Per-client/alias default, forced and clamped parameters.
- Request tags/metadata; opt-in full request/response logging.
- Admin: multiple users with roles, playground, provider presets, maintenance/pause modes.
- Agent-friendly: session-aware scheduling, per-session fair share, run budgets, loop
  detection, queue-depth hints.
- API compatibility: OpenAI Chat/Completions, OpenAI Responses API, Anthropic Messages API.
- Refusal fallback (reroute to another model when one refuses) and structured-output repair.
- All administration in a secure web UI; only bootstrap settings in env.

**Non-goals (for now)**
- Multiple QueueRouter replicas / distributed queue.
- Response caching.
- Persisting queued requests across restarts (clients resend).
- Terminating TLS itself.

**About "never reject"**: QueueRouter never sheds load because an upstream is busy. It only
rejects for policy/safety reasons the admin configured: bad key, model not allowed, a client's
own caps, oversized body. Rate limits default to **delaying** (queue) rather than rejecting.

---

## 2. Architecture

```
 SillyTavern  scripts  coding tools  n8n              Admin (browser)
      └─────────┴──── HTTPS ───┴──────┘                     │ HTTPS
                       ▼                                    ▼
               ┌──────────────────────── Traefik (TLS, no buffering) ───┐
               └───────────────┬────────────────────────────┬───────────┘
                               ▼ /v1/*                       ▼ /admin/*
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │ QueueRouter (1 process, 1 worker)                                           │
 │                                                                             │
 │ Auth ─► Policy ─► Alias ─► Rate limiter ─► Dispatcher ─────────► Upstream A │─► ArliAI key 1 (2 slots)
 │ (key→   (models,   resolve  (RPM/RPD/      (priority lanes,      Upstream B │─► ArliAI key 2 (2 slots)
 │ client)  body,     model    TPM/TPD,       aging, picks an       Upstream C │─► other provider
 │          caps)              delay)         upstream w/ free slot)           │
 │                                              ▲ circuit breaker per upstream │
 │                                                                             │
 │ Usage recorder ─► SQLite (/data)  ◄── Admin web UI (FastAPI + Jinja2 + HTMX)│
 │ Alert engine  ─► webhooks                                                   │
 └─────────────────────────────────────────────────────────────────────────────┘
```

| Module | Responsibility |
|---|---|
| `config.py` | Bootstrap env settings only |
| `db.py` | SQLite (WAL), migrations, repositories |
| `state.py` | In-memory snapshot of config (clients, upstreams, aliases…), reloaded on admin change |
| `auth.py` | Client key lookup (hashed), admin sessions, CSRF |
| `policy.py` | Model allow-list, body size, per-client caps, timeouts |
| `aliases.py` | Alias → (model, allowed upstreams) resolution |
| `ratelimit.py` | Per-client RPM/RPD/TPM/TPD windows |
| `dispatcher.py` | Priority lanes, aging, upstream selection, slot accounting |
| `upstreams.py` | Upstream pool, httpx clients, circuit breakers, health probes |
| `proxy.py` | Route handlers, streaming / non-streaming, retries |
| `usage.py` | Token extraction, usage + cost rows, rollups |
| `alerts.py` | Rule evaluation, webhook delivery (Discord / Slack / ntfy / generic) |
| `params.py` | Default / override / clamp / strip rules |
| `context.py` | Token estimation, context-length pre-check |
| `affinity.py` | Session keys, sticky upstream map |
| `jobs.py` | Async jobs, scheduling, job worker, callbacks |
| `batches.py` | OpenAI Files + Batch API on top of jobs |
| `presets.py` | Provider presets |
| `modes.py` | running / paused / draining / maintenance |
| `adapters/` | `anthropic.py`, `responses.py`: translate to/from chat completions (incl. streaming) |
| `sessions.py` | Session tracking, run budgets, loop detection |
| `refusal.py` | Refusal detection (errors, finish_reason, heuristics, optional judge) |
| `structured.py` | JSON / schema validation and repair |
| `notifications.py` | In-UI notifications (stored + pushed via SSE) |
| `admin/` | Web UI routes, templates, static CSS |
| `main.py` | Wiring, lifespan, `/health` |

---

## 3. Data model (SQLite in `/data/queuerouter.db`)

All admin-editable configuration lives here; edits in the UI apply **immediately** (in-memory
snapshot swapped atomically, no restart).

| Table | Key fields |
|---|---|
| `admin_users` | username, password_hash (argon2id), role (owner/admin/operator/viewer), can_view_content, totp_secret (optional), enabled, created_at |
| `admin_sessions` | id, user, expires_at, csrf_token |
| `limit_pools` | name, plan (e.g. ArliAI Pro), max_concurrency, context_cap, min_spacing_ms (one per provider account) |
| `upstreams` | name, base_url, api_key (encrypted), limit_pool, weight, enabled, paused, models (cached list), circuit settings |
| `clients` | name, key_hash, key_prefix (for display), enabled, priority, expires_at, notes |
| `client_policies` | client, allowed_models (list or `*`), rpm, rpd, tpm, tpd, on_limit (`delay`/`reject`), max_queued, max_active, max_retries, queue_timeout_s, total_timeout_s, nonstream_keepalive, max_body_mb, allow_priority_raise, log_content, params (defaults/overrides/clamps/strip JSON), default_tags |
| `aliases` | alias, chain (ordered list of {model, upstreams?}), fallback_on (unavailable / errors / context / wait / refused), refusal settings, structured-repair settings, params JSON, enabled |
| `models` | upstream, model, display_name, native_context (fetched), context_override (manual), max_output_tokens, slot_cost (default 1), raw upstream metadata JSON, hidden, enabled |
| `jobs` | id, client, batch_id, status, priority, request JSON, run_at, deadline, idle_only, callback_url, attempts, result JSON (encrypted), error, tags, created/started/finished_at |
| `batches` | id, client, input_file_id, output_file_id, error_file_id, status, counts, completion_window, created/finished_at |
| `files` | id, client, purpose, filename, bytes, path (in `/data/files`), created_at, expires_at |
| `request_content` | request_id, request_body, response_body (both encrypted, size-capped), expires_at — **only for clients with logging enabled** |
| `affinity` | (in memory only) session key → upstream, last_seen |
| `agent_sessions` | (in memory, periodically snapshotted) session/run key → client, requests, tokens, started_at, last_seen, recent request hashes |
| `stored_responses` | Responses API `previous_response_id` support: id, client, conversation items (encrypted), expires_at |
| `notifications` | id, ts, severity, title, body, link, read_by (per admin user) |
| `model_prices` | model, input_per_1m, output_per_1m, currency (optional, for cost estimates) |
| `alert_rules` | type, threshold, window, enabled, cooldown_s |
| `webhooks` | name, kind (discord/slack/ntfy/generic), url (encrypted), enabled |
| `settings` | key/value global settings (spacing, aging, retry defaults, keep-alive, limits…) |
| `requests` | id, ts, client, alias, model, upstream, stream, status, outcome, queue_ms, upstream_ms, retries, prompt_tokens, completion_tokens, cost, tags, session_key, job_id, fallback_used — **metadata only, never content** |
| `usage_daily` | date, client, model, upstream, tag, requests, prompt_tokens, completion_tokens, cost (rollup) |
| `audit_log` | ts, admin, action, target, diff |

- Retention: `requests` kept N days (setting, default 30), `usage_daily` forever.
- Daily limits, rollups and charts use a configurable time zone (setting, default UTC).
- Secrets (upstream keys, webhook URLs) encrypted with Fernet; the key is `SECRET_KEY` env or
  auto-generated into `/data/secret.key` on first start. **Back up that key with the data** —
  without it, encrypted secrets in a backup can't be restored.
- Schema migrations: numbered SQL files, applied automatically on startup; DB backed up to
  `/data/backups/` before each migration.
- Client keys are stored only as SHA-256 hashes; shown **once** at creation.

---

## 4. Request lifecycle

```
RECEIVED ─► AUTHED ─► POLICY OK ─► ALIAS RESOLVED ─► RATE OK ─► QUEUED ─► DISPATCHED ─► STREAMING ─► DONE
   │401        │403 model /      │404 unknown       │ delay or     │            │ 429/5xx/conn    │ client left
   │           │413 body /        │model             │ 429 (reject  │ client     ▼                 ▼
   │           │429 client cap    │                  │ mode)        │ left    BACKOFF / try     ABORTED
   ▼           ▼                  ▼                  ▼              ▼         another upstream
 REJECTED   REJECTED          REJECTED           (waits)       CANCELLED
                                                                 │ queue_timeout_s / total_timeout_s
                                                                 ▼
                                                              TIMED_OUT (504)
```

Other early exits not drawn: `503` in draining/maintenance mode, `429 run_budget_exceeded`,
`429 loop_detected` (if set to reject), `400 context_length_exceeded` (no fallback target).
Fallback (unavailable / errors / refused / wait) loops back to ALIAS RESOLVED with the next target,
re-entering the queue at the front of its lane.

Rules:
1. A slot (in the upstream's limit pool) is taken at dispatch and released only at a terminal
   state — for streams, after the last upstream byte.
2. Every terminal path releases exactly once (`try/finally`).
3. Cancelled queued requests never take a slot.
4. Every request writes one `requests` row on completion (async batched writes).

---

## 5. Upstreams, load balancing, circuit breakers

**Upstream** = base URL + API key (+ weight), belonging to a **limit pool**.

**Limit pools** — concurrency is enforced per pool, not per upstream entry, because provider
limits usually apply per *account*, not per key:
- ArliAI preset has a **plan selector** that fills the pool (from arliai.com/pricing, Oct 2026):

  | Plan | Price | Parallel | Context cap |
  |---|---|---|---|
  | Free | $0 | 1 | 12K |
  | Core | $15 | 1 | 32,768 |
  | Plus | $20 | 1 | 131,072 |
  | **Pro (current)** | **$30** | **2** | **262,144** |
  | Max | $160 | 6 | 524,288 |

  All paid plans: models up to 355B, unlimited tokens. Upgrading = change the plan in the UI;
  slots and advertised context update instantly.
- ArliAI account (Pro, 2 parallel) → one pool `arliai-main` with `max_concurrency: 2`. Several keys or
  base URLs on that same account all go in this pool and **share** the 2 slots.
- A second ArliAI **account** → its own pool → 2 more slots.
- A local llama.cpp server → its own pool with whatever it can handle.
- Adding an upstream from a preset auto-creates a pool for it; you pick an existing pool when the
  new key belongs to an account you already added.

**Slot cost per model** — some providers count big models as more than one parallel request.
Each model has `slot_cost` (default 1); a request takes that many slots from the pool and waits
until that many are free. ArliAI's pricing page doesn't mention large models using extra slots,
so the ArliAI preset uses 1 for every model (re-check if 429s appear with 2 large-model requests).

Other OpenAI-compatible providers (OpenRouter, local llama.cpp/vLLM…) are added the same way.

**Model availability**: each upstream's `/v1/models` is fetched on add and refreshed hourly
(and via "Refresh" button). A request can only go to upstreams that serve its model.

**Load-balancing strategy** (global setting):
- `least-busy` (default): upstream with the most free slots, ties broken by weight then round-robin.
- `weighted`: weighted random among upstreams with a free slot.
- `ordered` (failover): always first healthy upstream in priority order; others only when it's
  full or down.

**Circuit breaker** per upstream:
- `closed` → normal.
- Opens after `N` consecutive failures (5xx, connect errors, timeouts; default 5) or
  error rate > X% in last M requests. 429s do **not** open it (that's normal "busy").
- `open` for `cooldown_s` (default 60 s): no traffic routed there.
- `half-open`: one probe request (or a cheap `/v1/models` probe); success → closed, failure → open again.
- State changes emit alerts and show in the UI.

**Health**: background probe of `/v1/models` every 60 s for upstreams that are idle.

**Sticky routing (session affinity)**: when more than one upstream can serve a request, prefer
the upstream that served the same conversation last, so the provider's prompt/KV cache is reused
(faster time-to-first-token on long chats and agent loops).
- Session key: `X-Session-Id` header if sent; otherwise a hash of client + model + system prompt +
  first user message.
- Map session → upstream in memory with TTL (default 30 min).
- If the sticky upstream is busy, wait up to `affinity_wait_s` (default 5 s) for it, then use any
  free upstream (and re-pin). Ignored if the sticky upstream is down or paused.
- No effect with a single upstream.

**Provider presets**: adding an upstream starts from a preset that fills base URL, sensible
`max_concurrency`, models endpoint, extra headers, whether `include_usage` is supported, and
context-length source. Presets: ArliAI, OpenRouter, OpenAI, Groq, Together, DeepInfra, Mistral,
Ollama, LM Studio, vLLM, llama.cpp server, KoboldCpp, TabbyAPI, "Custom OpenAI-compatible".
Presets ship in code (updated with releases); every field stays editable.

---

## 6. Dispatcher (the core queue)

**Lanes**: `high`, `normal`, `low` (FIFO deques). Waiter = future, request id, client, model,
candidate upstreams, enqueued_at.

**On every slot release / new request / circuit change / rate-window tick / mode change**, loop:
1. Order waiters by effective lane (lane bumped one up per `AGING_S`, default 120 s), then by the
   in-lane ordering below.
2. For each waiter in that order, check **eligibility**: mode is running; client below
   `max_active`; client's rate-limit window has room (this is how `on_limit: delay` waits);
   job `run_at` reached and `idle_only` satisfied; global job cap not hit.
3. Find candidate upstreams: serve the model, enabled, not paused, circuit not open, and their
   pool has ≥ `slot_cost` free slots. Pick one by strategy (sticky upstream first, §5).
4. Grant: take the slots from the pool, resolve the future with the chosen upstream.
5. Stop when no pool has free slots.

Because different waiters can target different upstreams (model availability), a waiter that
can't be served doesn't block one behind it that can — but head-of-lane order is kept among
waiters competing for the same upstream.

**Spacing gate** per limit pool: `min_spacing_ms` between dispatches into the same pool.

**Internal requests** (refusal judge, health probes that hit generation endpoints, playground)
go through the same dispatcher — they never bypass the pool limits. The judge runs in the `high`
lane so it doesn't stall the request waiting on it.

**Ordering within a lane** (replaces plain FIFO once sessions are known):
1. **Session-aware ("finish what you started")**: a waiter whose session had a request finish
   within `session_active_window_s` (default 60 s) sorts ahead of waiters from new sessions.
   An agent mid-task keeps moving instead of every agent crawling in parallel; total time to
   *complete* tasks drops.
2. **Fair share**: among equals, round-robin by client, then by session within the client — one
   client's 10-agent swarm alternates with others instead of occupying the front.
3. Then age (FIFO).

Aging (lane bump) still applies, so new sessions and low-priority work can't starve. Requests
without a session key are treated as single-request sessions. Both behaviours toggleable in Settings.

All on one event loop → no thread locks.

---

## 7. Client policy

Configured per client in the UI (with global defaults):

| Policy | Behaviour |
|---|---|
| **Priority** | high / normal / low lane; `X-QueueRouter-Priority` header may **lower** it per request (raise only if `allow_priority_raise`) |
| **Allowed models** | list of models and/or aliases, or `*`. Others → 403. `/v1/models` only lists allowed ones |
| **Rate limits** | RPM, RPD, TPM, TPD (any can be empty). Token limits use actual usage after completion; a request is admitted if the window still has budget |
| **On limit** | `delay` (default): request waits in queue until the window allows; `reject`: 429 with `Retry-After` |
| **max_queued / max_active** | caps on waiting / in-flight requests for this client |
| **queue_timeout_s** | max time waiting in queue → 504 (empty = unlimited) |
| **total_timeout_s** | max total request time including generation → abort + 504 |
| **max_retries** | retry budget (`-1` forever, `0` none); request header may lower it |
| **nonstream_keepalive** | whitespace keep-alive for non-streaming (n8n) |
| **max_body_mb** | request size limit |
| **max_context** | optional lower context cap for this client (advertised in `/v1/models` and enforced by the pre-check) |
| **Expiry** | key stops working after a date |
| **Parameters** | defaults / overrides / clamps / strip (see below) |
| **Content logging** | opt-in full request/response logging (§11e) |
| **Default tags** | tags added to every request from this client |
| **Run budgets** | per session/run: max tokens, max requests, max duration (§11d) |
| **Loop detection** | threshold + action `alert` / `reject` (§11d) |

Rate-limit windows are sliding windows in memory, seeded from the `requests` table on startup
so a restart doesn't reset daily limits.

### Default / forced parameters

Rules can be set on an **alias** and on a **client**; applied to the JSON body before forwarding:

| Rule | Example | Effect |
|---|---|---|
| `defaults` | `temperature: 0.7` | set only if the request didn't |
| `overrides` | `top_p: 0.9` | always set, replacing the client's value |
| `clamps` | `max_tokens: ≤ 4096` | numeric min/max limits |
| `strip` | `logit_bias`, `metadata` | remove params the upstream rejects |

Order: alias defaults → client defaults → alias overrides → client overrides → clamps → strip.
The UI shows a live "effective request" preview for a sample body.

### Context-length pre-check

- Each model has a `context_length` (from the upstream's model list when available, otherwise
  set in the UI).
- Before queuing, estimate prompt tokens (`tiktoken` `o200k_base` as a generic estimator — model
  tokenizers differ — plus a safety margin, default 10%), add
  `max_tokens` (or a default completion reserve).
- Too long →
  - if the alias chain has a target with a larger context: **fall back** to it (§8);
  - else return OpenAI-style `400 context_length_exceeded` immediately — no queue time wasted.
- Per-model toggle; estimate-only so it never blocks a request that's within ~5% of the limit
  (upstream decides those).

---

## 8. Model aliases

- Alias name (e.g. `smart`) → an ordered **chain** of targets, each a model plus optional
  upstream subset. A single-target alias is just a chain of one.
- Clients send `"model": "smart"`; the proxy rewrites the body to the real model before
  forwarding; the response's `model` field is left as upstream returned it.
- Aliases appear in `/v1/models` for clients allowed to use them.
- Change targets in the UI → every client switches instantly.

### Fallback chains

Move to the next target in the chain when (each trigger toggleable per alias):
- **unavailable** — every upstream for the current target is circuit-open, disabled or paused;
- **errors** — retry budget exhausted on 5xx/connect errors;
- **context** — pre-check says the prompt doesn't fit (§7);
- **wait** (optional) — queued longer than `fallback_after_wait_s` while another target has a
  free slot;
- **refused** — the model rejected or refused the request (§8b).

Not triggered by 429 (that just means "wait"). A fallback is recorded on the request row
(`fallback_used`) and in the `X-QueueRouter-Model` response header. Clients must be allowed to use
the alias; fallback targets inherit that permission.

---

## 8a. Model and context discovery for clients

Apps must still see the **right models and the right context size** through the router —
otherwise SillyTavern, coding tools, etc. truncate too early or send prompts that are too long.

**Effective context** for a model on an upstream:
```
effective = min(native_context,          # from upstream model list (max_model_len / context_length)
                pool.context_cap,        # plan cap, e.g. ArliAI Pro 262,144
                context_override)        # optional manual value in the UI
```
Per client, an optional `max_context` policy can lower it further (e.g. limit n8n to 32K).
The Models page shows each value and where it came from (upstream / plan cap / manual / client).

**Effective context for an alias**: the primary target's effective context by default; or
"smallest in chain" (safe if fallbacks may be smaller); or a manual value. The UI warns when a
fallback target has less context than the alias advertises; the pre-check (§7) skips targets the
prompt doesn't fit.

**What `GET /v1/models` returns** (served from the catalog cache — no upstream call, no slot):
- Only models/aliases the calling client is allowed to use, minus hidden ones.
- Upstream's original fields kept, plus normalised fields that common apps read:
  ```json
  {"id": "smart", "object": "model", "owned_by": "queuerouter",
   "context_length": 262144,        // OpenRouter-style (Cline, LibreChat, many tools)
   "max_model_len": 262144,         // vLLM/Aphrodite-style (SillyTavern text-completion)
   "max_context_length": 262144,
   "max_output_tokens": 16384,      // when known
   "queuerouter": {"type": "alias", "targets": ["…"], "upstreams": ["arliai-main"]}}
  ```
- Same model on several upstreams → listed once, with the smallest effective context across them.
- `GET /v1/models/{id}` for a single model.
- If the request carries `anthropic-version`, the list is returned in **Anthropic format**
  (`id`, `display_name`, `type: "model"`, `created_at`) for Anthropic-SDK clients.

**Token counting endpoints** (SillyTavern and some tools use them for exact counts):
- `POST /v1/tokenize`, `/v1/detokenize` (and `/tokenize`, `/detokenize`) → forwarded to an
  upstream serving that model if its preset supports it (short call; whether it uses a slot is a
  per-preset setting), otherwise answered locally with an estimate (`"estimated": true`).
- `POST /v1/messages/count_tokens` (Anthropic) → same mechanism.

**Keeping it fresh**: catalog refreshed hourly and on demand ("Refresh models"); new upstream
models appear automatically (optionally hidden until approved — setting); models that disappear
upstream are marked unavailable, and aliases pointing at them raise a notification.

**Client setup notes** (README): SillyTavern Chat Completion "Custom (OpenAI-compatible)" doesn't
auto-read context — set the context slider to the value shown in the UI (the Models page has a
copy button per model); text-completion (vLLM/Aphrodite type) and OpenRouter-aware tools pick it
up automatically.

---

## 8b. Refusal fallback

Use a strong primary model and let a second model handle what the first one rejects or declines.
Configured on an alias: `smart` → [strong model] → on refusal → [alternate model].

**Detection layers** (each toggleable per alias):
1. **Hard rejections** — upstream 400/403/422 with content-policy / moderation error codes or
   messages (patterns per provider preset).
2. **finish_reason** — `content_filter` (OpenAI-style) or `refusal` field / stop reasons that
   indicate a refusal (Anthropic-style `refusal`).
3. **Soft-refusal heuristics** — the start of the reply matches refusal patterns ("I can't help
   with", "I'm unable to", "As an AI…"); pattern list editable in the UI, with per-pattern hit
   counters so false positives are visible.
4. **Judge (optional)** — a small/cheap model classifies the first ~300 characters as
   refusal / not refusal. Costs an extra (short) request and a slot, so it's off by default and
   only runs when heuristics are inconclusive.

**Streaming**: refusals show up at the start of a reply, so the proxy buffers a **sniff
window** (default first 200 characters or 2 s, whichever first) before forwarding anything.
Not a refusal → flush buffer and stream normally. Refusal → abort that upstream call, release
its slot, and re-dispatch to the next chain target (front of lane). Keep-alive comments keep the
client connection alive meanwhile. Adds a small time-to-first-token delay, so it's per alias.

**Non-streaming**: check the full response, then fall back.

**Rules**
- The request is resent **unchanged** to the next model (after that target's param rules).
- Max one refusal-fallback hop per target; the last target's answer is returned as-is.
- Recorded as `outcome=fallback_refused` with the original model; refusal rate per model shown
  in Usage, and an alert rule for spikes.
- `X-QueueRouter-Model` tells the client which model actually answered.

**Not included — automatic prompt rewriting to get past refusals.** Rephrasing a declined
request so a model accepts it is an automated jailbreak: it defeats the provider's safety
behaviour, usually breaks the provider's terms of service (risking the ArliAI account), and makes
answers unpredictable. Rerouting the unchanged request to a model whose policy fits the use case
achieves the legitimate goal. (Legitimate request shaping — forced params, system prompts set by
the admin per alias — is covered by §7.)

---

## 8c. Structured-output repair

When the request asks for JSON (`response_format` `json_object` / `json_schema`) or contains tool
calls:
1. Validate the result (JSON parse; JSON Schema if provided; tool-call `arguments` parse).
2. Invalid → **local repair**: strip code fences/prose around JSON, fix trailing commas, close
   truncated brackets when `finish_reason=length` is not set; re-validate.
3. Still invalid → **retry once** (same request; optionally next chain target), counted against
   the retry budget.
4. Still invalid → return the original response unchanged plus header
   `X-QueueRouter-Structured: invalid`.

Non-streaming: always possible. Streaming: only when the alias enables buffering for structured
requests (the whole reply is buffered, then sent as a stream). Per-alias toggle; repairs logged
as metadata.

---

## 9. Streaming, keep-alive, retries, disconnects

**Streaming**: `200 text/event-stream` immediately; SSE comments every `KEEPALIVE_INTERVAL_S`
while queued/backing off (`: queued position=3 waited=42s`); then upstream bytes piped
unchanged; final failure → `data: {"error":…}` + `data: [DONE]`.

**Non-streaming**: wait silently and return upstream status/body; or, per client,
`nonstream_keepalive` (200 + whitespace every N s; errors arrive as 200 + error body).

**Retries** (before the first byte is forwarded only):
- Triggers: 429, 500/502/503/504, connect errors, timeouts.
- On **429**: keep the slot, back off (`Retry-After` or exponential + jitter), retry same upstream.
- On **5xx / connect error**: release the slot, count toward that upstream's circuit breaker,
  re-queue at the **front** of the lane, preferring a different upstream if one serves the model.
- Budget: request header `X-QueueRouter-Max-Retries` (can only lower) → client policy → global default (8).
- Other 4xx → returned immediately.

**Disconnects**: queued → removed; streaming → generator cancelled, upstream closed, slot freed;
non-stream in flight → upstream call cancelled.

---

## 10. Usage and cost tracking

- Tokens from `usage` in responses; for streams from the final chunk. The proxy sets
  `stream_options.include_usage=true` on streamed chat/completions requests if the client didn't (since v0.44.0) and
  leaves that usage-only chunk out of the client's stream; a client that set `include_usage` itself (true or false)
  gets what it asked for.
- Each request row stores tokens + optional cost from `model_prices`.
- ArliAI is flat-rate, so cost is optional — useful for pay-per-token upstreams added later.
- Nightly rollup into `usage_daily`; UI charts per client / model / upstream / day.
- Rate limits (TPM/TPD) and quotas read the same counters.

---

## 11. Alerts and webhooks

**Rule types** (each with threshold, window, cooldown, enabled):
- Upstream circuit opened / closed
- Upstream health probe failing
- Queue length > N for > T seconds
- Oldest wait > T seconds
- Error rate > X% over window
- Client hit rate limit / quota (RPD/TPD)
- Client key expiring within N days
- Daily tokens (or cost) > threshold — per client or global
- QueueRouter started / stopped (restart detection)

**Delivery**: Discord, Slack, ntfy, generic JSON POST (optional HMAC signature header).
Retries with backoff; failures visible in the UI; "Send test" button per webhook.
Cooldowns prevent alert storms.

---

## 11b. Async jobs, Batch API, scheduling

For callers that shouldn't hold a connection open (n8n, scripts, agents with short timeouts).
Jobs are **persisted in SQLite**, so unlike live requests they survive restarts.

**Native jobs API**
- `POST /v1/jobs` — body: `{"request": {...normal chat/completions body...}, "endpoint":
  "/v1/chat/completions", "priority": "low", "run_at": "2026-10-20T02:00:00Z",
  "deadline": "...", "idle_only": false, "callback_url": "https://…", "tags": ["nightly"]}`
  → `202 {"id": "job_…", "status": "queued"}`.
- `GET /v1/jobs/{id}` — status (`scheduled | queued | running | succeeded | failed | cancelled |
  expired`), timings, tokens, result when done.
- `GET /v1/jobs?status=&tag=` — list own jobs. `DELETE /v1/jobs/{id}` — cancel.
- Also accepted as a header on a normal request: `X-QueueRouter-Async: true` → returns 202 + job
  id instead of waiting (no client code changes beyond one header).

**OpenAI-compatible Batch API** (so OpenAI SDK batch code works unchanged)
- `POST /v1/files` (JSONL, `purpose=batch`), `GET /v1/files/{id}/content`
- `POST /v1/batches`, `GET /v1/batches/{id}`, `GET /v1/batches`, `POST /v1/batches/{id}/cancel`
- Each JSONL line becomes a job sharing the `batch_id`; output/error JSONL files assembled when done.
- `completion_window` honoured as the deadline.

**Execution**
- A job worker feeds due jobs into the same dispatcher as live requests, at the job's priority
  (default `low`), always non-streaming internally. Same policies, aliases, fallback, rate limits.
- `run_at` → **scheduled/deferred**: not eligible before that time.
- `idle_only` → only dispatched when no live requests are waiting (uses spare capacity, never
  delays interactive users).
- `deadline` → if not started by then: `expired`.
- Upstream failures follow the retry budget; then `failed` with the error stored.
- Max concurrent jobs in flight (global setting) so jobs never take every slot unless idle.

**Results and callbacks**
- Results stored encrypted, retention setting (default 7 days), size cap.
- `callback_url`: POST result (or a "done" notice + fetch URL, setting) with an HMAC signature
  header; retried with backoff; delivery status visible on the job.
- Callback URLs must be HTTPS and can be restricted to an allow-list (prevents using the proxy to
  hit internal addresses).

**Admin**: Jobs page — filter, inspect (metadata; body only with content permission), cancel,
retry, bulk-cancel by batch/tag. Phase 3: recurring schedules (cron).

---

## 11c. Request tags and metadata

- Sources (merged): client `default_tags`; header `X-QueueRouter-Tags: project-x,agent-planner`;
  body `metadata` object (OpenAI field) — **stripped before forwarding** unless the upstream
  supports it.
- Also recorded: `X-Session-Id`, `user` body field.
- Tags stored on request/job rows; Usage page groups and filters by tag; rate limits are still
  per client.
- Limits: max 10 tags, 64 chars each, `[a-z0-9._:-]`.

---

## 11d. Agent support

Agents fire bursts of parallel calls and long chains of dependent calls. QueueRouter can't add
capacity (only more upstreams can), but it can make those calls finish sooner and safer.

**Session / run identity**
- `X-Session-Id` (conversation) and `X-Run-Id` (one agent task, may span sessions/subagents).
  Without headers: session derived from a prompt-prefix hash; run = session.
- Agents that can't set headers can embed them in the key name: one client key per agent.

**Session-aware scheduling + fair share** — see §6.

**Run budgets** (per client policy, overridable per request downward via
`X-QueueRouter-Run-Budget-Tokens`):
- max tokens per run, max requests per run, max run duration.
- Exceeded → `429` with error code `run_budget_exceeded` (a deliberate safety stop, not load
  shedding) + notification/webhook. Usage of a run visible in the UI.

**Loop detection**
- Each request gets a normalised hash (messages + tools + key params, ignoring ids/timestamps).
- Same hash ≥ N times (default 4) within a window (default 5 min) in one session/run → action per
  client: `alert` (default) or `reject` (`429 loop_detected`).
- Also flags "near loops": the same last assistant message repeated N times.

**Queue-depth hints** (response headers on every queued request, plus first SSE comment):
| Header | Meaning |
|---|---|
| `X-QueueRouter-Queue-Depth` | requests waiting (all lanes) when this one was admitted |
| `X-QueueRouter-Position` | its position at admission |
| `X-QueueRouter-Est-Wait-Ms` | estimate from recent upstream durations and free slots |
| `X-QueueRouter-Slots` | `active/total` |

Plus `GET /v1/queue` (client key): current depth, your client's queued/active, estimated wait —
lets an orchestrator decide how many subagents to spawn.

---

## 11e. Full request/response logging (opt-in)

- **Off by default.** Enabled per client by an admin (and optionally only for N hours — auto-off).
- Stores request body and final response (streams reassembled) in `request_content`,
  **encrypted at rest**, size-capped (default 256 KB each), retention default 7 days.
- Viewing requires an admin with `can_view_content`; every view is written to the audit log.
- Clients see an `X-QueueRouter-Logging: on` response header so logging is never secret.
- Purge button per client / all.

---

## 11f. Maintenance and pause modes

Global mode (Dashboard toggle; pause also per upstream and per limit pool):

| Mode | New requests | Queued | In flight | Jobs |
|---|---|---|---|---|
| **running** | accepted | dispatched | continue | run |
| **paused** | accepted & queued | held (keep-alives continue) | continue | held |
| **draining** | 503 + message | dispatched until empty | continue | held |
| **maintenance** | 503 + `Retry-After` + message | cancelled with 503 | aborted after grace period | held (resume later) |

- Per-upstream pause: no new dispatches to it; others keep serving.
- Optional scheduled maintenance window (start/end) and custom message.
- Mode changes are audited and can trigger webhooks.

---

## 12. Admin web UI

**Tech**: server-rendered Jinja2 templates + HTMX for partial updates + SSE for live views;
Pico.css (or similar classless CSS); no Node build. Served at `/admin`.

**Look and feel**
- **Dark mode**: follows the OS by default, toggle in the header (remembered per user).
- **Mobile-friendly**: responsive layout, tables collapse to cards, dashboard usable on a phone
  (queue, slots, mode toggle, cancel).

**Notifications in the UI**
- Bell icon with unread count, pushed live over SSE; dropdown list + full page.
- Same events as alert rules (§11) — each rule can target UI, webhooks, or both.
- Severity (info/warning/critical), link to the related page, mark read per user.
- Optional browser notifications (Web Notifications API) when the tab is open.

**Pages**
| Page | Contents |
|---|---|
| Dashboard | live slots per upstream, queue by lane with waiting requests, recent requests, error rate, tokens today, **mode toggle (running/paused/draining/maintenance)**, cancel a queued request |
| Clients | list, create (copy button; Show displays a key again, audited), rename (history moves with it), edit policy incl. params/tags/logging, enable/disable, regenerate key, expiry |
| Upstreams | add from **preset** or custom, edit (URL, key, concurrency, weight), pause, circuit state, "Test" and "Refresh models" |
| Models & Aliases | models per upstream with context length, aliases + fallback chains, param rules, effective-request preview, prices |
| Playground | chat with any model/alias **as a chosen client** (that client's policy applies), streaming, shows queue wait, upstream, fallback, tokens; raw request/response view |
| Jobs & Batches | list/filter, inspect, cancel, retry, bulk actions |
| Usage | charts and tables by client / model / upstream / tag / day; CSV export |
| Requests | searchable request log (metadata), filters by client/tag/status/outcome; content view if logged and permitted |
| Alerts | rules and webhooks CRUD, test send, delivery history |
| Settings | global defaults (spacing, aging, keep-alive, retries, LB strategy, affinity, retention, job limits), backup/restore |
| Users | admin users and roles (owner only) |
| Account | change password, enable TOTP 2FA, sessions list |
| Audit log | who changed what, when |

**Roles**
| Role | Can |
|---|---|
| **owner** | everything, incl. users and backup/restore |
| **admin** | all configuration (clients, upstreams, aliases, alerts, settings) |
| **operator** | dashboard, mode toggle, pause upstreams, cancel requests/jobs, playground, view usage/logs |
| **viewer** | read-only dashboard, usage, request metadata |

`can_view_content` is a separate flag (any role) for reading logged prompts/responses.
There is always at least one owner; the last owner can't be demoted or deleted.

**Security of the UI**
- First start: no admin exists → a one-time **setup token** is printed in the container log;
  `/admin/setup` requires it to create the first admin. (Or `ADMIN_USERNAME` + `ADMIN_PASSWORD`
  env for unattended setup.)
- argon2id password hashing; optional TOTP 2FA.
- Session cookie: `HttpOnly`, `Secure`, `SameSite=Strict`, idle timeout; CSRF token on every form.
- Login throttling per IP and per username.
- Optional `ADMIN_ALLOWED_IPS` (CIDR list) for extra lockdown.
- Every change written to `audit_log`.

**Backup/restore**: export all config (clients' key hashes, upstreams with secrets encrypted,
aliases, policies, rules) as a JSON file from Settings; import to restore. Plus the plain
`/data` volume can be backed up.

---

## 13. Endpoints

| Method | Path | Auth | Queued | Notes |
|---|---|---|---|---|
| POST | `/v1/chat/completions` | client key | yes | |
| POST | `/v1/completions` | client key | yes | |
| POST | `/v1/{path}` | client key | yes | catch-all (embeddings…) |
| GET | `/v1/models`, `/v1/models/{id}` | client key | no | catalog with effective context, filtered by allow-list (§8a) |
| POST | `/v1/tokenize`, `/v1/detokenize` (+ unprefixed) | client key | per preset | forwarded or estimated (§8a) |
| POST/GET/DELETE | `/v1/jobs…` | client key | via jobs | async jobs (§11b) |
| * | `/v1/files…`, `/v1/batches…` | client key | via jobs | OpenAI Batch API |
| POST | `/v1/messages` | client key | yes | Anthropic Messages API (§13b) |
| POST | `/v1/messages/count_tokens` | client key | no | estimate |
| POST | `/v1/responses` | client key | yes | OpenAI Responses API (§13b) |
| GET/DELETE | `/v1/responses/{id}` | client key | no | stored responses |
| GET | `/v1/queue` | client key | no | queue-depth hints (§11d) |
| GET | `/health` | none | no | `{"status":"ok"}` only |
| * | `/admin/*` | admin session | no | web UI |
| GET | `/admin/api/stats` | admin session | no | JSON stats (for UI / scripts) |

- FastAPI `/docs`, `/redoc`, `/openapi.json` disabled.
- Client auth: `Authorization: Bearer` or `x-api-key`.

**Request headers (all optional, all stripped before forwarding)**
| Header | Effect |
|---|---|
| `X-QueueRouter-Priority` | `high/normal/low`; can only lower unless allowed |
| `X-QueueRouter-Max-Retries` | lower the retry budget |
| `X-QueueRouter-Tags` | comma-separated tags |
| `X-Session-Id` | session key for sticky routing, scheduling, grouping |
| `X-Run-Id` | agent run key for budgets and loop detection |
| `X-QueueRouter-Run-Budget-Tokens` | lower the run token budget |
| `X-QueueRouter-Async` | `true` → returns 202 + job id |

**Response headers**
| Header | Meaning |
|---|---|
| `X-Request-Id` | request id |
| `X-Queue-Wait-Ms` | time spent queued (non-stream) |
| `X-QueueRouter-Model` | real model used (shows fallback) |
| `X-QueueRouter-Upstream` | upstream name (optional setting) |
| `X-QueueRouter-Logging` | `on` when content logging applies |
| `X-QueueRouter-Queue-Depth`, `-Position`, `-Est-Wait-Ms`, `-Slots` | queue hints (§11d) |
| `X-QueueRouter-Structured` | `repaired` / `invalid` when structured-output repair ran |

---

## 13b. API compatibility adapters

Internally everything is an OpenAI **chat completion**. Adapters translate other API shapes in
and out, so the queue, policies, aliases, fallback, usage and logging work identically.

**Anthropic Messages (`/v1/messages`)** — for coding agents/tools that speak Anthropic format.
- Request: `system` → system message; content blocks (text, image) → OpenAI content parts;
  `tools` → functions; `tool_use` / `tool_result` blocks ↔ assistant `tool_calls` / `tool`
  messages; `max_tokens`, `stop_sequences`, `temperature`, `top_p`, `tool_choice` mapped.
- Response: choices → content blocks; `finish_reason` → `stop_reason` (`end_turn`, `max_tokens`,
  `tool_use`, `stop_sequence`); usage → `input_tokens` / `output_tokens`.
- Streaming: OpenAI chunks → `message_start`, `content_block_start/delta/stop`
  (`text_delta`, `input_json_delta`), `message_delta`, `message_stop`; keep-alives as `ping` events.
- Auth via `x-api-key` (already accepted); `anthropic-version` header accepted and ignored.
- Unsupported features (thinking blocks, prompt-cache controls, server tools) → ignored or a
  clear 400, documented.

**OpenAI Responses (`/v1/responses`)** — for newer OpenAI agent SDKs.
- `input` (string or items), `instructions`, function `tools`, `tool_choice`,
  `max_output_tokens`, `text.format` (JSON schema) → chat completions.
- Output items (`message`, `function_call`) and streaming events (`response.created`,
  `response.output_text.delta`, `response.function_call_arguments.delta`, `response.completed`…).
- `previous_response_id` / `store: true` supported via `stored_responses` (encrypted, retention
  default 7 days); conversation rebuilt and resent each turn (upstreams are stateless).
- Built-in hosted tools (web search, file search, code interpreter, computer use) → 400 "not
  supported".

Both adapters: covered by golden-file tests with real SDKs (Anthropic SDK, OpenAI SDK, OpenAI
Agents SDK) against the mock upstream.

---

## 14. Security

- TLS via Traefik (or Caddy); QueueRouter port not published to the host.
- Hashed client keys, constant-time compare; encrypted upstream secrets.
- Auth-failure throttling per IP (real IP via `X-Forwarded-For` from `TRUSTED_PROXIES` only).
- Never log prompts, completions or keys to stdout; content is stored only for clients with
  opt-in logging (§11e), encrypted.
- Container: non-root, read-only root FS (writable `/data` volume only), `no-new-privileges`,
  `cap_drop: ALL`.
- Pinned deps, Dependabot, Trivy image scan in CI.

---

## 15. Bootstrap configuration (env — the only non-UI settings)

| Env var | Default | Meaning |
|---|---|---|
| `DATA_DIR` | `/data` | SQLite + secret key |
| `SECRET_KEY` | auto-generated in `/data/secret.key` | encrypts secrets at rest |
| `ADMIN_USERNAME` / `ADMIN_PASSWORD` | unset | optional unattended first admin |
| `ADMIN_ALLOWED_IPS` | unset (any) | CIDR allow-list for `/admin` |
| `TRUSTED_PROXIES` | `127.0.0.1/32,::1/128` | trust `X-Forwarded-For` / `X-Forwarded-Proto` from these (set to the reverse proxy's network) |
| `PUBLIC_URL` | unset | used in UI and alert links |
| `LOG_LEVEL` | `INFO` | |
| `DOMAIN` | — | Traefik router rule / Caddy TLS |

Everything else (upstreams and their keys, clients, limits, retries, spacing, aging, keep-alive,
LB strategy, alerts) is set in the web UI.

---

## 16. Observability

- JSON log line per request to stdout (same metadata as the `requests` table).
- Live stats in the dashboard; `/admin/api/stats` for scripts.
- Phase 3: Prometheus `/metrics`.

---

## 17. Deployment

```
QueueRouter/
  app/  main.py config.py db.py state.py auth.py modes.py
        policy.py params.py context.py aliases.py ratelimit.py sessions.py affinity.py
        dispatcher.py upstreams.py presets.py proxy.py refusal.py structured.py
        usage.py alerts.py notifications.py jobs.py batches.py
        adapters/ anthropic.py responses.py
        admin/ routes/ templates/ static/
        migrations/
  tests/
  deploy/Caddyfile
  Dockerfile  docker-compose.yml  docker-compose.dev.yml  requirements.txt
  .env.example  .gitignore  .dockerignore  LICENSE  README.md
  .github/workflows/ci.yml  .github/workflows/docker.yml  .github/dependabot.yml
```

**docker-compose.yml** (outline):
```yaml
services:
  queuerouter:
    image: ghcr.io/mrachini/queuerouter:latest
    env_file: .env
    volumes: ["qr_data:/data"]
    expose: ["8000"]
    restart: unless-stopped
    read_only: true
    tmpfs: ["/tmp"]
    security_opt: ["no-new-privileges:true"]
    cap_drop: [ALL]
    stop_grace_period: 60s
    healthcheck: { test: ["CMD", "python", "-c", "…/health"], interval: 30s }
    deploy: { resources: { limits: { cpus: "0.5", memory: 384M } } }

  caddy:
    image: caddy:2
    ports: ["80:80", "443:443"]
    environment: [DOMAIN=${DOMAIN}]
    volumes: ["./deploy/Caddyfile:/etc/caddy/Caddyfile:ro", "caddy_data:/data"]
    restart: unless-stopped

volumes: { qr_data: {}, caddy_data: {} }
```

**Caddyfile**:
```
{$DOMAIN} {
    reverse_proxy queuerouter:8000 {
        flush_interval -1
        transport http { read_timeout 1h }
    }
}
```

- Exactly one QueueRouter replica, uvicorn `--workers 1` (queue and slots are in memory).
- Existing reverse proxy users drop the `caddy` service; README lists required settings.
- Update: `docker compose pull && docker compose up -d` (DB migrations run on startup).

---

## 18. Repository, CI and image publishing

- Repo public, MIT, default branch `main`; image public on GHCR.
- **ci.yml**: Python 3.12, `ruff`, `pytest` on every push/PR.
- **docker.yml** (after tests): buildx, GHCR login via `GITHUB_TOKEN`, `linux/amd64` +
  `linux/arm64`, GHA cache, OCI labels, Trivy scan.
  - push to `main` → `main`, `sha-<short>`
  - tag `v1.2.3` → `1.2.3`, `1.2`, `1`, `latest`
  - PR → build only
- Dependabot for pip, Docker base image, Actions. Releases with auto-generated notes.

---

## 19. Testing plan

- **Mock upstreams** (2+ instances) enforcing their own concurrency (429 beyond), configurable
  streaming delays, injectable 429/5xx/disconnects/slow responses.
- Unit: lanes, aging, `max_active`, upstream selection per strategy, circuit breaker state
  machine, rate-limit windows (delay vs reject), alias resolution, allow-lists, retry precedence,
  usage extraction, cost calc, alert rule evaluation/cooldown, key hashing, encryption.
- Integration: 50 mixed requests across 4 clients and 2 upstreams → no upstream ever sees more
  than its limit; all succeed; priorities respected; failover when one upstream dies; circuit
  recovers.
- Admin UI: setup token flow, login throttling, CSRF rejection, session expiry, config changes
  take effect without restart, audit entries written.
- Security: no content in logs/DB unless logging enabled for that client, `/docs` disabled,
  admin IP allow-list, role permissions enforced on every admin route, callback URL restrictions.
- Jobs: persistence across restart, run_at / idle_only / deadline, Batch API round-trip with the
  OpenAI SDK, callback signatures.
- Routing: fallback triggers, sticky routing hit/miss, param rule order, context pre-check.
- **Tool-call passthrough**: `tools`, `tool_choice`, parallel tool calls, streamed
  `tool_calls` deltas, `response_format` / JSON schema, `logprobs`, `seed`, images in content —
  byte-identical through the proxy (golden files), and correct through both adapters.
- Agents: session-aware ordering, per-session fair share, run budgets, loop detection,
  queue-hint accuracy; a simulated 10-agent swarm vs. a chat client (chat latency stays low).
- Refusal fallback: hard error, `content_filter`, heuristic hit inside the sniff window
  (stream aborted and rerouted cleanly), no false trigger on normal replies.
- Structured output: repair cases, retry once, invalid passthrough header.

---

## 20. Roadmap

### Phase 0 — MVP: opencode + ArliAI without hitting the limit

**Why opencode needs it**: opencode streams, fires a title-generation request (`small_model`)
alongside the main one, runs subagents and retries — easily 3+ simultaneous requests against
ArliAI Pro's 2.

**In scope (smallest thing that solves that)**
| # | Item | Notes |
|---|---|---|
| 1 | `POST /v1/chat/completions` (+ catch-all `POST /v1/*`) through the queue | opencode uses `@ai-sdk/openai-compatible` → chat completions |
| 2 | FIFO `SlotQueue`, `MAX_CONCURRENCY=2`, slot released after last streamed byte | core of §6 without lanes/pools |
| 3 | Streaming passthrough + SSE keep-alive comments while queued | headers sent at once → opencode's `headerTimeout` never fires |
| 4 | Non-streaming path | |
| 5 | 429/5xx retry: keep slot, honour `Retry-After`, exponential backoff, `MAX_RETRIES` | |
| 6 | Client disconnect handling (queued + mid-stream) | opencode aborts on Esc / new prompt — slot must free immediately |
| 7 | `GET /v1/models` passthrough, cached 5 min, no slot | |
| 8 | One local access key (`ACCESS_KEY` env) | ArliAI key never leaves the server |
| 9 | Config via `.env` | **temporary** — replaced by the web UI in Phase 1a, which imports these values on first start |
| 10 | `/health`, minimal `/stats` (active, queued, waits), JSON request logs | |
| 11 | Dockerfile + compose with Traefik labels (default) or Caddy (alternative), no host port | runs on a separate server |
| 12 | Tests: SlotQueue unit tests + mock upstream (enforces 2, returns 429 beyond) | proves it never exceeds 2, streams, frees slots on disconnect |
| 13 | GitHub repo + CI + GHCR image publish | §18 |
| 14 | README with the opencode config below | |

**Out of scope for MVP**: everything else (UI, DB, priorities, multi-upstream, aliases,
adapters, jobs…). The code is structured so these slot in without rewrites: `SlotQueue` grows
lanes and pools, env settings move to the DB.

**Build order**
1. Repo skeleton, `config.py`, `SlotQueue` + unit tests.
2. Mock upstream + proxy streaming/non-streaming + integration tests.
3. Retries, disconnects, keep-alive, `/v1/models`, auth, `/health`, `/stats`, logs.
4. Dockerfile, compose, README.
5. CI + GHCR workflows, push to GitHub, tag `v0.1.0`.
6. Smoke test against real ArliAI with opencode (verify `/v1/models` fields, slot behaviour).

**Done when**: opencode runs a session with subagents + title generation through QueueRouter,
zero 429s reach opencode, `/stats` never shows more than 2 active, cancelling in opencode frees
the slot.

**opencode config** (`opencode.json`):
```json
{
  "$schema": "https://opencode.ai/config.json",
  "provider": {
    "queuerouter": {
      "npm": "@ai-sdk/openai-compatible",
      "name": "ArliAI via QueueRouter",
      "options": {
        "baseURL": "http://localhost:8000/v1",
        "apiKey": "{env:QUEUEROUTER_KEY}",
        "timeout": false
      },
      "models": {
        "<arliai-model-id>": {
          "name": "<display name>",
          "limit": { "context": 262144, "output": 16384 }
        }
      }
    }
  },
  "model": "queuerouter/<arliai-model-id>",
  "small_model": "queuerouter/<arliai-model-id>"
}
```
- `timeout: false` — queued requests may wait longer than opencode's 300 s default.
- `limit.context` — min(model's native context, Pro cap 262,144); opencode doesn't read it from
  `/v1/models`, so it's set here.
- `small_model` — point it at a smaller/faster ArliAI model if available; it still goes through
  the queue.

**Status**: shipped as v0.1.0 and verified with opencode + ArliAI Pro (no 429s, queue absorbed
bursts of side requests).

### v0.2 — Scheduling for agents (pulled forward from 1a/1b)

Motivated by v0.1 logs: opencode's side requests (titles, subagents) queued behind each other.
Still env-configured, no UI/DB.
- Named access keys with priority: `ACCESS_KEYS=name:key:priority`.
- Priority lanes + aging (§6), `X-QueueRouter-Priority` (lower only).
- Session-aware ordering ("finish what you started") with derived session keys (§11d).
- Fair share between clients (round-robin per client; per-session fair share comes later).
- Queue hints: response headers + `GET /v1/queue` (§11d); per-client and per-lane `/stats`.

### Phase 1a — Gateway core (usable end-to-end)
Dispatcher (lanes, aging, priority header), multi-upstream load balancing, circuit breakers,
retries, streaming keep-alive, client keys + policies (allow-list, rate limits, timeouts, caps),
aliases, usage tracking, tags, SQLite, maintenance/pause modes, Docker + Traefik labels, CI + GHCR.
Admin UI: setup/login, users + roles, Clients, Upstreams (with presets), Aliases, Settings,
basic Dashboard.

Tool-call passthrough tests from day one. Dark mode + mobile layout from the first UI page.

**Status: shipped as v0.3.0** (slices: store → admin auth/UI → clients → upstreams/pools/
failover/circuits → policies/modes → aliases → usage). Differences from the plan above:
- Request **tags** moved to Phase 1b (with sessions/run ids, where they're most useful).
- Usage pages and the request log (planned for Phase 2) shipped now; charts are still Phase 2.
- Aliases are single-target; **fallback chains** remain Phase 1b.
- Retries on 5xx/connection errors **fail over** to another upstream (re-queued with the same
  place in line); 429 still keeps the slot and backs off on the same upstream.
- While every upstream for a request is down, the request **waits in the queue** (circuit
  breakers + health probes bring upstreams back) instead of erroring, unless a queue/total
  timeout applies.
- Maintenance mode lets in-flight requests finish rather than aborting them after a grace period.
- Client keys use SHA-256 lookup (not constant-time compare over all keys): the hash lookup
  doesn't leak key prefixes and scales.

### Phase 1b — Smarter routing + agents
Fallback chains (incl. refusal fallback), context-length pre-check, default/forced parameters,
sticky routing, session-aware scheduling + fair share, run budgets, loop detection, queue-depth
hints, Playground.

**Status: shipped as v0.5.0** (session-aware scheduling, fair share and queue hints already came
in v0.2). Also delivered on the way: opencode auto-configuration (v0.3.4) and cost at official
prices with plan savings (v0.4.0). Notes vs. the plan:
- Refusal detection uses errors, `finish_reason` and opening-phrase heuristics; the optional
  "judge" model was not built (heuristics proved sufficient so far).
- Fallback targets are plain model names on any upstream (the primary may be pinned).
- Context pre-check uses a character-based estimate with a 15% margin, so it only refuses prompts
  that clearly can't fit (no tokenizer download needed).
- Request tags moved here from Phase 1a.

### Phase 2 — Compatibility, jobs, insight and alerts
Anthropic Messages + OpenAI Responses adapters, structured-output repair, async jobs + Batch API
+ scheduled/idle execution, live dashboard (SSE), Usage charts + CSV, Requests log, opt-in
content logging, alerts + webhooks + UI notifications, cost estimates, TOTP 2FA, audit log,
backup/restore.

**Progress:** opencode plugin (session/task headers, low-priority titles, model list without
login) shipped as v0.6.0; alerts, webhooks (Discord, Slack, ntfy, signed JSON) and in-app
notifications as v0.7.0; dashboard live chart and Usage charts as v0.8.0 (polling every 2 s was kept
instead of SSE: same freshness, simpler behind proxies).
Two-factor sign-in (TOTP + recovery codes) and an NVIDIA provider preset as v0.9.0. Backup/restore (download,
daily automatic copies, staged restore applied on a self-restart) as v0.10.0. Anthropic Messages and
OpenAI Responses adapters as v0.11.0 (tested with the official Python SDKs). Async jobs, the
`X-QueueRouter-Async` header and the OpenAI Batch API as v0.12.0 (callbacks: HTTPS to public hosts only,
HMAC-signed when the job gives a `callback_secret`). Opt-in content logging and
structured-output repair as v0.13.0 — **Phase 2 complete**. Notes: repair is local only (no automatic
re-ask) and for non-streaming replies; JSON Schema is not validated beyond parsing.

### Phase 3 — Extras
Prometheus metrics, monthly token quotas, recurring (cron) jobs, OpenTelemetry.

**Status: shipped as v0.14.0.** Notes: metrics and the OTLP exporter are written by hand (no
client/SDK dependencies); quotas also cover requests and cost, not only tokens; cron is UTC.

v0.15.0 (from real use with Google Gemini): provider 429 messages surfaced; daily-quota 429s fail fast /
fall back; per-upstream parameter rules and retry limit; Gemini preset; unknown-model 404 with suggestions;
pricing table limited to offered and recently used models.
v0.16.0: live dashboard over SSE (Now panel, 1 s chart), live Usage page, time to first byte per request
(Requests page, `queuerouter_time_to_first_byte_seconds`).
v0.17.0: fixes from the security review of v0.6-v0.14 — client data deleted/renamed with the client;
request-size limits while reading (admin forms, /v1/files); callbacks pinned to the checked address,
port 443, no error details; tracing headers cleared when the collector URL changes; webhook tests
show only the status; restore zips size-limited; callback URLs encrypted.
v0.18.0: the opencode plugin keeps the model list current while opencode runs (ETag on
/v1/opencode-config; reload via opencode's instance dispose only when no session is busy).
v0.19.0: the plugin also loads in opencode's newer plugin system (desktop app): models via the catalog,
live refresh with catalog.reload(); opencode's own session headers group subagents and tag requests.
v0.19.1: opencode 2.x (desktop 2.1.2): provider domain + provider.reload(), model.request hook for agent
tags and low-priority titles, built-in package @opencode/ai/providers/openai-compatible.
v0.19.2: one refresh timer per plugin copy (opencode runs the plugin per project folder; a shared timer
left all but the last folder stale), stopped by setup's cleanup / dispose.
v0.20.0: allowed apps per client (User-Agent prefixes; refusals logged, not counted as usage); every
request's User-Agent recorded and shown.
v0.20.1: certifi 2026.10.19 (GHSA-6q48-6c7w-9gw2) and pip removed from the image (its vendored certifi was
flagged too); v0.20.0 never published (blocked by the scan).
v0.21.0: outgoing User-Agent per client and per upstream (keep / neutral queuerouter/<version> / template
with {original} {client} {priority} {model} {upstream} {version}; client overrides upstream; literal
replacement, one ASCII line, max 500 chars). Allowed-apps checks still use the app's own User-Agent.
v0.21.1: the User-Agent actually sent upstream is recorded (requests.sent_user_agent, only when a rule changed
it) and shown under the app's own on the Requests page; both copy to the clipboard on click.
v0.21.2: opencode model names show which upstreams serve them (`model · nvidia, arliai`, aliases by their
pinned upstreams); model IDs are unchanged.
v0.22.0: context sizes learned from providers' overflow errors (per upstream + model) and set by hand per model
(Models page > Context sizes); overflow errors from any provider rewritten to OpenAI's form (400
context_length_exceeded, "maximum context length is N tokens"), which opencode 2.x compacts on (checked in
its source); an overflow can also trigger an alias's "context" fallback.
v0.23.0: compaction per alias, trim mode (app/compaction.py): from 80% of the target model's context down to 50%;
old tool outputs shortened (start + end), then old middle messages dropped (the kept part starts at an assistant
message, a note on the task says so); plans remembered per conversation in memory (1 h idle) and reused while
they fit; a context fallback is preferred when the alias has one. The size estimate now counts tool-call
arguments too.
v0.24.0: summarise mode — rolling summaries by a chosen model through the normal queue (tag "compaction", the
request's own priority; idle updates at low priority), chunked to fit the summariser, cached per conversation
(client + session + start) in memory or saved encrypted (Settings > Compaction), extended rather than rewritten;
idle update after ~45 s quiet for conversations above 70%; a failed summary falls back to trim; "context told
to apps" per alias (opencode config and /v1/models).
v0.25.0: separate embedding slots per limit pool (embedding_concurrency; a second set of slots under the negated
pool id, left out of the chat slot totals, shown as "<pool> (embeddings)"); ArliAI plans fill it (= parallel).
v0.25.1: request lists (Requests page, dashboard) show input and output tokens in separate columns (cached input
in brackets / on hover).
v0.25.2: compaction fixes from real use — the cut may reach into the current agent turn (all but the last 6
messages; the user's latest message moves next to the task), summarise mode doesn't wait below 90% (the request
goes out at once, the summary is extended alongside), and sizes are judged in the provider's tokens (per model
ratio of reported prompt tokens to the estimate, learned from finished requests).
v0.26.0: Requests page "Now" table — queued and running requests live with the list's columns (filters apply,
the finished list refreshes in place). Compaction: conversations recognised by structure (tool results by call id),
so an agent clearing old tool output keeps its summary; background summaries at low priority; summaries up to
8,000 tokens with a ~2,500-word guide.
v0.26.1: a streaming request that must wait for a summary is answered at once (headers, then SSE keep-alive
comments) and served when the summary is ready — opencode gave up waiting for headers and retried; the waiting
request shows as "compacting" in the Now table.
v0.27.0: model council per alias (app/council.py): pick / combine, 2-5 members, judge (blank = target), on
planning steps or every request; members and judge are internal requests through the queue (tag council), the
client's own row records the decision and isn't usage; a council with no answering member falls back to the
target; streaming answers start at once.
v0.27.1: every request's identifiers recorded (requests.client_headers: all headers except credentials, connection
plumbing and our own, plus body user / safety_identifier / prompt_cache_key / metadata) and shown on the Requests
page and in the Now table, click to copy.
v0.28.0: per-upstream model choice (upstreams.model_filter, Catalogs filters: hidden models leave every list and
aren't routed there, except for aliases, summary models and council members; "offer new models" switch; adding an
upstream opens its choice); the provider's error text on every failed request (retried 5xx, passed-through 4xx/5xx,
streaming or not, connection errors); dashboard per-client tokens split into in / out.
v0.28.1: an alias can't point at itself; a council alias tells apps the smallest context of its target, judge and
members (opencode showed 128k, its default, for a council whose target was its own name).
v0.28.2: a request shows in the Now list only once its work starts, and a stream passed on (council judge,
answer after a summary) is closed when the app leaves — abandoned council answers stayed listed as queued for hours.
v0.28.3: requests QueueRouter makes for another one (council members and judge, summaries) carry the app's identity
(its headers minus credentials: User-Agent, session ids) and are labelled with their role and the request they
served (requests.role / parent); request ids shown on the Requests page.
v0.28.4: a council takes 1 to 64 members (one member + combine = the judge reviews it; pick needs no judge for one
answer); the judge reads at most ~120k characters of answers in all, shared between them.
v0.29.0: alias editor — one page per alias (and for a new one) with sections (model, fallbacks, compaction,
council, parameter rules) and search-and-pick chips for every model field (providers, context, hidden models; drag
to reorder fallbacks; unlisted names can be typed); errors are shown on the page with what was typed; the Models
page lists aliases compactly.
v0.29.1: judge instructions per council alias (aliases.council_instructions), added after the built-in instruction;
in pick mode the reply format is repeated last so they can't change it.
v0.30.0: web search for council members (app/websearch.py: Tavily / Brave / SearXNG; per alias on/off, searches per
member, results per search; members re-asked with the results, the judge sees each member's sources, the tool is
never passed to the app; secret-looking queries refused; search count and errors on the council row). Pick judge:
4,000 output tokens so thinking models can reason, verdict read from the final answer (inline <think> skipped),
unclear verdicts marked.
v0.30.1: council progress streamed to the app as reasoning (opencode's Thought: members asked, answered, searches,
the judge's choice); an abandoned council now also cancels its members' requests (slots freed at once).
v0.31.0: orchestrator phase 1 (council mode "orchestrate"): tools ask_member (briefs, parallel), web_search,
web_fetch (built in: readable text, private / local addresses and secret-looking URLs refused, redirects checked),
calculator; today's date in its and members' instructions; budgets (rounds, member tasks, seconds); progress as
reasoning; rows "orchestrator" / "orchestrator member X". Members (council and orchestrator) may read pages too.
v0.32.0: orchestrator phase 2 — a notebook per conversation (tool queuerouter_notebook: plan, done, findings, open
questions; shown in its instructions on every step; memory or saved encrypted with summaries); with a notebook, the
orchestrator also takes the conversation's tool-result steps, so it carries a turn through the app's tool calls.
v0.33.0: orchestrator phase 3 — member roles per alias ("member = role: instructions" lines: the orchestrator's team
list and briefs, council members' instructions, role labels for the judge); a trace page per council / orchestrator
request (timeline of its steps with times, and every call made for it with tokens and errors; excerpts of the
conversation only for clients with content logging on); links from the Requests page.
v0.34.0: orchestrator phase 4 — ask_member(propose=true) lets a member propose the app's tool calls (it gets the
app's tool definitions); proposals are numbered P1, P2… and the orchestrator adopts chosen ones with
queuerouter_adopt_proposals (sent to the app exactly as proposed, optional text); two proposals changing the same
file are refused together (read-only tools never conflict).
v0.34.1: Anthropic (Claude) preset (OpenAI-compatible endpoint; temperature / top_p stripped: Claude 4.7+ reject
them); its model list asked with anthropic-version and limit=1000; max_input_tokens / max_tokens read as context /
output sizes; a refused model list shows the provider's reason on the Upstreams page.
v0.35.0: model@upstream — wherever a model is named (alias target, fallbacks, council members, judge, summary
model, or a request's model) a name ending in @<upstream name> is served only by that upstream (the provider gets the
plain name); split only when the suffix is exactly an upstream's name; the alias editor's picker lists models
offered by several upstreams once per provider.
v0.35.1: fixes from a six-part code review. Admin/storage: "can read logged content" on the Users page; trace
excerpts encrypted, purged with content and shown only to admins with that permission; restore checks every
encrypted value; nan/inf settings refused; client renames can't hand data to another client. Jobs/adapters: 429/5xx
retried with back-off (up to 8 times, within the deadline); offset-less times are UTC; adapter endpoints can't run as
jobs; one batch completion at a time; webhooks resent after a restart; tool-calling replies are "tool_use"; image
tool results noted; top_k stripped by the OpenAI/Gemini/Claude presets. Request path: a half-open circuit's trial
released on a quota failover; every network error is a failed attempt; mid-stream errors start a new SSE event;
responses closed when cancelled during inspection; no requeue in maintenance; waiting ends when no enabled upstream
is left; dot-segment paths refused; keep-alive at least 0.1 s; the model header sanitised. Council/compaction:
the conversation is compacted for the council's smallest model first; odd messages and tool calls tolerated;
proposals only for the app's tools; adopt/notebook allowed in the closing round; judge replies and fetch errors kept
out of plain logs; web fetches connect to the address that was checked (no DNS rebinding), and addresses are
refused only for real key shapes or secret parameters; calculator results over 4,300 digits refused politely.
Config: blank env vars use defaults; Anthropic's "input length and max_tokens exceed context limit" is an overflow;
overflow errors state the provider's own context; bare-list model lists accepted; api-key, x-goog-api-key and
OpenAI organization/project headers not forwarded; plugin refresh kept within 5 s–1 day.
v0.35.2: GET /v1/models/{id} answered by QueueRouter (aliases and model@upstream included); other unknown /v1/ GETs
are refused unless their path is listed in Settings → "GET paths passed to the provider" (default none): they used to
go to the first usable upstream with its key, so any client could read the provider account (credits, key details).
v0.35.3: fixes from a second six-part review. Request path: the background loop survives database and probe errors;
a non-streaming council stops when the app goes away; network errors before a stream's body get an SSE error; 429
text and fallback notes kept on one line; double-encoded dot segments refused. Council/compaction: the client's model
list and queue limits no longer refuse the council's and summariser's own requests; requests don't wait for a background
summary while they fit under 90%; the orchestrator's rounds are fitted to its context; the judge prompt joins the last
user message; tool arguments sent as objects accepted; trim plans per conversation start; the summary note kept when no
user message precedes the cut. Adapters/jobs: Anthropic tool results no longer duplicated or leak images (a v0.35.1
regression); "max_tokens" when a tool call was cut off; Responses calls without arguments get "{}"; job callbacks for
expired/cancelled jobs and unfinished batches picked up every 30 s; plain-text upstream errors don't crash a job;
retry count fixed. Storage/security: key expiry checked on every request; conversation text kept out of the error column;
restore checks client data too and reports a damaged zip; non-ASCII CSRF/setup tokens refused cleanly; a failing
schedule is switched off; "anonymous" reserved. Admin: inf hours/thresholds refused; usernames over 100 characters
refused; viewers can't mark notifications read; content links only where they open; emptied numbers go back to their
default; CSV cells can't start a formula; more than 20 allowed apps refused. Config: float clamps keep fractions; Groq
context sizes and limits read; blank DATA_DIR/LOG_LEVEL/UPSTREAM_URL use defaults; the plugin retries a failed reload.
v0.36.0: TRUSTED_PROXIES defaults to 127.0.0.1 and ::1 only (was every private range, so any LAN or Docker peer
could claim any address in X-Forwarded-For and pass ADMIN_ALLOWED_IPS or dodge per-IP limits). Upgrading behind
Traefik/Caddy without TRUSTED_PROXIES in .env: set it to the proxy's network (README → Behind a reverse proxy); an
"untrusted_proxy" log line points it out. Supply chain: every Python package pinned with hashes (requirements.in →
requirements.txt via pip-compile; the image installs with --require-hashes), the base image pinned by digest, and CI
fails on fixable HIGH vulnerabilities as well as CRITICAL ones.
v0.36.1: the same as v0.36.0, with its version number set (v0.36.0 reported itself as 0.35.3).
v0.37.0: strength order in opencode (Settings → opencode → Order models by strength): the model list is numbered,
aliases first ("00 · …"), then the strongest models ("01 · …"), then models without a score; opencode's desktop app
sorts by name, so the numbers carry the order (its terminal keeps the order sent). Scores: the administrator's own per
model (Models page, also for aliases), else the Artificial Analysis index (intelligence, coding or agentic; free key,
read daily into model-scores.json, credited on the Models page). Names are matched loosely: organisation prefixes,
fine-tune and quantisation tags, trailing versions and dates are dropped, and the longest index name the model starts
with wins at a word boundary, so GLM-4.6-Derestricted-v5 inherits GLM-4.6 (shown as "from GLM-4.6") and GLM-4.7
isn't claimed by GLM-4. Migration 32: model_scores.
v0.37.1: Settings checkboxes with long help text no longer shrink to a dot (the "Order models by strength" box couldn't
be seen); "-free" / ":free" model names take their base model's score.
v0.38.0: thinking levels. opencode 2.x shows a level selector next to models that can think (through the plugin;
re-install it once): the levels come from the upstream's own list (OpenRouter's reasoning block: supported_efforts,
default_effort, mandatory; ArliAI: none, see v0.38.1), else from OpenRouter's public model list by name
(read daily, no key), or from the administrator (Models → Thinking levels; "-" = no selector). opencode sends the
choice as reasoning_effort, translated per upstream in build_request: OpenRouter gets reasoning.effort, vLLM servers
(ArliAI, or engine "vllm") get chat_template_kwargs enable_thinking/thinking plus low/medium/high, OpenAI-style APIs
get reasoning_effort as is, Claude's OpenAI-compatible endpoint (which ignores it) none. The remote config
opencode reads itself is unchanged; the plugin asks /v1/opencode-config?plugin=2. Without an Artificial Analysis key the
strength order uses the index scores in OpenRouter's public list. Migration 33: model_thinking.
v0.38.1: no thinking levels for ArliAI. Tested live through QueueRouter and directly: DeepSeek-V4.1-Flash-1017 and
Qwen3.8-Flash-Next think whatever is sent (chat_template_kwargs enable_thinking/thinking, top-level enable_thinking,
reasoning {enabled/effort}, thinking {type: disabled}, reasoning_effort none/low, /no_think), with 400-6,000 thinking
tokens for the same prompt either way. ArliAI is style "fixed": nothing offered, requests passed on unchanged. Other vLLM
servers (engine "vllm") keep the chat-template switch.
v0.39.0: prices in opencode. The plugin tells opencode each model's price, so its session cost (bottom bar) counts
QueueRouter models too: your manual price (Models → Reference prices) when set, else per upstream nothing extra on a
pool with a monthly price (ArliAI), the upstream's own price from its model list (OpenRouter's pricing), or the
models.dev match; the highest when several upstreams serve a model. Plain aliases get their target's price; councils
none (their extra calls aren't visible to opencode). Calls opencode can't see (councils, summaries) stay on the Usage
page only.
v0.39.1: images in opencode through QueueRouter. The plugin declared every model text-only, so opencode refused to
send images (its read tool: "this model does not support image input"). Models now declare image input where a list
says so: ArliAI's vlm, OpenRouter's architecture.input_modalities, a plain input_modalities, else OpenRouter's public
list by name (so Synthetic's hf:deepseek-ai/DeepSeek-V4.1-Flash-1017 reads images); unknown stays text-only.
v0.39.2: the Requests page refreshes cheaply. An open Requests page fetched the whole page again (300 rows, ~1 MB with
opencode's identifiers) after every finished request, which on a busy day kept the 0.5-CPU container busy and slowed
streaming. It now fetches only the rows written since its newest one (/admin/requests/rows?after=, same filters) and
adds them on top, and a hidden tab fetches nothing until it is shown again.
v0.40.0: response times per model and upstream. Each streamed request now records the provider's own timings, from
when its (last) attempt was sent: headers (the provider's queue), first token (thinking included), first answer token,
output speed (completion tokens / first to last token, from 16 tokens and 0.5 s), longest pause between tokens; and,
for any request, the time waited before retries. A Response times page (nav) shows per model and upstream the median
and worst 5% over 1 h / 24 h / 7 d, and a chart of the busiest pairs' median first token. The Requests page shows them
per row. Alert "Slow first token" (off by default): worst-5% first token over N s in the window, at least 5 requests.
Not done: typical speeds from Artificial Analysis (needs its key; its field names unverified), Prometheus metrics.
v0.41.0: the opencode plugin updates itself. /v1/opencode-config (200 and 304) carries X-QueueRouter-Plugin, the
fingerprint of the plugin file QueueRouter serves (sha256 of app/opencode_plugin.js, 16 hex), which is also written
into the served file (const PLUGIN). When the two differ the plugin downloads /opencode/queuerouter.js, checks it
(that fingerprint, the same BASE, its default export, at most 1 MB), writes it beside itself and renames it over its
own file (copy when Windows refuses the rename); opencode loads it at its next start, and the terminal UI shows a
toast. A failed try waits an hour; copies in other project folders see the file already updated. Off for everyone:
Settings → opencode (opencode_plugin_updates; no header is sent); on one machine: QUEUEROUTER_AUTO_UPDATE=0; never
for a plugin loaded from node_modules or not from a file. Not done: signing the plugin in CI (a compromised server
could hand out code; the README says so). Plugins from before 0.41 need one manual re-install.
v0.41.1: the update message no longer insists on a restart: tested on Windows, the opencode desktop app reloads the
plugin as soon as its file changes (the reloaded copy carries the served fingerprint, so it doesn't update again).
v0.41.2: an upstream whose model list is unknown (a Claude account whose /models fails) no longer gets models other
upstreams list: a council with "hf:..." members sent some of them there by load, and they came back 404. Such an
upstream only gets models no list names. A 404 from an upstream fails over to the next candidate (not counted
against its circuit breaker).
v0.41.3: the real cause of that council case: QueueRouter's own requests (council members, judge, summaries) were
routed like a client's, so a member model hidden on Synthetic (Upstreams page) could only go to the upstream without
a list. They now route like an alias (hidden models allowed, as documented), and a model listed but hidden somewhere
never goes to an upstream whose list can't be fetched (a client asking for it gets 503, as with no upstream).
v0.41.4: council members and judges (and summaries) whose model reads text only get the conversation with each image
replaced by a note ("an image was attached here; this model can't see images") instead of being refused (Synthetic:
"does not appear to support image inputs"). Whether a model reads images is judged as for opencode: its upstream's
list, else the reference list, else text only.
v0.42.0: Clients page. Fixed: timed content logging ("for N hours") made the page fail (500): the template's
per-client live counts were named `now`, hiding the current time it compared with. Clients can be renamed (the Save
form's name field): the key stays the same; jobs, files, batches, schedules, saved conversations, logged content and
now also the request log and daily usage move with it (usage rows merge with any the new name already has; monthly
budgets follow within a minute). Keys are kept encrypted too (clients.key_secret, schema 35, with SECRET_KEY like
upstream keys): Show on the Clients page fetches one (POST /admin/clients/{id}/key, CSRF header, no-store; audited as
client.show_key). Keys made before 0.42 only have their hash: New key makes them showable.
Admin pages use the whole window width (was capped at 1100 px; phones keep a 16 px margin).
v0.43.0: with strength order on, opencode's model names carry the score after the number: `03 · 45.1 · model ·
upstreams` (one decimal; `~45.1` when it's the base model's score, as for fine-tunes). Aliases without a score of
their own stay `00 · …`; models without a score keep their plain name.
v0.43.1: Settings → Circuit breaker threshold 0 means never take an upstream out of rotation (it meant "out on the
first failure"); setting it while one is out brings it back at once. Its failures still show as the upstream's last
error, and requests keep their retries.
v0.44.0: fixes from a third six-part code review.
- Request log and usage: a value with a lone surrogate (JSON "\ud800", e.g. in `user`) made the whole write fail
  and every row in it was lost (a client could stay out of its budgets that way): such strings are stored with "?".
  Logged content: a reply streamed in an unexpected shape (content parts, tool arguments as an object) blocked every
  later save for good and piled up in memory; now read as far as possible, else kept as it came.
- Streams: QueueRouter asks for the usage chunk itself (see §10); without it most providers sent no token counts,
  so token limits, budgets and the Usage page counted 0 for apps that didn't ask.
- Councils: the combine judge is QueueRouter's own call like the members (a key allowed only the alias got 403
  after all members had answered); members and the orchestrator only get the web tools the alias has on (with
  search on and page reading off, a member could still have a page read); page reads end after 45 s and turn a bad
  address or an odd charset into "couldn't read" instead of failing the council.
- Retries: an upstream's own retry limit (the Upstreams field, now "Retries", was labelled "429 retries" but also
  covers 5xx and connection errors) no longer stops failing over to another upstream; the client's / global budget
  decides that.
- "Allow without key" holds only while no client exists at all: disabling or expiring every client no longer opens
  the API.
- A client's own `model@upstream` no longer reaches a model hidden on that upstream (aliases and QueueRouter's own
  calls still may).
- A 2FA code typed in other digits (Arabic-Indic ١٢٣...) works instead of failing with a 500.
- Rate-limit history is pruned for clients without limits too (it grew for good).
- A base URL httpx can't use ("host:8080v1") is that upstream's error (failover, circuit) instead of a 500.
- Batches: one whose files can't be written (results that can't be read, too big) is marked failed; it used to be
  tried again on every tick, holding up every other job, and made QueueRouter fail to start.
- opencode plugin: updates itself only over https (or from QueueRouter on the same machine), follows no
  redirects for it, and skips a download bigger than a plugin can be; over plain http anyone on the network path
  could have handed it other code to run.

### Later (agreed, not scheduled)

- **Separate embedding slots per limit pool** (done: v0.25.0). ArliAI (and others) count `/v1/embeddings` against its
  own parallel limit (Pro: 2 chat + 2 embedding), but QueueRouter queues embeddings in the chat slots,
  so an indexing burst (Continue / Roo / Kilo codebase indexing, RAG in our own apps) makes chat wait
  needlessly. Plan: an optional `embedding_concurrency` per pool (blank = shared with chat, as now),
  a second SlotQueue per pool for `/v1/embeddings`, shown on the dashboard/metrics; plan presets fill
  it (ArliAI 2). opencode itself doesn't use embeddings (it searches with grep/LSP tools).
- **Automatic context sizes + overflow errors opencode understands** (done: v0.22.0). Many providers (NVIDIA, Gemini's
  OpenAI endpoint...) don't report a context size, so QueueRouter skips its check and opencode assumes
  131,072. Plan: (1) learn the limit from the provider's own "too long" error ("maximum context length is
  N", "...allowed (N)") and store it per upstream + model; (2) a manual context/output override per model on
  the Models page (also fixes wrong reported values); (3) rewrite every provider's overflow error into the
  standard 400 `context_length_exceeded`, so opencode compacts and retries. Verify on opencode 2.1.2 that it
  compacts on that error (it does compact on token usage).
- **Compaction in QueueRouter (opt-in per alias).** Done: v0.23.0 (trim), v0.24.0 (summarise). Agreed design (2026-10-22):
  - *Why here:* after switching e.g. from a 1M model to a 200k one with 800k of history, opencode's own
    compaction would use the small model and overflow too; QueueRouter can use another model.
  - *When:* decided per request, on the whole request (history + current message + system prompt + tools
    + room for the answer) against the limit of the model it is about to go to. Compact at ~80% of that
    limit down to ~50%, in steps, so the start of the request stays the same between compactions and
    provider prompt caching keeps working. The same conversation goes in full to a model it fits.
  - *Kept in full:* the current message, system prompt, tool definitions and the last few turns. If the
    current message alone doesn't fit: the alias's fallback to a bigger model, else the standard
    `context_length_exceeded` error. Single oversized old messages (giant logs) are trimmed (start + end).
  - *Modes:* "trim" (old tool outputs first, then old middle messages, with a marker; no model calls, no
    cache needed) and "summarise" with a chosen "compaction model". The summary is general (what was done,
    decisions, files changed, open problems), not tailored to the current question, so it can be reused.
  - *Rolling + chunked:* each update reads the previous summary + the newly-old part only. A compaction
    model smaller than the history works by reading it in chunks that fit (~150k for a 200k model), one
    after another with the running summary (not in parallel: chunks summarised blind muddle decisions).
    Only the first compaction of a big history is slow (a 1M history = ~6-7 calls); later ones are small.
  - *Cache:* per session, keyed by a hash of the summarised message prefix (history is append-only);
    hits swap the summary in with no model call. Undo/edit/revert or opencode's own compaction: reuse the
    longest prefix that still matches, summarise the rest. Not caching (or a 10 s cache) would summarise
    on most of the 10-30 requests one agent message makes: slow, costly, a shifting past, no prompt
    caching. Default: in memory only, idle timeout ~1 h (configurable, resets on each request), least
    recently used dropped first; a "saved" option keeps them in the DB encrypted, with retention, deleted
    with the client. (Memory-only vs saved: a setting; default memory.)
  - *Idle update:* when a session has been quiet ~30-60 s after its last answer and is above ~70% of its
    model's limit, update (not replace) the summary in the background so the next message doesn't wait.
    Low-priority lane only, once per idle period, only recently active sessions, recent turns untouched.
    Skipped below ~70%: moving the summary boundary early would reset provider prompt caching for nothing.
  - *Virtual context:* such an alias can advertise a big context to opencode so it doesn't compact early;
    each request is fitted to the model it actually goes to. Recommended: the biggest real model's size
    (1M). Bigger (e.g. 5M) works technically with rolling summaries but opencode uploads the full history
    every message (5M tokens = 20 MB+), the model still only sees summary + recent turns, and opencode
    itself slows down; past the virtual size, opencode compacts on its own with the 1M alias.
  - *Shown:* Requests page "compacted 800k -> 150k" (mode, cache hit/miss); opencode's own token and
    cost numbers will show the full history. Needs a body limit big enough (MAX_BODY_MB, default 20).
  - *Depends on* the automatic context sizes item above (it must know each model's real limit).
- **Model council (alias mode)** (done: v0.27.0). Agreed design (2026-10-22):
  - *What:* an alias sends one request to several member models at once (non-streaming), then a judge
    model decides; the answer goes back in the normal format, so the app (opencode) doesn't know.
  - *Modes:* "pick" (the judge only names the best candidate: fast, and works for tool calls, which can't
    be merged meaningfully) and "combine" (the judge writes a merged answer: for plans and explanations).
  - *Only on planning steps:* the first step after a user message, or opencode's plan agent (tags it already
    sends, e.g. #agent:plan); ordinary tool steps (read a file, run a command) go to one model as usual.
  - *Members across providers* (e.g. ArliAI + OpenCode + Gemini), so a council doesn't take all of one
    account's slots (3 members + judge = 4 calls per step); member calls go through the normal queue
    (internal requests, like summaries), tagged "council".
  - *Costs:* you wait for the slowest member plus the judge, nothing streams meanwhile (keep-alive comments
    hold the connection); several times the tokens.
  - *Shown:* the Requests page lists each member's call and the judge's choice.
- **Orchestrator (alias mode).** Agreed design (2026-10-24); phases 1-4 done (v0.31.0-v0.34.0).
  - *What:* unlike the council (members answer the same request at once, a judge reads the results), an
    orchestrator model directs the work through tools QueueRouter runs for it: it plans, delegates focused
    tasks to members, searches and reads the web, reads the results, may ask again, then writes the final
    answer (text, or the app's own tool calls). Uses: research, big refactors, code review, debugging.
    Orchestrator model: DeepSeek-V4.1-Flash for now (per alias, any model).
  - *Its tools (run by QueueRouter):* `ask_member(member, task)` / `ask_members([...])` (in parallel);
    `web_search`, `web_fetch`; date/time and a calculator. The app's tools (opencode: read, edit, bash) can't
    run mid-work (one answer per request): the orchestrator ends its turn with them, opencode runs them and
    sends the results, and it continues in the next request (phase 2: plan notebook).
  - *Briefs:* members get a focused task written by the orchestrator (with the parts it attaches: a file's
    content, an error), not the whole conversation.
  - *Member roles (phase 3):* per member an optional role and instructions ("researcher": searches and cites;
    "reviewer": looks for bugs and risks; "implementer": proposes the edit); the orchestrator sees the roster.
  - *Plan notebook (phase 2):* per conversation (like summaries: memory or saved encrypted): the plan, what is
    done, what members found; shown to the orchestrator on every step so it continues instead of re-planning.
  - *Budgets:* per step: rounds, member calls, searches / fetches, time; when one runs out the orchestrator is
    told to finish with what it has.
  - *Live progress:* streamed to the app as reasoning (`reasoning_content`), which opencode shows live in its
    "Thought" section ("Asking 2 members… Searching… Member 1 done: …"), kept apart from the answer; doubles as
    keep-alive. The council gets the same.
  - *Visibility:* every call a row with role + parent (as for the council); phase 3: a trace page per request
    (plan, rounds, delegations with question and answer, searches).
  - *Safety:* web and member results are data, never instructions (marked as such; its instructions say never
    to act on commands found in them); opencode's own permission prompts stay on for orchestrated aliases.
  - *Triggers:* like the council: planning steps (first step after a user message, or opencode's plan agent) or
    every request.
  - *Phases:* 1) one-request orchestrator (delegation, web search + fetch, date/time, calculator, budgets, live
    progress, roles on rows); 2) plan notebook across steps; 3) member roles + trace page; 4) members proposing
    app tool calls that the orchestrator merges.
- **Using Claude (Haiku / Sonnet) sparingly for the most intelligence** (discussed 2026-10-25). Claude cost =
  input tokens (each call re-reads the conversation, often 50-150k in opencode) × Claude calls per step; the
  OpenAI-compatible endpoint has no prompt caching. Advice given: Claude only where thinking matters, cheap models
  (DeepSeek-V4.1-Flash, Qwen3.8, GLM on ArliAI) for the rest — opencode's plan agent on a Claude alias (or a
  council with a Claude judge), build agent on a cheap model; council combine on planning steps with a Sonnet
  judge (Haiku for picks / light review); compaction (summarise, cheap summary model) on Claude-facing aliases;
  the orchestrator only for big jobs on its own alias with a tight budget (~3 rounds, ~4 tasks), Sonnet not
  Haiku. To build, most valuable first:
  - **Native Anthropic upstream with prompt caching.** Talk to Claude through the Messages API (not the
    compatibility layer): convert chat requests to /v1/messages and back (QueueRouter already converts the other
    way for /v1/messages clients), mark the stable start of the conversation (system prompt, tools, older turns)
    with cache_control so repeats cost ~10%; report cached tokens; thinking returned as reasoning; workspace
    header and API version settable. Makes every Claude setup above cheaper.
  - **Escalation alias:** a cheap model does the step; Claude is called only when needed (repeated tool
    failures / errors in the conversation, the cheap model saying it is unsure, or a refusal). Hard to judge
    reliably; start with explicit signals (failed tests twice, error loops).
  - **Plan-then-build alias:** the first step after a user message goes to one model (Claude), every following
    tool step of that turn to another (cheap) — like the council's planning-steps mode, with one model, no judge,
    and without switching agents in opencode.
- **QueueRouter's own tools** (for council members and the orchestrator; switched on per alias, counted in
  budgets, shown on the Requests page). Server-side only: anything touching the user's machine (files, shell,
  git) stays with the app; nothing that writes outside QueueRouter (posting, sending, deploying).
  - *Built:* `web_search` (Tavily / Brave / SearXNG; SearXNG next to QueueRouter is the recommended free one).
  - *Phase 1:* `web_fetch(url)` built in (no key): readable text from HTML, size (~2 MB) and time (~20 s)
    limits, text-like content only, refuses private / local addresses (also after redirects: SSRF) and URLs
    that look like they carry secrets; optional fallback Jina Reader for JavaScript-heavy pages. Date/time
    (also added to prompts) and a calculator (maths expressions only, no code).
  - *Next:* package and release lookup (PyPI, npm, crates.io, Docker Hub, GitHub releases; free APIs);
    GitHub reading (files, READMEs, issues, PRs of public repos; optional token).
  - *Then:* an MCP client: MCP servers set up in the admin UI (read-only ones), their tools offered per alias.
  - *Later:* a knowledge base (upload documents, indexed with embeddings in their own slots, searchable like
    the web) and a code sandbox (isolated container: no network, CPU / memory / time limits, nothing mounted).
- **Choose which models an upstream offers** (done: v0.28.0; aliases may use hidden models). When adding or editing an
  upstream, pick which of its models are offered (all by default; a checklist from its model list, with
  search and select all / none, since some providers list 100+). Hidden models don't appear in /v1/models,
  the opencode config / plugin (so they drop out of opencode's picker on the next refresh), the Models page
  lists or the Playground, and requests for them aren't routed to that upstream (another upstream offering
  the same model still can). New models a provider adds later: an "offer new models automatically" switch
  per upstream (on = they appear, off = only the ticked ones). Aliases can still point at a hidden model
  explicitly (or should they? decide when building).
- **Team board: opencode sessions of different people cooperating through QueueRouter** (designed 2026-10-27,
  not scheduled). For e.g. two people (two client keys) on one shared project.
  - *Teams* (admin): a name, member client keys, the project's git remote(s). A session joins when its folder's git
    remote matches (the plugin reads it and sends it), so paths may differ per machine. A key sees only its teams.
  - *Board* per team (stored encrypted, like summaries): tasks (open / claimed / blocked / done, owner, branch,
    closing summary), messages (to one member or all, read state), notes (decisions, conventions), file claims
    that expire unless renewed.
  - *Tools* as an MCP server QueueRouter serves (/team/mcp, client key): team_status (one short digest: who's on
    what, open tasks, unread messages, recent notes), team_post, team_task_add, team_claim (with files), team_done
    (summary, branch), team_note. MCP rather than plugin tools because opencode 2.x plugin tools need its `effect`
    Schema, which a single-file plugin can't import; MCP also lets other MCP clients join. The plugin adds the
    MCP server automatically (2.x ctx.mcp.transform; 1.19 config hook) — verify on both first; fallback: an
    `mcp` entry for opencode.json shown on the Clients page.
  - *Hints*: when a member's board changed, QueueRouter adds one short line at the END of that member's next
    request ("Team Acme: 2 new messages from Bilal, a task finished. Call team_status."), so prompt caching holds.
  - *Team page* (admin): the board live; add tasks, step in.
  - Code moves through git (a branch per task); the board says which branch holds what.
  - Limits: identity is per person, not per session (opencode sends one set of MCP headers), fine between people;
    idle sessions only see news when they next run (waking them via 2.x ctx.session.prompt: later, opt-in, it
    spends tokens unasked); cooperation quality depends on the model; messages size-limited and checked for
    secrets like web search queries.
  - Releases: 1) teams, board, MCP tools, Team page, plugin adds the MCP server (VM test: two clients, 2.x and
    1.19); 2) the hint line; 3) optional waking of idle sessions.

---

## 21. Open questions

1. Domain name for the public endpoint?
2. ~~Reverse proxy~~ — resolved: existing Traefik with Docker labels (Caddy compose kept as alternative).
3. ArliAI: `stream_options.include_usage` support? Does `GET /v1/models` count toward concurrency?
   (Verify during build.)
4. Will you add a second ArliAI **account** or another provider soon? (A second key on the same
   account adds no capacity — it shares the pool.)
5. Should the admin UI live on the same domain under `/admin`, or a separate subdomain?
6. ArliAI (plan: **Pro $30 — 2 parallel, 262,144 context cap**): verify during build with the
   real key what `/v1/models` returns (does it include `max_model_len`/context per model?),
   whether `/v1/tokenize` exists and whether it uses a parallel slot, and that large models
   really cost 1 slot.
7. For the MVP: will QueueRouter run on the same machine as opencode (localhost, no TLS), or on a
   separate server — resolved: separate server, behind Traefik, access keys required.
