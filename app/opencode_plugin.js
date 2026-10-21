// QueueRouter plugin for opencode, served by QueueRouter at /opencode/queuerouter.js.
// Install: save it in opencode's global plugin folder (~/.config/opencode/plugin/) and set
// QUEUEROUTER_KEY to a client key (or log in with `opencode auth login <QueueRouter URL>`).
//
// One file for both of opencode's plugin systems:
// - setup()  — the newer one (the desktop app): puts your QueueRouter models and aliases into
//   opencode's model catalog and keeps them current while opencode runs (checked every minute,
//   QUEUEROUTER_REFRESH_S, 0 = off); the model picker updates by itself.
// - server() — the older one (the terminal UI): loads the models at start, keeps them current
//   (reloading opencode's providers when no session is busy), tags requests with the agent name
//   and sends title/summary requests at low priority.
// QueueRouter itself groups a conversation's requests (subagents included) from the session
// headers opencode sends, with or without this plugin.

import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const BASE = "__QUEUEROUTER_BASE__"; // filled in by QueueRouter when it serves this file
const PROVIDER = "queuerouter";
const KEY_ENV = "QUEUEROUTER_KEY";
const LOW_PRIORITY_AGENTS = new Set(["title", "summary"]);
const REFRESH_S = Number(process.env.QUEUEROUTER_REFRESH_S ?? 60);
const DEFAULT_CONTEXT = 131072;
const DEFAULT_OUTPUT = 16384;

// The client key: QUEUEROUTER_KEY, else the one `opencode auth login <QueueRouter URL>` stored.
function readKey() {
  if (process.env[KEY_ENV]) return process.env[KEY_ENV];
  try {
    const data = process.env.XDG_DATA_HOME || join(homedir(), ".local", "share");
    const auth = JSON.parse(readFileSync(join(data, "opencode", "auth.json"), "utf8"));
    for (const [url, entry] of Object.entries(auth)) {
      if (url.replace(/\/windows\/?$/, "").replace(/\/$/, "") === BASE && entry?.token) return entry.token;
    }
  } catch {}
  return undefined;
}

// QueueRouter's model list for this key: { tag, provider } (or { tag } when unchanged, or null).
async function fetchModels(previousTag) {
  const key = readKey();
  if (!key) return null;
  try {
    const res = await fetch(`${BASE}/v1/opencode-config`, {
      headers: { Authorization: `Bearer ${key}`, ...(previousTag ? { "If-None-Match": previousTag } : {}) },
      signal: AbortSignal.timeout(10000),
    });
    if (res.status === 304) return { tag: previousTag };
    if (!res.ok) return null;
    const config = await res.json();
    return { tag: res.headers.get("etag"), provider: config.provider?.[PROVIDER], key };
  } catch {
    return null; // QueueRouter unreachable: keep what we have
  }
}

// One refresh timer per opencode process and plugin system, even if an old copy isn't told to stop.
function startTimer(name, tick) {
  clearInterval(globalThis[name]);
  const timer = REFRESH_S > 0 ? setInterval(tick, REFRESH_S * 1000) : undefined;
  globalThis[name] = timer;
  return timer;
}

// -- newer plugin system (opencode desktop) --------------------------------------------------

function applyCatalog(catalog, current) {
  if (!current?.provider) return;
  catalog.provider.update(PROVIDER, (provider) => {
    provider.name = "QueueRouter";
    provider.api = { type: "aisdk", package: "@ai-sdk/openai-compatible", url: `${BASE}/v1`, settings: {} };
    provider.request.body.apiKey = current.key;
  });
  const models = current.provider.models ?? {};
  const record = catalog.provider.get(PROVIDER);
  for (const id of [...(record?.models?.keys() ?? [])]) {
    if (!(id in models)) catalog.model.remove(PROVIDER, id); // removed in QueueRouter
  }
  for (const [id, info] of Object.entries(models)) {
    catalog.model.update(PROVIDER, id, (model) => {
      model.name = info.name ?? id;
      model.api = { id, type: "aisdk", package: "@ai-sdk/openai-compatible" }; // URL and key from the provider
      model.capabilities = { tools: true, input: ["text"], output: ["text"] };
      model.limit = { context: info.limit?.context ?? DEFAULT_CONTEXT, output: info.limit?.output ?? DEFAULT_OUTPUT };
      model.status = "active";
      model.enabled = true;
    });
  }
}

async function setup(ctx) {
  let current = await fetchModels();
  await ctx.catalog.transform((catalog) => applyCatalog(catalog, current));
  startTimer("__queuerouterCatalogRefresh", async () => {
    const next = await fetchModels(current?.tag);
    if (!next || !next.provider || next.tag === current?.tag) return;
    current = next;
    await ctx.catalog.reload(); // the desktop app re-reads its model list on this
  });
}

// -- older plugin system (opencode terminal UI) ----------------------------------------------

async function server({ client }) {
  const roots = new Map(); // session id -> root session id (subagents belong to their parent's)
  let seen = null; // ETag of the model list this opencode instance loaded
  let reloadWanted = null; // the ETag of a newer list waiting to be loaded
  const busy = new Set(); // sessions working right now: never reload under them

  async function checkModels() {
    const next = await fetchModels(seen);
    if (!next?.tag || next.tag === seen) return;
    if (seen === null) seen = next.tag; // first look: what this instance started with
    else {
      reloadWanted = next.tag;
      await reloadIfIdle();
    }
  }

  async function reloadIfIdle() {
    if (!reloadWanted || busy.size) return;
    seen = reloadWanted; // reload once per change, even if this copy of the plugin keeps running
    reloadWanted = null;
    try {
      await client.tui?.showToast?.({ body: { message: "QueueRouter: model list updated", variant: "info" } });
    } catch {}
    try {
      await client.instance.dispose(); // opencode rebuilds its providers; the terminal UI re-reads them
    } catch {}
  }

  const timer = startTimer("__queuerouterRefresh", checkModels);
  if (timer) checkModels();

  async function rootSession(id) {
    if (roots.has(id)) return roots.get(id);
    let current = id;
    for (let depth = 0; depth < 8; depth++) {
      let parent;
      try {
        const res = await client.session.get({ path: { id: current } });
        parent = res?.data?.parentID;
      } catch {
        break;
      }
      if (!parent) break;
      current = parent;
    }
    roots.set(id, current);
    return current;
  }

  return {
    async config(config) {
      if (config.provider?.[PROVIDER]) return; // a config or the login already provides it
      const current = await fetchModels();
      if (!current?.provider) return;
      seen = current.tag ?? seen; // the list this instance starts with
      // {env:...} placeholders are already resolved when this hook runs, so set the key itself.
      current.provider.options = { ...(current.provider.options ?? {}), apiKey: current.key };
      config.provider = { ...(config.provider ?? {}), [PROVIDER]: current.provider };
    },

    async event({ event }) {
      if (event.type === "session.status") {
        const { sessionID, status } = event.properties ?? {};
        if (status?.type === "idle") busy.delete(sessionID);
        else if (sessionID) busy.add(sessionID);
      } else if (event.type === "session.idle") {
        busy.delete(event.properties?.sessionID);
      } else return;
      await reloadIfIdle(); // a change seen while busy is applied once everything is idle
    },

    async dispose() {
      clearInterval(timer); // only ours: a newer copy may already be running
    },

    async "chat.headers"(input, output) {
      const provider = input.provider?.info?.id ?? input.model?.providerID;
      if (provider !== PROVIDER) return;
      output.headers["X-Session-Id"] = await rootSession(input.sessionID);
      if (input.message?.id) output.headers["X-Run-Id"] = input.message.id;
      const agent = String(input.agent ?? "")
        .toLowerCase()
        .replace(/[^a-z0-9._-]/g, "-")
        .slice(0, 50);
      output.headers["X-QueueRouter-Tags"] = agent ? `opencode,agent:${agent}` : "opencode";
      if (LOW_PRIORITY_AGENTS.has(agent)) output.headers["X-QueueRouter-Priority"] = "low";
    },
  };
}

export default { id: "queuerouter", server, setup };
