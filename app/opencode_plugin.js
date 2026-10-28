// QueueRouter plugin for opencode, served by QueueRouter at /opencode/queuerouter.js.
// Install: save it in opencode's global plugin folder (~/.config/opencode/plugin/) and set
// QUEUEROUTER_KEY to a client key (or log in with `opencode auth login <QueueRouter URL>`).
//
// One file for every opencode plugin system:
// - setup()  — the newer ones (desktop app; opencode 2.x and 1.19): adds QueueRouter as a provider
//   with your models and aliases, keeps them current while opencode runs (checked every minute,
//   QUEUEROUTER_REFRESH_S, 0 = off; the model picker updates by itself) and, on 2.x, tags requests
//   with the agent name and sends title requests at low priority.
// - server() — the older one (the terminal UI): loads the models at start, keeps them current
//   (reloading opencode's providers when no session is busy), tags requests with the agent name
//   and sends title/summary requests at low priority.
// QueueRouter itself groups a conversation's requests (subagents included) from the session
// headers opencode sends, with or without this plugin.
// It also updates itself when QueueRouter serves a newer plugin (QUEUEROUTER_AUTO_UPDATE=0 = off).

import { copyFileSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const BASE = "__QUEUEROUTER_BASE__"; // filled in by QueueRouter when it serves this file
const PLUGIN = "__QUEUEROUTER_PLUGIN__"; // this file's fingerprint, also filled in by QueueRouter
const PROVIDER = "queuerouter";
const KEY_ENV = "QUEUEROUTER_KEY";
const LOW_PRIORITY_AGENTS = new Set(["title", "summary"]);
const REFRESH_S = Number(process.env.QUEUEROUTER_REFRESH_S ?? 60);
const DEFAULT_CONTEXT = 131072;
const DEFAULT_OUTPUT = 16384;
const OPENCODE2_PACKAGE = "@opencode/ai/providers/openai-compatible"; // opencode 2.x built-in (1.x: @ai-sdk/openai-compatible)

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
    const res = await fetch(`${BASE}/v1/opencode-config?plugin=2`, { // with thinking levels
      headers: { Authorization: `Bearer ${key}`, ...(previousTag ? { "If-None-Match": previousTag } : {}) },
      signal: AbortSignal.timeout(10000),
    });
    selfUpdate(res.headers.get("x-queuerouter-plugin")); // in the background
    if (res.status === 304) return { tag: previousTag };
    if (!res.ok) return null;
    const config = await res.json();
    return { tag: res.headers.get("etag"), provider: config.provider?.[PROVIDER], key };
  } catch {
    return null; // QueueRouter unreachable: keep what we have
  }
}

// -- self-update ------------------------------------------------------------------------------
// Every model list comes with the fingerprint of the plugin QueueRouter serves now. When it differs, the
// plugin downloads that file, checks it and swaps it in for its own; the desktop app reloads it, else opencode
// runs it from its next start.
// QUEUEROUTER_AUTO_UPDATE=0 turns this off here; QueueRouter's own setting turns it off for everyone.
const AUTO_UPDATE = !/^(0|false|no|off)$/i.test(process.env.QUEUEROUTER_AUTO_UPDATE ?? "");
// Only over https, or from this computer: over plain http anyone on the way could hand over a "plugin" of
// their own, and it would run with opencode's rights from then on.
const UPDATE_SAFE =
  /^https:\/\//i.test(BASE) || /^http:\/\/(localhost|127\.0\.0\.1|\[::1\])(:\d+)?(\/|$)/i.test(BASE);
const MAX_PLUGIN_CHARS = 1024 * 1024;
const UPDATE_RETRY_MS = 3600 * 1000;
let installed = PLUGIN; // the fingerprint in the file now (this copy keeps running the one it loaded)
let lastTry = { fingerprint: null, at: 0 };
let announce = (message) => console.log(message); // the terminal UI shows a toast instead

// This plugin's own file; none when it wasn't loaded from a file of its own (e.g. an npm package).
function ownFile() {
  try {
    const file = import.meta.url.startsWith("file:") ? fileURLToPath(import.meta.url) : null;
    return file && !/[\\/]node_modules[\\/]/.test(file) ? file : null;
  } catch {
    return null;
  }
}

async function selfUpdate(latest) {
  if (!AUTO_UPDATE || !UPDATE_SAFE || !latest || latest === installed || !/^[0-9a-f]{16,64}$/.test(latest)) return;
  if (lastTry.fingerprint === latest && Date.now() - lastTry.at < UPDATE_RETRY_MS) return;
  lastTry = { fingerprint: latest, at: Date.now() };
  const file = ownFile();
  if (!file) return;
  const marker = `const PLUGIN = "${latest}";`;
  try {
    if (readFileSync(file, "utf8").includes(marker)) return void (installed = latest); // another copy did it
  } catch {
    return;
  }
  const temp = `${file}.${process.pid}.update`;
  try {
    // no redirects: the plugin comes from this QueueRouter itself, and no bigger than a plugin can be
    const res = await fetch(`${BASE}/opencode/queuerouter.js`, { signal: AbortSignal.timeout(30000), redirect: "error" });
    const size = Number(res.headers.get("content-length") ?? 0);
    const text = res.ok && size <= MAX_PLUGIN_CHARS * 4 ? await res.text() : "";
    const expected =
      text.length <= MAX_PLUGIN_CHARS &&
      text.includes(marker) &&
      text.includes(`const BASE = ${JSON.stringify(BASE)};`) && // the same QueueRouter
      /\nexport default \{ id: "queuerouter",/.test(text);
    if (!expected) return; // not the plugin we asked for: keep this one
    writeFileSync(temp, text);
    try {
      renameSync(temp, file); // in one step: never a half-written plugin
    } catch {
      copyFileSync(temp, file); // Windows may refuse to replace a file that is open
    }
    installed = latest;
    announce("QueueRouter plugin updated (opencode reloads it, or restart if it doesn't)");
  } catch {
    // unreachable or not writable: the next try is an hour away
  } finally {
    try {
      rmSync(temp, { force: true });
    } catch {}
  }
}

// opencode runs (and even loads) the plugin separately for each open project folder, so every copy
// keeps its own timer. opencode stops it through the cleanup that setup() returns (newer systems)
// or the dispose hook (terminal UI).
function startTimer(tick) {
  // within 5 s .. 1 day: a bigger delay than a timer can hold would make it fire constantly
  return REFRESH_S > 0 ? setInterval(tick, Math.min(Math.max(REFRESH_S, 5), 86400) * 1000) : undefined;
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
      model.capabilities = { tools: true, input: inputOf(info), output: ["text"] };
      model.limit = { context: info.limit?.context ?? DEFAULT_CONTEXT, output: info.limit?.output ?? DEFAULT_OUTPUT };
      model.status = "active";
      model.enabled = true;
    });
  }
}

async function setup(ctx) {
  if (ctx.provider?.transform) return setupProviders(ctx); // opencode 2.x
  if (ctx.catalog?.transform) return setupCatalog(ctx); // opencode 1.19
}

async function setupCatalog(ctx) {
  let current = await fetchModels();
  await ctx.catalog.transform((catalog) => applyCatalog(catalog, current));
  const timer = startTimer(async () => {
    const next = await fetchModels(current?.tag);
    if (!next || !next.provider || next.tag === current?.tag) return;
    const previous = current;
    current = next;
    try {
      await ctx.catalog.reload(); // the desktop app re-reads its model list on this
    } catch {
      current = previous; // not applied: the next tick tries again
    }
  });
  return () => clearInterval(timer);
}

// What a model reads: text, plus images where QueueRouter knows it does (opencode refuses images otherwise).
function inputOf(item) {
  return Array.isArray(item?.input) && item.input.includes("image") ? ["text", "image"] : ["text"];
}

// A model's price (USD per million tokens) as opencode 2.x costs; none when QueueRouter doesn't know it.
function costOf(item) {
  const c = item?.cost;
  const ok = (v) => typeof v === "number" && Number.isFinite(v) && v >= 0;
  if (!c || !ok(c.input) || !ok(c.output)) return [];
  const read = ok(c.cache_read) ? c.cache_read : c.input;
  return [{ input: c.input, output: c.output, cache: { read, write: c.input } }];
}

// A model's thinking levels as opencode 2.x variants: each sends its level as reasoning_effort.
function variantsOf(item) {
  const variants = Array.isArray(item?.thinking?.variants) ? item.thinking.variants : [];
  return variants
    .filter((v) => Array.isArray(v) && typeof v[0] === "string" && typeof v[1] === "string")
    .map(([id, effort]) => ({ id, settings: { reasoningEffort: effort } }));
}

// opencode 2.x: QueueRouter as a provider with complete model definitions.
function applyProviders(editor, current) {
  if (!current?.provider) return;
  const info = {
    id: PROVIDER,
    name: "QueueRouter",
    activation: "enabled",
    package: OPENCODE2_PACKAGE,
    settings: { baseURL: `${BASE}/v1`, apiKey: current.key, timeout: false }, // queued requests may wait long
  };
  const models = Object.entries(current.provider.models ?? {}).map(([id, item]) => ({
    id,
    modelID: id,
    providerID: PROVIDER,
    name: item.name ?? id,
    package: OPENCODE2_PACKAGE,
    capabilities: { tools: true, input: inputOf(item), output: ["text"] }, // images where the model reads them
    variants: variantsOf(item), // the thinking levels selector (QueueRouter asks each upstream its own way)
    time: { released: 0 },
    cost: costOf(item), // QueueRouter's price: opencode works out the session cost from it
    status: "active",
    enabled: true,
    limit: { context: item.limit?.context ?? DEFAULT_CONTEXT, output: item.limit?.output ?? DEFAULT_OUTPUT },
  }));
  if (editor.get(PROVIDER)) {
    editor.update(PROVIDER, (provider) => Object.assign(provider, info));
    editor.models.set(PROVIDER, models); // replaces the list: removed aliases disappear
  } else {
    editor.add({ info, models });
  }
}

async function setupProviders(ctx) {
  let current = await fetchModels();
  await ctx.provider.transform((editor) => applyProviders(editor, current));
  if (ctx.session?.hook) {
    // Every model call: which agent asked, and title requests at low priority.
    await ctx.session.hook("model.request", (event) => {
      if (event.model?.providerID !== PROVIDER) return;
      const agent = String(event.agent ?? "")
        .toLowerCase()
        .replace(/[^a-z0-9._-]/g, "-")
        .slice(0, 50);
      event.headers["X-QueueRouter-Tags"] = agent ? `opencode,agent:${agent}` : "opencode";
      if (event.kind === "title") event.headers["X-QueueRouter-Priority"] = "low";
    });
  }
  const timer = startTimer(async () => {
    const next = await fetchModels(current?.tag);
    if (!next || !next.provider || next.tag === current?.tag) return;
    const previous = current;
    current = next;
    try {
      await ctx.provider.reload(); // the desktop app re-reads providers and models on this
    } catch {
      current = previous; // not applied: the next tick tries again
    }
  });
  return () => clearInterval(timer); // opencode calls this when the plugin is unloaded
}

// -- older plugin system (opencode terminal UI) ----------------------------------------------

async function server({ client }) {
  const roots = new Map(); // session id -> root session id (subagents belong to their parent's)
  let seen = null; // ETag of the model list this opencode instance loaded
  let reloadWanted = null; // the ETag of a newer list waiting to be loaded
  const busy = new Set(); // sessions working right now: never reload under them
  announce = async (message) => {
    try {
      await client.app?.log?.({ body: { service: "queuerouter", level: "info", message } });
      await client.tui?.showToast?.({ body: { message, variant: "info" } });
    } catch {}
  };

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

  const timer = startTimer(checkModels);
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
