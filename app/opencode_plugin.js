// QueueRouter plugin for opencode, served by QueueRouter at /opencode/queuerouter.js.
// Install: save it in opencode's global plugin folder (~/.config/opencode/plugin/) and set
// QUEUEROUTER_KEY to a client key (or log in with `opencode auth login <QueueRouter URL>`).
//
// - Loads the QueueRouter provider (your models and aliases with their context limits) on every
//   start, unless a config or the login already provides it.
// - Tags each request with opencode's session and task, so QueueRouter finishes a conversation it
//   has started (subagents included) before new ones, and run budgets / loop detection apply per
//   task.
// - Sends title and summary requests at low priority so they never hold up the main model.
// - Keeps the model list current while opencode runs: checks QueueRouter every minute
//   (QUEUEROUTER_REFRESH_S, 0 = off) and, when models or aliases changed and no session is busy,
//   reloads opencode's providers (the model picker updates by itself; nothing is restarted).

const BASE = "__QUEUEROUTER_BASE__"; // filled in by QueueRouter when it serves this file
const PROVIDER = "queuerouter";
const KEY_ENV = "QUEUEROUTER_KEY";
const LOW_PRIORITY_AGENTS = new Set(["title", "summary"]);
const REFRESH_S = Number(process.env.QUEUEROUTER_REFRESH_S ?? 60);

export const QueueRouter = async ({ client }) => {
  const roots = new Map(); // session id -> root session id (subagents belong to their parent's)

  // -- live model list ---------------------------------------------------------------------
  let seen = null; // ETag of the model list this opencode instance loaded
  let reloadWanted = null; // the ETag of a newer list waiting to be loaded
  const busy = new Set(); // sessions working right now: never reload under them

  async function checkModels() {
    const key = process.env[KEY_ENV];
    if (!key) return;
    try {
      const res = await fetch(`${BASE}/v1/opencode-config`, {
        headers: { Authorization: `Bearer ${key}`, ...(seen ? { "If-None-Match": seen } : {}) },
        signal: AbortSignal.timeout(10000),
      });
      const tag = res.headers.get("etag");
      if (res.status === 304 || !res.ok || !tag) return;
      if (seen === null) seen = tag; // first look: what this instance started with
      else if (tag !== seen) {
        reloadWanted = tag;
        await reloadIfIdle();
      }
    } catch {
      // QueueRouter unreachable: try again next time
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
      await client.instance.dispose(); // opencode rebuilds its providers; the TUI re-reads them
    } catch {}
  }

  // One timer per opencode process, even if an old copy of this plugin isn't told to stop.
  clearInterval(globalThis.__queuerouterRefresh);
  const timer = REFRESH_S > 0 ? setInterval(checkModels, REFRESH_S * 1000) : undefined;
  globalThis.__queuerouterRefresh = timer;
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
      const key = process.env[KEY_ENV];
      if (!key || config.provider?.[PROVIDER]) return;
      try {
        const res = await fetch(`${BASE}/v1/opencode-config`, {
          headers: { Authorization: `Bearer ${key}` },
          signal: AbortSignal.timeout(10000),
        });
        if (!res.ok) return;
        seen = res.headers.get("etag") ?? seen; // the list this instance starts with
        const generated = await res.json();
        // {env:...} placeholders are already resolved when this hook runs, so set the key itself.
        const ours = generated.provider?.[PROVIDER];
        if (ours) ours.options = { ...(ours.options ?? {}), apiKey: key };
        config.provider = { ...(config.provider ?? {}), ...generated.provider };
      } catch {
        // QueueRouter unreachable: opencode starts without the provider rather than failing.
      }
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
};
