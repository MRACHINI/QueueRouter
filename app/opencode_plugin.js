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

const BASE = "__QUEUEROUTER_BASE__"; // filled in by QueueRouter when it serves this file
const PROVIDER = "queuerouter";
const KEY_ENV = "QUEUEROUTER_KEY";
const LOW_PRIORITY_AGENTS = new Set(["title", "summary"]);

export const QueueRouter = async ({ client }) => {
  const roots = new Map(); // session id -> root session id (subagents belong to their parent's)

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
        const generated = await res.json();
        // {env:...} placeholders are already resolved when this hook runs, so set the key itself.
        const ours = generated.provider?.[PROVIDER];
        if (ours) ours.options = { ...(ours.options ?? {}), apiKey: key };
        config.provider = { ...(config.provider ?? {}), ...generated.provider };
      } catch {
        // QueueRouter unreachable: opencode starts without the provider rather than failing.
      }
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
