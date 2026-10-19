// QueueRouter admin — theme, confirmations, copy buttons, live dashboard.
// Loaded in <head> (not deferred) so the saved theme applies before first paint.
"use strict";

(function theme() {
  const order = ["auto", "light", "dark"];
  let saved = "auto";
  try { saved = localStorage.getItem("qr-theme") || "auto"; } catch (_) { /* storage blocked */ }
  const apply = (mode) => {
    if (mode === "auto") delete document.documentElement.dataset.theme;
    else document.documentElement.dataset.theme = mode;
  };
  apply(saved);
  document.addEventListener("DOMContentLoaded", () => {
    const button = document.getElementById("theme-toggle");
    if (!button) return;
    const label = () => { button.title = "Theme: " + saved; };
    label();
    button.addEventListener("click", () => {
      saved = order[(order.indexOf(saved) + 1) % order.length];
      try { localStorage.setItem("qr-theme", saved); } catch (_) { /* ignore */ }
      apply(saved);
      label();
    });
  });
})();

document.addEventListener("DOMContentLoaded", () => {
  // Confirm destructive forms.
  document.querySelectorAll("form[data-confirm]").forEach((form) => {
    form.addEventListener("submit", (event) => {
      if (!window.confirm(form.dataset.confirm)) event.preventDefault();
    });
  });

  // Copy-to-clipboard buttons: <button data-copy="#element-id">.
  document.querySelectorAll("[data-copy]").forEach((button) => {
    button.addEventListener("click", async () => {
      const target = document.querySelector(button.dataset.copy);
      if (!target) return;
      try {
        await navigator.clipboard.writeText(target.textContent.trim());
        const original = button.textContent;
        button.textContent = "Copied";
        setTimeout(() => { button.textContent = original; }, 1500);
      } catch (_) {
        window.getSelection().selectAllChildren(target);
      }
    });
  });

  // Local times for <time data-ts="unix seconds">.
  document.querySelectorAll("time[data-ts]").forEach((el) => {
    const date = new Date(Number(el.dataset.ts) * 1000);
    el.textContent = date.toLocaleString();
    el.title = date.toISOString();
  });

  // Upstream forms: a preset fills the base URL; an ArliAI plan fills parallel + context cap.
  document.querySelectorAll("[data-preset-select]").forEach((select) => {
    const form = select.closest("form");
    const url = form.querySelector("[data-base-url-input]");
    const note = form.querySelector("[data-preset-note]");
    const sync = (overwrite) => {
      const option = select.selectedOptions[0];
      if (url && (overwrite || !url.value)) url.value = option.dataset.baseUrl || "";
      if (note) note.textContent = option.dataset.note || "";
      const parallel = form.querySelector('[name="max_concurrency"]');
      if (parallel && !parallel.value) parallel.placeholder = option.dataset.concurrency || "";
    };
    select.addEventListener("change", () => sync(true));
    sync(false);
  });
  document.querySelectorAll("[data-plan-select]").forEach((select) => {
    select.addEventListener("change", () => {
      const option = select.selectedOptions[0];
      const form = select.closest("form");
      if (!option.dataset.concurrency) return;
      form.querySelector('[name="max_concurrency"]').value = option.dataset.concurrency;
      form.querySelector('[name="context_cap"]').value = option.dataset.context;
      const price = form.querySelector('[name="monthly_price"]');
      if (price) price.value = option.dataset.price;
    });
  });
  document.querySelectorAll("[data-pool-select]").forEach((select) => {
    const form = select.closest("form");
    const sync = () => form.querySelectorAll("[data-new-pool]").forEach((el) => {
      el.hidden = select.value !== "new";
    });
    select.addEventListener("change", sync);
    sync();
  });

  const dashboard = document.getElementById("dashboard");
  if (dashboard) liveDashboard(dashboard);
});

function liveDashboard(root) {
  const endpoint = root.dataset.endpoint;
  const status = document.getElementById("live-status");
  const set = (key, value) => {
    root.querySelectorAll(`[data-k="${key}"]`).forEach((el) => {
      if (el.tagName === "METER") el.value = value;
      else el.textContent = value;
    });
  };
  const cell = (text, label) => {
    const td = document.createElement("td");
    td.textContent = text === null || text === undefined ? "—" : String(text);
    if (label) td.dataset.label = label;
    return td;
  };
  const fill = (tbody, rows) => {
    tbody.replaceChildren(...rows);
    if (!rows.length) {
      const tr = document.createElement("tr");
      const td = cell("Nothing yet.");
      td.colSpan = 8;
      td.className = "muted";
      tr.append(td);
      tbody.append(tr);
    }
  };

  async function refresh() {
    if (document.hidden) return;
    let s;
    try {
      const response = await fetch(endpoint, { credentials: "same-origin", headers: { Accept: "application/json" } });
      if (response.redirected || response.status === 401 || response.status === 403) {
        window.location.reload();
        return;
      }
      s = await response.json();
      status.textContent = "live · " + new Date().toLocaleTimeString();
    } catch (_) {
      status.textContent = "offline — retrying";
      return;
    }
    set("active", s.active);
    set("max_concurrency", s.max_concurrency);
    const meter = root.querySelector('[data-k="slots"]');
    if (meter) { meter.max = s.max_concurrency; meter.value = s.active; }
    set("queued", s.queued);
    set("lane_high", s.queued_by_lane.high);
    set("lane_normal", s.queued_by_lane.normal);
    set("lane_low", s.queued_by_lane.low);
    set("oldest_wait_s", s.oldest_wait_s);
    set("wait_p95", s.wait_ms.p95);
    set("done", s.outcomes.done || 0);
    set("errors", s.outcomes.error || 0);
    set("cancelled", s.outcomes.cancelled || 0);
    set("retries", s.retries);
    set("tokens_prompt", s.tokens.prompt);
    set("tokens_completion", s.tokens.completion);
    set("tokens_total", s.tokens.prompt + s.tokens.completion);
    set("uptime", s.uptime_s);
    set("upstream", s.upstream || "none");

    const upstreamRows = (s.upstreams || []).map((u) => {
      const tr = document.createElement("tr");
      const status = !u.usable ? "disabled/paused" : u.state === "closed" ? "healthy" : u.state;
      const pool = (s.pools || {})[u.pool];
      tr.append(
        cell(u.name, "Upstream"),
        cell(pool ? `${u.pool} (${pool.active}/${pool.capacity})` : u.pool, "Pool"),
        cell(status + (u.last_error && u.state !== "closed" ? ` — ${u.last_error}` : ""), "Status"),
        cell(u.active, "Active"),
        cell(u.catalog_models === null ? "unknown" : u.catalog_models, "Models"),
      );
      return tr;
    });
    fill(document.getElementById("upstreams-body"), upstreamRows);

    const names = new Set([...Object.keys(s.clients || {}), ...Object.keys(s.per_client || {})]);
    const clientRows = [...names].sort().map((name) => {
      const live = (s.clients || {})[name] || { active: 0, queued: 0 };
      const totals = (s.per_client || {})[name] || { outcomes: {}, tokens: { prompt: 0, completion: 0 } };
      const tr = document.createElement("tr");
      tr.append(
        cell(name, "Client"), cell(live.active, "Active"), cell(live.queued, "Queued"),
        cell(totals.outcomes.done || 0, "Done"), cell(totals.outcomes.error || 0, "Errors"),
        cell(totals.tokens.prompt + totals.tokens.completion, "Tokens"),
      );
      return tr;
    });
    fill(document.getElementById("clients-body"), clientRows);

    const recentRows = (s.recent || []).slice().reverse().map((r) => {
      const tr = document.createElement("tr");
      const tokens = (r.prompt_tokens || 0) + (r.completion_tokens || 0);
      tr.append(
        cell(new Date(r.ts * 1000).toLocaleTimeString(), "Time"), cell(r.client, "Client"),
        cell(r.priority, "Prio"), cell(r.model, "Model"), cell(r.outcome, "Outcome"),
        cell(r.queue_ms, "Queue ms"), cell(r.upstream_ms, "Upstream ms"), cell(tokens || null, "Tokens"),
      );
      return tr;
    });
    fill(document.getElementById("recent-body"), recentRows);
  }

  refresh();
  setInterval(refresh, 2000);
  document.addEventListener("visibilitychange", refresh);
}
