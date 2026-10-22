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

  // Click-to-copy text: <button data-copy-text="exact value">; delegated so live rows work too.
  document.addEventListener("click", async (event) => {
    const button = event.target.closest("[data-copy-text]");
    if (!button) return;
    try {
      await navigator.clipboard.writeText(button.dataset.copyText);
      button.classList.add("copied");
      setTimeout(() => button.classList.remove("copied"), 1200);
    } catch (_) {
      window.getSelection().selectAllChildren(button);
    }
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
      const embeddings = form.querySelector('[name="embedding_concurrency"]');
      if (embeddings) embeddings.value = option.dataset.embeddings || "";
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
  const usage = document.getElementById("usage-live");
  if (usage) liveUsage(usage);
  const playground = document.getElementById("playground");
  if (playground) playgroundPage(playground);
});

function playgroundPage(root) {
  const $ = (id) => document.getElementById(id);
  const log = $("pg-log");
  const meta = $("pg-meta");
  let history = [];

  const bubble = (role, text) => {
    const div = document.createElement("div");
    div.className = "bubble " + role;
    div.textContent = text;
    log.append(div);
    div.scrollIntoView({ block: "end" });
    return div;
  };

  $("pg-clear").addEventListener("click", () => { history = []; log.replaceChildren(); meta.textContent = ""; });
  $("pg-input").addEventListener("keydown", (e) => {
    if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) { e.preventDefault(); $("pg-form").requestSubmit(); }
  });

  $("pg-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const text = $("pg-input").value.trim();
    if (!text) return;
    $("pg-input").value = "";
    $("pg-send").disabled = true;
    history.push({ role: "user", content: text });
    bubble("user", text);
    const out = bubble("assistant", "…");
    const stream = $("pg-stream").checked;
    const system = $("pg-system").value.trim();
    const payload = {
      client: $("pg-client").value,
      model: $("pg-model").value.trim(),
      messages: (system ? [{ role: "system", content: system }] : []).concat(history),
      stream,
      max_tokens: Number($("pg-max").value) || undefined,
    };
    if (stream) payload.stream_options = { include_usage: true };
    const started = performance.now();
    let answer = "";
    let usage = null;
    let failed = null;
    try {
      const response = await fetch("/admin/playground/send", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json", "X-CSRF-Token": root.dataset.csrf },
        body: JSON.stringify(payload),
      });
      const position = response.headers.get("x-queuerouter-position");
      meta.textContent = position && position !== "0" ? `queued at position ${position}…` : "running…";
      if (!stream || !response.body || !(response.headers.get("content-type") || "").includes("event-stream")) {
        const data = await response.json();
        if (data.error) failed = data.error.message || JSON.stringify(data.error);
        else {
          answer = data.choices?.[0]?.message?.content ?? "";
          usage = data.usage;
        }
        const model = response.headers.get("x-queuerouter-model");
        if (model) meta.dataset.model = model;
      } else {
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split("\n");
          buffer = lines.pop();
          for (const line of lines) {
            if (line.startsWith(": ")) { meta.textContent = line.slice(2); continue; }
            if (!line.startsWith("data:")) continue;
            const data = line.slice(5).trim();
            if (data === "[DONE]") continue;
            try {
              const chunk = JSON.parse(data);
              if (chunk.error) { failed = chunk.error.message || JSON.stringify(chunk.error); continue; }
              const delta = chunk.choices?.[0]?.delta?.content;
              if (delta) { answer += delta; out.textContent = answer; out.scrollIntoView({ block: "end" }); }
              if (chunk.usage) usage = chunk.usage;
            } catch (_) { /* partial or non-JSON line */ }
          }
        }
      }
    } catch (error) {
      failed = String(error);
    }
    const seconds = ((performance.now() - started) / 1000).toFixed(1);
    if (failed) {
      out.textContent = "Error: " + failed;
      out.classList.add("error");
      history.pop();
    } else {
      out.textContent = answer || "(empty reply)";
      history.push({ role: "assistant", content: answer });
    }
    const tokens = usage ? ` · ${usage.prompt_tokens ?? "?"} in / ${usage.completion_tokens ?? "?"} out tokens` : "";
    const model = meta.dataset.model ? ` · model ${meta.dataset.model}` : "";
    meta.textContent = `${seconds} s${tokens}${model}`;
    delete meta.dataset.model;
    $("pg-send").disabled = false;
    $("pg-input").focus();
  });
}

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
  // App User-Agent, plus what the provider got when a rule changed it.
  const uaTitle = (r) => [r.user_agent, r.sent_user_agent && `sent: ${r.sent_user_agent}`].filter(Boolean).join("\n");
  const withTitle = (td, text) => {
    if (text) td.title = `User-Agent: ${text}`; // which app made the request (and what was sent on)
    return td;
  };
  const fill = (tbody, rows, empty = "Nothing yet.") => {
    tbody.replaceChildren(...rows);
    if (!rows.length) {
      const tr = document.createElement("tr");
      const td = cell(empty);
      td.colSpan = 9;
      td.className = "muted";
      tr.append(td);
      tbody.append(tr);
    }
  };

  // Running / queued lines from the server's samples ([ts, active, queued] every 5 s, 30 min).
  function drawHistory(svg, points, capacity) {
    if (!svg) return;
    const W = 720, H = 160, L = 30, T = 8, B = 18, span = 1800;
    const now = Date.now() / 1000;
    const top = Math.max(2, capacity || 0, ...points.map((p) => Math.max(p[1], p[2])));
    const x = (ts) => L + ((W - L) * (ts - (now - span))) / span;
    const y = (v) => T + (H - T - B) * (1 - v / top);
    const ns = "http://www.w3.org/2000/svg";
    const el = (name, attrs, text) => {
      const node = document.createElementNS(ns, name);
      for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
      if (text !== undefined) node.textContent = text;
      return node;
    };
    const nodes = [];
    for (const v of [0, Math.round(top / 2), top]) {
      nodes.push(el("line", { class: "grid", x1: L, x2: W, y1: y(v), y2: y(v) }));
      nodes.push(el("text", { class: "axis", x: L - 4, y: y(v) + 4, "text-anchor": "end" }, String(v)));
    }
    for (const [label, ago] of [["-30m", span], ["-15m", span / 2], ["now", 0]]) {
      const anchor = ago === span ? "start" : ago === 0 ? "end" : "middle";
      nodes.push(el("text", { class: "axis", x: x(now - ago), y: H - 4, "text-anchor": anchor }, label));
    }
    const visible = points.filter((p) => p[0] >= now - span);
    for (const [index, css] of [[1, "s1"], [2, "s3"]]) {
      if (!visible.length) break;
      const path = visible.map((p) => `${x(p[0]).toFixed(1)},${y(p[index]).toFixed(1)}`).join(" ");
      nodes.push(el("polyline", { class: `line ${css}`, points: path }));
    }
    svg.replaceChildren(...nodes);
  }

  // Polling: only when the live stream isn't available.
  async function refresh() {
    if (document.hidden) return;
    try {
      const response = await fetch(endpoint, { credentials: "same-origin", headers: { Accept: "application/json" } });
      if (response.redirected || response.status === 401 || response.status === 403) {
        window.location.reload();
        return;
      }
      render(await response.json(), "polling");
    } catch (_) {
      status.textContent = "offline — retrying";
    }
  }

  // Chart points kept here: the stream sends the whole history once, then only the newest points.
  let points = [];
  function mergeHistory(incoming) {
    if (incoming.length > 5) points = incoming.slice();
    else {
      const last = points.length ? points[points.length - 1][0] : 0;
      points.push(...incoming.filter((p) => p[0] > last));
    }
    const cutoff = Date.now() / 1000 - 1800;
    while (points.length && points[0][0] < cutoff) points.shift();
  }

  // The "Now" panel: elapsed times tick here between updates.
  const fmtMs = (ms) => (ms === null || ms === undefined ? "—" : ms < 10000 ? `${(ms / 1000).toFixed(1)} s` : `${Math.round(ms / 1000)} s`);
  function renderNow(rows) {
    const tbody = document.getElementById("now-body");
    if (!tbody) return;
    const now = performance.now();
    fill(tbody, rows.map((r) => {
      const tr = document.createElement("tr");
      const elapsed = cell(fmtMs(r.elapsed_ms), "Elapsed");
      elapsed.dataset.start = String(now - r.elapsed_ms);
      elapsed.className = "mono";
      const stage = cell(r.stage + (r.retries ? ` · ${r.retries} retries` : ""), "Stage");
      stage.className = r.stage === "queued" ? "muted" : "";
      if (r.error) stage.title = r.error;
      tr.append(
        withTitle(cell(r.client, "Client"), uaTitle(r)), cell(r.model, "Model"), cell(r.upstream, "Upstream"), stage, elapsed,
        cell(r.ttft_ms === null ? (r.stage === "running" ? "waiting…" : "—") : fmtMs(r.ttft_ms), "First byte"),
      );
      return tr;
    }), "Nothing running.");
    set("inflight_count", rows.length);
  }
  function tick() {
    const now = performance.now();
    root.querySelectorAll("#now-body [data-start]").forEach((td) => {
      td.textContent = fmtMs(now - Number(td.dataset.start));
    });
  }

  function render(s, how) {
    status.textContent = `${how} · ${new Date().toLocaleTimeString()}`;
    renderNow(s.inflight || []);
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
    mergeHistory(s.history || []);
    drawHistory(document.getElementById("live-chart"), points, s.max_concurrency);
    const badge = document.getElementById("nav-alerts-count");
    if (badge) {
      badge.textContent = s.alerts_unread || 0;
      badge.hidden = !s.alerts_unread;
    }

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
        cell(new Date(r.ts * 1000).toLocaleTimeString(), "Time"), withTitle(cell(r.client, "Client"), uaTitle(r)),
        cell(r.priority, "Prio"), cell(r.model, "Model"), cell(r.outcome, "Outcome"),
        cell(r.queue_ms, "Queue ms"), cell(r.ttft_ms, "First byte ms"), cell(r.upstream_ms, "Upstream ms"),
        cell(tokens || null, "Tokens"),
      );
      return tr;
    });
    fill(document.getElementById("recent-body"), recentRows);
  }

  // Live stream (server-sent events); polling every 2 s if it can't connect.
  let polling = null;
  const startPolling = () => {
    if (polling) return;
    refresh();
    polling = setInterval(refresh, 2000);
    document.addEventListener("visibilitychange", refresh);
  };
  if (window.EventSource && root.dataset.stream) {
    const source = new EventSource(root.dataset.stream);
    source.onmessage = (event) => {
      if (polling) {
        clearInterval(polling);
        polling = null;
      }
      render(JSON.parse(event.data), "live");
    };
    source.onerror = () => {
      status.textContent = "reconnecting…"; // the browser retries on its own
      setTimeout(() => {
        if (source.readyState !== EventSource.OPEN) startPolling();
      }, 5000);
    };
  } else {
    startPolling();
  }
  setInterval(tick, 250);
}

// Usage page: when the live stream says new usage was written, reload this page's numbers,
// charts and table in place (same filters), without a full page reload.
function liveUsage(root) {
  const status = document.getElementById("usage-status");
  if (!window.EventSource || !root.dataset.stream) return;
  let version = null;
  let timer = null;
  const reload = async () => {
    timer = null;
    try {
      const response = await fetch(window.location.href, { credentials: "same-origin" });
      if (!response.ok || response.redirected) return;
      const page = new DOMParser().parseFromString(await response.text(), "text/html");
      const fresh = page.getElementById("usage-live");
      if (fresh) {
        root.replaceChildren(...fresh.childNodes);
        if (status) status.textContent = "Updated " + new Date().toLocaleTimeString() + " · updates live as requests finish.";
      }
    } catch (_) {
      /* next update tries again */
    }
  };
  const source = new EventSource(root.dataset.stream);
  source.onmessage = (event) => {
    const next = JSON.parse(event.data).usage_version;
    if (version !== null && next !== version && !timer) timer = setTimeout(reload, 1000); // batch bursts
    version = next;
  };
  source.onerror = () => {
    if (status) status.textContent = "Live updates paused — reconnecting…";
  };
}
