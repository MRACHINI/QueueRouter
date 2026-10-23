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
  localTimes(document);

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
  document.querySelectorAll("[data-model-picker]").forEach(modelPicker);
  const choicesData = document.getElementById("model-choices");
  if (choicesData) {
    const choices = JSON.parse(choicesData.textContent || "[]");
    document.querySelectorAll("[data-picker]").forEach((root) => chipPicker(root, choices));
  }
  const liveRequestsRoot = document.getElementById("requests-live");
  if (liveRequestsRoot) liveRequests(liveRequestsRoot);
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
      td.colSpan = tbody.closest("table")?.tHead?.rows[0]?.cells.length || 9;
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
        cell(totals.tokens.prompt.toLocaleString(), "In tokens"), cell(totals.tokens.completion.toLocaleString(), "Out tokens"),
      );
      return tr;
    });
    fill(document.getElementById("clients-body"), clientRows);

    const recentRows = (s.recent || []).slice().reverse().map((r) => {
      const tr = document.createElement("tr");
      const count = (n) => (n === null || n === undefined ? null : Number(n).toLocaleString());
      const input = cell(count(r.prompt_tokens), "In tokens");
      if (r.cached_tokens) input.title = `${count(r.cached_tokens)} of them cached`;
      tr.append(
        cell(new Date(r.ts * 1000).toLocaleTimeString(), "Time"), withTitle(cell(r.client, "Client"), uaTitle(r)),
        cell(r.priority, "Prio"), cell(r.model, "Model"), cell(r.outcome, "Outcome"),
        cell(r.queue_ms, "Queue ms"), cell(r.ttft_ms, "First byte ms"), cell(r.upstream_ms, "Upstream ms"),
        input, cell(count(r.completion_tokens), "Out tokens"),
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

// Local times for <time data-ts="unix seconds">.
function localTimes(scope) {
  scope.querySelectorAll("time[data-ts]").forEach((el) => {
    const date = new Date(Number(el.dataset.ts) * 1000);
    el.textContent = date.toLocaleString();
    el.title = date.toISOString();
  });
}

// Requests page: a live "Now" table (queued and running requests, same columns as the list below,
// filtered the same way), and the finished list refreshed in place as requests finish.
function liveRequests(root) {
  if (!window.EventSource || !root.dataset.stream) return;
  const body = document.getElementById("now-requests");
  const count = document.getElementById("now-count");
  const filters = new URLSearchParams(window.location.search);
  const want = (key) => filters.get(key) || "";
  const matches = (r) =>
    !want("outcome") &&
    (!want("client") || r.client === want("client")) &&
    (!want("model") || r.requested_model === want("model") || r.target_model === want("model")) &&
    (!want("upstream") || r.upstream === want("upstream")) &&
    (!want("tag") || (r.tags || "").split(",").includes(want("tag")));
  const el = (tag, className, text) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  };
  const td = (label, ...children) => {
    const cell = el("td");
    cell.dataset.label = label;
    cell.append(...children);
    return cell;
  };
  const copyLine = (text, prefix, title) => {
    const line = el("div", "small muted mono");
    const button = el("button", "copy-text", prefix + text);
    button.type = "button";
    button.dataset.copyText = text;
    button.title = title;
    line.append(button);
    return line;
  };
  const seconds = (ms) => (ms === null || ms === undefined ? "—" : String(Math.round(ms)));
  let started = 0; // performance.now() when the last update arrived
  const openIds = new Set();
  let rows = [];

  function render() {
    const now = performance.now();
    const shown = rows.filter(matches);
    if (count) count.textContent = String(shown.length);
    if (!shown.length) {
      const empty = el("tr");
      const cell = el("td", "muted", "Nothing queued or running.");
      cell.colSpan = 11;
      empty.append(cell);
      body.replaceChildren(empty);
      return;
    }
    body.replaceChildren(...shown.map((r) => {
      const tr = el("tr");
      const time = el("time");
      time.dataset.ts = String(r.ts);
      const client = td("Client", r.client || "—", " ", el("span", `badge ${r.priority || ""}`, r.priority || ""));
      if (r.user_agent) client.append(copyLine(r.user_agent, "", "App's User-Agent — click to copy"));
      if (r.sent_user_agent) client.append(copyLine(r.sent_user_agent, "→ sent: ", "User-Agent sent to the provider — click to copy"));
      if (r.tags) client.append(el("div", "small muted", r.tags.split(",").map((t) => `#${t}`).join(" ")));
      let ids = {};
      try {
        ids = JSON.parse(r.client_headers || "{}");
      } catch (_) {
        ids = {};
      }
      const names = Object.keys(ids);
      if (names.length) {
        const details = el("details", "small ids");
        details.open = openIds.has(r.id); // stays open across live updates
        details.addEventListener("toggle", () => (details.open ? openIds.add(r.id) : openIds.delete(r.id)));
        details.append(el("summary", "muted", `${names.length} identifiers`));
        for (const name of names) {
          const line = el("div", "mono");
          const button = el("button", "copy-text inline", ids[name]);
          button.type = "button";
          button.dataset.copyText = ids[name];
          line.append(el("span", "muted", `${name}: `), button);
          details.append(line);
        }
        client.append(details);
      }
      const model = td("Model", r.requested_model || r.model || "—");
      model.classList.add("wrap-any");
      if (r.target_model) model.append(" ", el("span", "muted small", `→ ${r.target_model}`));
      for (const [key, label] of [["compaction", "compacted"], ["council", "council"]]) {
        if (!r[key]) continue;
        const line = el("div", "small");
        line.append(el("span", "badge", `${label}: ${r[key]}`));
        model.append(line);
      }
      if (r.role) { // made by QueueRouter for another request (council member or judge, summary)
        const line = el("div", "small");
        line.append(el("span", "badge", r.role));
        if (r.parent) line.append(" ", el("span", "muted mono", `for ${r.parent}`));
        model.append(line);
      }
      // waiting for its conversation's summary, or for its council's members and judge
      const waiting = r.stage === "compacting" || r.stage === "council";
      const queued = r.stage === "queued" || waiting;
      const status = waiting ? `${r.stage}…` : queued ? `queued${r.position ? ` #${r.position}` : ""}` : "running";
      const outcome = td("Outcome", el("strong", queued ? "muted" : "", status));
      if (r.retries) outcome.append(el("span", "muted small", ` · ${r.retries} retries`));
      if (r.error) {
        outcome.title = r.error;
        outcome.append(el("div", "small muted wrap-any", r.error));
      }
      const elapsed = r.elapsed_ms + (now - started);
      const queueMs = td("Queue ms", seconds(queued ? elapsed : r.queue_ms));
      const upstreamMs = td("Upstream ms", queued ? "—" : seconds(elapsed - (r.queue_ms || 0)));
      if (queued) queueMs.dataset.tick = String(r.elapsed_ms);
      else upstreamMs.dataset.tick = String(r.elapsed_ms - (r.queue_ms || 0));
      const ttft = td("First byte ms", r.ttft_ms !== null && r.ttft_ms !== undefined ? seconds(r.ttft_ms) : queued ? "—" : "waiting…");
      const input = td("In tokens", r.estimate ? `~${Number(r.estimate).toLocaleString()}` : "—");
      if (r.estimate) input.title = "QueueRouter's estimate; the provider's count arrives when it finishes";
      tr.append(
        td("Time", time, el("div", "small muted mono", r.id || "")), client, model, td("Upstream", r.upstream || "—"), outcome, queueMs, ttft, upstreamMs,
        input, td("Out tokens", "—"), td("Finish", r.stream ? "streaming…" : "—"),
      );
      return tr;
    }));
    localTimes(body);
  }

  // Elapsed times tick between updates.
  setInterval(() => {
    const extra = performance.now() - started;
    body.querySelectorAll("[data-tick]").forEach((cell) => {
      cell.textContent = String(Math.round(Number(cell.dataset.tick) + extra));
    });
  }, 500);

  // The finished list: reloaded in place (same filters) when new requests have been written.
  let version = null;
  let timer = null;
  const reload = async () => {
    timer = null;
    try {
      const response = await fetch(window.location.href, { credentials: "same-origin" });
      if (!response.ok || response.redirected) return;
      const page = new DOMParser().parseFromString(await response.text(), "text/html");
      const fresh = page.getElementById("finished-requests");
      const current = document.getElementById("finished-requests");
      if (fresh && current) {
        current.replaceChildren(...fresh.childNodes);
        localTimes(current);
      }
    } catch (_) {
      /* next update tries again */
    }
  };

  const source = new EventSource(root.dataset.stream);
  source.onmessage = (event) => {
    const state = JSON.parse(event.data);
    rows = state.inflight || [];
    started = performance.now();
    render();
    if (version !== null && state.usage_version !== version && !timer) timer = setTimeout(reload, 1000);
    version = state.usage_version;
  };
}

// Upstream > choose models: filter the list, tick / untick what's shown, count what's offered.
function modelPicker(form) {
  const items = [...form.querySelectorAll("[data-picker-item]")];
  const count = form.querySelector("[data-picker-count]");
  const update = () => {
    if (count) count.textContent = String(form.querySelectorAll('[data-picker-item] input:checked').length);
  };
  const shown = () => items.filter((item) => !item.hidden);
  form.querySelector("[data-picker-search]")?.addEventListener("input", (event) => {
    const words = event.target.value.toLowerCase().split(/\s+/).filter(Boolean);
    items.forEach((item) => {
      item.hidden = !words.every((word) => item.dataset.pickerItem.includes(word));
    });
  });
  const setAll = (value) => () => {
    shown().forEach((item) => {
      item.querySelector("input").checked = value;
    });
    update();
  };
  form.querySelector("[data-picker-all]")?.addEventListener("click", setAll(true));
  form.querySelector("[data-picker-none]")?.addEventListener("click", setAll(false));
  form.addEventListener("change", update);
}

// Alias editor: search-and-pick chips for model fields. The hidden input keeps the value as the server
// reads it ("a, b, c"; one name for single pickers).
function chipPicker(root, choices) {
  const single = root.dataset.picker === "single";
  const ordered = root.hasAttribute("data-ordered");
  const hidden = root.querySelector('input[type="hidden"]');
  const chips = root.querySelector(".picker-chips");
  const search = root.querySelector(".picker-search");
  const menu = root.querySelector(".picker-menu");
  const byId = new Map(choices.map((c) => [c.id, c]));
  let values = hidden.value.split(",").map((v) => v.trim()).filter(Boolean);
  let shown = [];
  let active = 0;
  let dragged = null;

  const describe = (c) => {
    if (!c) return "not listed by any upstream";
    if (c.alias) return "alias";
    const size = c.context ? ` · ${c.context >= 1000 ? Math.round(c.context / 1000) + "k" : c.context}` : "";
    return `${c.upstreams.join(", ")}${size}${c.hidden ? " · hidden" : ""}`;
  };
  const save = () => {
    hidden.value = values.join(", ");
    renderChips();
  };
  const add = (id) => {
    id = id.trim();
    if (!id) return;
    values = single ? [id] : values.includes(id) ? values : [...values, id];
    search.value = "";
    save();
    close();
  };
  function renderChips() {
    chips.replaceChildren(...values.map((id, i) => {
      const chip = document.createElement("span");
      chip.className = "chip";
      chip.title = describe(byId.get(id));
      if (!byId.has(id)) chip.classList.add("unknown");
      chip.textContent = (ordered && values.length > 1 ? `${i + 1}. ` : "") + id;
      const remove = document.createElement("button");
      remove.type = "button";
      remove.textContent = "×";
      remove.setAttribute("aria-label", `Remove ${id}`);
      remove.addEventListener("click", () => {
        values = values.filter((v) => v !== id);
        save();
      });
      chip.append(remove);
      if (ordered && !single) {
        chip.draggable = true;
        chip.addEventListener("dragstart", () => { dragged = i; });
        chip.addEventListener("dragover", (event) => event.preventDefault());
        chip.addEventListener("drop", (event) => {
          event.preventDefault();
          if (dragged === null || dragged === i) return;
          const moved = values.splice(dragged, 1)[0];
          values.splice(i, 0, moved);
          dragged = null;
          save();
        });
      }
      return chip;
    }));
    search.placeholder = single && values.length ? "Change…" : search.getAttribute("aria-label");
  }
  function open() {
    const words = search.value.toLowerCase().split(/\s+/).filter(Boolean);
    shown = choices
      .filter((c) => single || !values.includes(c.id))
      .filter((c) => words.every((w) => c.id.toLowerCase().includes(w) || describe(c).toLowerCase().includes(w)))
      .slice(0, 60);
    active = 0;
    menu.replaceChildren(...shown.map((c, i) => {
      const item = document.createElement("li");
      item.setAttribute("role", "option");
      item.className = i === active ? "active" : "";
      const name = document.createElement("span");
      name.className = "mono";
      name.textContent = c.id;
      const meta = document.createElement("span");
      meta.className = "muted small";
      meta.textContent = ` ${describe(c)}`;
      item.append(name, meta);
      item.addEventListener("mousedown", (event) => {
        event.preventDefault(); // keep focus in the search box
        add(c.id);
      });
      return item;
    }));
    if (!shown.length && search.value.trim()) {
      const item = document.createElement("li");
      item.className = "muted small";
      item.textContent = `Enter: add "${search.value.trim()}" (not listed by any upstream)`;
      menu.append(item);
    }
    menu.hidden = !menu.children.length;
  }
  function close() {
    menu.hidden = true;
  }
  const highlight = () => [...menu.children].forEach((li, i) => li.classList.toggle("active", i === active));

  search.addEventListener("focus", open);
  search.addEventListener("input", open);
  search.addEventListener("blur", () => setTimeout(close, 100));
  search.addEventListener("keydown", (event) => {
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      if (menu.hidden) open();
      active = Math.max(0, Math.min(shown.length - 1, active + (event.key === "ArrowDown" ? 1 : -1)));
      highlight();
      menu.children[active]?.scrollIntoView({ block: "nearest" });
    } else if (event.key === "Enter") {
      event.preventDefault(); // never submit the form from here
      if (!menu.hidden && shown[active]) add(shown[active].id);
      else add(search.value);
    } else if (event.key === "Escape") {
      close();
    } else if (event.key === "Backspace" && !search.value && values.length) {
      values = values.slice(0, -1);
      save();
    }
  });
  // a name typed but not added (no Enter) still counts when the form is sent
  root.closest("form")?.addEventListener("submit", () => {
    if (search.value.trim()) add(search.value);
  });
  renderChips();
}
