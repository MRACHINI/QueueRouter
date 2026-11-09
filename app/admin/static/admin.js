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

  // Client keys: <button data-show-key="url" data-target="#code" data-csrf>; shows the key, then copies it.
  document.addEventListener("click", async (event) => {
    const button = event.target.closest("[data-show-key]");
    if (!button) return;
    const target = document.querySelector(button.dataset.target);
    try {
      const response = await fetch(button.dataset.showKey, {
        method: "POST",
        credentials: "same-origin",
        headers: { "X-CSRF-Token": button.dataset.csrf },
      });
      const data = await response.json();
      if (!response.ok || !data.key) throw new Error(data.error || `HTTP ${response.status}`);
      if (target) target.textContent = data.key;
      // The Connect opencode card, now with this client's key.
      Object.entries(data.install || {}).forEach(([platform, command]) => {
        const block = document.getElementById(`plugin-${platform}`);
        if (block) block.textContent = command;
      });
      const filledFor = document.getElementById("plugin-for");
      if (filledFor) filledFor.textContent = `With the key of ${button.dataset.client}.`;
      button.removeAttribute("data-show-key");
      button.dataset.copyText = data.key;  // the click-to-copy handler above takes the next click
      button.textContent = "Copy";
    } catch (error) {
      button.textContent = "Couldn't show it";
      button.title = String(error.message || error);
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
  document.querySelectorAll("[data-every-model]").forEach((box) => {
    const list = box.closest("fieldset").querySelector("[data-model-list]");
    const sync = () => { list.hidden = box.checked; };
    box.addEventListener("change", sync);
    sync();
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
  const lists = {}; // each picker's choices: the models, or another list the page names (data-choices)
  document.querySelectorAll("[data-picker]").forEach((root) => {
    const id = root.dataset.choices || "model-choices";
    if (!(id in lists)) lists[id] = JSON.parse(document.getElementById(id)?.textContent || "null");
    if (lists[id]) chipPicker(root, lists[id]);
  });
  document.querySelectorAll("[data-kind-form]").forEach(kindForm);
  const liveRequestsRoot = document.getElementById("requests-live");
  if (liveRequestsRoot) liveRequests(liveRequestsRoot);
  const liveEditsRoot = document.getElementById("edits-live");
  if (liveEditsRoot) liveEdits(liveEditsRoot);
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
        withTitle(cell(r.client, "Client"), uaTitle(r)), cell(r.label, "Model"), stage, elapsed,
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
    const drainLeft = document.getElementById("drain-left");
    if (drainLeft) {
      drainLeft.textContent = s.active || s.queued ? `${s.active} running, ${s.queued} queued.` : "Nothing left: safe to redeploy.";
    }
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
      const parts = [];
      if (r.cached_tokens) parts.push(`${count(r.cached_tokens)} of them cached`);
      if (r.cache_write_tokens) parts.push(`${count(r.cache_write_tokens)} written to the cache`);
      if (parts.length) input.title = parts.join(", ");
      tr.append(
        cell(new Date(r.ts * 1000).toLocaleTimeString(), "Time"), withTitle(cell(r.client, "Client"), uaTitle(r)),
        cell(r.priority, "Prio"), cell(r.label, "Model"), cell(r.outcome, "Outcome"),
        cell(r.queue_ms, "Queue ms"), cell(r.ttft_ms, "First byte ms"), cell(r.upstream_ms, "Upstream ms"),
        input, cell(count(r.completion_tokens), "Out tokens"),
      );
      return tr;
    });
    fill(document.getElementById("recent-body"), recentRows);
  }

  // Live stream (server-sent events); polling every 2 s if it can't connect.
  let polling = null;
  let refreshOnShow = false;
  const startPolling = () => {
    if (polling) return;
    refresh();
    polling = setInterval(refresh, 2000);
    if (!refreshOnShow) { // once: the stream can drop and come back many times
      refreshOnShow = true;
      document.addEventListener("visibilitychange", () => { if (polling) refresh(); });
    }
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

// The live connection of the Requests and Usage pages. A browser's EventSource only retries by itself after a
// network error: an error page (a 502 while QueueRouter restarts) ends it for good, and a connection that died while
// the computer slept may never say so. So the server sends something every few seconds and silence reconnects,
// waiting longer each time; a hidden tab holds no connection; and every (re)connection calls resync() to fetch what
// was missed.
function liveStream(url, status, { message, resync }) {
  const SILENCE_MS = 15000;
  const FIRST_WAIT_MS = 1000;
  const MAX_WAIT_MS = 30000;
  let source = null;
  let heard = 0;
  let wait = FIRST_WAIT_MS;
  let timer = null;
  let behind = true; // resync on the next message
  let ended = false;
  const say = (text, state) => {
    if (!status || ended) return;
    status.textContent = text;
    status.dataset.state = state;
  };
  const close = () => {
    clearTimeout(timer);
    if (source) source.close();
    source = null;
  };
  const retry = () => {
    close();
    behind = true;
    say("Reconnecting…", "down");
    resync(); // also how a page finds out that the session has ended
    timer = setTimeout(open, wait);
    wait = Math.min(wait * 2, MAX_WAIT_MS);
  };
  function open() {
    close();
    if (ended || document.hidden) return;
    heard = Date.now();
    const stream = new EventSource(url);
    source = stream;
    stream.onmessage = (event) => {
      heard = Date.now();
      wait = FIRST_WAIT_MS;
      if (behind) {
        behind = false;
        say("Live", "live");
        resync();
      }
      message(JSON.parse(event.data));
    };
    stream.onerror = () => {
      if (stream !== source) return;
      if (stream.readyState === EventSource.CLOSED) {
        retry();
      } else { // the browser is already trying again
        behind = true;
        say("Reconnecting…", "down");
      }
    };
  }
  setInterval(() => {
    if (source && !document.hidden && Date.now() - heard > SILENCE_MS) retry();
  }, 3000);
  const back = () => {
    behind = true;
    wait = FIRST_WAIT_MS;
    open();
  };
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) close();
    else back();
  });
  window.addEventListener("online", back);
  window.addEventListener("pageshow", (event) => {
    if (event.persisted) back();
  });
  open();
  return {
    say,
    signedOut() {
      if (ended) return;
      ended = true;
      close();
      const link = document.createElement("a");
      link.href = `/admin/login?next=${encodeURIComponent(window.location.pathname)}`;
      link.textContent = "Signed out: sign in again";
      if (status) {
        status.dataset.state = "down";
        status.replaceChildren(link);
      }
    },
  };
}

// A GET for those pages: the response, or null if it failed (the next update tries again).
async function liveGet(url, stream, options) {
  const response = await fetch(url, { credentials: "same-origin", ...options });
  if (response.redirected || response.status === 401) {
    stream.signedOut();
    return null;
  }
  return response.ok ? response : null;
}

// Makes ``node`` look like ``fresh`` by changing only what differs: nothing jumps, and hover and selection stay.
function morph(node, fresh) {
  if (node.nodeType !== fresh.nodeType || node.nodeName !== fresh.nodeName) {
    node.replaceWith(fresh);
  } else if (node.nodeType !== Node.ELEMENT_NODE) {
    if (node.nodeValue !== fresh.nodeValue) node.nodeValue = fresh.nodeValue;
  } else {
    for (const { name, value } of [...fresh.attributes]) {
      if (node.getAttribute(name) !== value) node.setAttribute(name, value);
    }
    for (const { name } of [...node.attributes]) {
      if (!fresh.hasAttribute(name)) node.removeAttribute(name);
    }
    morphChildren(node, fresh);
  }
}

function morphChildren(node, fresh) {
  const before = [...node.childNodes];
  const after = [...fresh.childNodes];
  after.forEach((child, i) => (before[i] ? morph(before[i], child) : node.append(child)));
  before.slice(after.length).forEach((child) => child.remove());
}

// Usage page: when new usage is written its numbers are fetched and only what changed is touched; the period and
// grouping controls work the same way, without a page load. A row opens to what it's made of (a model's clients, a
// client's models) and stays open through updates.
function liveUsage(root) {
  const form = document.getElementById("usage-filters");
  const custom = document.getElementById("usage-custom");
  const csv = document.getElementById("usage-csv");
  const csvModels = document.getElementById("usage-csv-models");
  const query = new URLSearchParams(window.location.search);
  const CUSTOM = ["from", "from_hour", "to", "to_hour"];
  const opened = new Set();
  const reopen = () => {
    root.querySelectorAll("tr[data-key]").forEach((row) => {
      const button = row.querySelector("button.expand");
      if (button) button.setAttribute("aria-expanded", String(opened.has(row.dataset.key)));
    });
    root.querySelectorAll("tr[data-of]").forEach((row) => {
      row.hidden = !opened.has(row.dataset.of);
    });
  };
  let version = null;
  let timer = null;
  let loading = false;
  let again = false;
  const refresh = async () => {
    clearTimeout(timer);
    timer = null;
    if (loading) {
      again = true;
      return;
    }
    loading = true;
    try {
      const response = await liveGet(`${root.dataset.body}?${query}`, stream);
      if (response) {
        const fresh = document.createElement("template");
        fresh.innerHTML = await response.text();
        morphChildren(root, fresh.content);
        reopen();
        stream.say(`Live · updated ${new Date().toLocaleTimeString()}`, "live");
      }
    } catch (_) {
      /* the next update tries again */
    }
    loading = false;
    if (again) {
      again = false;
      refresh();
    }
  };
  const stream = liveStream(root.dataset.stream, document.getElementById("usage-status"), {
    message(state) {
      if (version !== null && state.usage_version !== version && !timer) timer = setTimeout(refresh, 300); // batch bursts
      version = state.usage_version;
    },
    resync: refresh,
  });
  // the address follows the form (the custom days only with Custom), so a reload or a shared link shows the same
  const sync = () => {
    const chosen = new FormData(form);
    const isCustom = chosen.get("range") === "custom";
    if (custom) custom.hidden = !isCustom;
    for (const [name, value] of chosen) {
      if (value && (isCustom || !CUSTOM.includes(name))) query.set(name, value);
      else query.delete(name);
    }
    for (const old of ["days", "model_days", "by"]) query.delete(old); // from before 0.65
    window.history.replaceState(null, "", `${window.location.pathname}?${query}`);
    if (csv) csv.href = `/admin/usage.csv?${query}`;
    if (csvModels) {
      const perModel = new URLSearchParams(query);
      perModel.set("by", "model");
      csvModels.href = `/admin/usage.csv?${perModel}`;
    }
    refresh();
  };
  form.addEventListener("change", sync);
  form.addEventListener("submit", (event) => event.preventDefault());
  root.addEventListener("click", (event) => {
    const link = event.target.closest("[data-group]");
    if (link) {
      event.preventDefault();
      form.elements.group.value = link.dataset.group;
      sync();
      return;
    }
    const row = event.target.closest("tr[data-key]");
    if (!row || event.target.closest("a") || String(window.getSelection())) return; // not while selecting text
    const key = row.dataset.key;
    if (!opened.delete(key)) opened.add(key);
    reopen();
  });
}

// Local times for <time data-ts="unix seconds">.
function localTimes(scope) {
  scope.querySelectorAll("time[data-ts]").forEach((el) => {
    const date = new Date(Number(el.dataset.ts) * 1000);
    el.textContent = date.toLocaleString();
    el.title = date.toISOString();
  });
}

// Requests page. The page itself is empty: the newest rows are fetched, older ones as you scroll to them, and a row's
// details when you open it. Queued and running requests come from the live stream, and only the rows that changed
// are redrawn; finished ones are fetched as they're written, and wait behind a button while you're scrolled down.
function liveRequests(root) {
  const COLUMNS = 12;
  const AT_TOP_PX = 150;
  const nowBody = document.getElementById("now-requests");
  const nowCount = document.getElementById("now-count");
  const list = document.getElementById("finished-requests");
  const placeholder = list.querySelector(".placeholder");
  const note = document.getElementById("requests-more");
  const newRows = document.getElementById("new-rows");
  const form = document.getElementById("request-filters");
  const catchUp = Number(root.dataset.catchUp) || 300;
  const query = new URLSearchParams(window.location.search);
  const want = (key) => query.get(key) || "";
  const opened = new Set(); // ids of open rows, kept open as they're redrawn and when they finish
  const topbar = document.querySelector(".topbar");
  const sticky = () => root.style.setProperty("--topbar", `${topbar.offsetHeight}px`); // the table's header stays under it
  sticky();
  new ResizeObserver(sticky).observe(topbar);

  const el = (tag, className, text) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  };
  const td = (label, className, ...children) => {
    const cell = el("td", className);
    cell.dataset.label = label;
    cell.append(...children);
    return cell;
  };
  const text = (label, value) => {
    const cell = td(label, "", value || "—");
    if (value) cell.title = value;
    return cell;
  };
  const milliseconds = (ms) => (ms === null || ms === undefined ? "—" : String(Math.round(ms)));
  const money = (v) => {
    if (v >= 100) return `$${Math.round(v).toLocaleString("en-US")}`;
    if (v >= 1) return `$${v.toFixed(2)}`;
    return v > 0 && v < 0.00005 ? "<$0.0001" : `$${v.toFixed(4)}`;
  };
  const spend = (label, tokens, cost) => {
    if (tokens === null || tokens === undefined) return td(label, "num", "—");
    const value = el("span", "", Number(tokens).toLocaleString("en-US"));
    value.append(el("span", "price", tokens && cost ? money(cost) : ""));
    return td(label, "num", value);
  };
  const clock = (ts) => {
    const date = new Date(ts * 1000);
    const two = (n) => String(n).padStart(2, "0");
    const time = `${two(date.getHours())}:${two(date.getMinutes())}`;
    if (date.toDateString() === new Date().toDateString()) return `${time}:${two(date.getSeconds())}`;
    return `${two(date.getMonth() + 1)}-${two(date.getDate())} ${time}`;
  };
  const timeCell = (r) => {
    const button = el("button", "row-toggle", "▸");
    button.type = "button";
    button.setAttribute("aria-label", "Details");
    button.setAttribute("aria-expanded", "false");
    const time = el("time", "", clock(r.ts));
    time.title = new Date(r.ts * 1000).toLocaleString();
    const value = el("span");
    value.append(button, time);
    return td("Time", "", value);
  };
  const clientCell = (r) => {
    const cell = text("Client", r.client);
    if (r.priority && r.priority !== "normal") cell.append(" ", el("span", `badge ${r.priority}`, r.priority));
    return cell;
  };
  const modelCell = (r) => {
    const marks = [];
    if (r.council) marks.push("council");
    if (r.compaction) marks.push("compacted");
    if (r.answer_check) marks.push(/→ failed|' failed it|^failed/.test(r.answer_check) ? "check failed" : "checked");
    for (const tag of (r.answer_tags || "").split(",")) if (tag) marks.push(`#${tag}`);
    if (r.via) marks.push(`via ${r.via}`);
    if (r.edited) marks.push("edited");
    if (r.role) marks.push(r.role);
    const cell = td("Model", "");
    cell.title = [r.label, ...marks].join(" · ");
    for (const mark of marks) cell.append(el("span", "badge", mark), " ");
    cell.append(r.label || "—");
    return cell;
  };

  // What opens under a row: for a finished request fetched then, for a running one what the stream sent.
  const describe = (box, d) => {
    const line = (name, ...value) => {
      const row = el("div");
      row.append(el("span", "muted", `${name}: `), ...value);
      box.append(row);
    };
    const copy = (value) => {
      const button = el("button", "copy-text inline mono", value);
      button.type = "button";
      button.dataset.copyText = value;
      button.title = "Click to copy";
      return button;
    };
    box.replaceChildren();
    line("Id", copy(d.id));
    const realId = d.target_model || d.requested_model || d.model;
    if (realId) line("Model ID", copy(realId));
    if (d.priority) line("Priority", d.priority);
    if (d.path) line("Path", d.path);
    if (d.ip) line("Address", copy(d.ip));
    if (d.user_agent) line("App", copy(d.user_agent));
    if (d.sent_user_agent) line("Sent as", copy(d.sent_user_agent));
    if (d.tags) line("Tags", d.tags.split(",").map((t) => `#${t}`).join(" "));
    if (d.council) line("Council", d.council);
    if (d.compaction) line("Compacted", d.compaction);
    if (d.fallback) line("Fell back", d.fallback);
    if (d.via) line("Forwarded", d.via);
    let edited = null;
    if (d.edits) {
      try {
        edited = JSON.parse(d.edits);
      } catch (_) {
        edited = null;
      }
    }
    if (edited) {
      const parts = [];
      if (edited.changed?.length) parts.push(`changed by ${edited.changed.join(", ")}`);
      if (edited.missed?.length) parts.push(`found nothing: ${edited.missed.join(", ")}`);
      if (!parts.length) parts.push("no change");
      const [before, after] = edited.size || [0, 0];
      if (before !== after) parts.push(`${before.toLocaleString()} → ${after.toLocaleString()} characters`);
      line("Prompt edits", parts.join(" · "));
      for (const note of edited.notes || []) line("Prompt edits", note);
    }
    if (d.answer_check) line("Answer check", d.answer_check);
    if (d.answer_tags) line("Answer tags", d.answer_tags.split(",").join(", "));
    if (d.thinking) line("Thinking level", d.thinking);
    if (d.role) line("Made by QueueRouter", d.role);
    if (d.parent) {
      const link = el("a", "mono", d.parent);
      link.href = `/admin/requests/${d.parent}/trace`;
      line("For", link);
    }
    if (d.retries) line("Retries", String(d.retries));
    if (d.backoff_ms) line("Retry wait", `${d.backoff_ms} ms`);
    if (d.total_ms !== null && d.total_ms !== undefined) line("Total time", `${d.total_ms} ms`);
    if (d.first_token_ms !== null && d.first_token_ms !== undefined) {
      const parts = [`queue ${d.headers_ms}`, `token ${d.first_token_ms}`];
      if (d.answer_ms !== null && d.answer_ms > d.first_token_ms) parts.push(`answer ${d.answer_ms}`);
      if (d.tokens_per_s !== null) parts.push(`${Math.round(d.tokens_per_s)} tok/s`);
      if (d.stall_ms) parts.push(`longest pause ${d.stall_ms}`);
      line("Provider's own times (ms)", parts.join(" · "));
    }
    if (d.error) line("Error", d.error);
    let ids = d.client_headers || {};
    if (typeof ids === "string") { // running requests: as stored
      try {
        ids = JSON.parse(ids);
      } catch (_) {
        ids = {};
      }
    }
    for (const [name, value] of Object.entries(ids)) line(name, copy(String(value)));
    const links = [];
    if (d.has_content) links.push(["Content", `/admin/requests/${d.id}/content`]);
    if (d.has_content && edited?.changed?.length) links.push(["Prompt changes", `/admin/requests/${d.id}/edits`]);
    const editCalls = (edited?.notes || []).some((n) => n.includes("model"));
    if (d.council || d.answer_check || d.via || editCalls) links.push(["Trace", `/admin/requests/${d.id}/trace`]);
    if (links.length) {
      const row = el("div", "links");
      for (const [name, href] of links) {
        const link = el("a", "", name);
        link.href = href;
        row.append(link, " ");
      }
      box.append(row);
    }
  };
  const openRow = (tr, fill) => {
    const more = el("tr", "req-more");
    const cell = el("td");
    const box = el("div", "req-details", "Loading…");
    cell.colSpan = COLUMNS;
    cell.append(box);
    more.append(cell);
    tr.after(more);
    tr.more = more;
    tr.classList.add("open");
    tr.querySelector(".row-toggle").setAttribute("aria-expanded", "true");
    fill(box);
  };
  const closeRow = (tr) => {
    tr.more.remove();
    tr.more = null;
    tr.classList.remove("open");
    tr.querySelector(".row-toggle").setAttribute("aria-expanded", "false");
  };
  const fetched = (id) => async (box) => {
    try {
      const response = await liveGet(`${root.dataset.rows}/${encodeURIComponent(id)}`, stream);
      if (response) describe(box, await response.json());
      else box.textContent = "Couldn't load its details.";
    } catch (_) {
      box.textContent = "Couldn't load its details.";
    }
  };
  const toggle = (fill) => (event) => {
    const tr = event.target.closest("tr.req");
    if (!tr || event.target.closest("a, .copy-text")) return;
    if (!event.target.closest(".row-toggle") && String(window.getSelection())) return; // selecting text to copy
    if (tr.more) {
      opened.delete(tr.dataset.id);
      closeRow(tr);
    } else {
      opened.add(tr.dataset.id);
      openRow(tr, fill(tr.dataset.id));
    }
  };

  // -- queued and running ----------------------------------------------------------------------------------------
  const matches = (r) =>
    !want("outcome") &&
    !want("answer_tag") && // a running request's answers aren't tagged yet
    (!want("client") || r.client === want("client")) &&
    (!want("model") || r.requested_model === want("model") || r.target_model === want("model")) &&
    (!want("upstream") || r.upstream === want("upstream")) &&
    (!want("tag") || (r.tags || "").split(",").includes(want("tag")));
  const running = new Map(); // id -> { tr, signature, r, tick: the cell that counts up }
  let inflight = [];
  let frame = null;
  let started = 0; // performance.now() when the last update arrived
  const runningRow = (r) => {
    // waiting for its conversation's summary, or for its council's members and judge
    const waiting = r.stage === "compacting" || r.stage === "council";
    const queued = r.stage === "queued" || waiting;
    const shown = waiting || r.stage === "checking"; // its answer is being checked (alias answer check)
    const status = shown ? `${r.stage}…` : queued ? `queued${r.position ? ` #${r.position}` : ""}` : "running";
    const outcome = text("Outcome", status);
    if (r.retries) outcome.append(el("span", "muted", ` ↻${r.retries}`));
    const queue = td("Queue ms", "num", milliseconds(queued ? r.elapsed_ms : r.queue_ms));
    const took = td("Upstream ms", "num", "—");
    const first = r.ttft_ms !== null && r.ttft_ms !== undefined ? milliseconds(r.ttft_ms) : queued ? "—" : "waiting…";
    const input = td("In", "num", r.estimate ? `~${Number(r.estimate).toLocaleString("en-US")}` : "—");
    if (r.estimate) input.title = "QueueRouter's estimate; the provider's count arrives when it finishes";
    const tr = el("tr", "req live");
    tr.dataset.id = r.id;
    tr.append(
      timeCell(r), clientCell(r), modelCell(r), outcome,
      queue, td("First byte ms", "num", first), took, input, td("Cached", "num", "—"), td("Out", "num", "—"),
      td("Total", "num", "—"), text("Finish", r.stream ? "streaming" : ""),
    );
    return { tr, tick: queued ? queue : took };
  };
  const renderRunning = () => {
    frame = null;
    const shown = inflight.filter(matches);
    nowCount.textContent = String(shown.length);
    let cursor = nowBody.firstElementChild;
    const place = (node) => {
      if (node === cursor) cursor = cursor.nextElementSibling;
      else nowBody.insertBefore(node, cursor);
    };
    for (const r of shown) {
      const signature = JSON.stringify({ ...r, elapsed_ms: 0, ts: 0 });
      let entry = running.get(r.id);
      if (!entry || entry.signature !== signature) {
        const built = runningRow(r);
        if (entry) {
          if (cursor === entry.tr) cursor = built.tr;
          entry.tr.replaceWith(built.tr);
          if (entry.tr.more) { // stays open, with what's new in it
            built.tr.more = entry.tr.more;
            built.tr.classList.add("open");
            built.tr.querySelector(".row-toggle").setAttribute("aria-expanded", "true");
            describe(built.tr.more.querySelector(".req-details"), r);
          }
        }
        entry = { ...built, signature };
        running.set(r.id, entry);
      }
      entry.r = r;
      entry.tick.dataset.tick = String(entry.tick.dataset.label === "Queue ms" ? r.elapsed_ms : r.elapsed_ms - (r.queue_ms || 0));
      place(entry.tr);
      if (opened.has(r.id) && !entry.tr.more) openRow(entry.tr, (box) => describe(box, r));
      if (entry.tr.more) place(entry.tr.more);
    }
    while (cursor) {
      const next = cursor.nextElementSibling;
      cursor.remove();
      cursor = next;
    }
    const ids = new Set(shown.map((r) => r.id));
    for (const id of running.keys()) if (!ids.has(id)) running.delete(id);
  };
  setInterval(() => { // elapsed times count up between updates
    if (document.hidden) return;
    const extra = performance.now() - started;
    nowBody.querySelectorAll("[data-tick]").forEach((cell) => {
      cell.textContent = String(Math.round(Number(cell.dataset.tick) + extra));
    });
  }, 500);

  // -- finished --------------------------------------------------------------------------------------------------
  const finishedRow = (r) => {
    const outcome = text("Outcome", `${r.outcome || "—"}${r.stopped ? ` ${r.stopped}` : r.status ? ` ${r.status}` : ""}`);
    if (r.retries) outcome.append(el("span", "muted", ` ↻${r.retries}`));
    if (r.outcome && r.outcome !== "done") outcome.classList.add("bad");
    const total = td("Total", "num", r.cost ? money(r.cost) : "—");
    if (r.calls) total.title = `Adds up its ${r.calls} calls`;
    if (r.cost !== null && !r.priced) {
      total.append(el("span", "muted", "*"));
      total.title = "Some tokens have no price";
    }
    const input = spend("In", r.tokens_in, r.cost_in);
    if (r.tokens_written) input.title = `${Number(r.tokens_written).toLocaleString("en-US")} of them written to the cache`;
    const tr = el("tr", "req");
    tr.dataset.id = r.id;
    tr.append(
      timeCell(r), clientCell(r), modelCell(r), outcome,
      td("Queue ms", "num", milliseconds(r.queue_ms)), td("First byte ms", "num", milliseconds(r.ttft_ms)),
      td("Upstream ms", "num", milliseconds(r.upstream_ms)), input,
      spend("Cached", r.tokens_cached, r.cost_cached), spend("Out", r.tokens_out, r.cost_out), total,
      text("Finish", r.finish_reason),
    );
    return tr;
  };
  let rows = []; // shown, newest first: { r, tr }
  const known = new Set(); // ids shown or waiting
  let waiting = []; // fetched while scrolled down
  let gap = false; // there are rows between the ones waiting and the ones shown
  let hasOlder = false;
  let loadingOlder = false;
  let latest = null; // the newest row's time
  let loaded = false;
  let loadingFirst = false;
  let controller = null;
  let generation = 0; // goes up when the filters change: what was being fetched no longer counts

  const updateNote = () => {
    if (loaded && !rows.length) {
      placeholder.firstElementChild.textContent = [...query.keys()].length ? "No requests match these filters." : "No requests logged yet.";
      list.replaceChildren(placeholder);
    }
    note.textContent = loadingOlder ? "Loading…" : rows.length && !hasOlder ? "That's the end of the log." : "";
  };
  const addRows = (fresh, append) => {
    const built = fresh.map((r) => ({ r, tr: finishedRow(r) }));
    if (!built.length) return;
    placeholder.remove();
    if (append) {
      list.append(...built.map((row) => row.tr));
      rows = [...rows, ...built];
    } else {
      list.prepend(...built.map((row) => row.tr));
      built.forEach((row) => row.tr.classList.add("fresh"));
      rows = [...built, ...rows];
    }
    for (const { r, tr } of built) if (opened.has(r.id)) openRow(tr, fetched(r.id));
  };
  const clearRows = () => {
    list.replaceChildren();
    known.clear();
    for (const row of waiting) known.add(row.id);
    rows = [];
  };
  const settle = () => {
    if (waiting.length && window.scrollY > AT_TOP_PX) {
      newRows.textContent = `${waiting.length}${gap ? "+" : ""} new`;
      newRows.hidden = false;
      return;
    }
    newRows.hidden = true;
    if (!waiting.length) return;
    if (gap) {
      clearRows();
      hasOlder = true;
    }
    addRows(waiting, false);
    waiting = [];
    gap = false;
    while (rows.length > catchUp) { // what you've scrolled through can go once you're back at the top
      const { r, tr } = rows.pop();
      if (tr.more) tr.more.remove();
      tr.remove();
      known.delete(r.id);
      hasOlder = true;
    }
    updateNote();
  };
  const load = async () => {
    if (controller) controller.abort();
    controller = new AbortController();
    const mine = ++generation;
    loadingFirst = true;
    try {
      const response = await liveGet(`${root.dataset.rows}?${query}`, stream, { signal: controller.signal });
      if (response) {
        const data = await response.json();
        if (mine !== generation) return;
        waiting = [];
        gap = false;
        clearRows();
        data.rows.forEach((r) => known.add(r.id));
        addRows(data.rows, true);
        latest = data.rows.length ? data.rows[0].ts : null;
        hasOlder = data.more;
        loaded = true;
        updateNote();
        settle();
      }
    } catch (_) {
      /* changed filters, or the next update tries again */
    }
    if (mine === generation) loadingFirst = false;
  };
  let fetching = false;
  let again = false;
  let timer = null;
  const fetchNew = async () => {
    clearTimeout(timer);
    timer = null;
    if (!loaded) {
      if (!loadingFirst) load();
      return;
    }
    if (fetching) {
      again = true;
      return;
    }
    fetching = true;
    const mine = generation;
    try {
      const params = new URLSearchParams(query);
      params.set("after", String(latest ?? 0));
      const response = await liveGet(`${root.dataset.rows}?${params}`, stream);
      if (response) {
        const data = await response.json();
        const fresh = data.rows.filter((r) => !known.has(r.id));
        if (mine === generation && fresh.length) {
          fresh.forEach((r) => known.add(r.id));
          latest = Math.max(latest ?? 0, fresh[0].ts);
          waiting = [...fresh, ...waiting].slice(0, catchUp);
          gap = gap || data.more;
          settle();
        }
      }
    } catch (_) {
      /* the next update tries again */
    }
    fetching = false;
    if (again) {
      again = false;
      fetchNew();
    }
  };
  const nearBottom = () => note.getBoundingClientRect().top < window.innerHeight + 400;
  const loadOlder = async () => {
    if (!hasOlder || loadingOlder || !rows.length) return;
    loadingOlder = true;
    updateNote();
    const mine = generation;
    const last = rows[rows.length - 1].r;
    const params = new URLSearchParams(query);
    params.set("before", String(last.ts));
    params.set("before_id", last.id);
    let more = false;
    try {
      const response = await liveGet(`${root.dataset.rows}?${params}`, stream);
      if (response) {
        const data = await response.json();
        if (mine === generation) {
          const older = data.rows.filter((r) => !known.has(r.id));
          older.forEach((r) => known.add(r.id));
          addRows(older, true);
          hasOlder = data.more;
          more = true;
        }
      }
    } catch (_) {
      /* tried again the next time the end of the list comes into view */
    }
    loadingOlder = false;
    updateNote();
    if (more && nearBottom()) loadOlder();
  };
  new IntersectionObserver((entries) => {
    if (entries[0].isIntersecting) loadOlder();
  }, { rootMargin: "400px" }).observe(note);
  window.addEventListener("scroll", () => {
    if (waiting.length && window.scrollY <= AT_TOP_PX) settle();
  }, { passive: true });
  newRows.addEventListener("click", () => {
    window.scrollTo(0, 0);
    settle();
  });

  nowBody.addEventListener("click", toggle((id) => (box) => {
    const entry = running.get(id);
    if (entry) describe(box, entry.r);
  }));
  list.addEventListener("click", toggle(fetched));

  // -- filters: applied at once, without a page load ---------------------------------------------------------------
  const applied = () => {
    window.history.replaceState(null, "", `${window.location.pathname}?${query}`);
    renderRunning();
    load();
  };
  form.addEventListener("change", (event) => {
    if (event.target.value) query.set(event.target.name, event.target.value);
    else query.delete(event.target.name);
    applied();
  });
  form.addEventListener("submit", (event) => event.preventDefault());
  document.getElementById("filters-reset").addEventListener("click", (event) => {
    event.preventDefault();
    form.querySelectorAll("select").forEach((select) => {
      select.value = "";
      query.delete(select.name);
    });
    applied();
  });
  const fillFilters = async () => {
    try {
      const response = await liveGet(root.dataset.filters, stream);
      if (!response) return;
      const choices = await response.json();
      form.querySelectorAll("select").forEach((select) => {
        const have = new Set([...select.options].map((option) => option.value));
        for (const value of choices[select.name] || []) {
          if (have.has(value)) continue;
          const option = el("option", "", (choices.labels || {})[value] || value);
          option.value = value;
          select.append(option);
        }
      });
    } catch (_) {
      /* the filters still work for what the address says */
    }
  };

  let version = null;
  const stream = liveStream(root.dataset.stream, document.getElementById("requests-status"), {
    message(state) {
      inflight = state.inflight || [];
      started = performance.now();
      if (!frame) frame = requestAnimationFrame(renderRunning);
      if (version !== null && state.usage_version !== version && !timer) timer = setTimeout(fetchNew, 300);
      version = state.usage_version;
    },
    resync: fetchNew,
  });
  load();
  fillFilters();
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
  const strict = root.hasAttribute("data-strict"); // only listed choices (a typed name isn't added)
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
    if (!c) return strict ? "doesn't exist any more" : "not listed by any upstream";
    if (c.note !== undefined) return c.note;
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
    if (strict && !byId.has(id)) { // a typed name: the choice it names, if any
      const typed = choices.find((c) => (c.label || c.id).toLowerCase() === id.toLowerCase());
      if (!typed) return;
      id = typed.id;
    }
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
      chip.textContent = (ordered && values.length > 1 ? `${i + 1}. ` : "") + (byId.get(id)?.label || id);
      chip.title = id;
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
      .filter((c) => words.every((w) => `${c.id} ${c.label || ""} ${describe(c)}`.toLowerCase().includes(w)))
      .slice(0, 60);
    active = 0;
    menu.replaceChildren(...shown.map((c, i) => {
      const item = document.createElement("li");
      item.setAttribute("role", "option");
      item.className = i === active ? "active" : "";
      const name = document.createElement("span");
      name.className = "mono";
      name.textContent = c.label || c.id;
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
      item.textContent = strict ? "No match" : `Enter: add "${search.value.trim()}" (not listed by any upstream)`;
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

// Edit editor: only the fields of the chosen kind are shown ([data-kinds="text pattern"]); all are sent, and the
// server reads the kind's own.
function kindForm(form) {
  const select = form.querySelector("[data-kind-select]");
  if (!select) return;
  const sync = () => {
    form.querySelectorAll("[data-kinds]").forEach((el) => {
      el.hidden = !el.dataset.kinds.split(" ").includes(select.value);
    });
  };
  select.addEventListener("change", sync);
  sync();
}

// Edits > Live: requests whose prompt edits ran, newest first. New ones are fetched whenever the live stream says
// requests were written; older ones with "Older". The filters reload the list without a page load.
function liveEdits(root) {
  const list = document.getElementById("edited-requests");
  const more = document.getElementById("edits-more");
  const form = document.getElementById("edit-filters");
  const status = document.getElementById("edits-status");
  let newest = null;
  let oldest = null;
  let version = null;
  let busy = false;

  const el = (tag, className, text) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  };
  const filters = () => {
    const query = new URLSearchParams();
    for (const [key, value] of new FormData(form)) if (value) query.set(key, value);
    return query;
  };
  const clock = (ts) => {
    const date = new Date(ts * 1000);
    const two = (n) => String(n).padStart(2, "0");
    const time = `${two(date.getHours())}:${two(date.getMinutes())}:${two(date.getSeconds())}`;
    return date.toDateString() === new Date().toDateString() ? time : `${two(date.getMonth() + 1)}-${two(date.getDate())} ${time}`;
  };
  const chips = (names, className) => {
    const cell = el("span");
    for (const name of names) cell.append(el("span", `badge ${className}`, name), " ");
    if (!names.length) cell.append(el("span", "muted", "—"));
    return cell;
  };
  const row = (r) => {
    const tr = el("tr", "edited");
    tr.dataset.id = r.id;
    const cell = (label, ...children) => {
      const td = el("td");
      td.dataset.label = label;
      td.append(...children);
      tr.append(td);
      return td;
    };
    const time = el("time", "", clock(r.ts));
    time.title = new Date(r.ts * 1000).toLocaleString();
    cell("Time", time);
    cell("Client", r.client || "—");
    const model = cell("Model", r.target ? `${r.model} → ${r.target}` : r.model || "—");
    if (r.via) model.append(" ", el("span", "badge", `via ${r.via}`));
    if (r.outcome && r.outcome !== "done") model.append(" ", el("span", "badge", r.outcome));
    const changed = el("div"); // the chips with the notes under them (one block: a card on a phone keeps it whole)
    changed.append(chips(r.changed, "ok"));
    for (const note of r.notes) changed.append(el("div", "small muted", note));
    cell("Changed", changed);
    cell("Found nothing", chips(r.missed, ""));
    const [before, after] = r.size;
    const size = cell("Size", `${before.toLocaleString()} → ${after.toLocaleString()}`);
    size.className = "num";
    const link = el("a", "", "changes");
    if (r.view) {
      link.href = `/admin/requests/${encodeURIComponent(r.id)}/edits`;
      cell("", link);
    } else {
      cell("", el("span", "small muted", r.changed.length ? "no text kept" : ""));
    }
    return tr;
  };
  const placeholder = (text) => {
    const tr = el("tr", "placeholder");
    const td = el("td", "muted", text);
    td.colSpan = 7;
    tr.append(td);
    return tr;
  };
  const fetchRows = async (extra) => {
    const query = filters();
    for (const [key, value] of Object.entries(extra)) query.set(key, value);
    const response = await liveGet(`${root.dataset.rows}?${query}`, stream);
    return response ? response.json() : null;
  };
  const load = async () => {
    newest = oldest = null;
    const data = await fetchRows({});
    if (!data) return;
    list.replaceChildren(...data.rows.map(row));
    if (!data.rows.length) list.append(placeholder(filters().toString() ? "No requests match these filters." : "No requests changed by edits yet."));
    if (data.rows.length) {
      newest = data.rows[0].ts;
      oldest = data.rows[data.rows.length - 1];
    }
    showMore(data.more);
  };
  const fetchNew = async () => {
    if (busy) return;
    busy = true;
    try {
      if (newest === null) return await load();
      const data = await fetchRows({ after: newest });
      if (!data) return;
      const fresh = data.rows.filter((r) => !list.querySelector(`tr[data-id="${CSS.escape(r.id)}"]`));
      if (!fresh.length) return;
      list.querySelector(".placeholder")?.remove();
      list.prepend(...fresh.map((r) => {
        const tr = row(r);
        tr.classList.add("fresh");
        return tr;
      }));
      newest = Math.max(newest, ...data.rows.map((r) => r.ts));
    } finally {
      busy = false;
    }
  };
  const showMore = (hasMore) => {
    more.replaceChildren();
    if (!hasMore || !oldest) return;
    const button = el("button", "secondary", "Older");
    button.type = "button";
    button.addEventListener("click", async () => {
      const data = await fetchRows({ before: oldest.ts, before_id: oldest.id });
      if (!data) return;
      list.append(...data.rows.map(row));
      if (data.rows.length) oldest = data.rows[data.rows.length - 1];
      showMore(data.more);
    });
    more.append(button);
  };
  form.addEventListener("change", () => {
    const query = filters();
    history.replaceState(null, "", query.toString() ? `?${query}` : window.location.pathname);
    load();
  });
  const stream = liveStream(root.dataset.stream, status, {
    message(state) {
      if (version !== null && state.usage_version !== version) fetchNew();
      version = state.usage_version;
    },
    resync: fetchNew,
  });
}
